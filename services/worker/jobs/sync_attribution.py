"""
Attribution sync jobs — pulls signups and conversions from prod Postgres
via BigQuery EXTERNAL_QUERY and upserts into attribution_events.

Jobs:
  sync_attribution_signups()    — every 15 min, watermark on users.created_at
  sync_attribution_conversions() — every 15 min, watermark on payment_date
  refresh_conversion_mv()        — every hour, REFRESH MATERIALIZED VIEW CONCURRENTLY

Watermarks are stored in attribution_sync_cursor.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from services.shared.bq_client import BQClient
from services.shared.config import settings
from services.shared.db import AsyncSessionLocal
from services.worker.upsert import track_run

log = logging.getLogger(__name__)

_CHUNK_DAYS = 7          # process N days per run in normal mode
_BQ_PAGE    = 10_000     # rows per BQ result page


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_cursor(session, job_name: str) -> datetime:
    row = await session.execute(
        text("SELECT last_processed_time FROM attribution_sync_cursor WHERE job_name = :n"),
        {"n": job_name},
    )
    val = row.scalar_one_or_none()
    return val or datetime(2022, 8, 1, tzinfo=timezone.utc)


async def _advance_cursor(session, job_name: str, until: datetime,
                          rows: int, bytes_proc: int):
    await session.execute(
        text("""
            UPDATE attribution_sync_cursor
               SET last_processed_time      = :until,
                   last_run_at             = NOW(),
                   rows_ingested_last_run  = :rows,
                   bytes_processed_last_run = :bytes,
                   error                   = NULL
             WHERE job_name = :name
        """),
        {"until": until, "rows": rows, "bytes": bytes_proc, "name": job_name},
    )


async def _log_bq_cost(session, label: str, bytes_proc: int, rows: int, duration_ms: int):
    await session.execute(
        text("""
            INSERT INTO bq_query_costs (query_label, bytes_processed, rows_returned, duration_ms)
            VALUES (:label, :bytes, :rows, :dur)
        """),
        {"label": label, "bytes": bytes_proc, "rows": rows, "dur": duration_ms},
    )


async def _upsert_attribution_events(session, rows: list[dict]) -> int:
    """Upsert attribution_events rows. PK = (id, install_date)."""
    if not rows:
        return 0

    from services.shared.models import AttributionEvent

    # Chunked upsert
    chunk_size = 500
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        stmt = pg_insert(AttributionEvent).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id", "install_date"],
            set_={
                c.name: stmt.excluded[c.name]
                for c in AttributionEvent.__table__.columns
                if c.name not in ("id", "install_date")
            },
        )
        result = await session.execute(stmt)
        total += result.rowcount or len(chunk)

    await session.commit()
    return total


def _parse_row(row: dict) -> dict:
    """Coerce BQ types → Postgres-compatible dict."""
    import decimal

    def _bool(v) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "t", "yes")
        return bool(v) if v is not None else False

    def _int(v):
        try:
            return int(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    def _dec(v):
        try:
            return decimal.Decimal(str(v)) if v is not None else None
        except Exception:
            return None

    return {
        "id":               str(row.get("id", "")),
        "user_id":          _int(row.get("user_id")),
        "event_name":       row.get("event_name"),
        "event_time":       row.get("event_time"),
        "install_date":     row.get("install_date"),
        "days_since_signup": _int(row.get("days_since_signup")),
        "network":          row.get("network"),
        "publisher_site":   row.get("publisher_site"),
        "meta_campaign_id": row.get("meta_campaign_id"),
        "meta_adset_id":    row.get("meta_adset_id"),
        "meta_creative_id": row.get("meta_creative_id"),
        "campaign_name":    row.get("campaign_name"),
        "adset_name":       row.get("adset_name"),
        "creative_name":    row.get("creative_name"),
        "revenue_inr":      _dec(row.get("revenue_inr")),
        "plan_id":          row.get("plan_id"),
        "is_trial":         _bool(row.get("is_trial")),
        "is_first_payment": _bool(row.get("is_first_payment")),
        "is_reattributed":  _bool(row.get("is_reattributed")),
        "is_organic":       _bool(row.get("is_organic")),
        "is_viewthrough":   _bool(row.get("is_viewthrough")),
        "platform":         row.get("platform"),
        "os_version":       row.get("os_version"),
        "device_brand":     row.get("device_brand"),
        "device_model":     row.get("device_model"),
        "priority":         row.get("priority"),
        "source_table":     row.get("source_table", ""),
        "raw":              row,
    }


# ---------------------------------------------------------------------------
# sync_attribution_signups
# ---------------------------------------------------------------------------

async def sync_attribution_signups():
    """Ingest new user signups with Singular attribution."""
    bq = BQClient()
    import asyncio, time

    async with AsyncSessionLocal() as session:
        since = await _get_cursor(session, "signups")
        until = datetime.now(tz=timezone.utc)

        log.info("attribution_signups: pulling since=%s until=%s", since, until)

        sql = bq.load_sql("signups", since=since.isoformat(), until=until.isoformat())

        # Dry-run cost check
        try:
            bytes_est = await asyncio.get_event_loop().run_in_executor(
                None, lambda: bq.dry_run(sql, label="signups")
            )
        except RuntimeError as exc:
            log.error("signups dry_run blocked: %s", exc)
            return

        t0 = time.monotonic()
        raw_rows, bytes_proc = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bq.stream_rows(sql, label="signups")
        )
        duration_ms = int((time.monotonic() - t0) * 1000)

        parsed = [_parse_row(r) for r in raw_rows]

        async with track_run("attribution_signups") as run:
            inserted = await _upsert_attribution_events(session, parsed)
            run.rows_upserted = inserted

        await _advance_cursor(session, "signups", until, inserted, bytes_proc)
        await _log_bq_cost(session, "signups", bytes_proc, len(raw_rows), duration_ms)
        await session.commit()

        log.info("attribution_signups done: rows=%d bytes=%d", inserted, bytes_proc)


# ---------------------------------------------------------------------------
# sync_attribution_conversions
# ---------------------------------------------------------------------------

async def sync_attribution_conversions():
    """Ingest charged transactions (trials + conversions) with attribution."""
    bq = BQClient()
    import asyncio, time

    async with AsyncSessionLocal() as session:
        since = await _get_cursor(session, "conversions")
        until = datetime.now(tz=timezone.utc)

        log.info("attribution_conversions: pulling since=%s until=%s", since, until)

        sql = bq.load_sql("conversions", since=since.isoformat(), until=until.isoformat())

        try:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: bq.dry_run(sql, label="conversions")
            )
        except RuntimeError as exc:
            log.error("conversions dry_run blocked: %s", exc)
            return

        t0 = time.monotonic()
        raw_rows, bytes_proc = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bq.stream_rows(sql, label="conversions")
        )
        duration_ms = int((time.monotonic() - t0) * 1000)

        parsed = [_parse_row(r) for r in raw_rows]

        async with track_run("attribution_conversions") as run:
            inserted = await _upsert_attribution_events(session, parsed)
            run.rows_upserted = inserted

        await _advance_cursor(session, "conversions", until, inserted, bytes_proc)
        await _log_bq_cost(session, "conversions", bytes_proc, len(raw_rows), duration_ms)
        await session.commit()

        log.info("attribution_conversions done: rows=%d bytes=%d", inserted, bytes_proc)


# ---------------------------------------------------------------------------
# refresh_conversion_mv
# ---------------------------------------------------------------------------

async def refresh_conversion_mv():
    """Refresh mv_campaign_conversions and mv_adset_conversions concurrently."""
    async with AsyncSessionLocal() as session:
        for mv in ("mv_campaign_conversions", "mv_adset_conversions"):
            try:
                await session.execute(
                    text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                )
                await session.commit()
                log.info("refreshed %s", mv)
            except Exception as exc:
                log.error("failed to refresh %s: %s", mv, exc)
                await session.rollback()


# ---------------------------------------------------------------------------
# backfill_attribution
# ---------------------------------------------------------------------------

async def backfill_attribution(
    event_type: str,      # "signups" | "conversions"
    since: str,           # YYYY-MM-DD
    until: str,           # YYYY-MM-DD
):
    """
    Historical backfill for a single event type, chunked by month.
    Runs sequentially — never call in parallel.
    """
    from datetime import date, timedelta
    import asyncio

    bq = BQClient()
    since_dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc)
    until_dt = datetime.fromisoformat(until).replace(tzinfo=timezone.utc)

    # Walk month boundaries
    cursor = since_dt
    total_rows = 0

    while cursor < until_dt:
        # next month boundary
        if cursor.month == 12:
            chunk_end = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            chunk_end = cursor.replace(month=cursor.month + 1, day=1)
        chunk_end = min(chunk_end, until_dt)

        log.info("backfill %s chunk %s → %s", event_type, cursor.date(), chunk_end.date())

        sql = bq.load_sql(event_type, since=cursor.isoformat(), until=chunk_end.isoformat())

        try:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda s=sql: bq.dry_run(s, label=f"backfill_{event_type}")
            )
        except RuntimeError as exc:
            log.error("backfill dry_run blocked for %s: %s", event_type, exc)
            cursor = chunk_end
            continue

        raw_rows, bytes_proc = await asyncio.get_event_loop().run_in_executor(
            None, lambda s=sql: bq.stream_rows(s, label=f"backfill_{event_type}")
        )

        parsed = [_parse_row(r) for r in raw_rows]
        async with AsyncSessionLocal() as session:
            inserted = await _upsert_attribution_events(session, parsed)
            await _log_bq_cost(session, f"backfill_{event_type}", bytes_proc, len(raw_rows), 0)
            await session.commit()

        total_rows += inserted
        log.info("backfill %s chunk done: rows=%d", event_type, inserted)
        cursor = chunk_end

    log.info("backfill %s complete: total_rows=%d", event_type, total_rows)
    return {"event_type": event_type, "since": since, "until": until, "rows": total_rows}
