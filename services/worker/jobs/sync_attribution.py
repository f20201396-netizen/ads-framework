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
from datetime import date, datetime, timezone
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


def _make_raw_safe(row: dict) -> dict:
    """Convert non-JSON-serializable types in a BQ row dict to safe equivalents."""
    import decimal
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, date):
            out[k] = v.isoformat()
        elif isinstance(v, decimal.Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out


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
        "is_mandate":       _bool(row.get("is_mandate")),
        "is_reattributed":  _bool(row.get("is_reattributed")),
        "is_organic":       _bool(row.get("is_organic")),
        "is_viewthrough":   _bool(row.get("is_viewthrough")),
        "platform":         row.get("platform"),
        "os_version":       row.get("os_version"),
        "device_brand":     row.get("device_brand"),
        "device_model":     row.get("device_model"),
        "priority":         row.get("priority"),
        "source_table":     row.get("source_table", ""),
        "raw":              _make_raw_safe(row),
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


# ---------------------------------------------------------------------------
# sync_user_devices
# ---------------------------------------------------------------------------

async def sync_user_devices():
    """
    Mirror prod user_devices into the local user_devices table.
    Watermark: max(user_id) already stored locally — fetches all higher user_ids.
    Runs incrementally so subsequent runs are cheap.
    """
    bq = BQClient()
    import asyncio, time
    from sqlalchemy import func as sa_func
    from services.shared.models import UserDevice
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    async with AsyncSessionLocal() as session:
        # Watermark: highest user_id we already have
        row = await session.execute(
            text("SELECT COALESCE(MAX(user_id), 0) FROM user_devices")
        )
        min_user_id = int(row.scalar() or 0)

        log.info("sync_user_devices: pulling user_id > %d", min_user_id)

        sql = bq.load_sql("user_devices", min_user_id=min_user_id)

        try:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: bq.dry_run(sql, label="user_devices")
            )
        except RuntimeError as exc:
            log.error("user_devices dry_run blocked: %s", exc)
            return

        t0 = time.monotonic()
        raw_rows, bytes_proc = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bq.stream_rows(sql, label="user_devices")
        )
        duration_ms = int((time.monotonic() - t0) * 1000)

        if not raw_rows:
            log.info("sync_user_devices: no new rows")
            return

        chunk_size = 1000
        inserted = 0
        for i in range(0, len(raw_rows), chunk_size):
            chunk = raw_rows[i : i + chunk_size]
            values = [
                {"user_id": int(r["user_id"]), "os": r.get("os")}
                for r in chunk if r.get("user_id") is not None
            ]
            if not values:
                continue
            stmt = pg_insert(UserDevice).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["user_id"],
                set_={"os": stmt.excluded.os},
            )
            result = await session.execute(stmt)
            inserted += result.rowcount or len(chunk)

        await session.commit()
        await _log_bq_cost(session, "user_devices", bytes_proc, len(raw_rows), duration_ms)
        await session.commit()

        log.info("sync_user_devices done: rows=%d bytes=%d", inserted, bytes_proc)


# ---------------------------------------------------------------------------
# sync_singular_campaign_metrics
# ---------------------------------------------------------------------------

async def sync_singular_campaign_metrics():
    """
    Mirror prod singular_campaign_metrics into the local table.
    Syncs a rolling 90-day window to capture late-arriving Singular data.
    For initial historical backfill use backfill_singular_campaign_metrics().
    """
    bq = BQClient()
    import asyncio, time
    from datetime import date as date_type, timedelta
    from decimal import Decimal as _Decimal
    from services.shared.models import SingularCampaignMetric
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    since = (date_type.today() - timedelta(days=90)).isoformat()
    until = date_type.today().isoformat()

    log.info("sync_singular_campaign_metrics: %s → %s", since, until)

    async with AsyncSessionLocal() as session:
        sql = bq.load_sql("singular_campaign_metrics", since=since, until=until)

        try:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: bq.dry_run(sql, label="singular_campaign_metrics")
            )
        except RuntimeError as exc:
            log.error("singular_campaign_metrics dry_run blocked: %s", exc)
            return

        t0 = time.monotonic()
        raw_rows, bytes_proc = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bq.stream_rows(sql, label="singular_campaign_metrics")
        )
        duration_ms = int((time.monotonic() - t0) * 1000)

        if not raw_rows:
            log.info("sync_singular_campaign_metrics: no rows")
            return

        chunk_size = 1000
        inserted = 0
        for i in range(0, len(raw_rows), chunk_size):
            chunk = raw_rows[i : i + chunk_size]
            values = []
            for r in chunk:
                if r.get("date") is None:
                    continue
                values.append({
                    "date":          r["date"] if isinstance(r["date"], date_type) else date_type.fromisoformat(str(r["date"])),
                    "source":        str(r.get("source") or ""),
                    "campaign_name": str(r.get("campaign_name") or ""),
                    "os":            str(r.get("os") or ""),
                    "cost":          _Decimal(str(r["cost"])) if r.get("cost") is not None else None,
                    "installs":      int(r["installs"]) if r.get("installs") is not None else None,
                    "clicks":        int(r["clicks"])   if r.get("clicks")   is not None else None,
                    "impressions":   int(r["impressions"]) if r.get("impressions") is not None else None,
                })
            if not values:
                continue
            stmt = pg_insert(SingularCampaignMetric).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "source", "campaign_name", "os"],
                set_={
                    "cost":        stmt.excluded.cost,
                    "installs":    stmt.excluded.installs,
                    "clicks":      stmt.excluded.clicks,
                    "impressions": stmt.excluded.impressions,
                    "synced_at":   text("NOW()"),
                },
            )
            result = await session.execute(stmt)
            inserted += result.rowcount or len(chunk)

        await session.commit()
        await _log_bq_cost(session, "singular_campaign_metrics", bytes_proc, len(raw_rows), duration_ms)
        await session.commit()

        log.info("sync_singular_campaign_metrics done: rows=%d bytes=%d", inserted, bytes_proc)


# ---------------------------------------------------------------------------
# backfill_singular_campaign_metrics
# ---------------------------------------------------------------------------

async def backfill_singular_campaign_metrics(since: str, until: str):
    """
    Historical backfill for singular_campaign_metrics, chunked by month.
    Run once after migration to seed Jan 2026 – present.

    Usage:
        python -c "
        import asyncio
        from services.worker.jobs.sync_attribution import backfill_singular_campaign_metrics
        asyncio.run(backfill_singular_campaign_metrics('2026-01-01', '2026-04-30'))
        "
    """
    from datetime import date as date_type, timedelta
    from decimal import Decimal as _Decimal
    import asyncio
    from services.shared.models import SingularCampaignMetric
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    bq = BQClient()
    since_dt = datetime.fromisoformat(since)
    until_dt = datetime.fromisoformat(until)
    cursor = since_dt
    total = 0

    while cursor < until_dt:
        if cursor.month == 12:
            chunk_end = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            chunk_end = cursor.replace(month=cursor.month + 1, day=1)
        chunk_end = min(chunk_end, until_dt)

        s = cursor.date().isoformat()
        u = chunk_end.date().isoformat()
        log.info("backfill singular_campaign_metrics %s → %s", s, u)

        sql = bq.load_sql("singular_campaign_metrics", since=s, until=u)
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda sq=sql: bq.dry_run(sq, label="backfill_scm")
            )
        except RuntimeError as exc:
            log.error("scm backfill dry_run blocked %s: %s", s, exc)
            cursor = chunk_end
            continue

        raw_rows, bytes_proc = await asyncio.get_event_loop().run_in_executor(
            None, lambda sq=sql: bq.stream_rows(sq, label="backfill_scm")
        )

        async with AsyncSessionLocal() as session:
            values = []
            for r in raw_rows:
                if r.get("date") is None:
                    continue
                values.append({
                    "date":          r["date"] if isinstance(r["date"], date_type) else date_type.fromisoformat(str(r["date"])),
                    "source":        str(r.get("source") or ""),
                    "campaign_name": str(r.get("campaign_name") or ""),
                    "os":            str(r.get("os") or ""),
                    "cost":          _Decimal(str(r["cost"])) if r.get("cost") is not None else None,
                    "installs":      int(r["installs"])    if r.get("installs")    is not None else None,
                    "clicks":        int(r["clicks"])      if r.get("clicks")      is not None else None,
                    "impressions":   int(r["impressions"]) if r.get("impressions") is not None else None,
                })
            if values:
                for i in range(0, len(values), 1000):
                    chunk = values[i : i + 1000]
                    stmt = pg_insert(SingularCampaignMetric).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["date", "source", "campaign_name", "os"],
                        set_={
                            "cost":        stmt.excluded.cost,
                            "installs":    stmt.excluded.installs,
                            "clicks":      stmt.excluded.clicks,
                            "impressions": stmt.excluded.impressions,
                            "synced_at":   text("NOW()"),
                        },
                    )
                    await session.execute(stmt)
                await _log_bq_cost(session, "backfill_scm", bytes_proc, len(raw_rows), 0)
                await session.commit()
                total += len(values)

        log.info("scm backfill chunk done: rows=%d", len(values))
        cursor = chunk_end

    log.info("backfill_singular_campaign_metrics complete: total=%d", total)
    return {"since": since, "until": until, "rows": total}
