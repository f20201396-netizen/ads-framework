"""Sync Meta Ads change log (account audit log) into meta_change_log.

Pulls from Meta's GET /{ad_account_id}/activities. Watermark is max(event_time)
per account in our local DB — first run grabs the most recent N events, every
subsequent run pulls only events newer than what we already have.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from services.shared.config import settings
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.models import MetaChangeLog
from services.shared.rate_limiter import RateLimiter


log = logging.getLogger(__name__)

INITIAL_LOOKBACK_DAYS = 15  # first run pulls every event within this window


def _parse_event_time(raw: Any) -> datetime | None:
    """Meta returns event_time as ISO-8601 with offset (e.g. '2026-05-13T12:34:56+0000')."""
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    try:
        # 2026-05-13T12:34:56+0000 → 2026-05-13T12:34:56+00:00
        s = str(raw)
        if len(s) >= 5 and s[-5] in "+-" and s[-3] != ":":
            s = s[:-2] + ":" + s[-2:]
        return datetime.fromisoformat(s)
    except Exception:
        log.warning("Could not parse event_time=%r", raw)
        return None


def _parse_row(account_id: str, raw: dict) -> dict | None:
    et = _parse_event_time(raw.get("event_time"))
    if et is None:
        return None
    return {
        "account_id":            account_id,
        "event_time":            et,
        "event_type":            raw.get("event_type") or "",
        "actor_id":              raw.get("actor_id"),
        "actor_name":            raw.get("actor_name"),
        "object_id":             raw.get("object_id"),
        "object_type":           raw.get("object_type"),
        "object_name":           raw.get("object_name"),
        "translated_event_type": raw.get("translated_event_type"),
        "application_id":        raw.get("application_id"),
        "application_name":      raw.get("application_name"),
        "extra_data":            raw.get("extra_data") or {},
    }


async def _watermark(session, account_id: str) -> datetime | None:
    """Most recent event_time we already have for this account."""
    result = await session.execute(
        select(func.max(MetaChangeLog.event_time)).where(MetaChangeLog.account_id == account_id)
    )
    return result.scalar()


async def _upsert(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = pg_insert(MetaChangeLog).values(rows)
    stmt = stmt.on_conflict_do_nothing(constraint="uq_meta_change_log_dedup")
    result = await session.execute(stmt)
    return result.rowcount or 0


async def sync_change_log() -> None:
    """Run a single sync pass across every ad account in settings."""
    log.info("sync_change_log: starting")
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=rl,
            )

            async with AsyncSessionLocal() as session:
                last_seen = await _watermark(session, account_id)

            if last_seen is None:
                # First run for this account — pull every event from the last N days
                since = datetime.now(tz=timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
                log.info("sync_change_log %s: first run, pulling since=%s (last %dd)",
                         account_id, since, INITIAL_LOOKBACK_DAYS)
                rows = []
                async for raw in client.list_activities(account_id, since=since):
                    parsed = _parse_row(account_id, raw)
                    if parsed:
                        rows.append(parsed)
            else:
                # Incremental — pull events strictly after our watermark
                log.info("sync_change_log %s: incremental since=%s", account_id, last_seen)
                rows = []
                async for raw in client.list_activities(account_id, since=last_seen):
                    parsed = _parse_row(account_id, raw)
                    if parsed and parsed["event_time"] > last_seen:
                        rows.append(parsed)

            async with AsyncSessionLocal() as session:
                inserted = await _upsert(session, rows)
                await session.commit()
            log.info("sync_change_log %s: fetched=%d, inserted=%d", account_id, len(rows), inserted)

    log.info("sync_change_log: done")
