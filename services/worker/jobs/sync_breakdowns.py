"""
sync_insights_breakdowns — §6.2 of the curl script.

Schedule: every 6 hours.
Window:   last 7 days.
Iterates: all 26 breakdowns, one API call each, sleep(1) between.
Errors:   permanent 400s (SKAN, DPA-only) are logged and skipped.
"""

import asyncio
import logging
from datetime import date, timedelta

import httpx

from services.shared.config import settings
from services.shared.constants import INSIGHT_BREAKDOWNS, INSIGHTS_BREAKDOWN_FIELDS
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaAPIError, MetaClient
from services.shared.models import InsightsDailyBreakdown
from services.shared.rate_limiter import RateLimiter
from services.worker.parsers import parse_insight_breakdown
from services.worker.upsert import track_run, upsert_facts

log = logging.getLogger(__name__)

_WINDOW = "7d_click"
_DAYS_BACK = 7
_BREAKDOWN_PK = ["ad_id", "date", "breakdown_type", "breakdown_key_hash", "attribution_window"]


async def sync_insights_breakdowns() -> None:
    log.info("sync_insights_breakdowns: starting")
    since, until = _sliding_window(_DAYS_BACK)
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            await _sync_one(http, account_id, since, until)
    log.info("sync_insights_breakdowns: done")


async def _sync_one(http: httpx.AsyncClient, account_id: str, since: str, until: str) -> None:
    rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
    client = MetaClient(
        access_token=settings.meta_access_token,
        app_secret=settings.meta_app_secret,
        http_client=http,
        rate_limiter=rl,
    )

    for breakdown in INSIGHT_BREAKDOWNS:
        await _sync_breakdown(client, account_id, breakdown, since, until)
        await asyncio.sleep(1)  # gentle pacing — matches curl script


async def _sync_breakdown(
    client: MetaClient,
    account_id: str,
    breakdown: str,
    since: str,
    until: str,
) -> None:
    async with track_run(f"insights_breakdown:{breakdown}", account_id) as run:
        rows: list[dict] = []
        try:
            async for raw in client.get_insights(
                object_id=account_id,
                level="ad",
                time_range={"since": since, "until": until},
                breakdowns=breakdown,
                fields=INSIGHTS_BREAKDOWN_FIELDS,
                action_attribution_windows=[_WINDOW],
            ):
                rows.append(parse_insight_breakdown(raw, breakdown, _WINDOW))
                run.rows_upserted += 1

        except MetaAPIError as exc:
            if exc.status_code == 400:
                # SKAN / DPA-only breakdowns fail on non-matching campaigns — skip
                log.warning(
                    "Skipping breakdown=%s account=%s: [400] code=%d %s",
                    breakdown,
                    account_id,
                    exc.code,
                    exc.message,
                )
                run.rows_upserted = 0
                return
            raise

        if rows:
            async with AsyncSessionLocal() as session:
                await upsert_facts(session, InsightsDailyBreakdown, rows, _BREAKDOWN_PK)


def _sliding_window(days_back: int) -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=days_back)).isoformat(), today.isoformat()
