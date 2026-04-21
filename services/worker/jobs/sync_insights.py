"""
sync_insights_daily — §6.1 of the curl script.

Schedule: hourly.
Window:   last 3 days (catches late attribution).
Level:    ad.
Window:   7d_click (primary attribution window; extend to all windows in backfill).
"""

import logging
from datetime import date, timedelta

import httpx

from services.shared.config import settings
from services.shared.constants import INSIGHTS_AD_FIELDS
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.models import InsightsDaily
from services.shared.rate_limiter import RateLimiter
from services.worker.parsers import parse_insight_ad
from services.worker.upsert import track_run, upsert_facts

log = logging.getLogger(__name__)

_WINDOW = "7d_click"
_DAYS_BACK = 3
_INSIGHT_PK = ["ad_id", "date", "attribution_window"]


async def sync_insights_daily() -> None:
    log.info("sync_insights_daily: starting")
    since, until = _sliding_window(_DAYS_BACK)
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            await _sync_one(http, account_id, since, until)
    log.info("sync_insights_daily: done")


async def _sync_one(http: httpx.AsyncClient, account_id: str, since: str, until: str) -> None:
    rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
    client = MetaClient(
        access_token=settings.meta_access_token,
        http_client=http,
        rate_limiter=rl,
    )
    async with track_run("insights_daily", account_id) as run:
        rows: list[dict] = []
        async for raw in client.get_insights(
            object_id=account_id,
            level="ad",
            time_range={"since": since, "until": until},
            fields=INSIGHTS_AD_FIELDS,
            action_attribution_windows=[_WINDOW],
        ):
            rows.append(parse_insight_ad(raw, _WINDOW))
            run.rows_upserted += 1

        async with AsyncSessionLocal() as session:
            await upsert_facts(session, InsightsDaily, rows, _INSIGHT_PK)


def _sliding_window(days_back: int) -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=days_back)).isoformat(), today.isoformat()
