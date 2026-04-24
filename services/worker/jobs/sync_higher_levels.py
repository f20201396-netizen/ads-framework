"""
sync_insights_higher_levels — §6.3 of the curl script.

Schedule: hourly.
Window:   last 3 days.
Levels:   adset, campaign, account — one job, three passes.
"""

import logging
from datetime import date, timedelta

import httpx

from services.shared.config import settings
from services.shared.constants import INSIGHTS_LEVEL_FIELDS
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.models import (
    InsightsAccountDaily,
    InsightsAdsetDaily,
    InsightsCampaignDaily,
)
from services.shared.rate_limiter import RateLimiter
from services.worker.parsers import parse_insight_level
from services.worker.upsert import track_run, upsert_facts

log = logging.getLogger(__name__)

_WINDOW = "7d_click"
_DAYS_BACK = 3

_LEVEL_CONFIG = [
    # (level_str, id_column, model, pk_columns)
    ("adset",    "adset_id",    InsightsAdsetDaily,    ["adset_id",    "date", "attribution_window"]),
    ("campaign", "campaign_id", InsightsCampaignDaily, ["campaign_id", "date", "attribution_window"]),
    ("account",  "account_id",  InsightsAccountDaily,  ["account_id",  "date", "attribution_window"]),
]


async def sync_insights_higher_levels() -> None:
    log.info("sync_insights_higher_levels: starting")
    since, until = _sliding_window(_DAYS_BACK)
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=rl,
            )
            for level, id_col, model, pk in _LEVEL_CONFIG:
                await _sync_level(client, account_id, level, id_col, model, pk, since, until)
    log.info("sync_insights_higher_levels: done")


async def _sync_level(
    client: MetaClient,
    account_id: str,
    level: str,
    id_col: str,
    model,
    pk: list[str],
    since: str,
    until: str,
) -> None:
    async with track_run(f"insights_{level}_daily", account_id) as run:
        rows: list[dict] = []
        async for raw in client.get_insights(
            object_id=account_id,
            level=level,
            time_range={"since": since, "until": until},
            fields=INSIGHTS_LEVEL_FIELDS,
            action_attribution_windows=[_WINDOW],
        ):
            rows.append(parse_insight_level(raw, id_col, account_id, _WINDOW))
            run.rows_upserted += 1

        if rows:
            async with AsyncSessionLocal() as session:
                await upsert_facts(session, model, rows, pk)


def _sliding_window(days_back: int) -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=days_back)).isoformat(), today.isoformat()
