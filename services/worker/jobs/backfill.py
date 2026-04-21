"""
historical_backfill — admin-triggered, uses async reports (§6.4).

Called by the FastAPI admin endpoint:
  POST /admin/backfill?since=2024-01-01&until=2024-12-31

Runs ad-level insights for every configured account across all 5
attribution windows.  Always forces the async report path
(use_async_if_range_days_gt=0) regardless of range size.
"""

import logging

import httpx

from services.shared.config import settings
from services.shared.constants import ACTION_ATTRIBUTION_WINDOWS, INSIGHTS_AD_FIELDS
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.models import InsightsDaily
from services.shared.rate_limiter import RateLimiter
from services.worker.parsers import parse_insight_ad
from services.worker.upsert import track_run, upsert_facts

log = logging.getLogger(__name__)

_INSIGHT_PK = ["ad_id", "date", "attribution_window"]


async def historical_backfill(since: str, until: str) -> dict:
    """
    Backfill ad-level insights for all accounts and all attribution windows.

    Args:
        since: ISO date string "YYYY-MM-DD"
        until: ISO date string "YYYY-MM-DD"

    Returns:
        Summary dict {"accounts": N, "rows_upserted": N}
    """
    log.info("historical_backfill: since=%s until=%s", since, until)
    total_rows = 0

    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rows = await _backfill_account(http, account_id, since, until)
            total_rows += rows

    summary = {"accounts": len(settings.ad_account_id_list), "rows_upserted": total_rows}
    log.info("historical_backfill: complete %s", summary)
    return summary


async def _backfill_account(
    http: httpx.AsyncClient,
    account_id: str,
    since: str,
    until: str,
) -> int:
    total = 0
    for attribution_window in ACTION_ATTRIBUTION_WINDOWS:
        rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
        client = MetaClient(
            access_token=settings.meta_access_token,
            http_client=http,
            rate_limiter=rl,
        )
        async with track_run(f"backfill:{attribution_window}", account_id) as run:
            rows: list[dict] = []
            async for raw in client.get_insights(
                object_id=account_id,
                level="ad",
                time_range={"since": since, "until": until},
                fields=INSIGHTS_AD_FIELDS,
                action_attribution_windows=[attribution_window],
                use_async_if_range_days_gt=0,  # always use async reports
            ):
                rows.append(parse_insight_ad(raw, attribution_window))
                run.rows_upserted += 1

            if rows:
                async with AsyncSessionLocal() as session:
                    await upsert_facts(session, InsightsDaily, rows, _INSIGHT_PK)

            total += run.rows_upserted
    return total
