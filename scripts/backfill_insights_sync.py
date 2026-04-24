"""
Backfill insights_daily for a specific date range using SYNCHRONOUS Meta API calls
(not async reports, which are failing). Only fetches 7d_click window.
Chunks into 3-day windows to stay well under rate limits.
"""
import asyncio
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
from services.worker.upsert import upsert_facts

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SINCE = date(2026, 4, 14)
UNTIL = date(2026, 4, 22)
WINDOW = "7d_click"
CHUNK_DAYS = 3
_INSIGHT_PK = ["ad_id", "date", "attribution_window"]


async def backfill_chunk(http, account_id, since: date, until: date):
    rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
    client = MetaClient(
        access_token=settings.meta_access_token,
        app_secret=settings.meta_app_secret,
        http_client=http,
        rate_limiter=rl,
    )
    rows = []
    log.info("Fetching %s → %s ...", since, until)
    async for raw in client.get_insights(
        object_id=account_id,
        level="ad",
        time_range={"since": since.isoformat(), "until": until.isoformat()},
        fields=INSIGHTS_AD_FIELDS,
        action_attribution_windows=[WINDOW],
        use_async_if_range_days_gt=30,   # force synchronous path
    ):
        parsed = parse_insight_ad(raw, WINDOW)
        if parsed.get("ad_id") is None:
            continue
        rows.append(parsed)

    if rows:
        async with AsyncSessionLocal() as session:
            await upsert_facts(session, InsightsDaily, rows, _INSIGHT_PK)
        log.info("  Upserted %d rows for %s → %s", len(rows), since, until)
    else:
        log.info("  No rows for %s → %s", since, until)
    return len(rows)


async def main():
    total = 0
    async with httpx.AsyncClient() as http:
        cursor = SINCE
        while cursor <= UNTIL:
            chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), UNTIL)
            for account_id in settings.ad_account_id_list:
                n = await backfill_chunk(http, account_id, cursor, chunk_end)
                total += n
            cursor = chunk_end + timedelta(days=1)

    print(f"\nDone — {total} rows upserted for {SINCE} → {UNTIL} ({WINDOW})")


asyncio.run(main())
