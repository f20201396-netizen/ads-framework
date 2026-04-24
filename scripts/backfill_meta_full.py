"""
Full Meta backfill: structure (campaigns/adsets/ads) + insights.

Usage:
    python scripts/backfill_meta_full.py --since 2024-01-01 --until 2026-04-23
    python scripts/backfill_meta_full.py --since 2024-01-01 --until 2026-04-23 --windows 7d_click
    python scripts/backfill_meta_full.py --since 2024-01-01 --until 2026-04-23 --skip-structure

Steps:
  1. Sync structure (campaigns → adsets → ads) — current snapshot from Meta API
  2. Backfill insights_daily in 3-day chunks for each attribution window

Attribution windows (default = all 5):
    1d_click, 7d_click, 28d_click, 1d_view, 7d_view
"""

import argparse
import asyncio
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
for lib in ("httpx", "httpcore", "google.auth", "urllib3"):
    logging.getLogger(lib).setLevel(logging.WARNING)

log = logging.getLogger(__name__)

from services.shared.config import settings
from services.shared.constants import ACTION_ATTRIBUTION_WINDOWS, INSIGHTS_AD_FIELDS
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.models import InsightsDaily
from services.shared.rate_limiter import RateLimiter
from services.worker.jobs.sync_structure import sync_account_structure
from services.worker.parsers import parse_insight_ad
from services.worker.upsert import upsert_facts

_INSIGHT_PK = ["ad_id", "date", "attribution_window"]
CHUNK_DAYS = 3


async def backfill_insights_chunk(
    http: httpx.AsyncClient,
    account_id: str,
    window: str,
    since: date,
    until: date,
) -> int:
    rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
    client = MetaClient(
        access_token=settings.meta_access_token,
        app_secret=settings.meta_app_secret,
        http_client=http,
        rate_limiter=rl,
    )
    rows = []
    async for raw in client.get_insights(
        object_id=account_id,
        level="ad",
        time_range={"since": since.isoformat(), "until": until.isoformat()},
        fields=INSIGHTS_AD_FIELDS,
        action_attribution_windows=[window],
        use_async_if_range_days_gt=30,   # synchronous path
    ):
        parsed = parse_insight_ad(raw, window)
        if parsed.get("ad_id") is None:
            continue
        rows.append(parsed)

    if rows:
        async with AsyncSessionLocal() as session:
            await upsert_facts(session, InsightsDaily, rows, _INSIGHT_PK)

    return len(rows)


async def backfill_insights(since: date, until: date, windows: list[str]):
    total = 0
    n_chunks = ((until - since).days + CHUNK_DAYS - 1) // CHUNK_DAYS
    log.info(
        "insights backfill: %s → %s  windows=%s  chunks=%d",
        since, until, windows, n_chunks * len(settings.ad_account_id_list),
    )

    async with httpx.AsyncClient() as http:
        for window in windows:
            log.info("── window: %s ──", window)
            cursor = since
            chunk_num = 0
            while cursor <= until:
                chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), until)
                chunk_num += 1
                for account_id in settings.ad_account_id_list:
                    n = await backfill_insights_chunk(http, account_id, window, cursor, chunk_end)
                    total += n
                    log.info(
                        "  [%s] chunk %d/%d  %s → %s  rows=%d  total=%d",
                        window, chunk_num, n_chunks, cursor, chunk_end, n, total,
                    )
                cursor = chunk_end + timedelta(days=1)

    return total


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--until", default=date.today().isoformat(), help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--windows", default="all",
                        help="Comma-separated windows or 'all' (default: all)")
    parser.add_argument("--skip-structure", action="store_true",
                        help="Skip structure sync (campaigns/adsets/ads)")
    parser.add_argument("--skip-insights", action="store_true",
                        help="Skip insights backfill (structure only)")
    args = parser.parse_args()

    since = date.fromisoformat(args.since)
    until = date.fromisoformat(args.until)

    if args.windows == "all":
        windows = ACTION_ATTRIBUTION_WINDOWS
    else:
        windows = [w.strip() for w in args.windows.split(",")]

    # ── Step 1: Structure ─────────────────────────────────────────────────────
    if not args.skip_structure:
        log.info("═══ Step 1: Syncing structure (campaigns / adsets / ads) ═══")
        await sync_account_structure()
        log.info("Structure sync complete.")
    else:
        log.info("Skipping structure sync.")

    # ── Step 2: Insights ──────────────────────────────────────────────────────
    if not args.skip_insights:
        log.info("═══ Step 2: Backfilling insights (%s → %s) ═══", since, until)
        total = await backfill_insights(since, until, windows)
        log.info("Insights backfill complete — %d rows upserted total.", total)
    else:
        log.info("Skipping insights backfill.")

    log.info("All done.")


asyncio.run(main())
