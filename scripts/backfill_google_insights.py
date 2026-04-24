"""
Backfill Google Ads insights_daily.

Usage:
    python scripts/backfill_google_insights.py --since 2024-01-01 --until 2026-04-24
    python scripts/backfill_google_insights.py --since 2024-01-01  # until=today
"""

import argparse
import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
for lib in ("httpx", "httpcore", "google.auth", "urllib3"):
    logging.getLogger(lib).setLevel(logging.WARNING)

from services.worker.jobs.sync_google_insights import backfill_google_insights
from services.worker.jobs.sync_google_structure import sync_google_structure


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", default=date.today().isoformat())
    parser.add_argument("--skip-structure", action="store_true")
    parser.add_argument("--chunk-days", type=int, default=7)
    args = parser.parse_args()

    since = date.fromisoformat(args.since)
    until = date.fromisoformat(args.until)

    if not args.skip_structure:
        print("Step 1: Syncing Google Ads structure...")
        await sync_google_structure()
        print("Structure sync done.")

    print(f"Step 2: Backfilling insights {since} → {until} (chunks={args.chunk_days}d)...")
    total = await backfill_google_insights(since, until, chunk_days=args.chunk_days)
    print(f"Done — {total:,} rows upserted.")


asyncio.run(main())
