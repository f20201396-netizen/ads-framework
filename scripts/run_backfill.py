#!/usr/bin/env python3
"""
Run attribution backfill directly (no API server needed).
Usage:
    python scripts/run_backfill.py signups   2022-08-01 2026-04-22
    python scripts/run_backfill.py conversions 2023-07-01 2026-04-22
"""
import asyncio
import logging
import sys
from pathlib import Path

# repo root on path
sys.path.insert(0, str(Path(__file__).parent.parent))

# configure logging before imports that might log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
# reduce noise
for lib in ("httpx", "httpcore", "google.auth", "urllib3"):
    logging.getLogger(lib).setLevel(logging.WARNING)

from services.worker.jobs.sync_attribution import backfill_attribution


async def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)

    event_type, since, until = sys.argv[1], sys.argv[2], sys.argv[3]
    print(f"Starting backfill: event_type={event_type}  {since} → {until}")
    result = await backfill_attribution(event_type=event_type, since=since, until=until)
    print("Done:", result)


asyncio.run(main())
