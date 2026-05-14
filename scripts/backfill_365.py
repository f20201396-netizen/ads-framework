#!/usr/bin/env python3
"""One-shot 365-day attribution backfill in weekly chunks.

Wipes nothing — assumes attribution_events for the window has already been deleted
and the cursor reset. Walks 7-day windows because monthly windows time out against
prod Postgres for the new filtered query."""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.shared.bq_client import BQClient
from services.shared.db import AsyncSessionLocal
from services.worker.jobs.sync_attribution import (
    _parse_row,
    _upsert_attribution_events,
    _log_bq_cost,
)


RETRYABLE = ("canceling statement", "terminating connection",
             "server closed the connection", "Failed to fetch row")


async def _run_chunk(bq, event_type, start, end, max_retries=4):
    """Execute one chunk with retries on transient replica errors."""
    sql = bq.load_sql(event_type, since=start.isoformat(), until=end.isoformat())
    last_err = None
    for attempt in range(max_retries):
        try:
            raw, bytes_p = await asyncio.get_event_loop().run_in_executor(
                None, lambda s=sql: bq.stream_rows(s, label=f"backfill_{event_type}")
            )
            return raw, bytes_p, None
        except Exception as e:
            msg = str(e)
            last_err = msg.replace("\n", " ")[:200]
            if not any(t in msg for t in RETRYABLE):
                return None, 0, last_err
            wait = 2 ** attempt  # 1s, 2s, 4s, 8s
            await asyncio.sleep(wait)
    return None, 0, last_err


async def chunked_backfill(event_type: str, days_back: int, chunk_days: int = 7):
    bq = BQClient()
    now = datetime.now(tz=timezone.utc)
    cursor = now - timedelta(days=days_back)
    total = 0
    chunk_idx = 0
    failed_chunks = []
    while cursor < now:
        chunk_end = min(cursor + timedelta(days=chunk_days), now)
        chunk_idx += 1
        raw, bytes_p, err = await _run_chunk(bq, event_type, cursor, chunk_end)
        if err is not None:
            print(f"  [{chunk_idx:02d}] {cursor.date()} -> {chunk_end.date()}: FAILED {err}", flush=True)
            failed_chunks.append((cursor, chunk_end))
            cursor = chunk_end
            continue
        parsed = [_parse_row(r) for r in raw]
        async with AsyncSessionLocal() as session:
            inserted = await _upsert_attribution_events(session, parsed)
            await _log_bq_cost(session, f"backfill_{event_type}", bytes_p, len(raw), 0)
            await session.commit()
        total += inserted
        print(f"  [{chunk_idx:02d}] {cursor.date()} -> {chunk_end.date()}: {inserted} rows", flush=True)
        cursor = chunk_end
    print(f"== {event_type} TOTAL: {total} (failed: {len(failed_chunks)}) ==", flush=True)
    return failed_chunks


async def retry_failed(bq, event_type, failed_chunks, sub_chunk_days=2):
    """Re-run failed chunks at smaller granularity (sub-chunks)."""
    if not failed_chunks:
        return
    print(f"== retrying {len(failed_chunks)} failed {event_type} chunks at {sub_chunk_days}d granularity ==", flush=True)
    for start, end in failed_chunks:
        c = start
        while c < end:
            e = min(c + timedelta(days=sub_chunk_days), end)
            raw, bytes_p, err = await _run_chunk(bq, event_type, c, e, max_retries=5)
            if err is not None:
                print(f"  retry {c.date()} -> {e.date()}: STILL-FAILED {err}", flush=True)
            else:
                parsed = [_parse_row(r) for r in raw]
                async with AsyncSessionLocal() as session:
                    inserted = await _upsert_attribution_events(session, parsed)
                    await _log_bq_cost(session, f"backfill_{event_type}", bytes_p, len(raw), 0)
                    await session.commit()
                print(f"  retry {c.date()} -> {e.date()}: {inserted} rows", flush=True)
            c = e


async def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 365
    chunk = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    only = sys.argv[3] if len(sys.argv) > 3 else "both"  # 'signups' | 'conversions' | 'both'
    bq = BQClient()
    if only in ("signups", "both"):
        print(f"=== SIGNUPS ({days}d, chunk={chunk}d) ===", flush=True)
        s_failed = await chunked_backfill("signups", days, chunk_days=chunk)
        await retry_failed(bq, "signups", s_failed, sub_chunk_days=2)
    if only in ("conversions", "both"):
        print(f"=== CONVERSIONS ({days}d, chunk={chunk}d) ===", flush=True)
        c_failed = await chunked_backfill("conversions", days, chunk_days=chunk)
        await retry_failed(bq, "conversions", c_failed, sub_chunk_days=2)


if __name__ == "__main__":
    asyncio.run(main())
