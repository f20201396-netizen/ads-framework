#!/usr/bin/env python3
"""Data-sync only (steps 1-6 + MV refresh) — NO sheet push.
Used to honor the freshness gate: sync DB, verify, THEN refresh sheets separately.
Reuses the step functions from scripts/sync.py."""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sync as S


async def run():
    print("[1/6] Ad-level insights (last 3 days)")
    await S.sync_ad_insights(3)

    print("\n[2/6] Attribution — signups + conversions")
    await S.sync_attribution()

    print("\n[3/6] Higher-level rollups")
    await S.sync_higher_levels()

    print("\n[4/6] Materialized views")
    S.run_psql("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_campaign_conversions;")
    S.run_psql("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_adset_conversions;")

    print("\n[5/6] Ad statuses (batch API)")
    try:
        await S.sync_ad_statuses()
    except Exception as e:
        print(f"  WARNING: ad statuses step failed: {e}; continuing.")

    print("\n[6/6] Structure delta")
    try:
        await S.sync_structure_delta()
    except Exception as e:
        print(f"  WARNING: structure delta failed: {e}; continuing.")

    # [6b] Change log — SKIPPED. Meta audit-event pagination hangs for many
    # minutes and only feeds the cosmetic Change Log tab (not a freshness gate).
    print("\n[6b] Change log — skipped (hang-prone, non-critical)")

    print("\n=== DATA SYNC COMPLETE ===")


if __name__ == "__main__":
    asyncio.run(run())
