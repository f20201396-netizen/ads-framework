#!/usr/bin/env python3
"""
Full sync + sheet refresh. Run with:
    .venv/bin/python3 scripts/sync.py [--sheet-id SHEET_ID] [--days N]

Steps (in order):
  1. Ad-level insights  — last N days per-day (default 3)
  2. Attribution        — signups + conversions from BQ
  3. Higher levels      — campaign/adset/account rollups
  4. Materialized views — mv_campaign_conversions, mv_adset_conversions
  5. Ad statuses        — batch API status check for active ads
  6. Structure delta    — batch-fetch any adsets/ads that appear in insights
                          but are missing from the structure tables
  7. Sheet refresh      — writes all tabs to Google Sheets
"""

import argparse
import asyncio
import subprocess
import sys
import os
from datetime import date, timedelta

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

# ── helpers ───────────────────────────────────────────────────────────────────

def step(n, label):
    print(f"\n[{n}/7] {label}")


def run_psql(sql):
    result = subprocess.run(
        ["psql", "meta_ads", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
    else:
        print(f"  OK: {result.stdout.strip()}")


# ── step 1: ad-level insights (per-day) ───────────────────────────────────────

async def sync_ad_insights(days: int):
    from services.shared.config import settings
    from services.shared.db import AsyncSessionLocal
    from services.shared.constants import INSIGHTS_AD_FIELDS
    from services.shared.meta_client import MetaClient
    from services.shared.models import InsightsDaily
    from services.shared.rate_limiter import RateLimiter
    from services.worker.parsers import parse_insight_ad
    from services.worker.upsert import upsert_facts

    WINDOW = "7d_click"
    today = date.today()
    dates = [(today - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]

    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=rl,
            )
            for d in dates:
                rows = []
                async for raw in client.get_insights(
                    object_id=account_id,
                    level="ad",
                    time_range={"since": d, "until": d},
                    fields=INSIGHTS_AD_FIELDS,
                    action_attribution_windows=[WINDOW],
                ):
                    parsed = parse_insight_ad(raw, WINDOW)
                    if parsed.get("ad_id") is None:
                        continue
                    rows.append(parsed)
                if rows:
                    async with AsyncSessionLocal() as session:
                        await upsert_facts(session, InsightsDaily, rows, ["ad_id", "date", "attribution_window"])
                print(f"  {d}: {len(rows)} rows")


# ── step 2: attribution (signups + conversions) ────────────────────────────────

async def sync_attribution():
    from services.worker.jobs.sync_attribution import (
        sync_attribution_signups,
        sync_attribution_conversions,
    )
    await sync_attribution_signups()
    print("  signups: done")
    await sync_attribution_conversions()
    print("  conversions: done")


# ── step 3: higher levels (campaign/adset/account rollups) ────────────────────

async def sync_higher_levels():
    from services.worker.jobs.sync_higher_levels import sync_insights_higher_levels
    await sync_insights_higher_levels()
    print("  campaign/adset/account rollups: done")


# ── step 5: ad statuses ───────────────────────────────────────────────────────

async def sync_ad_statuses():
    from services.worker.jobs.sync_structure import sync_ad_statuses
    await sync_ad_statuses()
    print("  ad statuses: done")


# ── step 5b: change log ─────────────────────────────────────────────────────

async def sync_change_log_step():
    from services.worker.jobs.sync_change_log import sync_change_log
    await sync_change_log()
    print("  change log: done")


# ── step 6: structure delta (backfill missing adsets + ads from insights) ────

async def sync_structure_delta():
    """Batch-fetch any adsets/ads referenced by recent insights but missing
    from the structure tables. Cheaper than full sync_account_structure
    (~10-15 batch calls vs hundreds), avoids Meta code=17 rate-limit trips.
    Order matters: adsets before ads (FK ads_adset_id_fkey)."""
    import json
    from sqlalchemy import text
    from services.shared.config import settings
    from services.shared.db import AsyncSessionLocal
    from services.shared.constants import AD_FIELDS, ADSET_FIELDS
    from services.shared.meta_client import MetaClient
    from services.shared.rate_limiter import RateLimiter
    from services.worker.parsers import parse_ad, parse_adset
    from services.worker.upsert import upsert_dims
    import services.shared.models as m

    async def fetch_batch(client, ids, fields, parser, account_id):
        rows = []
        for i in range(0, len(ids), 50):
            batch = ids[i : i + 50]
            reqs = [{"method": "GET", "relative_url": f"{aid}?fields={fields}"} for aid in batch]
            try:
                results = await client.batch(reqs)
            except Exception as e:
                print(f"    batch {i}: ERROR {e}")
                return rows
            for resp in results:
                body = json.loads(resp["body"])
                if "error" in body:
                    continue
                rows.append(parser(body, account_id))
        return rows

    async with AsyncSessionLocal() as s:
        miss_adsets = (await s.execute(text(
            "SELECT DISTINCT i.adset_id, i.account_id FROM insights_daily i "
            "LEFT JOIN adsets a ON a.id=i.adset_id "
            "WHERE i.date >= CURRENT_DATE - 7 AND a.id IS NULL"
        ))).fetchall()
        miss_ads = (await s.execute(text(
            "SELECT DISTINCT i.ad_id, i.account_id FROM insights_daily i "
            "LEFT JOIN ads a ON a.id=i.ad_id "
            "WHERE i.date >= CURRENT_DATE - 7 AND a.id IS NULL"
        ))).fetchall()

    print(f"  missing: {len(miss_adsets)} adsets, {len(miss_ads)} ads")
    if not miss_adsets and not miss_ads:
        print("  structure delta: nothing to backfill")
        return

    by_acct_adsets, by_acct_ads = {}, {}
    for aid, acct in miss_adsets:
        by_acct_adsets.setdefault(acct, []).append(aid)
    for aid, acct in miss_ads:
        by_acct_ads.setdefault(acct, []).append(aid)

    async with httpx.AsyncClient() as http:
        for acct in set(list(by_acct_adsets) + list(by_acct_ads)):
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=RateLimiter(db_factory=AsyncSessionLocal, account_id=acct),
            )
            if by_acct_adsets.get(acct):
                rows = await fetch_batch(client, by_acct_adsets[acct], ADSET_FIELDS, parse_adset, acct)
                if rows:
                    async with AsyncSessionLocal() as s:
                        await upsert_dims(s, m.AdSet, rows)
                    print(f"  {acct}: upserted {len(rows)} adsets")
            if by_acct_ads.get(acct):
                rows = await fetch_batch(client, by_acct_ads[acct], AD_FIELDS, parse_ad, acct)
                if rows:
                    async with AsyncSessionLocal() as s:
                        await upsert_dims(s, m.Ad, rows)
                    print(f"  {acct}: upserted {len(rows)} ads")


# ── main ──────────────────────────────────────────────────────────────────────

async def main(sheet_id: str, days: int):
    step(1, f"Ad-level insights (last {days} days)")
    await sync_ad_insights(days)

    step(2, "Attribution — signups + conversions")
    await sync_attribution()

    step(3, "Higher-level rollups — campaign / adset / account")
    await sync_higher_levels()

    step(4, "Materialized views")
    run_psql("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_campaign_conversions;")
    run_psql("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_adset_conversions;")

    step(5, "Ad statuses (batch API)")
    await sync_ad_statuses()

    step(6, "Structure delta (backfill missing adsets + ads)")
    await sync_structure_delta()

    # Change log (Meta audit events) — runs after structure so object names are fresh
    print("\n[6b/7] Change log (Meta account activities)")
    await sync_change_log_step()

    step(7, "Sheet refresh")
    cmd = [sys.executable, "scripts/update_meta_dashboard.py"]
    if sheet_id:
        cmd += ["--sheet-id", sheet_id]
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print("  ERROR: sheet refresh failed")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full Meta Ads sync + sheet refresh")
    parser.add_argument("--sheet-id", default="1EBu7vZWGdLUVdL4I6a0J22soLIoXKWWIRRWTGk3BZ7s",
                        help="Google Sheet ID")
    parser.add_argument("--days", type=int, default=3,
                        help="Number of recent days to sync ad-level insights (default 3)")
    args = parser.parse_args()

    asyncio.run(main(sheet_id=args.sheet_id, days=args.days))
