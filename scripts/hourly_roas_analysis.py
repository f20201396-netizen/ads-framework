#!/usr/bin/env python3
"""
Hourly ROAS analysis — compares Meta ad spend by hour vs attribution conversions by hour.

Run: .venv/bin/python3 scripts/hourly_roas_analysis.py
"""

import asyncio
import os
import sys
from datetime import date, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from sqlalchemy import text

from services.shared.config import settings
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.rate_limiter import RateLimiter

DAYS = 30
FIELDS = "ad_id,spend,impressions,clicks,date_start,hourly_stats_aggregated_by_advertiser_time_zone"


async def fetch_hourly_attribution_detail():
    """Pull signups/conversions/revenue by hour from local DB, also day-of-week breakdown."""
    async with AsyncSessionLocal() as session:
        # By hour of day
        by_hour = await session.execute(text("""
            SELECT
                EXTRACT(HOUR FROM event_time AT TIME ZONE 'Asia/Kolkata')::int        AS hour_ist,
                COUNT(DISTINCT CASE WHEN event_name = 'signup' THEN user_id END)       AS signups,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup = 0 THEN user_id END)       AS d0_conv,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup <= 6 THEN user_id END)      AS d6_conv,
                COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                                   AND days_since_signup <= 6
                                  THEN revenue_inr ELSE 0 END), 0)                    AS d6_revenue
            FROM attribution_events
            WHERE install_date >= CURRENT_DATE - CAST(:days AS integer)
              AND meta_creative_id IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """), {"days": DAYS})

        # By day of week
        by_dow = await session.execute(text("""
            SELECT
                TO_CHAR(install_date, 'Dy')                                            AS dow,
                EXTRACT(DOW FROM install_date)::int                                    AS dow_num,
                COUNT(DISTINCT CASE WHEN event_name = 'signup' THEN user_id END)       AS signups,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup <= 6 THEN user_id END)      AS d6_conv,
                COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                                   AND days_since_signup <= 6
                                  THEN revenue_inr ELSE 0 END), 0)                    AS d6_revenue
            FROM attribution_events
            WHERE install_date >= CURRENT_DATE - CAST(:days AS integer)
              AND meta_creative_id IS NOT NULL
            GROUP BY 1, 2 ORDER BY 2
        """), {"days": DAYS})

        return (
            {r.hour_ist: r._mapping for r in by_hour.fetchall()},
            {r.dow_num: r._mapping for r in by_dow.fetchall()},
        )


async def fetch_hourly_attribution():
    """Pull signups/conversions by hour of install from local DB."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT
                EXTRACT(HOUR FROM event_time AT TIME ZONE 'Asia/Kolkata')::int AS hour_ist,
                COUNT(DISTINCT CASE WHEN event_name = 'signup' THEN user_id END) AS signups,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup = 0 THEN user_id END)         AS d0_conv,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup <= 6 THEN user_id END)        AS d6_conv,
                COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                                   AND days_since_signup <= 6
                                  THEN revenue_inr ELSE 0 END), 0)                       AS d6_revenue
            FROM attribution_events
            WHERE install_date >= CURRENT_DATE - CAST(:days AS integer)
              AND meta_creative_id IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """), {"days": DAYS})
        return {r.hour_ist: r._mapping for r in result.fetchall()}


def is_market_hour(h):
    return 9 <= h <= 15  # 3:30 PM cutoff approximated as <=15


async def main():
    print(f"Analyzing last {DAYS} days (Meta hourly spend API unavailable — using install-time quality signals)\n")

    print("Fetching hourly attribution from local DB...")
    attr, dow = await fetch_hourly_attribution_detail()
    print(f"  Done.\n")

    GST = 1.18

    # ── Hourly table ──────────────────────────────────────────────────────────
    print(f"{'Hr':>3}  {'Window':<8}  {'Signups':>8}  {'D6 Conv':>8}  {'D6 Rev ₹':>12}  {'Rev/Signup ₹':>13}  {'Conv%':>6}")
    print("─" * 78)

    market_signups = market_d6conv = market_rev = 0
    off_signups    = off_d6conv    = off_rev    = 0

    for h in range(24):
        a = attr.get(h, {})
        signups = int(a.get("signups", 0))
        d6_conv = int(a.get("d6_conv", 0))
        d6_rev  = float(a.get("d6_revenue", 0)) * GST
        conv_pct   = round(d6_conv * 100 / signups, 1) if signups > 0 else None
        rev_signup  = round(d6_rev / signups, 0) if signups > 0 else None
        window = "MARKET" if is_market_hour(h) else "off"

        conv_str = f"{conv_pct:.1f}%" if conv_pct is not None else "—"
        rs_str   = f"₹{rev_signup:,.0f}" if rev_signup is not None else "—"
        rev_str  = f"₹{d6_rev:,.0f}"

        print(f"{h:02d}:00  {window:<8}  {signups:>8,}  {d6_conv:>8,}  {rev_str:>12}  {rs_str:>13}  {conv_str:>6}")

        if is_market_hour(h):
            market_signups += signups; market_d6conv += d6_conv; market_rev += d6_rev
        else:
            off_signups += signups; off_d6conv += d6_conv; off_rev += d6_rev

    print("─" * 78)

    # ── Summary ───────────────────────────────────────────────────────────────
    def summary(label, signups, d6conv, rev):
        cvr = round(d6conv * 100 / signups, 2) if signups > 0 else None
        rps = round(rev / signups, 0) if signups > 0 else None
        print(f"\n{label}")
        print(f"  Signups:          {signups:,}")
        print(f"  D6 Conv:          {d6conv:,}  ({cvr}% conv rate)")
        print(f"  D6 Revenue:       ₹{rev:,.0f}")
        print(f"  Revenue/Signup:   ₹{rps:,.0f}" if rps else "  Revenue/Signup:  —")

    summary("▶ MARKET HOURS (9 AM – 3:30 PM IST)", market_signups, market_d6conv, market_rev)
    summary("▶ OFF MARKET", off_signups, off_d6conv, off_rev)

    # ── Day of week ───────────────────────────────────────────────────────────
    print(f"\n\n{'Day':<6}  {'Signups':>8}  {'D6 Conv':>8}  {'Conv%':>6}  {'Rev/Signup ₹':>13}")
    print("─" * 50)
    for dn in range(7):
        d = dow.get(dn, {})
        signups = int(d.get("signups", 0))
        d6conv  = int(d.get("d6_conv", 0))
        rev     = float(d.get("d6_revenue", 0)) * GST
        name    = d.get("dow", ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"][dn])
        cvr     = round(d6conv * 100 / signups, 1) if signups > 0 else None
        rps     = round(rev / signups, 0) if signups > 0 else None
        flag    = " ← market day" if dn not in (0, 6) else " ← weekend"
        print(f"{name:<6}  {signups:>8,}  {d6conv:>8,}  {f'{cvr:.1f}%':>6}  {f'₹{rps:,.0f}' if rps else '—':>13}{flag}")


if __name__ == "__main__":
    asyncio.run(main())
