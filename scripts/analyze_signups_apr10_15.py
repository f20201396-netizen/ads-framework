"""
Reverse-engineer all Facebook signups with install_date 2026-04-10 → 2026-04-15.

For each signup, trace back to the Meta ad → adset → campaign via:
  attribution_events.meta_creative_id  → ads.id
  attribution_events.meta_adset_id     → adsets.id
  attribution_events.meta_campaign_id  → campaigns.id

Metrics per entity:
  signups, d0_trials, d6_conversions, D6%, avg_ltv_inr (revenue/signup),
  spend (Apr 10-15 insights), CAC (spend/signups), ROAS (revenue/spend)

Output:
  1. Global summary + campaign-level ranked table
  2. Detailed hierarchy for named (synced) campaigns: Campaign → Adset → Ad
  3. Compact rollup for unresolved (not-yet-synced) campaigns
"""

import asyncio
import logging
from datetime import date

from sqlalchemy import text

logging.basicConfig(level=logging.WARNING)

SINCE = date(2026, 4, 10)
UNTIL = date(2026, 4, 15)


QUERY = text("""
WITH
-- All non-reattributed Facebook events for install dates Apr 10-15
events AS (
    SELECT
        ae.meta_creative_id,
        ae.meta_adset_id,
        ae.meta_campaign_id,
        ae.user_id,
        ae.event_name,
        ae.days_since_signup,
        ae.revenue_inr
    FROM attribution_events ae
    WHERE ae.install_date BETWEEN :since AND :until
      AND ae.is_reattributed = FALSE
      AND ae.network = 'Facebook'
      AND ae.meta_creative_id IS NOT NULL
      AND ae.meta_creative_id <> 'N/A'
),

-- Ad-level aggregates
ad_agg AS (
    SELECT
        meta_creative_id                                                          AS ad_id,
        MAX(meta_adset_id)                                                        AS raw_adset_id,
        MAX(meta_campaign_id)                                                     AS raw_campaign_id,
        COUNT(DISTINCT CASE WHEN event_name = 'signup'
                            THEN user_id END)                                     AS signups,
        COUNT(DISTINCT CASE WHEN event_name = 'trial'
                            THEN user_id END)                                     AS d0_trials,
        COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                             AND days_since_signup <= 6
                            THEN user_id END)                                     AS d6_conversions,
        SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                 THEN revenue_inr ELSE 0 END)                                     AS total_revenue_inr
    FROM events
    GROUP BY meta_creative_id
),

-- Ad-level spend over the same window (7d_click)
spend AS (
    SELECT
        ad_id,
        SUM(spend) AS spend
    FROM insights_daily
    WHERE date BETWEEN :since AND :until
      AND attribution_window = '7d_click'
    GROUP BY ad_id
)

SELECT
    aa.ad_id,
    COALESCE(a.name,  aa.ad_id)           AS ad_name,
    a.adset_id,
    COALESCE(s.name,  aa.raw_adset_id)    AS adset_name,
    a.campaign_id,
    COALESCE(cs.name, aa.raw_campaign_id) AS campaign_name,
    (a.id IS NOT NULL)                    AS ad_in_db,
    aa.signups,
    aa.d0_trials,
    aa.d6_conversions,
    aa.total_revenue_inr,
    ROUND(aa.d6_conversions::numeric * 100
          / NULLIF(aa.signups, 0), 1)     AS d6_pct,
    ROUND(aa.total_revenue_inr::numeric
          / NULLIF(aa.signups, 0), 0)     AS avg_ltv_inr,
    ROUND(sp.spend::numeric, 0)           AS spend_inr,
    CASE WHEN sp.spend > 0 AND aa.signups > 0
         THEN ROUND(sp.spend::numeric / aa.signups, 0)
    END                                   AS cac_inr,
    CASE WHEN sp.spend > 0 AND aa.total_revenue_inr > 0
         THEN ROUND(aa.total_revenue_inr::numeric / sp.spend, 3)
    END                                   AS roas
FROM ad_agg aa
LEFT JOIN ads       a  ON a.id  = aa.ad_id
LEFT JOIN adsets    s  ON s.id  = a.adset_id
LEFT JOIN campaigns cs ON cs.id = a.campaign_id
LEFT JOIN spend     sp ON sp.ad_id = aa.ad_id
""")


def _inr(v) -> str:
    if v is None:
        return "—"
    return f"₹{int(v):,}"


def _roas(rev, spend) -> str:
    if not spend:
        return "—"
    return f"{rev / spend:.2f}x"


def _d6pct(d6, signups) -> str:
    if not signups:
        return "—"
    return f"{d6 * 100 / signups:.1f}%"


async def main():
    from services.shared.db import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        result = await session.execute(QUERY, {"since": SINCE, "until": UNTIL})
        rows = result.mappings().all()

    if not rows:
        print(f"No attributed signups found for {SINCE} → {UNTIL}.")
        return

    # ── Build campaign-level rollups ──────────────────────────────────────────
    # key: campaign_name → {adset_name → [ad rows]}
    named: dict[str, dict[str, list]]   = {}   # campaigns where all ads are in DB
    unknown: dict[str, dict[str, list]] = {}   # campaigns with raw IDs

    for r in rows:
        cname = r["campaign_name"] or "Unknown"
        aname = r["adset_name"]    or "Unknown"
        bucket = named if r["ad_in_db"] else unknown
        # a campaign could have SOME known and some unknown ads — put it in named
        # if ANY ad is known (we want full detail for those)
        if r["ad_in_db"] and cname in unknown:
            named[cname] = unknown.pop(cname)
        if cname not in bucket and cname not in named:
            bucket[cname] = {}
        dest = named if cname in named else unknown
        if aname not in dest[cname]:
            dest[cname][aname] = []
        dest[cname][aname].append(r)

    def camp_stats(adsets: dict) -> dict:
        all_rows = [r for adset_rows in adsets.values() for r in adset_rows]
        signups  = sum(r["signups"] or 0         for r in all_rows)
        d6       = sum(r["d6_conversions"] or 0  for r in all_rows)
        trials   = sum(r["d0_trials"] or 0        for r in all_rows)
        rev      = sum(float(r["total_revenue_inr"] or 0) for r in all_rows)
        spend    = sum(float(r["spend_inr"] or 0) for r in all_rows)
        return dict(signups=signups, d6=d6, trials=trials, rev=rev, spend=spend)

    def adset_stats(ad_rows: list) -> dict:
        signups = sum(r["signups"] or 0         for r in ad_rows)
        d6      = sum(r["d6_conversions"] or 0  for r in ad_rows)
        trials  = sum(r["d0_trials"] or 0        for r in ad_rows)
        rev     = sum(float(r["total_revenue_inr"] or 0) for r in ad_rows)
        spend   = sum(float(r["spend_inr"] or 0) for r in ad_rows)
        return dict(signups=signups, d6=d6, trials=trials, rev=rev, spend=spend)

    # Pre-compute campaign stats for ranking
    all_camps = {**named, **unknown}
    camp_rollup = {c: camp_stats(a) for c, a in all_camps.items()}

    total_signups = sum(s["signups"] for s in camp_rollup.values())
    total_d6      = sum(s["d6"]      for s in camp_rollup.values())
    total_rev     = sum(s["rev"]     for s in camp_rollup.values())
    total_spend   = sum(s["spend"]   for s in camp_rollup.values())

    W = 148
    # ── 1. HEADER + GLOBAL STATS ──────────────────────────────────────────────
    print(f"\n{'='*W}")
    print(
        f"  SIGNUP ATTRIBUTION   install_date {SINCE} → {UNTIL}"
    )
    print(
        f"  Total signups : {total_signups:,}     D6 conv : {total_d6:,} ({_d6pct(total_d6, total_signups)})"
        f"     Revenue : {_inr(total_rev)}     Spend : {_inr(total_spend)}"
        f"     ROAS : {_roas(total_rev, total_spend)}"
    )
    print(f"{'='*W}")

    # ── 2. CAMPAIGN SUMMARY TABLE (ranked by signups) ─────────────────────────
    print(f"\n  {'#':>3}  {'Campaign':<65} {'Sgn':>6} {'D6':>5} {'D6%':>6} {'LTV':>8} {'Spend':>10} {'CAC':>9} {'ROAS':>7}  {'Note'}")
    print(f"  {'─'*3}  {'─'*65} {'─'*6} {'─'*5} {'─'*6} {'─'*8} {'─'*10} {'─'*9} {'─'*7}  {'─'*12}")

    sorted_camps = sorted(camp_rollup.items(), key=lambda x: x[1]["signups"], reverse=True)
    for i, (cname, cs) in enumerate(sorted_camps, 1):
        label = cname[:65]
        note  = "" if cname in named else "unresolved"
        cac   = _inr(cs["spend"] / cs["signups"]) if cs["spend"] and cs["signups"] else "—"
        print(
            f"  {i:>3}.  {label:<65} {cs['signups']:>6} {cs['d6']:>5} "
            f"{_d6pct(cs['d6'], cs['signups']):>6} {_inr(cs['rev'] / cs['signups'] if cs['signups'] else None):>8} "
            f"{_inr(cs['spend']):>10} {cac:>9} {_roas(cs['rev'], cs['spend']):>7}  {note}"
        )

    # ── 3. DETAILED DRILL-DOWN — named (synced) campaigns ─────────────────────
    AD_HDR = (
        f"    {'Ad name':<58} {'Sgn':>5} {'Trl':>4} {'D6':>4} {'D6%':>6} "
        f"{'LTV':>8} {'Spend':>9} {'CAC':>8} {'ROAS':>7}"
    )
    AD_SEP = (
        f"    {'─'*58} {'─'*5} {'─'*4} {'─'*4} {'─'*6} "
        f"{'─'*8} {'─'*9} {'─'*8} {'─'*7}"
    )

    print(f"\n\n{'='*W}")
    print(f"  DETAILED VIEW — {len(named)} synced campaigns")
    print(f"{'='*W}")

    # Sort named campaigns by signups desc
    for cname in sorted((c for c in named), key=lambda c: camp_rollup[c]["signups"], reverse=True):
        adsets = named[cname]
        cs = camp_rollup[cname]
        cac = _inr(cs["spend"] / cs["signups"]) if cs["spend"] and cs["signups"] else "—"

        print(f"\n{'━'*W}")
        print(f"  CAMPAIGN  {cname}")
        print(
            f"  Signups: {cs['signups']:,}  Trials: {cs['trials']:,}  D6: {cs['d6']:,} ({_d6pct(cs['d6'], cs['signups'])})  "
            f"LTV: {_inr(cs['rev'] / cs['signups'] if cs['signups'] else None)}  |  "
            f"Spend: {_inr(cs['spend'])}  CAC: {cac}  ROAS: {_roas(cs['rev'], cs['spend'])}"
        )

        # Sort adsets by signups desc
        for aname in sorted(adsets, key=lambda a: adset_stats(adsets[a])["signups"], reverse=True):
            ad_rows = adsets[aname]
            s = adset_stats(ad_rows)
            s_cac = _inr(s["spend"] / s["signups"]) if s["spend"] and s["signups"] else "—"

            print(f"\n  {'─'*(W-2)}")
            print(f"    ADSET  {aname}")
            print(
                f"    Signups: {s['signups']:,}  Trials: {s['trials']:,}  D6: {s['d6']:,} ({_d6pct(s['d6'], s['signups'])})  "
                f"LTV: {_inr(s['rev'] / s['signups'] if s['signups'] else None)}  |  "
                f"Spend: {_inr(s['spend'])}  CAC: {s_cac}  ROAS: {_roas(s['rev'], s['spend'])}"
            )
            print(AD_HDR)
            print(AD_SEP)

            for r in sorted(ad_rows, key=lambda x: x["signups"] or 0, reverse=True):
                name  = (r["ad_name"] or r["ad_id"])[:58]
                sgn   = str(int(r["signups"]))          if r["signups"]            else "—"
                trl   = str(int(r["d0_trials"]))        if r["d0_trials"]          else "—"
                d6c   = str(int(r["d6_conversions"]))   if r["d6_conversions"] is not None else "—"
                d6p   = f"{r['d6_pct']:.1f}%"           if r["d6_pct"] is not None  else "—"
                ltv   = _inr(r["avg_ltv_inr"])
                spend = _inr(r["spend_inr"])
                cac   = _inr(r["cac_inr"])
                roas  = f"{r['roas']:.2f}x"             if r["roas"] is not None   else "—"
                print(
                    f"    {name:<58} {sgn:>5} {trl:>4} {d6c:>4} {d6p:>6} "
                    f"{ltv:>8} {spend:>9} {cac:>8} {roas:>7}"
                )

    # ── 4. UNRESOLVED — compact rollup ────────────────────────────────────────
    if unknown:
        print(f"\n\n{'='*W}")
        print(f"  UNRESOLVED — {len(unknown)} campaigns not yet synced to DB (raw IDs)")
        print(f"{'='*W}")
        print(f"  {'Campaign ID':<30} {'Sgn':>6} {'D6':>5} {'D6%':>6} {'LTV':>8} {'Spend':>10}")
        print(f"  {'─'*30} {'─'*6} {'─'*5} {'─'*6} {'─'*8} {'─'*10}")
        for cname in sorted(unknown, key=lambda c: camp_rollup[c]["signups"], reverse=True):
            cs = camp_rollup[cname]
            print(
                f"  {cname[:30]:<30} {cs['signups']:>6} {cs['d6']:>5} "
                f"{_d6pct(cs['d6'], cs['signups']):>6} "
                f"{_inr(cs['rev'] / cs['signups'] if cs['signups'] else None):>8} "
                f"{_inr(cs['spend']):>10}"
            )
        unresolved_signups = sum(camp_rollup[c]["signups"] for c in unknown)
        print(f"\n  Subtotal unresolved: {unresolved_signups:,} signups  "
              f"({unresolved_signups * 100 / total_signups:.1f}% of total)")

    print(f"\n{'='*W}")
    print(f"  {len(rows)} distinct ads attributed  |  "
          f"{sum(1 for r in rows if not r['ad_in_db'])} unresolved ad IDs")


asyncio.run(main())
