"""
Score ads that went live (created_time) between 2026-04-10 and 2026-04-15.

Scoring dimensions (all percentile-ranked within the cohort):
  media_score   = 0.35*CTR_pct + 0.35*(1-CPM_pct) + 0.30*(1-CPC_pct)
  conv_score    = 0.50*D6_conv_pct + 0.30*ROAS_pct + 0.20*(1-CAC_pct)
  composite     = 0.50*media_score + 0.50*conv_score

Attribution is TRUE ad-level: attribution_events.meta_creative_id = ads.id (Singular maps
tracker_creative_id → Meta ad_id, not the creative asset ID).

Output is grouped by campaign so bad creatives are easy to spot.
"""

import asyncio
import logging
from collections import defaultdict
from datetime import date

from sqlalchemy import text

logging.basicConfig(level=logging.WARNING)

COHORT_SINCE = date(2026, 4, 10)   # ads created from
COHORT_UNTIL = date(2026, 4, 15)   # ads created to
MEDIA_SINCE  = date(2026, 4, 10)   # insights window (all available data from creation onward)
MEDIA_UNTIL  = date(2026, 4, 22)
ATTR_SINCE   = date(2026, 4, 10)   # attribution window
ATTR_UNTIL   = date(2026, 4, 22)


QUERY = text("""
WITH
-- 1. Ads created in the window
cohort AS (
    SELECT
        a.id          AS ad_id,
        a.name        AS ad_name,
        a.adset_id,
        a.campaign_id,
        a.effective_status,
        a.created_time::date AS created_date,
        c.name        AS campaign_name,
        c.daily_budget,
        c.lifetime_budget,
        s.name        AS adset_name
    FROM ads a
    JOIN campaigns c ON c.id = a.campaign_id
    JOIN adsets    s ON s.id = a.adset_id
    WHERE a.created_time::date BETWEEN :cohort_since AND :cohort_until
      AND a.effective_status = 'ACTIVE'
      AND c.name NOT ILIKE '%Retar%'
),

-- 2. Aggregate insights over full available window (media performance since creation)
media AS (
    SELECT
        ad_id,
        SUM(spend)       AS spend,
        SUM(impressions) AS impressions,
        SUM(clicks)      AS clicks,
        CASE WHEN SUM(impressions) > 0
             THEN SUM(clicks)::numeric * 100 / SUM(impressions) END AS ctr,
        CASE WHEN SUM(impressions) > 0
             THEN SUM(spend)::numeric * 1000 / SUM(impressions) END AS cpm,
        CASE WHEN SUM(clicks) > 0
             THEN SUM(spend)::numeric / SUM(clicks) END AS cpc,
        COUNT(DISTINCT date) AS days_active
    FROM insights_daily
    WHERE date BETWEEN :media_since AND :media_until
      AND attribution_window = '7d_click'
    GROUP BY ad_id
),

-- 3. True ad-level attribution from attribution_events
--    meta_creative_id in attribution_events = ads.id (Singular tracker_creative_id → Meta ad_id)
attr AS (
    SELECT
        ae.meta_creative_id                                  AS ad_id,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                            THEN ae.user_id END)             AS signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                            THEN ae.user_id END)             AS d6_conversions,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                            THEN ae.user_id END)             AS d0_trials,
        ROUND(
            COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                                 AND ae.days_since_signup <= 6
                                THEN ae.user_id END)::numeric * 100
            / NULLIF(COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                                         THEN ae.user_id END), 0)
        , 2)                                                 AS d6_conv_pct,
        ROUND(
            SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                     THEN ae.revenue_inr ELSE 0 END)::numeric
            / NULLIF(SUM(CASE WHEN ae.event_name = 'signup' THEN 1 ELSE 0 END), 0)
        , 2)                                                 AS avg_ltv_inr
    FROM attribution_events ae
    WHERE ae.install_date BETWEEN :attr_since AND :attr_until
      AND ae.is_reattributed = FALSE
      AND ae.network = 'Facebook'
    GROUP BY ae.meta_creative_id
),

-- 4. Join everything
joined AS (
    SELECT
        co.*,
        m.spend, m.impressions, m.clicks,
        m.ctr, m.cpm, m.cpc,
        m.days_active,
        at.signups, at.d6_conversions, at.d0_trials, at.d6_conv_pct, at.avg_ltv_inr,
        CASE WHEN m.spend > 0 AND at.signups > 0
             THEN ROUND(m.spend::numeric / at.signups, 0) END AS cac_inr,
        CASE WHEN m.spend > 0 AND at.avg_ltv_inr IS NOT NULL
             THEN ROUND((at.avg_ltv_inr * at.signups) / m.spend, 3) END AS roas
    FROM cohort co
    LEFT JOIN media  m  ON m.ad_id = co.ad_id
    LEFT JOIN attr   at ON at.ad_id = co.ad_id
),

-- 5. Percentile ranks within cohort (0=worst, 1=best)
ranked AS (
    SELECT *,
        PERCENT_RANK() OVER (ORDER BY ctr         ASC NULLS LAST) AS ctr_pct,
        PERCENT_RANK() OVER (ORDER BY cpm         DESC NULLS LAST) AS cpm_pct,
        PERCENT_RANK() OVER (ORDER BY cpc         DESC NULLS LAST) AS cpc_pct,
        PERCENT_RANK() OVER (ORDER BY d6_conv_pct ASC NULLS LAST) AS d6_pct,
        PERCENT_RANK() OVER (ORDER BY roas        ASC NULLS LAST) AS roas_pct,
        PERCENT_RANK() OVER (ORDER BY cac_inr     DESC NULLS LAST) AS cac_pct
    FROM joined
),

-- 6. Composite score
scored AS (
    SELECT *,
        CASE WHEN ctr IS NOT NULL THEN
            ROUND((0.35*ctr_pct + 0.35*cpm_pct + 0.30*cpc_pct)::numeric * 100, 1)
        END AS media_score,
        CASE WHEN d6_conv_pct IS NOT NULL OR roas IS NOT NULL THEN
            ROUND((
                0.50*COALESCE(d6_pct,  0.5) +
                0.30*COALESCE(roas_pct, 0.5) +
                0.20*COALESCE(cac_pct,  0.5)
            )::numeric * 100, 1)
        END AS conv_score
    FROM ranked
)

SELECT
    ad_id,
    ad_name,
    adset_name,
    campaign_id,
    campaign_name,
    daily_budget,
    lifetime_budget,
    effective_status,
    created_date,
    days_active,
    ROUND(spend::numeric, 0)        AS spend_inr,
    impressions,
    ROUND(ctr::numeric, 3)          AS ctr_pct,
    ROUND(cpm::numeric, 1)          AS cpm,
    ROUND(cpc::numeric, 1)          AS cpc,
    signups,
    d0_trials,
    d6_conversions,
    d6_conv_pct,
    ROUND(avg_ltv_inr::numeric, 0)  AS avg_ltv_inr,
    roas,
    cac_inr,
    media_score,
    conv_score,
    ROUND(
        (0.50 * COALESCE(media_score, conv_score, 0) +
         0.50 * COALESCE(conv_score,  media_score, 0))::numeric
    , 1) AS composite_score
FROM scored
ORDER BY campaign_name, composite_score DESC NULLS LAST, spend_inr DESC NULLS LAST
""")


def _fmt_budget(daily_budget, lifetime_budget) -> str:
    """Return a human-readable budget string."""
    if daily_budget:
        try:
            amt = int(float(daily_budget))
            return f"daily ₹{amt:,}"
        except (TypeError, ValueError):
            pass
    if lifetime_budget:
        try:
            amt = int(float(lifetime_budget))
            return f"lifetime ₹{amt:,}"
        except (TypeError, ValueError):
            pass
    return "budget N/A"


def _tag(score) -> str:
    if score is None:
        return "NO DATA"
    s = float(score)
    if s >= 60:
        return "KEEP   "
    if s >= 30:
        return "REVIEW "
    return "REMOVE "


async def main():
    from services.shared.db import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        result = await session.execute(QUERY, {
            "cohort_since": COHORT_SINCE, "cohort_until": COHORT_UNTIL,
            "media_since": MEDIA_SINCE,   "media_until":  MEDIA_UNTIL,
            "attr_since":  ATTR_SINCE,    "attr_until":   ATTR_UNTIL,
        })
        rows = result.mappings().all()

    if not rows:
        print("No ads found created between Apr 10-15.")
        return

    # Group rows by campaign (preserving ORDER BY campaign_name from SQL)
    campaigns: dict[str, list] = defaultdict(list)
    camp_meta: dict[str, dict] = {}
    for r in rows:
        cname = r["campaign_name"] or r["campaign_id"]
        campaigns[cname].append(r)
        if cname not in camp_meta:
            camp_meta[cname] = {
                "daily_budget":    r["daily_budget"],
                "lifetime_budget": r["lifetime_budget"],
            }

    # ── Header ────────────────────────────────────────────────────────────────
    W = 140
    print(f"\n{'='*W}")
    print(f"AD CREATIVE SCORECARD — cohort created {COHORT_SINCE} → {COHORT_UNTIL}  |  "
          f"media window {MEDIA_SINCE}→{MEDIA_UNTIL}  |  {len(rows)} ads  |  {len(campaigns)} campaigns")
    print(f"Score ≥60 → KEEP   |  30–59 → REVIEW   |  <30 / no data → REMOVE")
    print(f"{'='*W}")

    COL_HDR = (
        f"  {'Score':>6} {'Media':>6} {'Conv':>6}  {'Tag':<10}|  "
        f"{'Spend':>9} {'CTR':>6} {'CPM':>7} {'CPC':>6}  |  "
        f"{'Sgn':>5} {'Trl':>4} {'D6':>4} {'D6%':>6} {'LTV':>8} {'ROAS':>6} {'CAC':>9}  |  Ad name"
    )
    COL_SEP = (
        f"  {'─'*6} {'─'*6} {'─'*6}  {'─'*10}|  "
        f"{'─'*9} {'─'*6} {'─'*7} {'─'*6}  |  "
        f"{'─'*5} {'─'*4} {'─'*4} {'─'*6} {'─'*8} {'─'*6} {'─'*9}  |  {'─'*55}"
    )

    total_with_media = sum(1 for r in rows if r["media_score"] is not None)
    total_with_conv  = sum(1 for r in rows if r["conv_score"] is not None)

    for cname, ads in campaigns.items():
        # Campaign-level aggregates
        camp_spend    = sum(float(r["spend_inr"])  for r in ads if r["spend_inr"]  is not None)
        camp_signups  = sum(int(r["signups"])       for r in ads if r["signups"]    is not None)
        camp_d6c      = sum(int(r["d6_conversions"]) for r in ads if r["d6_conversions"] is not None)
        camp_d6pct    = (camp_d6c * 100 / camp_signups) if camp_signups else None
        budget_str    = _fmt_budget(
            camp_meta[cname]["daily_budget"],
            camp_meta[cname]["lifetime_budget"],
        )

        print(f"\n{'─'*W}")
        print(
            f"  CAMPAIGN: {cname}"
        )
        print(
            f"  Budget: {budget_str}  |  Total spend: ₹{int(camp_spend):,}  |  "
            f"Signups: {camp_signups}  |  D6 conv: {camp_d6c}"
            + (f"  ({camp_d6pct:.1f}%)" if camp_d6pct is not None else "")
            + f"  |  Ads: {len(ads)}"
        )
        print(f"{'─'*W}")
        print(COL_HDR)
        print(COL_SEP)

        for r in ads:
            name  = (r["ad_name"] or r["ad_id"])[:55]
            spend = f"₹{int(r['spend_inr']):,}"    if r["spend_inr"]    is not None else "—"
            ctr   = f"{r['ctr_pct']:.2f}%"          if r["ctr_pct"]     is not None else "—"
            cpm   = f"₹{r['cpm']:.0f}"              if r["cpm"]         is not None else "—"
            cpc   = f"₹{r['cpc']:.0f}"              if r["cpc"]         is not None else "—"
            sgn   = str(int(r["signups"]))           if r["signups"]     is not None else "—"
            trl   = str(int(r["d0_trials"]))         if r["d0_trials"]   is not None else "—"
            d6c   = str(int(r["d6_conversions"]))    if r["d6_conversions"] is not None else "—"
            d6p   = f"{r['d6_conv_pct']:.1f}%"      if r["d6_conv_pct"] is not None else "—"
            ltv   = f"₹{int(r['avg_ltv_inr']):,}"   if r["avg_ltv_inr"] is not None else "—"
            roas  = f"{r['roas']:.2f}x"             if r["roas"]        is not None else "—"
            cac   = f"₹{int(r['cac_inr']):,}"       if r["cac_inr"]     is not None else "—"
            ms    = f"{r['media_score']:.0f}"        if r["media_score"] is not None else "—"
            cs    = f"{r['conv_score']:.0f}"         if r["conv_score"]  is not None else "—"
            score = f"{r['composite_score']:.0f}"    if r["composite_score"] is not None else "—"
            tag   = _tag(r["composite_score"])

            print(
                f"  {score:>6} {ms:>6} {cs:>6}  {tag:<10}|  "
                f"{spend:>9} {ctr:>6} {cpm:>7} {cpc:>6}  |  "
                f"{sgn:>5} {trl:>4} {d6c:>4} {d6p:>6} {ltv:>8} {roas:>6} {cac:>9}  |  {name}"
            )

    print(f"\n{'='*W}")
    print(
        f"Coverage: {total_with_media}/{len(rows)} ads have media data  |  "
        f"{total_with_conv}/{len(rows)} have attribution data"
    )
    print(
        f"Scoring: KEEP ≥60  |  REVIEW 30–59  |  REMOVE <30"
    )


asyncio.run(main())
