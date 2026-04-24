"""
/conversions/* endpoints — attribution event inspection + BQ cost log.

All metrics are served from pre-computed materialized views:
  mv_campaign_conversions  (campaign × install_date)
  mv_adset_conversions     (adset × install_date)

Individual event rows are served from attribution_events for drill-down.
"""

import logging
from datetime import date as _date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from services.api.deps import decode_cursor, encode_cursor, get_db
from services.api.schemas import Paginated

log = logging.getLogger(__name__)
router = APIRouter(prefix="/conversions", tags=["conversions"])

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000

# Columns returned by both MVs (superset)
_MV_COLS = """
    signups,
    d0_conversions, d0_trials,
    d6_conversions, d6_trials,
    total_revenue_inr,
    d0_conversion_pct, d0_trial_pct,
    d6_conversion_pct, d6_trial_pct,
    avg_ltv_inr,
    spend, impressions, clicks,
    cac_inr, attributed_roas
"""


# ---------------------------------------------------------------------------
# /conversions/campaign
# ---------------------------------------------------------------------------

@router.get("/campaign", summary="D0/D6 metrics aggregated by campaign × install_date")
async def get_campaign_conversions(
    campaign_id: str | None = Query(None),
    since: str = Query(..., description="YYYY-MM-DD (install_date lower bound)"),
    until: str = Query(..., description="YYYY-MM-DD (install_date upper bound)"),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Returns daily rows from mv_campaign_conversions with:
      signups, d0_conversions, d0_conversion_pct, d0_trials, d0_trial_pct,
      d6_conversions, d6_conversion_pct, d6_trials, d6_trial_pct,
      total_revenue_inr, avg_ltv_inr, spend, cac_inr, attributed_roas
    """
    offset = decode_cursor(cursor)

    where = "WHERE install_date >= :since AND install_date <= :until"
    params: dict[str, Any] = {"since": _date.fromisoformat(since), "until": _date.fromisoformat(until)}

    if campaign_id:
        where += " AND campaign_id = :campaign_id"
        params["campaign_id"] = campaign_id

    total_row = await db.execute(
        text(f"SELECT COUNT(*) FROM mv_campaign_conversions {where}"), params
    )
    total = total_row.scalar_one()

    rows = await db.execute(
        text(f"""
            SELECT campaign_id, install_date, {_MV_COLS}
            FROM mv_campaign_conversions
            {where}
            ORDER BY install_date DESC, campaign_id
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset},
    )

    data = [dict(r._mapping) for r in rows]
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return {"data": data, "cursor": next_cursor, "total": total}


# ---------------------------------------------------------------------------
# /conversions/adset
# ---------------------------------------------------------------------------

@router.get("/adset", summary="D0/D6 metrics aggregated by adset × install_date")
async def get_adset_conversions(
    adset_id: str | None = Query(None),
    campaign_id: str | None = Query(None),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    offset = decode_cursor(cursor)

    where = "WHERE install_date >= :since AND install_date <= :until"
    params: dict[str, Any] = {"since": _date.fromisoformat(since), "until": _date.fromisoformat(until)}

    if adset_id:
        where += " AND adset_id = :adset_id"
        params["adset_id"] = adset_id
    if campaign_id:
        where += " AND campaign_id = :campaign_id"
        params["campaign_id"] = campaign_id

    total_row = await db.execute(
        text(f"SELECT COUNT(*) FROM mv_adset_conversions {where}"), params
    )
    total = total_row.scalar_one()

    rows = await db.execute(
        text(f"""
            SELECT campaign_id, adset_id, install_date, {_MV_COLS}
            FROM mv_adset_conversions
            {where}
            ORDER BY install_date DESC, adset_id
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset},
    )

    data = [dict(r._mapping) for r in rows]
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return {"data": data, "cursor": next_cursor, "total": total}


# ---------------------------------------------------------------------------
# /conversions/events  — raw event drill-down
# ---------------------------------------------------------------------------

@router.get("/events", summary="Raw attribution_events rows")
async def list_events(
    event_name: str | None = Query(None, description="signup|trial|conversion|repeat_conversion"),
    meta_campaign_id: str | None = Query(None),
    meta_adset_id: str | None = Query(None),
    network: str | None = Query(None),
    since: str | None = Query(None, description="YYYY-MM-DD on install_date"),
    until: str | None = Query(None, description="YYYY-MM-DD on install_date"),
    include_reattributed: bool = Query(False),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    offset = decode_cursor(cursor)

    conditions = []
    params: dict[str, Any] = {}

    if event_name:
        conditions.append("event_name = :event_name")
        params["event_name"] = event_name
    if meta_campaign_id:
        conditions.append("meta_campaign_id = :meta_campaign_id")
        params["meta_campaign_id"] = meta_campaign_id
    if meta_adset_id:
        conditions.append("meta_adset_id = :meta_adset_id")
        params["meta_adset_id"] = meta_adset_id
    if network:
        conditions.append("network = :network")
        params["network"] = network
    if since:
        conditions.append("install_date >= :since")
        params["since"] = _date.fromisoformat(since)
    if until:
        conditions.append("install_date <= :until")
        params["until"] = _date.fromisoformat(until)
    if not include_reattributed:
        conditions.append("is_reattributed = FALSE")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    total_row = await db.execute(
        text(f"SELECT COUNT(*) FROM attribution_events {where}"), params
    )
    total = total_row.scalar_one()

    rows = await db.execute(
        text(f"""
            SELECT id, user_id, event_name, event_time, install_date,
                   days_since_signup, network, publisher_site,
                   meta_campaign_id, meta_adset_id, meta_creative_id,
                   campaign_name, adset_name, creative_name,
                   revenue_inr, plan_id, is_trial, is_first_payment,
                   is_reattributed, platform, priority, synced_at
            FROM attribution_events
            {where}
            ORDER BY install_date DESC, event_time DESC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset},
    )

    data = [dict(r._mapping) for r in rows]
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return {"data": data, "cursor": next_cursor, "total": total}


# ---------------------------------------------------------------------------
# /conversions/summary  — aggregate totals over a date range
# ---------------------------------------------------------------------------

@router.get("/summary", summary="Aggregate conversion metrics for a campaign or adset")
async def get_summary(
    level: str = Query(..., description="campaign | adset"),
    object_id: str = Query(...),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Rolls up D0/D6 metrics across install_dates for a single object.
    Percentages are recomputed from rolled-up totals.
    """
    if level == "campaign":
        mv, id_col = "mv_campaign_conversions", "campaign_id"
    elif level == "adset":
        mv, id_col = "mv_adset_conversions", "adset_id"
    else:
        raise HTTPException(status_code=422, detail="level must be 'campaign' or 'adset'")

    row = await db.execute(
        text(f"""
            SELECT
                SUM(signups)          AS signups,
                SUM(d0_conversions)   AS d0_conversions,
                SUM(d0_trials)        AS d0_trials,
                SUM(d6_conversions)   AS d6_conversions,
                SUM(d6_trials)        AS d6_trials,
                SUM(total_revenue_inr) AS total_revenue_inr,
                SUM(spend)            AS spend,
                SUM(impressions)      AS impressions,
                SUM(clicks)           AS clicks,
                ROUND(SUM(d0_conversions) * 100.0 / NULLIF(SUM(signups), 0), 2) AS d0_conversion_pct,
                ROUND(SUM(d0_trials)      * 100.0 / NULLIF(SUM(signups), 0), 2) AS d0_trial_pct,
                ROUND(SUM(d6_conversions) * 100.0 / NULLIF(SUM(signups), 0), 2) AS d6_conversion_pct,
                ROUND(SUM(d6_trials)      * 100.0 / NULLIF(SUM(signups), 0), 2) AS d6_trial_pct,
                ROUND(SUM(total_revenue_inr) / NULLIF(SUM(signups), 0), 2)       AS avg_ltv_inr,
                ROUND(SUM(spend) / NULLIF(SUM(signups), 0), 2)                   AS cac_inr,
                ROUND(SUM(total_revenue_inr) / NULLIF(SUM(spend), 0), 4)         AS attributed_roas
            FROM {mv}
            WHERE {id_col} = :object_id
              AND install_date >= :since
              AND install_date <= :until
        """),
        {"object_id": object_id, "since": _date.fromisoformat(since), "until": _date.fromisoformat(until)},
    )
    result = dict(row.mappings().one())
    return {"level": level, "object_id": object_id, "since": since, "until": until, **result}


# ---------------------------------------------------------------------------
# /conversions/platform-roas  — M0 ROAS split by Android / iOS
# ---------------------------------------------------------------------------

@router.get("/platform-roas", summary="M0 ROAS split by platform (Android/iOS)")
async def get_platform_roas(
    since: str = Query(..., description="YYYY-MM-DD (install_date lower bound)"),
    until: str = Query(..., description="YYYY-MM-DD (install_date upper bound)"),
    campaign_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Groups attribution_events by platform (Android / iOS) and computes:
      - signups, d0/d6/m0 conversions, m0_revenue_inr, total_revenue_inr
      - allocated_spend  (Singular MMP cost per OS for the same period)
      - m0_roas          = m0_revenue_inr / allocated_spend

    'm0' = revenue from that period's signups paid within the same calendar period
           (event_time within [since, until], same bounds as install_date).
    """
    params: dict[str, Any] = {
        "since": _date.fromisoformat(since),
        "until": _date.fromisoformat(until),
    }
    campaign_filter = ""
    if campaign_id:
        campaign_filter = "AND meta_campaign_id = :campaign_id"
        params["campaign_id"] = campaign_id

    rows = await db.execute(
        text(f"""
            WITH conv AS (
                SELECT
                    -- Platform: use user_devices.os as PRIMARY (actual device), fall back to Singular.
                    -- Singular sets platform='Android' for all Facebook users regardless of
                    -- actual device, so user_devices.os gives the true iOS/Android split.
                    COALESCE(
                        CASE
                            WHEN LOWER(ud.os) LIKE 'ios%%' OR LOWER(ud.os) = 'ipados' THEN 'iOS'
                            WHEN LOWER(ud.os) LIKE 'android%%' THEN 'Android'
                        END,
                        ae.platform
                    )                                                                        AS platform,
                    COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                                        THEN ae.user_id END)                                 AS signups,
                    COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                                         AND ae.days_since_signup = 0
                                        THEN ae.user_id END)                                 AS d0_conversions,
                    COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                                         AND ae.days_since_signup <= 6
                                        THEN ae.user_id END)                                 AS d6_conversions,
                    COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                                         AND DATE(ae.event_time) >= :since
                                         AND DATE(ae.event_time) <= :until
                                        THEN ae.user_id END)                                 AS m0_conversions,
                    SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                              AND DATE(ae.event_time) >= :since
                              AND DATE(ae.event_time) <= :until
                             THEN ae.revenue_inr ELSE 0 END)                                 AS m0_revenue_inr,
                    SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             THEN ae.revenue_inr ELSE 0 END)                                 AS total_revenue_inr
                FROM attribution_events ae
                LEFT JOIN user_devices ud ON ud.user_id = ae.user_id
                WHERE ae.network = 'Facebook'
                  AND ae.is_reattributed = FALSE
                  AND ae.install_date >= :since
                  AND ae.install_date <= :until
                  {campaign_filter}
                GROUP BY 1
            ),
            platform_spend AS (
                -- Direct per-OS spend from Singular MMP — no proportional allocation needed.
                SELECT
                    os AS platform,
                    COALESCE(SUM(cost), 0) AS spend
                FROM singular_campaign_metrics
                WHERE source = 'Facebook'
                  AND os IN ('Android', 'iOS')
                  AND date >= :since
                  AND date <= :until
                GROUP BY os
            )
            SELECT
                c.platform,
                c.signups,
                c.d0_conversions,
                c.d6_conversions,
                c.m0_conversions,
                ROUND(c.m0_revenue_inr::numeric, 2)                                          AS m0_revenue_inr,
                ROUND(c.total_revenue_inr::numeric, 2)                                       AS total_revenue_inr,
                COALESCE(ROUND(ps.spend::numeric, 2), 0)                                     AS allocated_spend,
                ROUND(
                    c.m0_revenue_inr / NULLIF(ps.spend, 0),
                    4
                )                                                                            AS m0_roas,
                ROUND(
                    c.m0_conversions * 100.0 / NULLIF(c.signups, 0), 2
                )                                                                            AS m0_conversion_pct,
                ROUND(
                    c.total_revenue_inr / NULLIF(ps.spend, 0),
                    4
                )                                                                            AS total_roas
            FROM conv c
            LEFT JOIN platform_spend ps ON ps.platform = c.platform
            ORDER BY signups DESC
        """),
        params,
    )

    data = [dict(r._mapping) for r in rows]
    return {
        "since": since,
        "until": until,
        "campaign_id": campaign_id,
        "note": (
            "M0 = revenue from period signups paid within [since, until]. "
            "Platform via user_devices.os (PRIMARY) falling back to Singular. "
            "Spend from singular_campaign_metrics (Singular MMP) per OS."
        ),
        "data": data,
    }


# ---------------------------------------------------------------------------
# /conversions/bq-costs
# ---------------------------------------------------------------------------

@router.get("/bq-costs", summary="BQ query cost log")
async def get_bq_costs(
    since: str | None = Query(None, description="YYYY-MM-DD"),
    until: str | None = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(50, le=500),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    offset = decode_cursor(cursor)
    conditions = []
    params: dict[str, Any] = {}
    if since:
        conditions.append("DATE(run_at) >= :since")
        params["since"] = _date.fromisoformat(since)
    if until:
        conditions.append("DATE(run_at) <= :until")
        params["until"] = _date.fromisoformat(until)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    total_row = await db.execute(
        text(f"SELECT COUNT(*) FROM bq_query_costs {where}"), params
    )
    total = total_row.scalar_one()

    rows = await db.execute(
        text(f"""
            SELECT id, query_label, bytes_processed, rows_returned, run_at, duration_ms
            FROM bq_query_costs {where}
            ORDER BY run_at DESC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset},
    )
    data = [dict(r._mapping) for r in rows]
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return {"data": data, "cursor": next_cursor, "total": total}
