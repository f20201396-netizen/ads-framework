"""
/insights/* endpoints.

timeseries  — daily metric series for any object at any level
breakdown   — breakdown-sliced metrics (age, gender, country, …)
compare     — side-by-side two date ranges with delta
top         — ranked entities by a single metric over a period
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from services.api.deps import decode_cursor, encode_cursor, get_db
from services.api.schemas import (
    InsightsBreakdownOut,
    InsightsCompareOut,
    InsightsTimeseriesOut,
    InsightsTopOut,
    RangeResult,
)
from services.shared.models import (
    InsightsAccountDaily,
    InsightsAdsetDaily,
    InsightsCampaignDaily,
    InsightsDaily,
    InsightsDailyBreakdown,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/insights", tags=["insights"])

# ---------------------------------------------------------------------------
# Level → (model, id_column_attr)
# ---------------------------------------------------------------------------

_LEVEL_MAP: dict[str, tuple[Any, str]] = {
    "ad":       (InsightsDaily,         "ad_id"),
    "adset":    (InsightsAdsetDaily,    "adset_id"),
    "campaign": (InsightsCampaignDaily, "campaign_id"),
    "account":  (InsightsAccountDaily,  "account_id"),
}

# All scalar metric column names (used for validation + safe column access)
_SCALAR_METRICS = {
    "impressions", "reach", "frequency", "spend", "cpm", "cpc", "cpp", "ctr",
    "clicks", "unique_clicks", "inline_link_clicks", "inline_link_click_ctr",
    "unique_inline_link_clicks", "unique_inline_link_click_ctr",
    "outbound_clicks", "unique_outbound_clicks", "outbound_clicks_ctr",
    "cost_per_inline_link_click", "cost_per_outbound_click",
    "cost_per_unique_outbound_click", "social_spend",
    "canvas_avg_view_time", "canvas_avg_view_percent",
    "instant_experience_clicks_to_open", "instant_experience_clicks_to_start",
    "full_view_impressions", "full_view_reach",
    "estimated_ad_recall_rate", "estimated_ad_recallers",
    "cost_per_estimated_ad_recallers",
}

# JSONB metrics available on InsightsDaily
_JSONB_METRICS = {
    "actions", "action_values", "conversions", "purchase_roas",
    "video_play_actions", "cost_per_action_type",
}


def _parse_metrics(raw: str | None) -> list[str]:
    """Validate and return requested metrics list."""
    if not raw:
        return ["impressions", "reach", "spend", "clicks", "ctr", "cpm"]
    metrics = [m.strip() for m in raw.split(",") if m.strip()]
    valid = _SCALAR_METRICS | _JSONB_METRICS
    bad = [m for m in metrics if m not in valid]
    if bad:
        raise HTTPException(status_code=422, detail=f"Unknown metrics: {bad}")
    return metrics


def _row_to_dict(row, id_col: str, metrics: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "date": str(getattr(row, "date")),
        id_col: getattr(row, id_col, None),
    }
    for m in metrics:
        out[m] = getattr(row, m, None)
    return out


def _require_level(level: str):
    if level not in _LEVEL_MAP:
        raise HTTPException(status_code=422, detail=f"level must be one of {list(_LEVEL_MAP)}")
    return _LEVEL_MAP[level]


# ---------------------------------------------------------------------------
# GET /insights/timeseries
# ---------------------------------------------------------------------------

@router.get("/timeseries", response_model=InsightsTimeseriesOut)
async def get_timeseries(
    level: str = Query(..., description="ad | adset | campaign | account"),
    object_id: str = Query(...),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
    metrics: str | None = Query(None, description="Comma-separated metric names"),
    attribution_window: str = Query("7d_click"),
    limit: int = Query(90, le=366),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    model, id_col = _require_level(level)
    metric_list = _parse_metrics(metrics)
    offset = decode_cursor(cursor)

    id_attr = getattr(model, id_col)
    stmt = (
        select(model)
        .where(
            id_attr == object_id,
            model.date >= since,
            model.date <= until,
            model.attribution_window == attribution_window,
        )
        .order_by(model.date)
        .offset(offset)
        .limit(limit)
    )
    rows = (await db.execute(stmt)).scalars().all()
    next_cursor = encode_cursor(offset + limit) if len(rows) == limit else None
    return InsightsTimeseriesOut(
        data=[_row_to_dict(r, id_col, metric_list) for r in rows],
        cursor=next_cursor,
    )


# ---------------------------------------------------------------------------
# GET /insights/breakdown
# ---------------------------------------------------------------------------

@router.get("/breakdown", response_model=InsightsBreakdownOut)
async def get_breakdown(
    level: str = Query(...),
    object_id: str = Query(...),
    breakdown: str = Query(..., description="e.g. age_gender, country"),
    since: str = Query(...),
    until: str = Query(...),
    metrics: str | None = Query(None),
    attribution_window: str = Query("7d_click"),
    limit: int = Query(200, le=1000),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    metric_list = _parse_metrics(metrics)
    offset = decode_cursor(cursor)

    # breakdown table is always at ad level; filter by parent if level != ad
    _, id_col = _require_level(level)
    filter_col = getattr(InsightsDailyBreakdown, id_col, None)
    if filter_col is None:
        raise HTTPException(status_code=422, detail=f"Breakdown not supported at level={level}")

    # Normalise breakdown name: API uses commas, UI may send underscores
    breakdown_type = breakdown.replace("_", ",") if "," not in breakdown and "_" in breakdown else breakdown

    stmt = (
        select(InsightsDailyBreakdown)
        .where(
            filter_col == object_id,
            InsightsDailyBreakdown.date >= since,
            InsightsDailyBreakdown.date <= until,
            InsightsDailyBreakdown.breakdown_type == breakdown_type,
            InsightsDailyBreakdown.attribution_window == attribution_window,
        )
        .order_by(InsightsDailyBreakdown.date)
        .offset(offset)
        .limit(limit)
    )
    rows = (await db.execute(stmt)).scalars().all()
    next_cursor = encode_cursor(offset + limit) if len(rows) == limit else None

    data = []
    for r in rows:
        row_dict: dict[str, Any] = {
            "date": str(r.date),
            id_col: getattr(r, id_col, None),
            "breakdown_type": r.breakdown_type,
            "breakdown_key": r.breakdown_key,
        }
        for m in metric_list:
            row_dict[m] = getattr(r, m, None)
        data.append(row_dict)

    return InsightsBreakdownOut(data=data, cursor=next_cursor)


# ---------------------------------------------------------------------------
# GET /insights/compare
# ---------------------------------------------------------------------------

@router.get("/compare", response_model=InsightsCompareOut)
async def compare_ranges(
    level: str = Query(...),
    object_id: str = Query(...),
    range_a_since: str = Query(...),
    range_a_until: str = Query(...),
    range_b_since: str = Query(...),
    range_b_until: str = Query(...),
    metrics: str | None = Query(None),
    attribution_window: str = Query("7d_click"),
    db: AsyncSession = Depends(get_db),
):
    model, id_col = _require_level(level)
    metric_list = _parse_metrics(metrics)
    id_attr = getattr(model, id_col)

    async def _fetch(since: str, until: str) -> list:
        stmt = (
            select(model)
            .where(
                id_attr == object_id,
                model.date >= since,
                model.date <= until,
                model.attribution_window == attribution_window,
            )
            .order_by(model.date)
        )
        return (await db.execute(stmt)).scalars().all()

    rows_a, rows_b = await _fetch(range_a_since, range_a_until), await _fetch(range_b_since, range_b_until)

    def _to_result(rows) -> RangeResult:
        data = [_row_to_dict(r, id_col, metric_list) for r in rows]
        totals: dict[str, float | None] = {}
        for m in metric_list:
            vals = [r[m] for r in data if isinstance(r.get(m), (int, float))]
            totals[m] = round(sum(vals), 6) if vals else None
        return RangeResult(data=data, totals=totals)

    result_a = _to_result(rows_a)
    result_b = _to_result(rows_b)

    delta: dict[str, dict[str, float | None]] = {}
    for m in metric_list:
        a, b = result_a.totals.get(m), result_b.totals.get(m)
        if a is not None and b is not None and isinstance(a, (int, float)) and isinstance(b, (int, float)):
            abs_d = round(b - a, 6)
            pct_d = round((b - a) / a * 100, 2) if a != 0 else None
            delta[m] = {"abs": abs_d, "pct": pct_d}
        else:
            delta[m] = {"abs": None, "pct": None}

    return InsightsCompareOut(range_a=result_a, range_b=result_b, delta=delta)


# ---------------------------------------------------------------------------
# GET /insights/top
# ---------------------------------------------------------------------------

@router.get("/top", response_model=InsightsTopOut)
async def get_top(
    level: str = Query(...),
    parent_id: str = Query(..., description="Account/campaign/adset ID to scope results"),
    metric: str = Query("spend"),
    since: str = Query(...),
    until: str = Query(...),
    attribution_window: str = Query("7d_click"),
    limit: int = Query(20, le=100),
    db: AsyncSession = Depends(get_db),
):
    model, id_col = _require_level(level)

    if metric not in _SCALAR_METRICS:
        raise HTTPException(status_code=422, detail=f"metric must be a scalar; got {metric!r}")

    metric_col = getattr(model, metric, None)
    if metric_col is None:
        raise HTTPException(status_code=422, detail=f"metric {metric!r} not on level={level}")

    # Determine the parent filter column
    # For ad-level, parent is account_id; for adset-level, parent is campaign_id; etc.
    _parent_map: dict[str, str] = {
        "ad": "account_id",
        "adset": "account_id",
        "campaign": "account_id",
        "account": "account_id",
    }
    parent_col_name = _parent_map.get(level, "account_id")
    parent_col = getattr(model, parent_col_name, None)
    id_attr = getattr(model, id_col)

    # Aggregate over the date range
    stmt = (
        select(id_attr, func.sum(metric_col).label(metric))
        .where(
            parent_col == parent_id,
            model.date >= since,
            model.date <= until,
            model.attribution_window == attribution_window,
        )
        .group_by(id_attr)
        .order_by(func.sum(metric_col).desc())
        .limit(limit)
    )
    result = (await db.execute(stmt)).all()
    data = [{id_col: row[0], metric: row[1]} for row in result]
    return InsightsTopOut(data=data)
