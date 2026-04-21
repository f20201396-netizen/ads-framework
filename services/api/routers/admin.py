"""
/admin/* endpoints — sync triggers, backfill, observability.

All routes require the X-Admin-Key header.
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.api.deps import decode_cursor, encode_cursor, get_db, require_admin
from services.api.schemas import (
    ApiRateLimitOut,
    BackfillTriggerOut,
    Paginated,
    SyncRunOut,
    SyncTriggerOut,
)
from services.shared.models import ApiRateLimit, SyncRun

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500

# Map job name → callable import path (imported lazily to avoid circular deps)
_JOB_MAP: dict[str, str] = {
    "sync_structure":              "services.worker.jobs.sync_structure:sync_account_structure",
    "sync_insights_daily":         "services.worker.jobs.sync_insights:sync_insights_daily",
    "sync_higher_levels":          "services.worker.jobs.sync_higher_levels:sync_insights_higher_levels",
    "sync_breakdowns":             "services.worker.jobs.sync_breakdowns:sync_insights_breakdowns",
    "sync_aux":                    "services.worker.jobs.sync_aux:sync_audiences_pixels_catalogs",
    "sync_pixel_stats":            "services.worker.jobs.sync_aux:sync_pixel_stats",
    "sync_attribution_signups":    "services.worker.jobs.sync_attribution:sync_attribution_signups",
    "sync_attribution_conversions":"services.worker.jobs.sync_attribution:sync_attribution_conversions",
    "refresh_conversion_mv":       "services.worker.jobs.sync_attribution:refresh_conversion_mv",
}


def _load_job(job_name: str):
    """Dynamically import and return the job coroutine function."""
    if job_name not in _JOB_MAP:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown job {job_name!r}. Valid jobs: {list(_JOB_MAP)}",
        )
    module_path, func_name = _JOB_MAP[job_name].split(":")
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, func_name)


# ---------------------------------------------------------------------------
# POST /admin/sync/{job_name}
# ---------------------------------------------------------------------------

@router.post("/sync/{job_name}", response_model=SyncTriggerOut)
async def trigger_sync(job_name: str):
    """Fire a sync job immediately (runs in-process, awaited)."""
    fn = _load_job(job_name)
    import asyncio
    asyncio.create_task(fn())   # fire-and-forget; don't block the HTTP response
    return SyncTriggerOut(job=job_name, status="triggered")


# ---------------------------------------------------------------------------
# POST /admin/backfill
# ---------------------------------------------------------------------------

@router.post("/backfill", response_model=BackfillTriggerOut)
async def trigger_backfill(
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
):
    """Kick off a historical backfill for the given date range (fire-and-forget)."""
    from services.worker.jobs.backfill import historical_backfill
    import asyncio
    asyncio.create_task(historical_backfill(since=since, until=until))
    return BackfillTriggerOut(since=since, until=until)


# ---------------------------------------------------------------------------
# GET /admin/sync-runs
# ---------------------------------------------------------------------------

@router.get("/sync-runs", response_model=Paginated[SyncRunOut])
async def list_sync_runs(
    entity_type: str | None = Query(None),
    status: str | None = Query(None),
    account_id: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy import func
    offset = decode_cursor(cursor)
    stmt = select(SyncRun).order_by(SyncRun.started_at.desc())
    if entity_type:
        stmt = stmt.where(SyncRun.entity_type == entity_type)
    if status:
        stmt = stmt.where(SyncRun.status == status)
    if account_id:
        stmt = stmt.where(SyncRun.account_id == account_id)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/sync-runs/{run_id}", response_model=SyncRunOut)
async def get_sync_run(run_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.get(SyncRun, run_id)
    if not row:
        raise HTTPException(status_code=404, detail="Sync run not found")
    return row


# ---------------------------------------------------------------------------
# GET /admin/rate-limits
# ---------------------------------------------------------------------------

@router.post("/attribution/backfill", summary="Backfill attribution events for a date range")
async def trigger_attribution_backfill(
    event_type: str = Query(..., description="signups | conversions"),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
) -> dict:
    """Fire-and-forget attribution backfill for a single event type, chunked by month."""
    if event_type not in ("signups", "conversions"):
        raise HTTPException(status_code=422, detail="event_type must be 'signups' or 'conversions'")
    from services.worker.jobs.sync_attribution import backfill_attribution
    import asyncio
    asyncio.create_task(backfill_attribution(event_type=event_type, since=since, until=until))
    return {"status": "triggered", "event_type": event_type, "since": since, "until": until}


@router.get("/attribution/cursors", summary="Current attribution sync watermarks")
async def get_attribution_cursors(db: AsyncSession = Depends(get_db)) -> dict:
    from sqlalchemy import text
    rows = await db.execute(text("SELECT * FROM attribution_sync_cursor ORDER BY job_name"))
    return {"data": [dict(r._mapping) for r in rows]}


@router.get("/rate-limits", response_model=Paginated[ApiRateLimitOut])
async def list_rate_limits(
    account_id: str | None = Query(None),
    endpoint: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy import func
    offset = decode_cursor(cursor)
    stmt = select(ApiRateLimit).order_by(ApiRateLimit.recorded_at.desc())
    if account_id:
        stmt = stmt.where(ApiRateLimit.account_id == account_id)
    if endpoint:
        stmt = stmt.where(ApiRateLimit.endpoint == endpoint)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)
