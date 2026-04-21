"""
/audiences/* endpoints — custom audiences, pixels, pixel stats, custom conversions.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.api.deps import decode_cursor, encode_cursor, get_db
from services.api.schemas import (
    CustomAudienceOut,
    CustomConversionOut,
    Paginated,
    PixelOut,
    PixelStatRow,
)
from services.shared.models import (
    AdsPixel,
    CustomAudience,
    CustomConversion,
    PixelEventStatsDaily,
)
from sqlalchemy import func

log = logging.getLogger(__name__)
router = APIRouter(prefix="/audiences", tags=["audiences"])

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500


# ---------------------------------------------------------------------------
# /audiences/custom-audiences
# ---------------------------------------------------------------------------

@router.get("/custom-audiences", response_model=Paginated[CustomAudienceOut])
async def list_custom_audiences(
    account_id: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(CustomAudience)
    if account_id:
        stmt = stmt.where(CustomAudience.account_id == account_id)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/custom-audiences/{audience_id}", response_model=CustomAudienceOut)
async def get_custom_audience(audience_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(CustomAudience, audience_id)
    if not row:
        raise HTTPException(status_code=404, detail="Custom audience not found")
    return row


# ---------------------------------------------------------------------------
# /audiences/pixels
# ---------------------------------------------------------------------------

@router.get("/pixels", response_model=Paginated[PixelOut])
async def list_pixels(
    account_id: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(AdsPixel)
    if account_id:
        stmt = stmt.where(AdsPixel.account_id == account_id)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/pixels/{pixel_id}", response_model=PixelOut)
async def get_pixel(pixel_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(AdsPixel, pixel_id)
    if not row:
        raise HTTPException(status_code=404, detail="Pixel not found")
    return row


@router.get("/pixels/{pixel_id}/stats", response_model=Paginated[PixelStatRow])
async def get_pixel_stats(
    pixel_id: str,
    since: str | None = Query(None, description="YYYY-MM-DD"),
    until: str | None = Query(None, description="YYYY-MM-DD"),
    event_name: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(PixelEventStatsDaily).where(PixelEventStatsDaily.pixel_id == pixel_id)
    if since:
        stmt = stmt.where(PixelEventStatsDaily.date >= since)
    if until:
        stmt = stmt.where(PixelEventStatsDaily.date <= until)
    if event_name:
        stmt = stmt.where(PixelEventStatsDaily.event_name == event_name)
    stmt = stmt.order_by(PixelEventStatsDaily.date.desc())

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


# ---------------------------------------------------------------------------
# /audiences/custom-conversions
# ---------------------------------------------------------------------------

@router.get("/custom-conversions", response_model=Paginated[CustomConversionOut])
async def list_custom_conversions(
    account_id: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(CustomConversion)
    if account_id:
        stmt = stmt.where(CustomConversion.account_id == account_id)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/custom-conversions/{conversion_id}", response_model=CustomConversionOut)
async def get_custom_conversion(conversion_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(CustomConversion, conversion_id)
    if not row:
        raise HTTPException(status_code=404, detail="Custom conversion not found")
    return row
