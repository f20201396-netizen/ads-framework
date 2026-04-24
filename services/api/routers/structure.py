"""
/structure/* endpoints — read-only entity hierarchy.

All list endpoints are paginated via ?limit=&cursor=.
?q= on campaigns does a case-insensitive name search.
Preview endpoint proxies Meta's ad preview API.
"""

import logging
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from services.api.deps import decode_cursor, encode_cursor, get_db
from services.api.schemas import (
    AccountOut,
    AdOut,
    AdSetOut,
    CampaignOut,
    CreativeOut,
    Paginated,
)
from services.shared.config import settings
from services.shared.meta_client import MetaClient
from services.shared.models import Ad, AdAccount, AdCreative, AdSet, Campaign
from services.shared.rate_limiter import RateLimiter
from services.shared.db import AsyncSessionLocal

log = logging.getLogger(__name__)
router = APIRouter(prefix="/structure", tags=["structure"])

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500


# ---------------------------------------------------------------------------
# /structure/accounts
# ---------------------------------------------------------------------------

@router.get("/accounts", response_model=Paginated[AccountOut])
async def list_accounts(
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    total_q = await db.execute(select(func.count()).select_from(AdAccount))
    total = total_q.scalar_one()
    rows = (
        await db.execute(select(AdAccount).offset(offset).limit(limit))
    ).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/accounts/{account_id}", response_model=AccountOut)
async def get_account(account_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(AdAccount, account_id)
    if not row:
        raise HTTPException(status_code=404, detail="Account not found")
    return row


# ---------------------------------------------------------------------------
# /structure/campaigns
# ---------------------------------------------------------------------------

@router.get("/campaigns", response_model=Paginated[CampaignOut])
async def list_campaigns(
    account_id: str | None = Query(None),
    status: str | None = Query(None),
    q: str | None = Query(None, description="Case-insensitive name search"),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(Campaign)
    if account_id:
        stmt = stmt.where(Campaign.account_id == account_id)
    if status:
        stmt = stmt.where(Campaign.effective_status == status.upper())
    if q:
        stmt = stmt.where(Campaign.name.ilike(f"%{q}%"))

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/campaigns/{campaign_id}", response_model=CampaignOut)
async def get_campaign(campaign_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(Campaign, campaign_id)
    if not row:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return row


# ---------------------------------------------------------------------------
# /structure/adsets
# ---------------------------------------------------------------------------

@router.get("/adsets", response_model=Paginated[AdSetOut])
async def list_adsets(
    campaign_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(AdSet)
    if campaign_id:
        stmt = stmt.where(AdSet.campaign_id == campaign_id)
    if status:
        stmt = stmt.where(AdSet.effective_status == status.upper())

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/adsets/{adset_id}", response_model=AdSetOut)
async def get_adset(adset_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(AdSet, adset_id)
    if not row:
        raise HTTPException(status_code=404, detail="Ad set not found")
    return row


# ---------------------------------------------------------------------------
# /structure/ads
# ---------------------------------------------------------------------------

@router.get("/ads", response_model=Paginated[AdOut])
async def list_ads(
    adset_id: str | None = Query(None),
    campaign_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(Ad)
    if adset_id:
        stmt = stmt.where(Ad.adset_id == adset_id)
    if campaign_id:
        stmt = stmt.where(Ad.campaign_id == campaign_id)
    if status:
        stmt = stmt.where(Ad.effective_status == status.upper())

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/ads/{ad_id}", response_model=AdOut)
async def get_ad(ad_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(Ad, ad_id)
    if not row:
        raise HTTPException(status_code=404, detail="Ad not found")
    return row


# ---------------------------------------------------------------------------
# /structure/creatives
# ---------------------------------------------------------------------------

@router.get("/creatives", response_model=Paginated[CreativeOut])
async def list_creatives(
    account_id: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(AdCreative)
    if account_id:
        stmt = stmt.where(AdCreative.account_id == account_id)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/creatives/{creative_id}", response_model=CreativeOut)
async def get_creative(creative_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(AdCreative, creative_id)
    if not row:
        raise HTTPException(status_code=404, detail="Creative not found")
    return row


@router.get("/creatives/{creative_id}/preview")
async def get_creative_preview(
    creative_id: str,
    format: str = Query("MOBILE_FEED_STANDARD"),
):
    """Proxy Meta's preview API — returns the iframe HTML snippet."""
    async with httpx.AsyncClient() as http:
        rl = RateLimiter(db_factory=AsyncSessionLocal)
        client = MetaClient(
            access_token=settings.meta_access_token,
            app_secret=settings.meta_app_secret,
            http_client=http,
            rate_limiter=rl,
        )
        data = await client.get_preview(creative_id, format)
    return {"data": data.get("data", [])}
