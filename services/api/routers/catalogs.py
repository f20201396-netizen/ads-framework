"""
/catalogs/* endpoints — product catalogs and product sets.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from services.api.deps import decode_cursor, encode_cursor, get_db
from services.api.schemas import CatalogOut, Paginated, ProductSetOut
from services.shared.models import ProductCatalog, ProductSet

log = logging.getLogger(__name__)
router = APIRouter(prefix="/catalogs", tags=["catalogs"])

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500


# ---------------------------------------------------------------------------
# /catalogs
# ---------------------------------------------------------------------------

@router.get("", response_model=Paginated[CatalogOut])
async def list_catalogs(
    business_id: str | None = Query(None),
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(ProductCatalog)
    if business_id:
        stmt = stmt.where(ProductCatalog.business_id == business_id)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/{catalog_id}", response_model=CatalogOut)
async def get_catalog(catalog_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(ProductCatalog, catalog_id)
    if not row:
        raise HTTPException(status_code=404, detail="Catalog not found")
    return row


# ---------------------------------------------------------------------------
# /catalogs/{catalog_id}/product-sets
# ---------------------------------------------------------------------------

@router.get("/{catalog_id}/product-sets", response_model=Paginated[ProductSetOut])
async def list_product_sets(
    catalog_id: str,
    limit: int = Query(_DEFAULT_LIMIT, le=_MAX_LIMIT),
    cursor: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    offset = decode_cursor(cursor)
    stmt = select(ProductSet).where(ProductSet.catalog_id == catalog_id)

    total_q = await db.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    rows = (await db.execute(stmt.offset(offset).limit(limit))).scalars().all()
    next_cursor = encode_cursor(offset + limit) if offset + limit < total else None
    return Paginated(data=rows, cursor=next_cursor, total=total)


@router.get("/{catalog_id}/product-sets/{product_set_id}", response_model=ProductSetOut)
async def get_product_set(
    catalog_id: str,
    product_set_id: str,
    db: AsyncSession = Depends(get_db),
):
    row = await db.get(ProductSet, product_set_id)
    if not row or row.catalog_id != catalog_id:
        raise HTTPException(status_code=404, detail="Product set not found")
    return row
