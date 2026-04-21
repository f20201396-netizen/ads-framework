"""
Auxiliary syncs — §7, §8, §9 of the curl script.

sync_audiences_pixels_catalogs  — daily 03:00 IST (21:30 UTC)
sync_pixel_stats                — daily 03:00 IST, last 90 days per active pixel
"""

import logging
from datetime import datetime, timedelta, timezone

import httpx

from services.shared.config import settings
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.models import AdsPixel
from services.shared.rate_limiter import RateLimiter
from services.worker.parsers import (
    parse_catalog,
    parse_custom_audience,
    parse_custom_conversion,
    parse_pixel,
    parse_product_feed,
    parse_product_set,
)
from services.worker.upsert import track_run, upsert_dims, upsert_pixel_stats

log = logging.getLogger(__name__)

UTC = timezone.utc
_PIXEL_STATS_DAYS = 90


async def sync_audiences_pixels_catalogs() -> None:
    """§7 + §8 + §9 — daily structural refresh for aux entities."""
    log.info("sync_audiences_pixels_catalogs: starting")
    business_id = settings.meta_business_id
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                http_client=http,
                rate_limiter=rl,
            )
            await _sync_audiences(client, account_id)
            await _sync_pixels_and_conversions(client, account_id)

        # Catalogs are business-scoped
        rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=None)
        client = MetaClient(
            access_token=settings.meta_access_token,
            http_client=http,
            rate_limiter=rl,
        )
        await _sync_catalogs(client, business_id)
    log.info("sync_audiences_pixels_catalogs: done")


async def sync_pixel_stats() -> None:
    """§8.2 — pixel event stats, last 90 days, all active pixels."""
    log.info("sync_pixel_stats: starting")
    now = datetime.now(UTC)
    end_ts = int(now.timestamp())
    start_ts = int((now - timedelta(days=_PIXEL_STATS_DAYS)).timestamp())

    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                http_client=http,
                rate_limiter=rl,
            )
            await _sync_pixel_stats_for_account(client, account_id, start_ts, end_ts)
    log.info("sync_pixel_stats: done")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _sync_audiences(client: MetaClient, account_id: str) -> None:
    import services.shared.models as m
    async with track_run("custom_audiences", account_id) as run:
        rows = []
        async for item in client.list_custom_audiences(account_id):
            rows.append(parse_custom_audience(item, account_id))
            run.rows_upserted += 1
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, m.CustomAudience, rows)


async def _sync_pixels_and_conversions(client: MetaClient, account_id: str) -> None:
    import services.shared.models as m

    # Pixels
    async with track_run("ads_pixels", account_id) as run:
        pixels = await client.list_pixels(account_id)
        rows = [parse_pixel(p, account_id) for p in pixels]
        run.rows_upserted = len(rows)
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, m.AdsPixel, rows)

    # Custom conversions
    async with track_run("custom_conversions", account_id) as run:
        conversions = await client.list_custom_conversions(account_id)
        rows = [parse_custom_conversion(c, account_id) for c in conversions]
        run.rows_upserted = len(rows)
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, m.CustomConversion, rows)


async def _sync_catalogs(client: MetaClient, business_id: str) -> None:
    import services.shared.models as m

    async with track_run("product_catalogs", None) as run:
        catalogs = await client.list_product_catalogs(business_id)
        cat_rows = [parse_catalog(c, business_id) for c in catalogs]
        run.rows_upserted += len(cat_rows)

        set_rows: list[dict] = []
        feed_rows: list[dict] = []
        for catalog in catalogs:
            cat_id = catalog["id"]
            sets = await client.list_product_sets(cat_id)
            set_rows.extend(parse_product_set(s, cat_id) for s in sets)
            feeds = await client.list_product_feeds(cat_id)
            feed_rows.extend(parse_product_feed(f, cat_id) for f in feeds)

        async with AsyncSessionLocal() as session:
            await upsert_dims(session, m.ProductCatalog, cat_rows)
            await upsert_dims(session, m.ProductSet, set_rows)
            await upsert_dims(session, m.ProductFeed, feed_rows)


async def _sync_pixel_stats_for_account(
    client: MetaClient,
    account_id: str,
    start_ts: int,
    end_ts: int,
) -> None:
    # Fetch active pixels from DB (already synced by _sync_pixels_and_conversions)
    from sqlalchemy import select

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AdsPixel.id).where(
                AdsPixel.account_id == account_id,
                AdsPixel.is_unavailable.is_not(True),
            )
        )
        pixel_ids = [row[0] for row in result.fetchall()]

    if not pixel_ids:
        log.debug("sync_pixel_stats: no active pixels for account=%s", account_id)
        return

    for pixel_id in pixel_ids:
        async with track_run("pixel_event_stats_daily", account_id) as run:
            data = await client.get_pixel_stats(pixel_id, start_ts, end_ts)
            rows = _parse_pixel_stats(data, pixel_id, account_id)
            run.rows_upserted = len(rows)
            if rows:
                async with AsyncSessionLocal() as session:
                    await upsert_pixel_stats(session, rows)


def _parse_pixel_stats(data: dict, pixel_id: str, account_id: str) -> list[dict]:
    """
    The /stats endpoint with aggregation=event returns a structure like:
    {"data": [{"event": "Purchase", "count": 42, "start_time": "...", ...}]}
    We store one row per (pixel_id, date, event_name).
    """
    rows = []
    for entry in data.get("data", []):
        # start_time is a Unix timestamp; derive date from it
        start_time = entry.get("start_time")
        if start_time:
            event_date = datetime.fromtimestamp(int(start_time), UTC).date().isoformat()
        else:
            continue
        rows.append({
            "pixel_id": pixel_id,
            "account_id": account_id,
            "date": event_date,
            "event_name": entry.get("event", "unknown"),
            "count": entry.get("count"),
        })
    return rows
