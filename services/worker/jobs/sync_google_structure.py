"""
Sync Google Ads structure: campaigns → ad_groups → ads.
Runs as a scheduled job (every 30 min alongside Meta structure sync).
"""

import logging
from datetime import date

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from services.shared.config import settings
from services.shared.db import AsyncSessionLocal
from services.shared.google_ads_client import micros_to_units, run_query
from services.shared.models import GoogleAd, GoogleAdGroup, GoogleCampaign

log = logging.getLogger(__name__)

_CID = settings.google_ads_customer_id_clean

# ---------------------------------------------------------------------------
# GAQL queries
# ---------------------------------------------------------------------------

_CAMPAIGN_QUERY = """
SELECT
    campaign.id,
    campaign.name,
    campaign.status,
    campaign.advertising_channel_type,
    campaign.bidding_strategy_type,
    campaign_budget.amount_micros
FROM campaign
WHERE campaign.status != 'REMOVED'
ORDER BY campaign.id
"""

_AD_GROUP_QUERY = """
SELECT
    ad_group.id,
    ad_group.name,
    ad_group.status,
    ad_group.type,
    ad_group.cpc_bid_micros,
    campaign.id
FROM ad_group
WHERE ad_group.status != 'REMOVED'
ORDER BY ad_group.id
"""

_AD_QUERY = """
SELECT
    ad_group_ad.ad.id,
    ad_group_ad.ad.name,
    ad_group_ad.ad.type,
    ad_group_ad.ad.final_urls,
    ad_group_ad.status,
    ad_group.id,
    campaign.id
FROM ad_group_ad
WHERE ad_group_ad.status != 'REMOVED'
ORDER BY ad_group_ad.ad.id
"""


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_campaign(row) -> dict:
    c = row.campaign
    b = row.campaign_budget
    return {
        "id":                       c.id,
        "customer_id":              _CID,
        "name":                     c.name or None,
        "status":                   c.status.name if c.status else None,
        "advertising_channel_type": c.advertising_channel_type.name if c.advertising_channel_type else None,
        "bidding_strategy_type":    c.bidding_strategy_type.name if c.bidding_strategy_type else None,
        "daily_budget":             micros_to_units(b.amount_micros) if b.amount_micros else None,
        "start_date":               None,
        "end_date":                 None,
    }


def _parse_ad_group(row) -> dict:
    ag = row.ad_group
    return {
        "id":          ag.id,
        "campaign_id": row.campaign.id,
        "customer_id": _CID,
        "name":        ag.name or None,
        "status":      ag.status.name if ag.status else None,
        "type":        ag.type_.name if ag.type_ else None,
        "cpc_bid":     micros_to_units(ag.cpc_bid_micros) if ag.cpc_bid_micros else None,
    }


def _parse_ad(row) -> dict:
    a = row.ad_group_ad
    return {
        "id":          a.ad.id,
        "ad_group_id": row.ad_group.id,
        "campaign_id": row.campaign.id,
        "customer_id": _CID,
        "name":        a.ad.name or None,
        "status":      a.status.name if a.status else None,
        "type":        a.ad.type_.name if a.ad.type_ else None,
        "final_urls":  list(a.ad.final_urls) if a.ad.final_urls else None,
    }


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

async def _upsert(session, model, rows: list[dict], pk_cols: list[str]):
    if not rows:
        return 0
    stmt = pg_insert(model).values(rows)
    update_cols = {c: stmt.excluded[c] for c in rows[0] if c not in pk_cols}
    stmt = stmt.on_conflict_do_update(index_elements=pk_cols, set_=update_cols)
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or len(rows)


# ---------------------------------------------------------------------------
# Main sync
# ---------------------------------------------------------------------------

async def sync_google_structure() -> None:
    log.info("sync_google_structure: starting")

    # Campaigns
    campaign_rows_raw = await _run_in_executor(_CAMPAIGN_QUERY)
    campaigns = [_parse_campaign(r) for r in campaign_rows_raw]
    async with AsyncSessionLocal() as session:
        n = await _upsert(session, GoogleCampaign, campaigns, ["id"])
    log.info("google_campaigns: %d upserted", n)

    # Ad groups
    ag_rows_raw = await _run_in_executor(_AD_GROUP_QUERY)
    ad_groups = [_parse_ad_group(r) for r in ag_rows_raw]
    async with AsyncSessionLocal() as session:
        n = await _upsert(session, GoogleAdGroup, ad_groups, ["id"])
    log.info("google_ad_groups: %d upserted", n)

    # Ads
    ad_rows_raw = await _run_in_executor(_AD_QUERY)
    ads = [_parse_ad(r) for r in ad_rows_raw]
    async with AsyncSessionLocal() as session:
        n = await _upsert(session, GoogleAd, ads, ["id"])
    log.info("google_ads: %d upserted", n)

    log.info("sync_google_structure: done — %d campaigns, %d ad_groups, %d ads",
             len(campaigns), len(ad_groups), len(ads))


async def _run_in_executor(query: str) -> list:
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: run_query(query))
