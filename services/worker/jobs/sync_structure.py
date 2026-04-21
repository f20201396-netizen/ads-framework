"""
sync_account_structure — §1-5 of the curl script.

Schedule: every 30 minutes, full refresh.

Order (FK-safe):
  businesses → ad_accounts → campaigns → ad_creatives → adsets → ads
"""

import logging

import httpx

from services.shared.config import settings
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.rate_limiter import RateLimiter
from services.worker.parsers import (
    parse_ad,
    parse_ad_account,
    parse_adset,
    parse_business,
    parse_campaign,
    parse_creative,
)
from services.worker.upsert import track_run, upsert_dims

log = logging.getLogger(__name__)


async def sync_account_structure() -> None:
    log.info("sync_account_structure: starting")
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                http_client=http,
                rate_limiter=rl,
            )
            await _sync_one_account(client, account_id)
    log.info("sync_account_structure: done")


async def _sync_one_account(client: MetaClient, account_id: str) -> None:
    business_id = settings.meta_business_id

    # ------------------------------------------------------------------ #
    # businesses                                                           #
    # ------------------------------------------------------------------ #
    async with track_run("businesses", account_id) as run:
        raw_biz_list = await client.list_businesses()
        async with AsyncSessionLocal() as session:
            rows = [parse_business(b) for b in raw_biz_list]
            run.rows_upserted += await upsert_dims(session, _models().Business, rows)

    # ------------------------------------------------------------------ #
    # ad_accounts (owned + client)                                         #
    # ------------------------------------------------------------------ #
    async with track_run("ad_accounts", account_id) as run:
        owned = await client.list_owned_ad_accounts(business_id)
        client_accts = await client.list_client_ad_accounts(business_id)
        rows = [parse_ad_account(a, is_client=False) for a in owned] + \
               [parse_ad_account(a, is_client=True) for a in client_accts]
        async with AsyncSessionLocal() as session:
            run.rows_upserted += await upsert_dims(session, _models().AdAccount, rows)

    # ------------------------------------------------------------------ #
    # campaigns                                                            #
    # ------------------------------------------------------------------ #
    async with track_run("campaigns", account_id) as run:
        rows: list[dict] = []
        async for item in client.list_campaigns(account_id):
            rows.append(parse_campaign(item, account_id))
            run.rows_upserted += 1
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, _models().Campaign, rows)

    # ------------------------------------------------------------------ #
    # ad_creatives (before ads so FK is satisfied)                         #
    # ------------------------------------------------------------------ #
    async with track_run("ad_creatives", account_id) as run:
        rows = []
        async for item in client.list_creatives(account_id):
            rows.append(parse_creative(item, account_id))
            run.rows_upserted += 1
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, _models().AdCreative, rows)

    # ------------------------------------------------------------------ #
    # adsets                                                               #
    # ------------------------------------------------------------------ #
    async with track_run("adsets", account_id) as run:
        rows = []
        async for item in client.list_adsets(account_id):
            rows.append(parse_adset(item, account_id))
            run.rows_upserted += 1
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, _models().AdSet, rows)

    # ------------------------------------------------------------------ #
    # ads                                                                  #
    # ------------------------------------------------------------------ #
    async with track_run("ads", account_id) as run:
        rows = []
        async for item in client.list_ads(account_id):
            rows.append(parse_ad(item, account_id))
            run.rows_upserted += 1
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, _models().Ad, rows)


def _models():
    """Lazy import to avoid circular imports at module load."""
    import services.shared.models as m
    return m
