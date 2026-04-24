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
                app_secret=settings.meta_app_secret,
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
        all_raw_accounts = owned + client_accts

        # System-user tokens often return 0 from me/businesses but each
        # ad account carries a nested "business" object — use that as fallback.
        if not raw_biz_list:
            seen_biz: set[str] = set()
            fallback_biz_rows = []
            for acct in all_raw_accounts:
                biz = acct.get("business") or {}
                biz_id = biz.get("id")
                if biz_id and biz_id not in seen_biz:
                    seen_biz.add(biz_id)
                    fallback_biz_rows.append(parse_business(biz))
            if fallback_biz_rows:
                async with AsyncSessionLocal() as session:
                    await upsert_dims(session, _models().Business, fallback_biz_rows)
                    log.info("businesses: seeded %d from ad_account.business fields", len(fallback_biz_rows))

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
    # ad_creatives — skipped in this run; FK was dropped temporarily.     #
    # Run sync_creatives_only() separately with the lean field set.        #
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    # adsets                                                               #
    # ------------------------------------------------------------------ #
    synced_adset_ids: set[str] = set()
    async with track_run("adsets", account_id) as run:
        rows = []
        async for item in client.list_adsets(account_id):
            parsed = parse_adset(item, account_id)
            rows.append(parsed)
            synced_adset_ids.add(parsed["id"])
            run.rows_upserted += 1
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, _models().AdSet, rows)

    # ------------------------------------------------------------------ #
    # ads                                                                  #
    # ------------------------------------------------------------------ #
    async with track_run("ads", account_id) as run:
        rows = []
        skipped = 0
        async for item in client.list_ads(account_id):
            parsed = parse_ad(item, account_id)
            if parsed.get("adset_id") not in synced_adset_ids:
                skipped += 1
                continue
            rows.append(parsed)
            run.rows_upserted += 1
        if skipped:
            log.info("ads: skipped %d ads whose adset_id is not in synced set", skipped)
        async with AsyncSessionLocal() as session:
            await upsert_dims(session, _models().Ad, rows)


def _models():
    """Lazy import to avoid circular imports at module load."""
    import services.shared.models as m
    return m
