"""
sync_account_structure — §1-5 of the curl script.

Schedule: every 30 minutes, full refresh.

Order (FK-safe):
  businesses → ad_accounts → campaigns → ad_creatives → adsets → ads
"""

import json
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


async def sync_ad_statuses() -> None:
    """Lightweight status sync via Meta Batch API.

    Fetches the live status of every ad with spend in the last 14 days plus
    any locally-ACTIVE ad. Covers both pause→active and active→pause
    transitions so the dashboard never shows a stale status. Still cheap
    relative to a full sync_account_structure() — typically <30 batch calls.
    """
    log.info("sync_ad_statuses: starting")
    from sqlalchemy import text

    # 1. Any ad with recent spend OR currently marked ACTIVE — covers
    #    both "paused on Meta but our DB says ACTIVE" and "resurrected
    #    on Meta but our DB still has stale paused/inactive status".
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT DISTINCT a.id FROM ads a
            WHERE a.effective_status = 'ACTIVE'
               OR EXISTS (
                   SELECT 1 FROM insights_daily i
                   WHERE i.ad_id = a.id
                     AND i.date >= CURRENT_DATE - 14
                     AND i.spend > 0
               )
        """))
        target_ids = [row[0] for row in result.fetchall()]

    if not target_ids:
        log.info("sync_ad_statuses: no candidate ads, skipping")
        return

    log.info("sync_ad_statuses: checking %d ads (active or recent spend)", len(target_ids))

    # 2. Batch-check live status (50 per request).
    #    Always overwrite local rows with what Meta returns — even if the
    #    new value equals the old one — so this is also a self-healing pass.
    updates: list[tuple[str, str, str, str]] = []
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=rl,
            )

            for i in range(0, len(target_ids), 50):
                batch = target_ids[i : i + 50]
                batch_requests = [
                    {"method": "GET", "relative_url": f"{aid}?fields=id,status,effective_status,configured_status"}
                    for aid in batch
                ]
                results = await client.batch(batch_requests)
                for resp in results:
                    body = json.loads(resp["body"])
                    if "error" in body:
                        continue
                    updates.append((
                        body.get("status", ""),
                        body.get("effective_status", ""),
                        body.get("configured_status", ""),
                        body["id"],
                    ))

    if updates:
        async with AsyncSessionLocal() as session:
            for status, eff_status, conf_status, aid in updates:
                await session.execute(
                    text(
                        "UPDATE ads SET status = :s, effective_status = :es, "
                        "configured_status = :cs WHERE id = :id"
                    ),
                    {"s": status, "es": eff_status, "cs": conf_status, "id": aid},
                )
            await session.commit()

    log.info("sync_ad_statuses: done — %d ads refreshed", len(updates))


def _models():
    """Lazy import to avoid circular imports at module load."""
    import services.shared.models as m
    return m
