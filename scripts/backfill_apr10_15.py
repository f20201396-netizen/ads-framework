"""
Backfill: structure + insights for 2026-04-10 → 2026-04-15.
Runs in order:
  1. sync_account_structure  (campaigns / adsets / ads)
  2. sync_insights_daily      (ad-level, 7d_click)
  3. sync_insights_higher_levels (adset / campaign / account, 7d_click)
"""
import asyncio
import logging

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SINCE = "2026-04-10"
UNTIL = "2026-04-15"


async def main() -> None:
    from services.shared.config import settings
    from services.shared.db import AsyncSessionLocal
    from services.shared.rate_limiter import RateLimiter
    from services.shared.meta_client import MetaClient
    from services.shared.constants import INSIGHTS_AD_FIELDS, INSIGHTS_LEVEL_FIELDS
    from services.shared.models import (
        InsightsDaily, InsightsAdsetDaily, InsightsCampaignDaily, InsightsAccountDaily,
    )
    from services.worker.parsers import parse_insight_ad, parse_insight_level
    from services.worker.upsert import track_run, upsert_facts
    from services.worker.jobs.sync_structure import sync_account_structure

    # ── 1. Structure ──────────────────────────────────────────────────────────
    log.info("=== STEP 1: sync_account_structure ===")
    await sync_account_structure()

    # ── 2. Ad-level insights ──────────────────────────────────────────────────
    log.info("=== STEP 2: ad-level insights %s → %s ===", SINCE, UNTIL)
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=rl,
            )
            async with track_run("insights_daily", account_id) as run:
                rows: list[dict] = []
                async for raw in client.get_insights(
                    object_id=account_id,
                    level="ad",
                    time_range={"since": SINCE, "until": UNTIL},
                    fields=INSIGHTS_AD_FIELDS,
                    action_attribution_windows=["7d_click"],
                ):
                    parsed = parse_insight_ad(raw, "7d_click")
                    if parsed.get("ad_id") is None:
                        continue
                    rows.append(parsed)
                    run.rows_upserted += 1
                log.info("ad insights: %d rows collected", len(rows))
                async with AsyncSessionLocal() as session:
                    await upsert_facts(session, InsightsDaily, rows, ["ad_id", "date", "attribution_window"])
            log.info("ad insights: upserted for %s", account_id)

    # ── 3. Higher-level insights ──────────────────────────────────────────────
    log.info("=== STEP 3: higher-level insights %s → %s ===", SINCE, UNTIL)
    _LEVEL_CONFIG = [
        ("adset",    "adset_id",    InsightsAdsetDaily,    ["adset_id",    "date", "attribution_window"]),
        ("campaign", "campaign_id", InsightsCampaignDaily, ["campaign_id", "date", "attribution_window"]),
        ("account",  "account_id",  InsightsAccountDaily,  ["account_id",  "date", "attribution_window"]),
    ]
    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=rl,
            )
            for level, id_col, model, pk in _LEVEL_CONFIG:
                async with track_run(f"insights_{level}_daily", account_id) as run:
                    rows = []
                    async for raw in client.get_insights(
                        object_id=account_id,
                        level=level,
                        time_range={"since": SINCE, "until": UNTIL},
                        fields=INSIGHTS_LEVEL_FIELDS,
                        action_attribution_windows=["7d_click"],
                    ):
                        rows.append(parse_insight_level(raw, id_col, account_id, "7d_click"))
                        run.rows_upserted += 1
                    log.info("%s insights: %d rows", level, len(rows))
                    if rows:
                        async with AsyncSessionLocal() as session:
                            await upsert_facts(session, model, rows, pk)

    # ── 4. Refresh MVs ────────────────────────────────────────────────────────
    log.info("=== STEP 4: refreshing materialized views ===")
    from services.shared.db import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_campaign_conversions"))
        await conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_adset_conversions"))
    log.info("MVs refreshed")

    log.info("=== ALL DONE ===")


asyncio.run(main())
