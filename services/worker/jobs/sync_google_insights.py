"""
Sync Google Ads daily insights (ad-level).
Runs hourly — pulls last 3 days to catch delayed reporting.

Backfill script: scripts/backfill_google_insights.py
"""

import asyncio
import logging
from datetime import date, timedelta

from sqlalchemy.dialects.postgresql import insert as pg_insert

from services.shared.config import settings
from services.shared.db import AsyncSessionLocal
from services.shared.google_ads_client import micros_to_units, run_query
from services.shared.models import GoogleInsightsDaily

log = logging.getLogger(__name__)

_CID = settings.google_ads_customer_id_clean
_INSIGHT_PK = ["ad_id", "date"]


# ---------------------------------------------------------------------------
# GAQL
# ---------------------------------------------------------------------------

def _insights_query(since: date, until: date) -> str:
    return f"""
SELECT
    ad_group_ad.ad.id,
    ad_group.id,
    campaign.id,
    segments.date,
    metrics.impressions,
    metrics.clicks,
    metrics.cost_micros,
    metrics.ctr,
    metrics.average_cpm,
    metrics.average_cpc,
    metrics.conversions,
    metrics.conversions_value,
    metrics.view_through_conversions
FROM ad_group_ad
WHERE segments.date BETWEEN '{since.isoformat()}' AND '{until.isoformat()}'
  AND metrics.impressions > 0
ORDER BY segments.date, ad_group_ad.ad.id
"""


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _parse_insight(row) -> dict:
    a = row.ad_group_ad
    m = row.metrics
    seg = row.segments
    return {
        "ad_id":                    a.ad.id,
        "ad_group_id":              row.ad_group.id,
        "campaign_id":              row.campaign.id,
        "customer_id":              _CID,
        "date":                     date.fromisoformat(seg.date),
        "impressions":              m.impressions or None,
        "clicks":                   m.clicks or None,
        "spend":                    micros_to_units(m.cost_micros),
        "ctr":                      round(m.ctr, 6) if m.ctr else None,
        "avg_cpm":                  micros_to_units(m.average_cpm),
        "avg_cpc":                  micros_to_units(m.average_cpc),
        "conversions":              m.conversions or None,
        "conversions_value":        m.conversions_value or None,
        "view_through_conversions": m.view_through_conversions or None,
    }


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

async def _upsert_insights(rows: list[dict]) -> int:
    if not rows:
        return 0
    chunk_size = 500
    total = 0
    async with AsyncSessionLocal() as session:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i: i + chunk_size]
            stmt = pg_insert(GoogleInsightsDaily).values(chunk)
            update_cols = {c: stmt.excluded[c] for c in chunk[0] if c not in _INSIGHT_PK}
            stmt = stmt.on_conflict_do_update(index_elements=_INSIGHT_PK, set_=update_cols)
            result = await session.execute(stmt)
            total += result.rowcount or len(chunk)
        await session.commit()
    return total


# ---------------------------------------------------------------------------
# Sync job (daily rolling window)
# ---------------------------------------------------------------------------

async def sync_google_insights_daily() -> None:
    """Pull last 3 days of Google Ads insights (catches delayed reporting)."""
    until = date.today()
    since = until - timedelta(days=3)
    log.info("sync_google_insights_daily: %s → %s", since, until)

    loop = asyncio.get_event_loop()
    raw = await loop.run_in_executor(None, lambda: run_query(_insights_query(since, until)))
    rows = [_parse_insight(r) for r in raw]
    n = await _upsert_insights(rows)
    log.info("google_insights_daily: %d rows upserted", n)


# ---------------------------------------------------------------------------
# Backfill helper (called by backfill script)
# ---------------------------------------------------------------------------

async def backfill_google_insights(since: date, until: date, chunk_days: int = 7) -> int:
    """Backfill Google Ads insights in chunks."""
    total = 0
    cursor = since
    loop = asyncio.get_event_loop()

    while cursor <= until:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), until)
        log.info("google insights chunk %s → %s", cursor, chunk_end)
        raw = await loop.run_in_executor(None, lambda s=cursor, e=chunk_end: run_query(_insights_query(s, e)))
        rows = [_parse_insight(r) for r in raw]
        n = await _upsert_insights(rows)
        total += n
        log.info("  upserted %d rows (total=%d)", n, total)
        cursor = chunk_end + timedelta(days=1)

    return total
