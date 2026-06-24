"""Sync hourly campaign-level insights into insights_campaign_hourly.

Uses Meta's `breakdowns=hourly_stats_aggregated_by_advertiser_time_zone` which
returns 24 rows per (campaign, day), each tagged with an hour range like
"03:00:00 - 03:59:59". We extract the leading hour integer and upsert.

One sync call per date in the requested window so async-report bodies stay
small (per-day pulls are reliable on Dev-Tier; multi-day breakdown queries
tend to time out).
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta

import httpx
from sqlalchemy import text

from services.shared.config import settings
from services.shared.constants import INSIGHTS_AD_FIELDS
from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaClient
from services.shared.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

_HOUR_RE = re.compile(r"^(\d{2}):")


def _parse_hour(s: str) -> int | None:
    """Extract hour 0-23 from a string like '03:00:00 - 03:59:59'."""
    if not s:
        return None
    m = _HOUR_RE.match(s.strip())
    if m:
        try:
            h = int(m.group(1))
            if 0 <= h <= 23:
                return h
        except ValueError:
            pass
    return None


async def sync_insights_campaign_hourly(days: int = 7, window: str = "7d_click") -> None:
    """Pull last `days` days of campaign hourly insights and upsert."""
    log.info("sync_insights_campaign_hourly: starting (days=%d)", days)
    today = date.today()
    # Keep both forms: ISO string for the Meta API time_range payload, and a
    # real `date` object for the asyncpg DATE bind (it rejects strings).
    dates = [(today - timedelta(days=i)) for i in range(days - 1, -1, -1)]

    async with httpx.AsyncClient() as http:
        for account_id in settings.ad_account_id_list:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                app_secret=settings.meta_app_secret,
                http_client=http,
                rate_limiter=rl,
            )
            for d in dates:
                d_iso = d.isoformat()
                rows: list[dict] = []
                try:
                    async for raw in client.get_insights(
                        object_id=account_id,
                        level="campaign",
                        time_range={"since": d_iso, "until": d_iso},
                        fields=INSIGHTS_AD_FIELDS,
                        action_attribution_windows=[window],
                        breakdowns="hourly_stats_aggregated_by_advertiser_time_zone",
                    ):
                        hour = _parse_hour(raw.get("hourly_stats_aggregated_by_advertiser_time_zone", ""))
                        cid = raw.get("campaign_id")
                        if hour is None or not cid:
                            continue
                        rows.append({
                            "campaign_id": cid,
                            "date": d,
                            "hour": hour,
                            "attribution_window": window,
                            "spend": float(raw.get("spend") or 0),
                            "impressions": int(raw.get("impressions") or 0),
                            "clicks": int(raw.get("clicks") or 0),
                            "actions": raw.get("actions"),
                            "conversions": raw.get("conversions"),
                            "action_values": raw.get("action_values"),
                        })
                except Exception as e:
                    log.warning("sync_insights_campaign_hourly %s %s: skipped (%s)",
                                account_id, d_iso, e)
                    continue

                if not rows:
                    log.info("sync_insights_campaign_hourly %s %s: 0 rows", account_id, d)
                    continue

                async with AsyncSessionLocal() as session:
                    # Manual upsert via INSERT … ON CONFLICT, bound params.
                    for r in rows:
                        await session.execute(text("""
                            INSERT INTO insights_campaign_hourly
                                (campaign_id, date, hour, attribution_window,
                                 spend, impressions, clicks,
                                 actions, conversions, action_values, synced_at)
                            VALUES (:campaign_id, :date, :hour, :attribution_window,
                                    :spend, :impressions, :clicks,
                                    CAST(:actions AS jsonb),
                                    CAST(:conversions AS jsonb),
                                    CAST(:action_values AS jsonb),
                                    NOW())
                            ON CONFLICT (campaign_id, date, hour, attribution_window)
                            DO UPDATE SET
                                spend         = EXCLUDED.spend,
                                impressions   = EXCLUDED.impressions,
                                clicks        = EXCLUDED.clicks,
                                actions       = EXCLUDED.actions,
                                conversions   = EXCLUDED.conversions,
                                action_values = EXCLUDED.action_values,
                                synced_at     = NOW()
                        """), {
                            **r,
                            "actions":       _to_jsonb(r["actions"]),
                            "conversions":   _to_jsonb(r["conversions"]),
                            "action_values": _to_jsonb(r["action_values"]),
                        })
                    await session.commit()
                log.info("sync_insights_campaign_hourly %s %s: upserted %d rows",
                         account_id, d_iso, len(rows))
    log.info("sync_insights_campaign_hourly: done")


def _to_jsonb(val) -> str | None:
    """JSON-encode for the bound parameter; None stays None."""
    if val is None:
        return None
    import json
    return json.dumps(val)
