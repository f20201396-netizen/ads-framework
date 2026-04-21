"""
Rate-limit header parser + backoff + persistence to api_rate_limits.

Meta sends three headers (any or all may be absent):
  X-Business-Use-Case-Usage  {"<business_id>": [{"call_count": N, ...}]}
  X-Ad-Account-Usage         {"<account_id>":  [{"call_count": N, ...}]}
  X-App-Usage                {"call_count": N, "total_cputime": N, "total_time": N}

If the highest call_count across all entries >= BACKOFF_THRESHOLD (75 %),
we sleep with exponential backoff before returning so the caller is
automatically throttled without any extra logic.
"""

import asyncio
import json
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from services.shared.models import ApiRateLimit

log = logging.getLogger(__name__)

BACKOFF_THRESHOLD = 75  # call_count % at which we start sleeping


class RateLimiter:
    """
    One instance per MetaClient.  Receives the raw httpx Headers after
    every successful (or rate-limited) API response, persists them, and
    sleeps if we're close to the ceiling.
    """

    def __init__(self, db_factory, account_id: str | None = None) -> None:
        self._db_factory = db_factory  # async_sessionmaker
        self._account_id = account_id

    async def record(self, headers, endpoint: str = "") -> None:
        buc = _parse(headers.get("x-business-use-case-usage"))
        aau = _parse(headers.get("x-ad-account-usage"))
        au = _parse(headers.get("x-app-usage"))

        await self._persist(endpoint, buc, aau, au)
        await _maybe_backoff(buc, aau, au)

    async def _persist(
        self,
        endpoint: str,
        buc: dict | None,
        aau: dict | None,
        au: dict | None,
    ) -> None:
        if buc is None and aau is None and au is None:
            return
        async with self._db_factory() as session:
            session: AsyncSession
            session.add(
                ApiRateLimit(
                    account_id=self._account_id,
                    endpoint=endpoint,
                    business_use_case_usage=buc,
                    ad_account_usage=aau,
                    app_usage=au,
                )
            )
            await session.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _max_call_count(
    buc: dict | None,
    aau: dict | None,
    au: dict | None,
) -> int:
    """Return the highest call_count % seen across all three header payloads."""
    peak = 0

    for usage in (buc, aau):
        if not isinstance(usage, dict):
            continue
        # Shape: {"entity_id": [{"call_count": N, ...}, ...], ...}
        for v in usage.values():
            if isinstance(v, list):
                for entry in v:
                    if isinstance(entry, dict):
                        peak = max(peak, entry.get("call_count", 0))

    # X-App-Usage: {"call_count": N, "total_cputime": N, "total_time": N}
    if isinstance(au, dict):
        peak = max(peak, au.get("call_count", 0))

    return peak


async def _maybe_backoff(
    buc: dict | None,
    aau: dict | None,
    au: dict | None,
) -> None:
    cc = _max_call_count(buc, aau, au)
    if cc < BACKOFF_THRESHOLD:
        return

    # Exponential steps of 5 % above threshold:
    # 75 % → 2 s, 80 % → 4 s, 85 % → 8 s, 90 % → 16 s, 95 % → 32 s, 100 % → 64 s
    steps = (cc - BACKOFF_THRESHOLD) // 5
    wait = min(2 ** steps, 300)
    log.warning(
        "Meta rate limit call_count=%d%% (threshold=%d%%), backing off %ds",
        cc,
        BACKOFF_THRESHOLD,
        wait,
    )
    await asyncio.sleep(wait)
