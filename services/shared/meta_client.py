"""
Async Meta Ads Graph API client (v21.0).

Every public method mirrors exactly one section of
scripts/meta-ads-full-fetch-curl.sh.  Field strings come from
services/shared/constants.py — do not inline them here.

Retry policy (tenacity):
  - 5xx, 429, httpx transport errors → always retry
  - Meta error codes 1, 2, 4, 17, 32, 613 → transient, retry
  - Exponential back-off: 4 s → 8 s → 16 s … capped at 120 s, 6 attempts

Async insights reports (§6.4):
  When the requested date range exceeds `use_async_if_range_days_gt` days,
  get_insights() submits a POST report job, polls every 10 s for completion,
  then paginates the results — transparent to the caller.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import date

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from services.shared.constants import (
    ACTION_ATTRIBUTION_WINDOWS,
    ACTION_BREAKDOWNS,
    AD_ACCOUNT_CLIENT_FIELDS,
    AD_ACCOUNT_FULL_FIELDS,
    AD_ACCOUNT_OWNED_FIELDS,
    AD_CREATIVE_FIELDS,
    AD_FIELDS,
    ADSET_FIELDS,
    ADS_PIXEL_FIELDS,
    ALL_STATUSES,
    BASE_URL,
    BUSINESSES_FIELDS,
    CAMPAIGN_FIELDS,
    CUSTOM_AUDIENCE_FIELDS,
    CUSTOM_CONVERSION_FIELDS,
    ENTITY_STATUS_FILTER,
    INSIGHTS_AD_FIELDS,
    PRODUCT_CATALOG_FIELDS,
    PRODUCT_FEED_FIELDS,
    PRODUCT_SET_FIELDS,
)
from services.shared.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

# Meta transient error codes — safe to retry
_META_RETRYABLE = {1, 2, 4, 17, 32, 613}

# Threshold beyond which get_insights() submits an async report job
_ASYNC_REPORT_THRESHOLD_DEFAULT = 30


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class MetaAPIError(Exception):
    def __init__(self, status_code: int, error: dict) -> None:
        self.status_code = status_code
        self.error = error
        self.code: int = error.get("code", 0)
        self.message: str = error.get("message", "unknown")
        super().__init__(f"HTTP {status_code} | Meta code={self.code}: {self.message}")


# ---------------------------------------------------------------------------
# Retry predicate
# ---------------------------------------------------------------------------

def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, MetaAPIError):
        return (
            exc.status_code == 429
            or exc.status_code >= 500
            or exc.code in _META_RETRYABLE
        )
    return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError))


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class MetaClient:
    """
    Async client that mirrors every curl section in the reference script.

    Usage::

        async with httpx.AsyncClient() as http:
            rl = RateLimiter(db_factory=AsyncSessionLocal, account_id=ad_account_id)
            client = MetaClient(
                access_token=settings.meta_access_token,
                http_client=http,
                rate_limiter=rl,
            )
            async for campaign in client.list_campaigns(ad_account_id):
                ...
    """

    def __init__(
        self,
        access_token: str,
        http_client: httpx.AsyncClient,
        rate_limiter: RateLimiter,
        base_url: str = BASE_URL,
    ) -> None:
        self._token = access_token
        self._http = http_client
        self._rl = rate_limiter
        self._base = base_url.rstrip("/")

    # ------------------------------------------------------------------ #
    # §1 — Business Manager + Ad Accounts                                  #
    # ------------------------------------------------------------------ #

    async def list_businesses(self) -> list[dict]:
        """§1.1 — List businesses accessible to the token."""
        results: list[dict] = []
        async for item in self._paginate("me/businesses", {"fields": BUSINESSES_FIELDS}):
            results.append(item)
        return results

    async def list_owned_ad_accounts(self, business_id: str) -> list[dict]:
        """§1.2 — Owned ad accounts under a business."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{business_id}/owned_ad_accounts",
            {"fields": AD_ACCOUNT_OWNED_FIELDS, "limit": 200},
        ):
            results.append(item)
        return results

    async def list_client_ad_accounts(self, business_id: str) -> list[dict]:
        """§1.3 — Client ad accounts (agency setup)."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{business_id}/client_ad_accounts",
            {"fields": AD_ACCOUNT_CLIENT_FIELDS, "limit": 200},
        ):
            results.append(item)
        return results

    # ------------------------------------------------------------------ #
    # §2 — Campaigns                                                       #
    # ------------------------------------------------------------------ #

    async def list_campaigns(
        self,
        ad_account_id: str,
        statuses: list[str] = ALL_STATUSES,
    ) -> AsyncIterator[dict]:
        """§2 — All campaigns including ARCHIVED/DELETED."""
        status_filter = json.dumps(
            [{"field": "effective_status", "operator": "IN", "value": statuses}]
        )
        async for item in self._paginate(
            f"{ad_account_id}/campaigns",
            {"fields": CAMPAIGN_FIELDS, "filtering": status_filter, "limit": 500},
        ):
            yield item

    # ------------------------------------------------------------------ #
    # §3 — Ad Sets                                                         #
    # ------------------------------------------------------------------ #

    async def list_adsets(
        self,
        ad_account_id: str,
        statuses: list[str] = ALL_STATUSES,
    ) -> AsyncIterator[dict]:
        """§3 — All ad sets."""
        status_filter = json.dumps(
            [{"field": "effective_status", "operator": "IN", "value": statuses}]
        )
        async for item in self._paginate(
            f"{ad_account_id}/adsets",
            {"fields": ADSET_FIELDS, "filtering": status_filter, "limit": 500},
        ):
            yield item

    # ------------------------------------------------------------------ #
    # §4 — Ads                                                             #
    # ------------------------------------------------------------------ #

    async def list_ads(
        self,
        ad_account_id: str,
        statuses: list[str] = ALL_STATUSES,
    ) -> AsyncIterator[dict]:
        """§4 — All ads."""
        status_filter = json.dumps(
            [{"field": "effective_status", "operator": "IN", "value": statuses}]
        )
        async for item in self._paginate(
            f"{ad_account_id}/ads",
            {"fields": AD_FIELDS, "filtering": status_filter, "limit": 500},
        ):
            yield item

    # ------------------------------------------------------------------ #
    # §5 — Ad Creatives                                                    #
    # ------------------------------------------------------------------ #

    async def list_creatives(self, ad_account_id: str) -> AsyncIterator[dict]:
        """§5.1 — All creatives."""
        async for item in self._paginate(
            f"{ad_account_id}/adcreatives",
            {"fields": AD_CREATIVE_FIELDS, "limit": 200},
        ):
            yield item

    async def get_preview(self, creative_id: str, ad_format: str) -> dict:
        """§5.2 — Ad preview HTML for one creative + format."""
        return await self._get(f"{creative_id}/previews", {"ad_format": ad_format})

    # ------------------------------------------------------------------ #
    # §6 — Insights                                                        #
    # ------------------------------------------------------------------ #

    async def get_insights(
        self,
        object_id: str,
        level: str,
        time_range: dict,
        time_increment: int = 1,
        breakdowns: str | list[str] | None = None,
        action_attribution_windows: list[str] = ACTION_ATTRIBUTION_WINDOWS,
        fields: str = INSIGHTS_AD_FIELDS,
        use_async_if_range_days_gt: int = _ASYNC_REPORT_THRESHOLD_DEFAULT,
    ) -> AsyncIterator[dict]:
        """
        §6 — Insights for any object (account/campaign/adset/ad) at any level.

        Automatically uses the async report API (§6.4) when the date range
        exceeds `use_async_if_range_days_gt` days.

        Args:
            object_id:  Ad account ID (for account/campaign/adset/ad level),
                        or campaign/adset/ad ID for entity-scoped queries.
            level:      "ad" | "adset" | "campaign" | "account"
            time_range: {"since": "YYYY-MM-DD", "until": "YYYY-MM-DD"}
            breakdowns: Single breakdown string or list (e.g. "age,gender").
                        None → no breakdown (§6.1 base call).
        """
        since = date.fromisoformat(time_range["since"])
        until = date.fromisoformat(time_range["until"])
        days = (until - since).days

        params: dict = {
            "level": level,
            "time_increment": time_increment,
            "time_range": json.dumps(time_range),
            "fields": fields,
            "action_attribution_windows": json.dumps(action_attribution_windows),
            "action_breakdowns": json.dumps(ACTION_BREAKDOWNS),
            "limit": 1000,
        }
        if breakdowns is not None:
            params["breakdowns"] = (
                breakdowns if isinstance(breakdowns, str) else ",".join(breakdowns)
            )

        if days > use_async_if_range_days_gt:
            log.info(
                "get_insights: range=%d days > threshold=%d, using async report for %s",
                days,
                use_async_if_range_days_gt,
                object_id,
            )
            async for item in self._async_report(object_id, params):
                yield item
        else:
            async for item in self._paginate(f"{object_id}/insights", params):
                yield item

    # ------------------------------------------------------------------ #
    # §7 — Custom Audiences                                                #
    # ------------------------------------------------------------------ #

    async def list_custom_audiences(self, ad_account_id: str) -> AsyncIterator[dict]:
        """§7 — Custom audiences + lookalikes."""
        async for item in self._paginate(
            f"{ad_account_id}/customaudiences",
            {"fields": CUSTOM_AUDIENCE_FIELDS, "limit": 200},
        ):
            yield item

    # ------------------------------------------------------------------ #
    # §8 — Pixels + Custom Conversions                                     #
    # ------------------------------------------------------------------ #

    async def list_pixels(self, ad_account_id: str) -> list[dict]:
        """§8.1 — Pixels on the ad account."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{ad_account_id}/adspixels",
            {"fields": ADS_PIXEL_FIELDS},
        ):
            results.append(item)
        return results

    async def get_pixel_stats(
        self,
        pixel_id: str,
        start_ts: int,
        end_ts: int,
        aggregation: str = "event",
    ) -> dict:
        """§8.2 — Pixel event stats for a time window."""
        return await self._get(
            f"{pixel_id}/stats",
            {"aggregation": aggregation, "start_time": start_ts, "end_time": end_ts},
        )

    async def list_custom_conversions(self, ad_account_id: str) -> list[dict]:
        """§8.3 — Custom conversions on the ad account."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{ad_account_id}/customconversions",
            {"fields": CUSTOM_CONVERSION_FIELDS},
        ):
            results.append(item)
        return results

    # ------------------------------------------------------------------ #
    # §9 — Catalogs + Product Sets + Feeds                                 #
    # ------------------------------------------------------------------ #

    async def list_product_catalogs(self, business_id: str) -> list[dict]:
        """§9.1 — Catalogs owned by the business."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{business_id}/owned_product_catalogs",
            {"fields": PRODUCT_CATALOG_FIELDS},
        ):
            results.append(item)
        return results

    async def list_product_sets(self, catalog_id: str) -> list[dict]:
        """§9.2 — Product sets under a catalog."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{catalog_id}/product_sets",
            {"fields": PRODUCT_SET_FIELDS},
        ):
            results.append(item)
        return results

    async def list_product_feeds(self, catalog_id: str) -> list[dict]:
        """§9.3 — Product feeds under a catalog."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{catalog_id}/product_feeds",
            {"fields": PRODUCT_FEED_FIELDS},
        ):
            results.append(item)
        return results

    # ------------------------------------------------------------------ #
    # §10 — Recommendations + Delivery Diagnostics                         #
    # ------------------------------------------------------------------ #

    async def get_recommendations(self, object_id: str) -> dict:
        """§10.1 — recommendations, issues_info, learning_stage_info for any entity."""
        return await self._get(
            object_id,
            {"fields": "recommendations,issues_info,learning_stage_info,ad_review_feedback"},
        )

    async def get_delivery_estimate(
        self,
        ad_account_id: str,
        optimization_goal: str,
        targeting_spec: dict,
    ) -> dict:
        """§10.2 — Delivery estimate for a targeting spec."""
        return await self._get(
            f"{ad_account_id}/delivery_estimate",
            {
                "optimization_goal": optimization_goal,
                "targeting_spec": json.dumps(targeting_spec),
            },
        )

    # ------------------------------------------------------------------ #
    # §11 — Account Metadata                                               #
    # ------------------------------------------------------------------ #

    async def get_ad_account_details(self, ad_account_id: str) -> dict:
        """§11 — Full account metadata."""
        return await self._get(ad_account_id, {"fields": AD_ACCOUNT_FULL_FIELDS})

    async def get_ads_volume(self, ad_account_id: str) -> dict:
        """§11 — Ads volume for the account."""
        return await self._get(f"{ad_account_id}/ads_volume")

    async def get_assigned_users(self, ad_account_id: str) -> list[dict]:
        """§11 — Users assigned to the account."""
        results: list[dict] = []
        async for item in self._paginate(
            f"{ad_account_id}/assigned_users",
            {"fields": "id,name,role,tasks"},
        ):
            results.append(item)
        return results

    # ------------------------------------------------------------------ #
    # §12 — Batch                                                          #
    # ------------------------------------------------------------------ #

    async def batch(self, requests: list[dict]) -> list[dict]:
        """
        §12 — Up to 50 sub-requests in a single HTTP call.

        Each request dict: {"method": "GET", "relative_url": "act_123/campaigns?..."}
        Returns a list of sub-responses (each has "code" and "body" as a JSON string).
        """
        resp = await self._http.post(
            f"{self._base}/",
            data={
                "access_token": self._token,
                "batch": json.dumps(requests),
            },
            timeout=120,
        )
        await self._rl.record(resp.headers, "batch")
        if resp.status_code >= 400:
            raise MetaAPIError(resp.status_code, resp.json().get("error", {}))
        return resp.json()

    # ------------------------------------------------------------------ #
    # Internal: HTTP + pagination + async reports                          #
    # ------------------------------------------------------------------ #

    async def _request(self, url: str, params: dict | None = None) -> dict:
        """
        Single GET with tenacity retry.
        Always injects access_token so callers don't have to.
        """
        p = {"access_token": self._token, **(params or {})}
        async for attempt in AsyncRetrying(
            retry=retry_if_exception(_is_retryable),
            wait=wait_exponential(multiplier=2, min=4, max=120),
            stop=stop_after_attempt(6),
            reraise=True,
        ):
            with attempt:
                resp = await self._http.get(url, params=p, timeout=60)
                await self._rl.record(resp.headers, url)
                if resp.status_code >= 400:
                    try:
                        err = resp.json().get("error", {})
                    except Exception:
                        err = {"message": resp.text}
                    raise MetaAPIError(resp.status_code, err)
                return resp.json()

    async def _get(self, path: str, params: dict | None = None) -> dict:
        """GET /{base}/{path} — convenience wrapper."""
        return await self._request(f"{self._base}/{path.lstrip('/')}", params)

    async def _paginate(
        self,
        path: str,
        params: dict | None = None,
    ) -> AsyncIterator[dict]:
        """
        Auto-paginate a list endpoint, following paging.next until exhausted.
        Mirrors the bash paginate() helper in the curl script.
        """
        url: str = f"{self._base}/{path.lstrip('/')}"
        # First call uses provided params; subsequent calls use the full paging.next
        # URL which already contains all query params — we still inject access_token.
        current_params: dict | None = params
        while url:
            data = await self._request(url, current_params)
            for item in data.get("data", []):
                yield item
            next_url: str | None = data.get("paging", {}).get("next")
            if not next_url:
                break
            url = next_url
            # paging.next is a fully-qualified URL with all params baked in;
            # _request will still add access_token (safe — same value).
            current_params = None

    async def _async_report(
        self,
        object_id: str,
        params: dict,
    ) -> AsyncIterator[dict]:
        """
        §6.4 — Async report flow:
          1. POST /{object_id}/insights  → report_run_id
          2. Poll /{report_run_id}       → async_status
          3. Paginate /{report_run_id}/insights
        """
        # Step 1 — submit
        log.info("Submitting async report job for %s", object_id)
        post_url = f"{self._base}/{object_id}/insights"
        resp = await self._http.post(
            post_url,
            data={"access_token": self._token, **params},
            timeout=60,
        )
        await self._rl.record(resp.headers, post_url)
        if resp.status_code >= 400:
            try:
                err = resp.json().get("error", {})
            except Exception:
                err = {"message": resp.text}
            raise MetaAPIError(resp.status_code, err)

        report_run_id: str = resp.json()["report_run_id"]
        log.info("Async report submitted: report_run_id=%s", report_run_id)

        # Step 2 — poll every 10 s
        while True:
            status_data = await self._get(report_run_id)
            async_status: str = status_data.get("async_status", "")
            pct: int = status_data.get("async_percent_completion", 0)
            log.debug(
                "Async report %s: status=%s completion=%d%%",
                report_run_id,
                async_status,
                pct,
            )
            if async_status == "Job Completed":
                break
            if async_status == "Job Failed":
                raise RuntimeError(
                    f"Async report {report_run_id} failed: {status_data}"
                )
            await asyncio.sleep(10)

        # Step 3 — paginate results
        log.info("Fetching async report results for %s", report_run_id)
        async for item in self._paginate(f"{report_run_id}/insights", {"limit": 1000}):
            yield item
