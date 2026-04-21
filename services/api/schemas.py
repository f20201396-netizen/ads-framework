"""Pydantic v2 response schemas for all public + admin API endpoints."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Envelope types
# ---------------------------------------------------------------------------

class Paginated(BaseModel, Generic[T]):
    data: list[T]
    cursor: str | None = None          # opaque cursor for next page; None = last page
    total: int | None = None           # row count before pagination (when cheap)


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


# ---------------------------------------------------------------------------
# Structure — Accounts
# ---------------------------------------------------------------------------

class AccountOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str | None = None
    name: str | None = None
    account_status: int | None = None
    currency: str | None = None
    timezone_name: str | None = None
    timezone_offset_hours_utc: float | None = None
    business_country_code: str | None = None
    spend_cap: str | None = None
    amount_spent: str | None = None
    balance: str | None = None
    is_client_account: bool = False
    disable_reason: int | None = None
    created_time: datetime | None = None
    last_synced_at: datetime | None = None


# ---------------------------------------------------------------------------
# Structure — Campaigns
# ---------------------------------------------------------------------------

class CampaignOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    name: str | None = None
    status: str | None = None
    effective_status: str | None = None
    objective: str | None = None
    buying_type: str | None = None
    bid_strategy: str | None = None
    daily_budget: str | None = None
    lifetime_budget: str | None = None
    budget_remaining: str | None = None
    start_time: datetime | None = None
    stop_time: datetime | None = None
    is_skadnetwork_attribution: bool | None = None
    created_time: datetime | None = None
    updated_time: datetime | None = None
    promoted_object: dict[str, Any] | None = None
    issues_info: list[Any] | None = None
    recommendations: list[Any] | None = None


# ---------------------------------------------------------------------------
# Structure — Ad Sets
# ---------------------------------------------------------------------------

class AdSetOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    campaign_id: str
    name: str | None = None
    status: str | None = None
    effective_status: str | None = None
    billing_event: str | None = None
    optimization_goal: str | None = None
    bid_strategy: str | None = None
    bid_amount: str | None = None
    daily_budget: str | None = None
    lifetime_budget: str | None = None
    budget_remaining: str | None = None
    destination_type: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    created_time: datetime | None = None
    updated_time: datetime | None = None
    is_dynamic_creative: bool | None = None
    learning_stage_info: dict[str, Any] | None = None
    issues_info: list[Any] | None = None
    recommendations: list[Any] | None = None
    targeting: dict[str, Any] | None = None
    attribution_spec: list[Any] | None = None


# ---------------------------------------------------------------------------
# Structure — Ads
# ---------------------------------------------------------------------------

class AdOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    adset_id: str
    campaign_id: str
    creative_id: str | None = None
    name: str | None = None
    status: str | None = None
    effective_status: str | None = None
    bid_amount: str | None = None
    preview_shareable_link: str | None = None
    created_time: datetime | None = None
    updated_time: datetime | None = None
    issues_info: list[Any] | None = None
    recommendations: list[Any] | None = None
    ad_review_feedback: dict[str, Any] | None = None
    tracking_specs: list[Any] | None = None


# ---------------------------------------------------------------------------
# Structure — Creatives
# ---------------------------------------------------------------------------

class CreativeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    name: str | None = None
    status: str | None = None
    object_type: str | None = None
    call_to_action_type: str | None = None
    image_url: str | None = None
    image_hash: str | None = None
    video_id: str | None = None
    thumbnail_url: str | None = None
    link_url: str | None = None
    title: str | None = None
    body: str | None = None
    instagram_permalink_url: str | None = None
    effective_object_story_id: str | None = None
    object_story_spec: dict[str, Any] | None = None
    asset_feed_spec: dict[str, Any] | None = None
    platform_customizations: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Audiences / Pixels / Conversions
# ---------------------------------------------------------------------------

class CustomAudienceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    name: str | None = None
    subtype: str | None = None
    approximate_count_lower_bound: int | None = None
    approximate_count_upper_bound: int | None = None
    is_value_based: bool | None = None
    retention_days: int | None = None
    time_created: datetime | None = None
    time_updated: datetime | None = None
    delivery_status: dict[str, Any] | None = None
    lookalike_spec: dict[str, Any] | None = None


class PixelOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    name: str | None = None
    last_fired_time: datetime | None = None
    is_unavailable: bool | None = None
    data_use_setting: str | None = None
    enable_automatic_matching: bool | None = None
    creation_time: datetime | None = None


class PixelStatRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    date: date
    event_name: str
    count: int | None = None


class CustomConversionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    name: str | None = None
    custom_event_type: str | None = None
    default_conversion_value: float | None = None
    is_archived: bool | None = None
    is_unavailable: bool | None = None
    first_fired_time: datetime | None = None
    last_fired_time: datetime | None = None


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

class CatalogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    business_id: str
    name: str | None = None
    product_count: int | None = None
    vertical: str | None = None


class ProductSetOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    catalog_id: str
    name: str | None = None
    product_count: int | None = None
    auto_creation_url: str | None = None
    filter: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Insights — flexible row (metrics are dynamic)
# ---------------------------------------------------------------------------

InsightsRowOut = dict[str, Any]   # keys: date, object_id, + requested metrics

class InsightsTimeseriesOut(BaseModel):
    data: list[InsightsRowOut]
    cursor: str | None = None


class InsightsBreakdownOut(BaseModel):
    data: list[InsightsRowOut]
    cursor: str | None = None


class RangeResult(BaseModel):
    data: list[InsightsRowOut]
    totals: dict[str, float | None]


class InsightsCompareOut(BaseModel):
    range_a: RangeResult
    range_b: RangeResult
    delta: dict[str, dict[str, float | None]]   # metric → {abs, pct}


class InsightsTopOut(BaseModel):
    data: list[InsightsRowOut]


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

class SyncRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    entity_type: str
    account_id: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    status: str
    rows_upserted: int = 0
    request_count: int = 0
    async_report_run_id: str | None = None
    error: dict[str, Any] | None = None


class ApiRateLimitOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    recorded_at: datetime
    account_id: str | None = None
    endpoint: str | None = None
    business_use_case_usage: dict[str, Any] | None = None
    ad_account_usage: dict[str, Any] | None = None
    app_usage: dict[str, Any] | None = None


class SyncTriggerOut(BaseModel):
    job: str
    status: str = "triggered"


class BackfillTriggerOut(BaseModel):
    status: str = "triggered"
    since: str
    until: str
