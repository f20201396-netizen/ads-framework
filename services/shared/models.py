"""
SQLAlchemy 2.x declarative models — mirrors the alembic migration exactly.
Partitioned fact tables use __abstract__ = True pattern; queries hit the
parent table and Postgres routes to the correct partition automatically.
"""
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime, Integer, Numeric,
    String, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# =============================================================================
# DIMENSION TABLES
# =============================================================================

class Business(Base):
    __tablename__ = "businesses"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    verification_status: Mapped[str | None] = mapped_column(String)
    timezone_id: Mapped[str | None] = mapped_column(String)
    vertical: Mapped[str | None] = mapped_column(String)
    primary_page: Mapped[dict | None] = mapped_column(JSONB)
    created_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AdAccount(Base):
    __tablename__ = "ad_accounts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str | None] = mapped_column(String)
    business_id: Mapped[str | None] = mapped_column(String)  # FK businesses.id
    name: Mapped[str | None] = mapped_column(String)
    account_status: Mapped[int | None] = mapped_column(Integer)
    age: Mapped[Decimal | None] = mapped_column(Numeric)
    currency: Mapped[str | None] = mapped_column(String(10))
    timezone_id: Mapped[int | None] = mapped_column(Integer)
    timezone_name: Mapped[str | None] = mapped_column(String)
    timezone_offset_hours_utc: Mapped[Decimal | None] = mapped_column(Numeric)
    business_city: Mapped[str | None] = mapped_column(String)
    business_country_code: Mapped[str | None] = mapped_column(String(10))
    business_name: Mapped[str | None] = mapped_column(String)
    business_state: Mapped[str | None] = mapped_column(String)
    business_street: Mapped[str | None] = mapped_column(String)
    business_street2: Mapped[str | None] = mapped_column(String)
    business_zip: Mapped[str | None] = mapped_column(String)
    disable_reason: Mapped[int | None] = mapped_column(Integer)
    spend_cap: Mapped[str | None] = mapped_column(String)
    amount_spent: Mapped[str | None] = mapped_column(String)
    balance: Mapped[str | None] = mapped_column(String)
    min_campaign_group_spend_cap: Mapped[str | None] = mapped_column(String)
    min_daily_budget: Mapped[int | None] = mapped_column(Integer)
    is_personal: Mapped[bool | None] = mapped_column(Boolean)
    is_prepay_account: Mapped[bool | None] = mapped_column(Boolean)
    is_tax_id_required: Mapped[bool | None] = mapped_column(Boolean)
    is_direct_deals_enabled: Mapped[bool | None] = mapped_column(Boolean)
    is_in_3ds_authorization_enabled_market: Mapped[bool | None] = mapped_column(Boolean)
    is_notifications_enabled: Mapped[bool | None] = mapped_column(Boolean)
    is_attribution_spec_system_default: Mapped[bool | None] = mapped_column(Boolean)
    is_client_account: Mapped[bool] = mapped_column(Boolean, server_default="false")
    offsite_pixels_tos_accepted: Mapped[bool | None] = mapped_column(Boolean)
    io_number: Mapped[str | None] = mapped_column(String)
    tax_id: Mapped[str | None] = mapped_column(String)
    tax_id_status: Mapped[int | None] = mapped_column(Integer)
    tax_id_type: Mapped[str | None] = mapped_column(String)
    fb_entity: Mapped[int | None] = mapped_column(Integer)
    end_advertiser: Mapped[str | None] = mapped_column(String)
    end_advertiser_name: Mapped[str | None] = mapped_column(String)
    media_agency: Mapped[str | None] = mapped_column(String)
    partner: Mapped[str | None] = mapped_column(String)
    funding_source: Mapped[str | None] = mapped_column(String)
    user_access_expire_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rf_spec: Mapped[dict | None] = mapped_column(JSONB)
    funding_source_details: Mapped[dict | None] = mapped_column(JSONB)
    capabilities: Mapped[list | None] = mapped_column(JSONB)
    failed_delivery_checks: Mapped[list | None] = mapped_column(JSONB)
    tos_accepted: Mapped[dict | None] = mapped_column(JSONB)
    user_tasks: Mapped[list | None] = mapped_column(JSONB)
    line_numbers: Mapped[list | None] = mapped_column(JSONB)
    agency_client_declaration: Mapped[dict | None] = mapped_column(JSONB)
    extended_credit_invoice_group: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Campaign(Base):
    __tablename__ = "campaigns"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String(50))
    effective_status: Mapped[str | None] = mapped_column(String(50))
    configured_status: Mapped[str | None] = mapped_column(String(50))
    objective: Mapped[str | None] = mapped_column(String(100))
    buying_type: Mapped[str | None] = mapped_column(String(50))
    bid_strategy: Mapped[str | None] = mapped_column(String(100))
    daily_budget: Mapped[str | None] = mapped_column(String)
    lifetime_budget: Mapped[str | None] = mapped_column(String)
    budget_remaining: Mapped[str | None] = mapped_column(String)
    spend_cap: Mapped[str | None] = mapped_column(String)
    start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stop_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_campaign_id: Mapped[str | None] = mapped_column(String)
    is_skadnetwork_attribution: Mapped[bool | None] = mapped_column(Boolean)
    smart_promotion_type: Mapped[str | None] = mapped_column(String)
    last_budget_toggling_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    can_use_spend_cap: Mapped[bool | None] = mapped_column(Boolean)
    can_create_brand_lift_study: Mapped[bool | None] = mapped_column(Boolean)
    created_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    special_ad_categories: Mapped[list | None] = mapped_column(JSONB)
    special_ad_category_country: Mapped[list | None] = mapped_column(JSONB)
    promoted_object: Mapped[dict | None] = mapped_column(JSONB)
    pacing_type: Mapped[list | None] = mapped_column(JSONB)
    issues_info: Mapped[list | None] = mapped_column(JSONB)
    recommendations: Mapped[list | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AdCreative(Base):
    __tablename__ = "ad_creatives"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String(50))
    object_type: Mapped[str | None] = mapped_column(String(100))
    object_story_id: Mapped[str | None] = mapped_column(String)
    title: Mapped[str | None] = mapped_column(Text)
    body: Mapped[str | None] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(Text)
    image_hash: Mapped[str | None] = mapped_column(String)
    video_id: Mapped[str | None] = mapped_column(String)
    thumbnail_url: Mapped[str | None] = mapped_column(Text)
    call_to_action_type: Mapped[str | None] = mapped_column(String(100))
    link_url: Mapped[str | None] = mapped_column(Text)
    link_destination_display_url: Mapped[str | None] = mapped_column(Text)
    instagram_permalink_url: Mapped[str | None] = mapped_column(Text)
    effective_instagram_media_id: Mapped[str | None] = mapped_column(String)
    effective_object_story_id: Mapped[str | None] = mapped_column(String)
    url_tags: Mapped[str | None] = mapped_column(Text)
    template_url: Mapped[str | None] = mapped_column(Text)
    product_set_id: Mapped[str | None] = mapped_column(String)
    use_page_actor_override: Mapped[bool | None] = mapped_column(Boolean)
    authorization_category: Mapped[str | None] = mapped_column(String)
    branded_content_sponsor_page_id: Mapped[str | None] = mapped_column(String)
    dynamic_ad_voice: Mapped[str | None] = mapped_column(String)
    object_story_spec: Mapped[dict | None] = mapped_column(JSONB)
    asset_feed_spec: Mapped[dict | None] = mapped_column(JSONB)
    degrees_of_freedom_spec: Mapped[dict | None] = mapped_column(JSONB)
    contextual_multi_ads: Mapped[dict | None] = mapped_column(JSONB)
    recommender_settings: Mapped[dict | None] = mapped_column(JSONB)
    platform_customizations: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AdSet(Base):
    __tablename__ = "adsets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    campaign_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String(50))
    effective_status: Mapped[str | None] = mapped_column(String(50))
    configured_status: Mapped[str | None] = mapped_column(String(50))
    daily_budget: Mapped[str | None] = mapped_column(String)
    lifetime_budget: Mapped[str | None] = mapped_column(String)
    budget_remaining: Mapped[str | None] = mapped_column(String)
    bid_amount: Mapped[str | None] = mapped_column(String)
    bid_strategy: Mapped[str | None] = mapped_column(String(100))
    billing_event: Mapped[str | None] = mapped_column(String(100))
    optimization_goal: Mapped[str | None] = mapped_column(String(100))
    optimization_sub_event: Mapped[str | None] = mapped_column(String(100))
    destination_type: Mapped[str | None] = mapped_column(String(100))
    use_new_app_click: Mapped[bool | None] = mapped_column(Boolean)
    rf_prediction_id: Mapped[str | None] = mapped_column(String)
    is_dynamic_creative: Mapped[bool | None] = mapped_column(Boolean)
    lifetime_min_spend_target: Mapped[str | None] = mapped_column(String)
    lifetime_spend_cap: Mapped[str | None] = mapped_column(String)
    daily_min_spend_target: Mapped[str | None] = mapped_column(String)
    daily_spend_cap: Mapped[str | None] = mapped_column(String)
    multi_optimization_goal_weight: Mapped[str | None] = mapped_column(String)
    start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    targeting: Mapped[dict | None] = mapped_column(JSONB)
    targeting_optimization_types: Mapped[dict | None] = mapped_column(JSONB)
    promoted_object: Mapped[dict | None] = mapped_column(JSONB)
    attribution_spec: Mapped[list | None] = mapped_column(JSONB)
    pacing_type: Mapped[list | None] = mapped_column(JSONB)
    frequency_control_specs: Mapped[list | None] = mapped_column(JSONB)
    learning_stage_info: Mapped[dict | None] = mapped_column(JSONB)
    issues_info: Mapped[list | None] = mapped_column(JSONB)
    recommendations: Mapped[list | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Ad(Base):
    __tablename__ = "ads"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    adset_id: Mapped[str] = mapped_column(String, nullable=False)
    campaign_id: Mapped[str] = mapped_column(String, nullable=False)
    creative_id: Mapped[str | None] = mapped_column(String)
    name: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String(50))
    effective_status: Mapped[str | None] = mapped_column(String(50))
    configured_status: Mapped[str | None] = mapped_column(String(50))
    source_ad_id: Mapped[str | None] = mapped_column(String)
    preview_shareable_link: Mapped[str | None] = mapped_column(Text)
    bid_amount: Mapped[str | None] = mapped_column(String)
    last_updated_by_app_id: Mapped[str | None] = mapped_column(String)
    engagement_audience: Mapped[bool | None] = mapped_column(Boolean)
    demolink_hash: Mapped[str | None] = mapped_column(String)
    display_sequence: Mapped[int | None] = mapped_column(Integer)
    created_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    tracking_specs: Mapped[list | None] = mapped_column(JSONB)
    conversion_specs: Mapped[list | None] = mapped_column(JSONB)
    issues_info: Mapped[list | None] = mapped_column(JSONB)
    recommendations: Mapped[list | None] = mapped_column(JSONB)
    ad_review_feedback: Mapped[dict | None] = mapped_column(JSONB)
    adlabels: Mapped[list | None] = mapped_column(JSONB)
    targeting: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CustomAudience(Base):
    __tablename__ = "custom_audiences"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(Text)
    subtype: Mapped[str | None] = mapped_column(String(100))
    approximate_count_lower_bound: Mapped[int | None] = mapped_column(BigInteger)
    approximate_count_upper_bound: Mapped[int | None] = mapped_column(BigInteger)
    customer_file_source: Mapped[str | None] = mapped_column(String)
    retention_days: Mapped[int | None] = mapped_column(Integer)
    rule_aggregation: Mapped[str | None] = mapped_column(String)
    time_created: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    time_updated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    time_content_updated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    opt_out_link: Mapped[str | None] = mapped_column(Text)
    is_value_based: Mapped[bool | None] = mapped_column(Boolean)
    pixel_id: Mapped[str | None] = mapped_column(String)
    page_id: Mapped[str | None] = mapped_column(String)
    data_source: Mapped[dict | None] = mapped_column(JSONB)
    delivery_status: Mapped[dict | None] = mapped_column(JSONB)
    operation_status: Mapped[dict | None] = mapped_column(JSONB)
    permission_for_actions: Mapped[dict | None] = mapped_column(JSONB)
    rule: Mapped[dict | None] = mapped_column(JSONB)
    lookalike_spec: Mapped[dict | None] = mapped_column(JSONB)
    external_event_source: Mapped[dict | None] = mapped_column(JSONB)
    sharing_status: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AdsPixel(Base):
    __tablename__ = "ads_pixels"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    code: Mapped[str | None] = mapped_column(Text)
    last_fired_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_created_by_business: Mapped[bool | None] = mapped_column(Boolean)
    is_unavailable: Mapped[bool | None] = mapped_column(Boolean)
    data_use_setting: Mapped[str | None] = mapped_column(String)
    first_party_cookie_status: Mapped[str | None] = mapped_column(String)
    enable_automatic_matching: Mapped[bool | None] = mapped_column(Boolean)
    can_proxy: Mapped[bool | None] = mapped_column(Boolean)
    creation_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    automatic_matching_fields: Mapped[list | None] = mapped_column(JSONB)
    owner_business: Mapped[dict | None] = mapped_column(JSONB)
    owner_ad_account: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CustomConversion(Base):
    __tablename__ = "custom_conversions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(Text)
    custom_event_type: Mapped[str | None] = mapped_column(String(100))
    default_conversion_value: Mapped[Decimal | None] = mapped_column(Numeric)
    event_source_type: Mapped[str | None] = mapped_column(String)
    aggregation_rule: Mapped[str | None] = mapped_column(String)
    retention_days: Mapped[int | None] = mapped_column(Integer)
    creation_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    first_fired_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_fired_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_archived: Mapped[bool | None] = mapped_column(Boolean)
    is_unavailable: Mapped[bool | None] = mapped_column(Boolean)
    rule: Mapped[dict | None] = mapped_column(JSONB)
    pixel: Mapped[dict | None] = mapped_column(JSONB)
    offline_conversion_data_set: Mapped[dict | None] = mapped_column(JSONB)
    data_sources: Mapped[list | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProductCatalog(Base):
    __tablename__ = "product_catalogs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    business_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    product_count: Mapped[int | None] = mapped_column(Integer)
    vertical: Mapped[str | None] = mapped_column(String)
    da_display_settings: Mapped[dict | None] = mapped_column(JSONB)
    business: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProductSet(Base):
    __tablename__ = "product_sets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    catalog_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    product_count: Mapped[int | None] = mapped_column(Integer)
    auto_creation_url: Mapped[str | None] = mapped_column(Text)
    filter: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProductFeed(Base):
    __tablename__ = "product_feeds"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    catalog_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    file_name: Mapped[str | None] = mapped_column(String)
    country: Mapped[str | None] = mapped_column(String(10))
    deletion_enabled: Mapped[bool | None] = mapped_column(Boolean)
    schedule: Mapped[dict | None] = mapped_column(JSONB)
    latest_upload: Mapped[dict | None] = mapped_column(JSONB)
    update_schedule: Mapped[dict | None] = mapped_column(JSONB)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# =============================================================================
# FACT TABLES (partitioned — ORM maps to parent table, Postgres routes)
# =============================================================================

class InsightsDaily(Base):
    __tablename__ = "insights_daily"

    ad_id: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    attribution_window: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str | None] = mapped_column(String)
    campaign_id: Mapped[str | None] = mapped_column(String)
    adset_id: Mapped[str | None] = mapped_column(String)
    account_name: Mapped[str | None] = mapped_column(String)
    account_currency: Mapped[str | None] = mapped_column(String)
    campaign_name: Mapped[str | None] = mapped_column(String)
    adset_name: Mapped[str | None] = mapped_column(String)
    ad_name: Mapped[str | None] = mapped_column(String)
    objective: Mapped[str | None] = mapped_column(String)
    buying_type: Mapped[str | None] = mapped_column(String)
    optimization_goal: Mapped[str | None] = mapped_column(String)
    attribution_setting: Mapped[str | None] = mapped_column(String)
    engagement_rate_ranking: Mapped[str | None] = mapped_column(String)
    quality_ranking: Mapped[str | None] = mapped_column(String)
    conversion_rate_ranking: Mapped[str | None] = mapped_column(String)
    impressions: Mapped[int | None] = mapped_column(BigInteger)
    reach: Mapped[int | None] = mapped_column(BigInteger)
    frequency: Mapped[Decimal | None] = mapped_column(Numeric)
    spend: Mapped[Decimal | None] = mapped_column(Numeric)
    cpm: Mapped[Decimal | None] = mapped_column(Numeric)
    cpc: Mapped[Decimal | None] = mapped_column(Numeric)
    cpp: Mapped[Decimal | None] = mapped_column(Numeric)
    ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    clicks: Mapped[int | None] = mapped_column(BigInteger)
    unique_clicks: Mapped[int | None] = mapped_column(BigInteger)
    inline_link_clicks: Mapped[int | None] = mapped_column(BigInteger)
    inline_link_click_ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    unique_inline_link_clicks: Mapped[int | None] = mapped_column(BigInteger)
    unique_inline_link_click_ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    outbound_clicks: Mapped[int | None] = mapped_column(BigInteger)
    unique_outbound_clicks: Mapped[int | None] = mapped_column(BigInteger)
    outbound_clicks_ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    cost_per_inline_link_click: Mapped[Decimal | None] = mapped_column(Numeric)
    cost_per_outbound_click: Mapped[Decimal | None] = mapped_column(Numeric)
    cost_per_unique_outbound_click: Mapped[Decimal | None] = mapped_column(Numeric)
    social_spend: Mapped[Decimal | None] = mapped_column(Numeric)
    canvas_avg_view_time: Mapped[Decimal | None] = mapped_column(Numeric)
    canvas_avg_view_percent: Mapped[Decimal | None] = mapped_column(Numeric)
    instant_experience_clicks_to_open: Mapped[int | None] = mapped_column(BigInteger)
    instant_experience_clicks_to_start: Mapped[int | None] = mapped_column(BigInteger)
    full_view_impressions: Mapped[int | None] = mapped_column(BigInteger)
    full_view_reach: Mapped[int | None] = mapped_column(BigInteger)
    estimated_ad_recall_rate: Mapped[Decimal | None] = mapped_column(Numeric)
    estimated_ad_recallers: Mapped[int | None] = mapped_column(BigInteger)
    cost_per_estimated_ad_recallers: Mapped[Decimal | None] = mapped_column(Numeric)
    actions: Mapped[list | None] = mapped_column(JSONB)
    action_values: Mapped[list | None] = mapped_column(JSONB)
    unique_actions: Mapped[list | None] = mapped_column(JSONB)
    cost_per_action_type: Mapped[list | None] = mapped_column(JSONB)
    cost_per_unique_action_type: Mapped[list | None] = mapped_column(JSONB)
    conversions: Mapped[list | None] = mapped_column(JSONB)
    conversion_values: Mapped[list | None] = mapped_column(JSONB)
    cost_per_conversion: Mapped[list | None] = mapped_column(JSONB)
    purchase_roas: Mapped[list | None] = mapped_column(JSONB)
    website_purchase_roas: Mapped[list | None] = mapped_column(JSONB)
    mobile_app_purchase_roas: Mapped[list | None] = mapped_column(JSONB)
    video_play_actions: Mapped[list | None] = mapped_column(JSONB)
    video_p25_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_p50_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_p75_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_p95_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_p100_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_avg_time_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_thruplay_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_30_sec_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_continuous_2_sec_watched_actions: Mapped[list | None] = mapped_column(JSONB)
    video_play_curve_actions: Mapped[list | None] = mapped_column(JSONB)
    catalog_segment_value: Mapped[list | None] = mapped_column(JSONB)
    instant_experience_outbound_clicks: Mapped[list | None] = mapped_column(JSONB)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InsightsDailyBreakdown(Base):
    __tablename__ = "insights_daily_breakdown"

    ad_id: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    attribution_window: Mapped[str] = mapped_column(String, primary_key=True)
    breakdown_type: Mapped[str] = mapped_column(String, primary_key=True)
    breakdown_key_hash: Mapped[str] = mapped_column(String, primary_key=True)
    breakdown_key: Mapped[dict] = mapped_column(JSONB, nullable=False)
    campaign_id: Mapped[str | None] = mapped_column(String)
    adset_id: Mapped[str | None] = mapped_column(String)
    impressions: Mapped[int | None] = mapped_column(BigInteger)
    reach: Mapped[int | None] = mapped_column(BigInteger)
    spend: Mapped[Decimal | None] = mapped_column(Numeric)
    clicks: Mapped[int | None] = mapped_column(BigInteger)
    ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    cpm: Mapped[Decimal | None] = mapped_column(Numeric)
    actions: Mapped[list | None] = mapped_column(JSONB)
    action_values: Mapped[list | None] = mapped_column(JSONB)
    conversions: Mapped[list | None] = mapped_column(JSONB)
    conversion_values: Mapped[list | None] = mapped_column(JSONB)
    purchase_roas: Mapped[list | None] = mapped_column(JSONB)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InsightsAdsetDaily(Base):
    __tablename__ = "insights_adset_daily"

    adset_id: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    attribution_window: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str | None] = mapped_column(String)
    campaign_id: Mapped[str | None] = mapped_column(String)
    impressions: Mapped[int | None] = mapped_column(BigInteger)
    reach: Mapped[int | None] = mapped_column(BigInteger)
    frequency: Mapped[Decimal | None] = mapped_column(Numeric)
    spend: Mapped[Decimal | None] = mapped_column(Numeric)
    cpm: Mapped[Decimal | None] = mapped_column(Numeric)
    cpc: Mapped[Decimal | None] = mapped_column(Numeric)
    ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    clicks: Mapped[int | None] = mapped_column(BigInteger)
    actions: Mapped[list | None] = mapped_column(JSONB)
    action_values: Mapped[list | None] = mapped_column(JSONB)
    conversions: Mapped[list | None] = mapped_column(JSONB)
    purchase_roas: Mapped[list | None] = mapped_column(JSONB)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InsightsCampaignDaily(Base):
    __tablename__ = "insights_campaign_daily"

    campaign_id: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    attribution_window: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str | None] = mapped_column(String)
    impressions: Mapped[int | None] = mapped_column(BigInteger)
    reach: Mapped[int | None] = mapped_column(BigInteger)
    frequency: Mapped[Decimal | None] = mapped_column(Numeric)
    spend: Mapped[Decimal | None] = mapped_column(Numeric)
    cpm: Mapped[Decimal | None] = mapped_column(Numeric)
    cpc: Mapped[Decimal | None] = mapped_column(Numeric)
    ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    clicks: Mapped[int | None] = mapped_column(BigInteger)
    actions: Mapped[list | None] = mapped_column(JSONB)
    action_values: Mapped[list | None] = mapped_column(JSONB)
    conversions: Mapped[list | None] = mapped_column(JSONB)
    purchase_roas: Mapped[list | None] = mapped_column(JSONB)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InsightsAccountDaily(Base):
    __tablename__ = "insights_account_daily"

    account_id: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    attribution_window: Mapped[str] = mapped_column(String, primary_key=True)
    impressions: Mapped[int | None] = mapped_column(BigInteger)
    reach: Mapped[int | None] = mapped_column(BigInteger)
    frequency: Mapped[Decimal | None] = mapped_column(Numeric)
    spend: Mapped[Decimal | None] = mapped_column(Numeric)
    cpm: Mapped[Decimal | None] = mapped_column(Numeric)
    cpc: Mapped[Decimal | None] = mapped_column(Numeric)
    ctr: Mapped[Decimal | None] = mapped_column(Numeric)
    clicks: Mapped[int | None] = mapped_column(BigInteger)
    actions: Mapped[list | None] = mapped_column(JSONB)
    action_values: Mapped[list | None] = mapped_column(JSONB)
    conversions: Mapped[list | None] = mapped_column(JSONB)
    purchase_roas: Mapped[list | None] = mapped_column(JSONB)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# =============================================================================
# OPERATIONAL TABLES
# =============================================================================

class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    account_id: Mapped[str | None] = mapped_column(String)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    rows_upserted: Mapped[int] = mapped_column(Integer, server_default="0")
    request_count: Mapped[int] = mapped_column(Integer, server_default="0")
    async_report_run_id: Mapped[str | None] = mapped_column(String)
    error: Mapped[dict | None] = mapped_column(JSONB)


class ApiRateLimit(Base):
    __tablename__ = "api_rate_limits"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    account_id: Mapped[str | None] = mapped_column(String)
    endpoint: Mapped[str | None] = mapped_column(Text)
    business_use_case_usage: Mapped[dict | None] = mapped_column(JSONB)
    ad_account_usage: Mapped[dict | None] = mapped_column(JSONB)
    app_usage: Mapped[dict | None] = mapped_column(JSONB)


# =============================================================================
# ATTRIBUTION TABLES (Phase 6)
# =============================================================================

class AttributionEvent(Base):
    """
    One row per attributed event (signup / trial / conversion / repeat_conversion).
    Partitioned monthly on install_date (= user signup date).
    """
    __tablename__ = "attribution_events"

    id: Mapped[str]                  = mapped_column(String,              primary_key=True)
    install_date: Mapped[date]       = mapped_column(Date,                primary_key=True)
    user_id: Mapped[int]             = mapped_column(BigInteger,           nullable=False)
    event_name: Mapped[str]          = mapped_column(String,               nullable=False)
    event_time: Mapped[datetime]     = mapped_column(DateTime(timezone=True), nullable=False)
    days_since_signup: Mapped[int | None]  = mapped_column(Integer)
    network: Mapped[str | None]      = mapped_column(String)
    publisher_site: Mapped[str | None] = mapped_column(String)
    meta_campaign_id: Mapped[str | None] = mapped_column(String)
    meta_adset_id: Mapped[str | None]    = mapped_column(String)
    meta_creative_id: Mapped[str | None] = mapped_column(String)
    campaign_name: Mapped[str | None]    = mapped_column(Text)
    adset_name: Mapped[str | None]       = mapped_column(Text)
    creative_name: Mapped[str | None]    = mapped_column(Text)
    revenue_inr: Mapped[Decimal | None]  = mapped_column(Numeric(12, 2))
    plan_id: Mapped[str | None]          = mapped_column(String)
    is_trial: Mapped[bool]               = mapped_column(Boolean, nullable=False, server_default="false")
    is_first_payment: Mapped[bool]       = mapped_column(Boolean, nullable=False, server_default="false")
    is_reattributed: Mapped[bool]        = mapped_column(Boolean, nullable=False, server_default="false")
    is_organic: Mapped[bool]             = mapped_column(Boolean, nullable=False, server_default="false")
    is_viewthrough: Mapped[bool]         = mapped_column(Boolean, nullable=False, server_default="false")
    platform: Mapped[str | None]         = mapped_column(String)
    os_version: Mapped[str | None]       = mapped_column(String)
    device_brand: Mapped[str | None]     = mapped_column(String)
    device_model: Mapped[str | None]     = mapped_column(String)
    priority: Mapped[str | None]         = mapped_column(String)
    source_table: Mapped[str]            = mapped_column(String, nullable=False)
    raw: Mapped[dict]                    = mapped_column(JSONB,  nullable=False, server_default="{}")
    synced_at: Mapped[datetime]          = mapped_column(DateTime(timezone=True), server_default=func.now())


class AttributionSyncCursor(Base):
    __tablename__ = "attribution_sync_cursor"

    job_name: Mapped[str]                    = mapped_column(String, primary_key=True)
    last_processed_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_run_at: Mapped[datetime | None]         = mapped_column(DateTime(timezone=True))
    rows_ingested_last_run: Mapped[int]          = mapped_column(Integer, server_default="0")
    bytes_processed_last_run: Mapped[int]        = mapped_column(BigInteger, server_default="0")
    error: Mapped[dict | None]                   = mapped_column(JSONB)


class BQQueryCost(Base):
    __tablename__ = "bq_query_costs"

    id: Mapped[int]                   = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    query_label: Mapped[str | None]   = mapped_column(String)
    bytes_processed: Mapped[int | None] = mapped_column(BigInteger)
    rows_returned: Mapped[int | None]   = mapped_column(Integer)
    run_at: Mapped[datetime]            = mapped_column(DateTime(timezone=True), server_default=func.now())
    duration_ms: Mapped[int | None]     = mapped_column(Integer)


class PixelEventStatsDaily(Base):
    __tablename__ = "pixel_event_stats_daily"
    __table_args__ = (
        UniqueConstraint("pixel_id", "date", "event_name", name="uq_pixel_event_stats_daily"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pixel_id: Mapped[str] = mapped_column(String, nullable=False)
    account_id: Mapped[str | None] = mapped_column(String)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    event_name: Mapped[str] = mapped_column(String(255), nullable=False)
    count: Mapped[int | None] = mapped_column(BigInteger)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
