"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-04-21

Covers every entity and field from scripts/meta-ads-full-fetch-curl.sh:
  §1  businesses, ad_accounts
  §2  campaigns
  §3  adsets
  §4  ads
  §5  ad_creatives
  §6  insights_daily, insights_daily_breakdown,
      insights_adset_daily, insights_campaign_daily, insights_account_daily
  §7  custom_audiences
  §8  ads_pixels, pixel_event_stats_daily, custom_conversions
  §9  product_catalogs, product_sets, product_feeds
  §13 api_rate_limits
  ops sync_runs
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gin(table: str, col: str, suffix: str | None = None) -> None:
    name = suffix or col
    op.execute(f"CREATE INDEX ix_{table}_{name}_gin ON {table} USING GIN ({col})")


def _btree(table: str, *cols: str) -> None:
    col_snake = "_".join(cols)
    op.execute(
        f"CREATE INDEX ix_{table}_{col_snake} ON {table} ({', '.join(cols)})"
    )


def _insight_partitions(parent: str) -> None:
    """Monthly partitions 2024-01 → 2026-12."""
    for year in range(2024, 2027):
        for month in range(1, 13):
            start = f"{year}-{month:02d}-01"
            end = f"{year + 1}-01-01" if month == 12 else f"{year}-{month + 1:02d}-01"
            op.execute(
                f"CREATE TABLE {parent}_{year}{month:02d} "
                f"PARTITION OF {parent} "
                f"FOR VALUES FROM ('{start}') TO ('{end}')"
            )


# ---------------------------------------------------------------------------
# upgrade
# ---------------------------------------------------------------------------

def upgrade() -> None:

    # =========================================================================
    # §1 — BUSINESSES
    # =========================================================================
    op.create_table(
        "businesses",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.String()),
        sa.Column("verification_status", sa.String()),
        sa.Column("timezone_id", sa.String()),
        sa.Column("vertical", sa.String()),
        sa.Column("primary_page", JSONB),
        sa.Column("created_time", sa.DateTime(timezone=True)),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _gin("businesses", "raw")

    # =========================================================================
    # §1 — AD ACCOUNTS
    # =========================================================================
    op.create_table(
        "ad_accounts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String()),
        sa.Column("business_id", sa.String(), sa.ForeignKey("businesses.id")),
        sa.Column("name", sa.String()),
        sa.Column("account_status", sa.Integer()),
        sa.Column("age", sa.Numeric()),
        sa.Column("currency", sa.String(10)),
        sa.Column("timezone_id", sa.Integer()),
        sa.Column("timezone_name", sa.String()),
        sa.Column("timezone_offset_hours_utc", sa.Numeric()),
        sa.Column("business_city", sa.String()),
        sa.Column("business_country_code", sa.String(10)),
        sa.Column("business_name", sa.String()),
        sa.Column("business_state", sa.String()),
        sa.Column("business_street", sa.String()),
        sa.Column("business_street2", sa.String()),
        sa.Column("business_zip", sa.String()),
        sa.Column("disable_reason", sa.Integer()),
        sa.Column("spend_cap", sa.String()),
        sa.Column("amount_spent", sa.String()),
        sa.Column("balance", sa.String()),
        sa.Column("min_campaign_group_spend_cap", sa.String()),
        sa.Column("min_daily_budget", sa.Integer()),
        sa.Column("is_personal", sa.Boolean()),
        sa.Column("is_prepay_account", sa.Boolean()),
        sa.Column("is_tax_id_required", sa.Boolean()),
        sa.Column("is_direct_deals_enabled", sa.Boolean()),
        sa.Column("is_in_3ds_authorization_enabled_market", sa.Boolean()),
        sa.Column("is_notifications_enabled", sa.Boolean()),
        sa.Column("is_attribution_spec_system_default", sa.Boolean()),
        sa.Column("is_client_account", sa.Boolean(), server_default="false"),
        sa.Column("offsite_pixels_tos_accepted", sa.Boolean()),
        sa.Column("io_number", sa.String()),
        sa.Column("tax_id", sa.String()),
        sa.Column("tax_id_status", sa.Integer()),
        sa.Column("tax_id_type", sa.String()),
        sa.Column("fb_entity", sa.Integer()),
        sa.Column("end_advertiser", sa.String()),
        sa.Column("end_advertiser_name", sa.String()),
        sa.Column("media_agency", sa.String()),
        sa.Column("partner", sa.String()),
        sa.Column("funding_source", sa.String()),
        sa.Column("user_access_expire_time", sa.DateTime(timezone=True)),
        sa.Column("created_time", sa.DateTime(timezone=True)),
        # JSONB
        sa.Column("rf_spec", JSONB),
        sa.Column("funding_source_details", JSONB),
        sa.Column("capabilities", JSONB),
        sa.Column("failed_delivery_checks", JSONB),
        sa.Column("tos_accepted", JSONB),
        sa.Column("user_tasks", JSONB),
        sa.Column("line_numbers", JSONB),
        sa.Column("agency_client_declaration", JSONB),
        sa.Column("extended_credit_invoice_group", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _gin("ad_accounts", "raw")

    # =========================================================================
    # §2 — CAMPAIGNS
    # =========================================================================
    op.create_table(
        "campaigns",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String(), sa.ForeignKey("ad_accounts.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("status", sa.String(50)),
        sa.Column("effective_status", sa.String(50)),
        sa.Column("configured_status", sa.String(50)),
        sa.Column("objective", sa.String(100)),
        sa.Column("buying_type", sa.String(50)),
        sa.Column("bid_strategy", sa.String(100)),
        sa.Column("daily_budget", sa.String()),
        sa.Column("lifetime_budget", sa.String()),
        sa.Column("budget_remaining", sa.String()),
        sa.Column("spend_cap", sa.String()),
        sa.Column("start_time", sa.DateTime(timezone=True)),
        sa.Column("stop_time", sa.DateTime(timezone=True)),
        sa.Column("source_campaign_id", sa.String()),
        sa.Column("is_skadnetwork_attribution", sa.Boolean()),
        sa.Column("smart_promotion_type", sa.String()),
        sa.Column("last_budget_toggling_time", sa.DateTime(timezone=True)),
        sa.Column("can_use_spend_cap", sa.Boolean()),
        sa.Column("can_create_brand_lift_study", sa.Boolean()),
        sa.Column("created_time", sa.DateTime(timezone=True)),
        sa.Column("updated_time", sa.DateTime(timezone=True)),
        # JSONB
        sa.Column("special_ad_categories", JSONB),
        sa.Column("special_ad_category_country", JSONB),
        sa.Column("promoted_object", JSONB),
        sa.Column("pacing_type", JSONB),
        sa.Column("issues_info", JSONB),
        sa.Column("recommendations", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _btree("campaigns", "account_id")
    _btree("campaigns", "effective_status")
    _gin("campaigns", "raw")

    # =========================================================================
    # §5 — AD CREATIVES  (before ads — ads FK to creatives)
    # =========================================================================
    op.create_table(
        "ad_creatives",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String(), sa.ForeignKey("ad_accounts.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("status", sa.String(50)),
        sa.Column("object_type", sa.String(100)),
        sa.Column("object_story_id", sa.String()),
        sa.Column("title", sa.Text()),
        sa.Column("body", sa.Text()),
        sa.Column("image_url", sa.Text()),
        sa.Column("image_hash", sa.String()),
        sa.Column("video_id", sa.String()),
        sa.Column("thumbnail_url", sa.Text()),
        sa.Column("call_to_action_type", sa.String(100)),
        sa.Column("link_url", sa.Text()),
        sa.Column("link_destination_display_url", sa.Text()),
        sa.Column("instagram_permalink_url", sa.Text()),
        sa.Column("effective_instagram_media_id", sa.String()),
        sa.Column("effective_object_story_id", sa.String()),
        sa.Column("url_tags", sa.Text()),
        sa.Column("template_url", sa.Text()),
        sa.Column("product_set_id", sa.String()),
        sa.Column("use_page_actor_override", sa.Boolean()),
        sa.Column("authorization_category", sa.String()),
        sa.Column("branded_content_sponsor_page_id", sa.String()),
        sa.Column("dynamic_ad_voice", sa.String()),
        # JSONB
        sa.Column("object_story_spec", JSONB),
        sa.Column("asset_feed_spec", JSONB),
        sa.Column("degrees_of_freedom_spec", JSONB),
        sa.Column("contextual_multi_ads", JSONB),
        sa.Column("recommender_settings", JSONB),
        sa.Column("platform_customizations", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _btree("ad_creatives", "account_id")
    _gin("ad_creatives", "raw")

    # =========================================================================
    # §3 — AD SETS
    # =========================================================================
    op.create_table(
        "adsets",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String(), sa.ForeignKey("ad_accounts.id"), nullable=False),
        sa.Column("campaign_id", sa.String(), sa.ForeignKey("campaigns.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("status", sa.String(50)),
        sa.Column("effective_status", sa.String(50)),
        sa.Column("configured_status", sa.String(50)),
        sa.Column("daily_budget", sa.String()),
        sa.Column("lifetime_budget", sa.String()),
        sa.Column("budget_remaining", sa.String()),
        sa.Column("bid_amount", sa.String()),
        sa.Column("bid_strategy", sa.String(100)),
        sa.Column("billing_event", sa.String(100)),
        sa.Column("optimization_goal", sa.String(100)),
        sa.Column("optimization_sub_event", sa.String(100)),
        sa.Column("destination_type", sa.String(100)),
        sa.Column("use_new_app_click", sa.Boolean()),
        sa.Column("rf_prediction_id", sa.String()),
        sa.Column("is_dynamic_creative", sa.Boolean()),
        sa.Column("lifetime_min_spend_target", sa.String()),
        sa.Column("lifetime_spend_cap", sa.String()),
        sa.Column("daily_min_spend_target", sa.String()),
        sa.Column("daily_spend_cap", sa.String()),
        sa.Column("multi_optimization_goal_weight", sa.String()),
        sa.Column("start_time", sa.DateTime(timezone=True)),
        sa.Column("end_time", sa.DateTime(timezone=True)),
        sa.Column("created_time", sa.DateTime(timezone=True)),
        sa.Column("updated_time", sa.DateTime(timezone=True)),
        # JSONB
        sa.Column("targeting", JSONB),
        sa.Column("targeting_optimization_types", JSONB),
        sa.Column("promoted_object", JSONB),
        sa.Column("attribution_spec", JSONB),
        sa.Column("pacing_type", JSONB),
        sa.Column("frequency_control_specs", JSONB),
        sa.Column("learning_stage_info", JSONB),
        sa.Column("issues_info", JSONB),
        sa.Column("recommendations", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _btree("adsets", "account_id")
    _btree("adsets", "campaign_id")
    _btree("adsets", "effective_status")
    _gin("adsets", "raw")

    # =========================================================================
    # §4 — ADS
    # =========================================================================
    op.create_table(
        "ads",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String(), sa.ForeignKey("ad_accounts.id"), nullable=False),
        sa.Column("adset_id", sa.String(), sa.ForeignKey("adsets.id"), nullable=False),
        sa.Column("campaign_id", sa.String(), sa.ForeignKey("campaigns.id"), nullable=False),
        sa.Column("creative_id", sa.String(), sa.ForeignKey("ad_creatives.id")),
        sa.Column("name", sa.String()),
        sa.Column("status", sa.String(50)),
        sa.Column("effective_status", sa.String(50)),
        sa.Column("configured_status", sa.String(50)),
        sa.Column("source_ad_id", sa.String()),
        sa.Column("preview_shareable_link", sa.Text()),
        sa.Column("bid_amount", sa.String()),
        sa.Column("last_updated_by_app_id", sa.String()),
        sa.Column("engagement_audience", sa.Boolean()),
        sa.Column("demolink_hash", sa.String()),
        sa.Column("display_sequence", sa.Integer()),
        sa.Column("created_time", sa.DateTime(timezone=True)),
        sa.Column("updated_time", sa.DateTime(timezone=True)),
        # JSONB
        sa.Column("tracking_specs", JSONB),
        sa.Column("conversion_specs", JSONB),
        sa.Column("issues_info", JSONB),
        sa.Column("recommendations", JSONB),
        sa.Column("ad_review_feedback", JSONB),
        sa.Column("adlabels", JSONB),
        sa.Column("targeting", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _btree("ads", "account_id")
    _btree("ads", "adset_id")
    _btree("ads", "campaign_id")
    _btree("ads", "effective_status")
    _gin("ads", "raw")

    # =========================================================================
    # §7 — CUSTOM AUDIENCES
    # =========================================================================
    op.create_table(
        "custom_audiences",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String(), sa.ForeignKey("ad_accounts.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("description", sa.Text()),
        sa.Column("subtype", sa.String(100)),
        sa.Column("approximate_count_lower_bound", sa.BigInteger()),
        sa.Column("approximate_count_upper_bound", sa.BigInteger()),
        sa.Column("customer_file_source", sa.String()),
        sa.Column("retention_days", sa.Integer()),
        sa.Column("rule_aggregation", sa.String()),
        sa.Column("time_created", sa.DateTime(timezone=True)),
        sa.Column("time_updated", sa.DateTime(timezone=True)),
        sa.Column("time_content_updated", sa.DateTime(timezone=True)),
        sa.Column("opt_out_link", sa.Text()),
        sa.Column("is_value_based", sa.Boolean()),
        sa.Column("pixel_id", sa.String()),
        sa.Column("page_id", sa.String()),
        # JSONB
        sa.Column("data_source", JSONB),
        sa.Column("delivery_status", JSONB),
        sa.Column("operation_status", JSONB),
        sa.Column("permission_for_actions", JSONB),
        sa.Column("rule", JSONB),
        sa.Column("lookalike_spec", JSONB),
        sa.Column("external_event_source", JSONB),
        sa.Column("sharing_status", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _btree("custom_audiences", "account_id")
    _gin("custom_audiences", "raw")

    # =========================================================================
    # §8 — ADS PIXELS
    # =========================================================================
    op.create_table(
        "ads_pixels",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String(), sa.ForeignKey("ad_accounts.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("code", sa.Text()),
        sa.Column("last_fired_time", sa.DateTime(timezone=True)),
        sa.Column("is_created_by_business", sa.Boolean()),
        sa.Column("is_unavailable", sa.Boolean()),
        sa.Column("data_use_setting", sa.String()),
        sa.Column("first_party_cookie_status", sa.String()),
        sa.Column("enable_automatic_matching", sa.Boolean()),
        sa.Column("can_proxy", sa.Boolean()),
        sa.Column("creation_time", sa.DateTime(timezone=True)),
        # JSONB
        sa.Column("automatic_matching_fields", JSONB),
        sa.Column("owner_business", JSONB),
        sa.Column("owner_ad_account", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _btree("ads_pixels", "account_id")
    _gin("ads_pixels", "raw")

    # =========================================================================
    # §8 — CUSTOM CONVERSIONS
    # =========================================================================
    op.create_table(
        "custom_conversions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("account_id", sa.String(), sa.ForeignKey("ad_accounts.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("description", sa.Text()),
        sa.Column("custom_event_type", sa.String(100)),
        sa.Column("default_conversion_value", sa.Numeric()),
        sa.Column("event_source_type", sa.String()),
        sa.Column("aggregation_rule", sa.String()),
        sa.Column("retention_days", sa.Integer()),
        sa.Column("creation_time", sa.DateTime(timezone=True)),
        sa.Column("first_fired_time", sa.DateTime(timezone=True)),
        sa.Column("last_fired_time", sa.DateTime(timezone=True)),
        sa.Column("is_archived", sa.Boolean()),
        sa.Column("is_unavailable", sa.Boolean()),
        # JSONB
        sa.Column("rule", JSONB),
        sa.Column("pixel", JSONB),
        sa.Column("offline_conversion_data_set", JSONB),
        sa.Column("data_sources", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _btree("custom_conversions", "account_id")
    _gin("custom_conversions", "raw")

    # =========================================================================
    # §9 — PRODUCT CATALOGS / SETS / FEEDS
    # =========================================================================
    op.create_table(
        "product_catalogs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("business_id", sa.String(), sa.ForeignKey("businesses.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("product_count", sa.Integer()),
        sa.Column("vertical", sa.String()),
        sa.Column("da_display_settings", JSONB),
        sa.Column("business", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _gin("product_catalogs", "raw")

    op.create_table(
        "product_sets",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("catalog_id", sa.String(), sa.ForeignKey("product_catalogs.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("product_count", sa.Integer()),
        sa.Column("auto_creation_url", sa.Text()),
        sa.Column("filter", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _gin("product_sets", "raw")

    op.create_table(
        "product_feeds",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("catalog_id", sa.String(), sa.ForeignKey("product_catalogs.id"), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("file_name", sa.String()),
        sa.Column("country", sa.String(10)),
        sa.Column("deletion_enabled", sa.Boolean()),
        sa.Column("schedule", JSONB),
        sa.Column("latest_upload", JSONB),
        sa.Column("update_schedule", JSONB),
        sa.Column("raw", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
    )
    _gin("product_feeds", "raw")

    # =========================================================================
    # §6 — FACT TABLES (declarative monthly partitioning on `date`)
    # =========================================================================

    # --- insights_daily -------------------------------------------------------
    op.execute("""
        CREATE TABLE insights_daily (
            ad_id                               TEXT NOT NULL,
            date                                DATE NOT NULL,
            attribution_window                  TEXT NOT NULL,
            account_id                          TEXT,
            campaign_id                         TEXT,
            adset_id                            TEXT,
            account_name                        TEXT,
            account_currency                    TEXT,
            campaign_name                       TEXT,
            adset_name                          TEXT,
            ad_name                             TEXT,
            objective                           TEXT,
            buying_type                         TEXT,
            optimization_goal                   TEXT,
            attribution_setting                 TEXT,
            engagement_rate_ranking             TEXT,
            quality_ranking                     TEXT,
            conversion_rate_ranking             TEXT,
            impressions                         BIGINT,
            reach                               BIGINT,
            frequency                           NUMERIC,
            spend                               NUMERIC,
            cpm                                 NUMERIC,
            cpc                                 NUMERIC,
            cpp                                 NUMERIC,
            ctr                                 NUMERIC,
            clicks                              BIGINT,
            unique_clicks                       BIGINT,
            inline_link_clicks                  BIGINT,
            inline_link_click_ctr               NUMERIC,
            unique_inline_link_clicks           BIGINT,
            unique_inline_link_click_ctr        NUMERIC,
            outbound_clicks                     BIGINT,
            unique_outbound_clicks              BIGINT,
            outbound_clicks_ctr                 NUMERIC,
            cost_per_inline_link_click          NUMERIC,
            cost_per_outbound_click             NUMERIC,
            cost_per_unique_outbound_click      NUMERIC,
            social_spend                        NUMERIC,
            canvas_avg_view_time                NUMERIC,
            canvas_avg_view_percent             NUMERIC,
            instant_experience_clicks_to_open   BIGINT,
            instant_experience_clicks_to_start  BIGINT,
            full_view_impressions               BIGINT,
            full_view_reach                     BIGINT,
            estimated_ad_recall_rate            NUMERIC,
            estimated_ad_recallers              BIGINT,
            cost_per_estimated_ad_recallers     NUMERIC,
            actions                             JSONB,
            action_values                       JSONB,
            unique_actions                      JSONB,
            cost_per_action_type                JSONB,
            cost_per_unique_action_type         JSONB,
            conversions                         JSONB,
            conversion_values                   JSONB,
            cost_per_conversion                 JSONB,
            purchase_roas                       JSONB,
            website_purchase_roas               JSONB,
            mobile_app_purchase_roas            JSONB,
            video_play_actions                  JSONB,
            video_p25_watched_actions           JSONB,
            video_p50_watched_actions           JSONB,
            video_p75_watched_actions           JSONB,
            video_p95_watched_actions           JSONB,
            video_p100_watched_actions          JSONB,
            video_avg_time_watched_actions      JSONB,
            video_thruplay_watched_actions      JSONB,
            video_30_sec_watched_actions        JSONB,
            video_continuous_2_sec_watched_actions JSONB,
            video_play_curve_actions            JSONB,
            catalog_segment_value               JSONB,
            instant_experience_outbound_clicks  JSONB,
            synced_at                           TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (ad_id, date, attribution_window)
        ) PARTITION BY RANGE (date)
    """)
    _insight_partitions("insights_daily")
    op.execute("CREATE INDEX ix_insights_daily_account_date  ON insights_daily (account_id,  date DESC)")
    op.execute("CREATE INDEX ix_insights_daily_campaign_date ON insights_daily (campaign_id, date DESC)")
    op.execute("CREATE INDEX ix_insights_daily_adset_date    ON insights_daily (adset_id,    date DESC)")
    _gin("insights_daily", "actions")
    _gin("insights_daily", "action_values")

    # --- insights_daily_breakdown ---------------------------------------------
    op.execute("""
        CREATE TABLE insights_daily_breakdown (
            ad_id               TEXT NOT NULL,
            date                DATE NOT NULL,
            attribution_window  TEXT NOT NULL,
            breakdown_type      TEXT NOT NULL,
            breakdown_key_hash  TEXT NOT NULL,
            breakdown_key       JSONB NOT NULL,
            campaign_id         TEXT,
            adset_id            TEXT,
            impressions         BIGINT,
            reach               BIGINT,
            spend               NUMERIC,
            clicks              BIGINT,
            ctr                 NUMERIC,
            cpm                 NUMERIC,
            actions             JSONB,
            action_values       JSONB,
            conversions         JSONB,
            conversion_values   JSONB,
            purchase_roas       JSONB,
            synced_at           TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (ad_id, date, breakdown_type, breakdown_key_hash, attribution_window)
        ) PARTITION BY RANGE (date)
    """)
    _insight_partitions("insights_daily_breakdown")
    op.execute("CREATE INDEX ix_insights_breakdown_ad_date   ON insights_daily_breakdown (ad_id, date DESC)")
    _gin("insights_daily_breakdown", "actions")
    _gin("insights_daily_breakdown", "breakdown_key")

    # --- insights_adset_daily -------------------------------------------------
    op.execute("""
        CREATE TABLE insights_adset_daily (
            adset_id            TEXT NOT NULL,
            date                DATE NOT NULL,
            attribution_window  TEXT NOT NULL,
            account_id          TEXT,
            campaign_id         TEXT,
            impressions         BIGINT,
            reach               BIGINT,
            frequency           NUMERIC,
            spend               NUMERIC,
            cpm                 NUMERIC,
            cpc                 NUMERIC,
            ctr                 NUMERIC,
            clicks              BIGINT,
            actions             JSONB,
            action_values       JSONB,
            conversions         JSONB,
            purchase_roas       JSONB,
            synced_at           TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (adset_id, date, attribution_window)
        ) PARTITION BY RANGE (date)
    """)
    _insight_partitions("insights_adset_daily")
    op.execute("CREATE INDEX ix_insights_adset_account_date ON insights_adset_daily (account_id, date DESC)")
    _gin("insights_adset_daily", "actions")

    # --- insights_campaign_daily ----------------------------------------------
    op.execute("""
        CREATE TABLE insights_campaign_daily (
            campaign_id         TEXT NOT NULL,
            date                DATE NOT NULL,
            attribution_window  TEXT NOT NULL,
            account_id          TEXT,
            impressions         BIGINT,
            reach               BIGINT,
            frequency           NUMERIC,
            spend               NUMERIC,
            cpm                 NUMERIC,
            cpc                 NUMERIC,
            ctr                 NUMERIC,
            clicks              BIGINT,
            actions             JSONB,
            action_values       JSONB,
            conversions         JSONB,
            purchase_roas       JSONB,
            synced_at           TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (campaign_id, date, attribution_window)
        ) PARTITION BY RANGE (date)
    """)
    _insight_partitions("insights_campaign_daily")
    op.execute("CREATE INDEX ix_insights_campaign_account_date ON insights_campaign_daily (account_id, date DESC)")
    _gin("insights_campaign_daily", "actions")

    # --- insights_account_daily -----------------------------------------------
    op.execute("""
        CREATE TABLE insights_account_daily (
            account_id          TEXT NOT NULL,
            date                DATE NOT NULL,
            attribution_window  TEXT NOT NULL,
            impressions         BIGINT,
            reach               BIGINT,
            frequency           NUMERIC,
            spend               NUMERIC,
            cpm                 NUMERIC,
            cpc                 NUMERIC,
            ctr                 NUMERIC,
            clicks              BIGINT,
            actions             JSONB,
            action_values       JSONB,
            conversions         JSONB,
            purchase_roas       JSONB,
            synced_at           TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (account_id, date, attribution_window)
        ) PARTITION BY RANGE (date)
    """)
    _insight_partitions("insights_account_daily")
    op.execute("CREATE INDEX ix_insights_account_date ON insights_account_daily (account_id, date DESC)")
    _gin("insights_account_daily", "actions")

    # =========================================================================
    # OPERATIONAL TABLES
    # =========================================================================

    op.create_table(
        "sync_runs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("entity_type", sa.String(100), nullable=False),
        sa.Column("account_id", sa.String()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("rows_upserted", sa.Integer(), server_default="0"),
        sa.Column("request_count", sa.Integer(), server_default="0"),
        sa.Column("async_report_run_id", sa.String()),
        sa.Column("error", JSONB),
    )
    _btree("sync_runs", "status")
    _btree("sync_runs", "entity_type")

    op.create_table(
        "api_rate_limits",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("account_id", sa.String()),
        sa.Column("endpoint", sa.Text()),
        sa.Column("business_use_case_usage", JSONB),
        sa.Column("ad_account_usage", JSONB),
        sa.Column("app_usage", JSONB),
    )
    _btree("api_rate_limits", "recorded_at")

    # §8.2 pixel_event_stats_daily
    op.create_table(
        "pixel_event_stats_daily",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("pixel_id", sa.String(), sa.ForeignKey("ads_pixels.id"), nullable=False),
        sa.Column("account_id", sa.String()),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("event_name", sa.String(255), nullable=False),
        sa.Column("count", sa.BigInteger()),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("pixel_id", "date", "event_name", name="uq_pixel_event_stats_daily"),
    )
    op.execute("CREATE INDEX ix_pixel_event_stats_pixel_date ON pixel_event_stats_daily (pixel_id, date DESC)")


# ---------------------------------------------------------------------------
# downgrade
# ---------------------------------------------------------------------------

def downgrade() -> None:
    # Partitioned tables — DROP CASCADE removes all child partitions
    for tbl in [
        "insights_daily",
        "insights_daily_breakdown",
        "insights_adset_daily",
        "insights_campaign_daily",
        "insights_account_daily",
    ]:
        op.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")

    for tbl in [
        "pixel_event_stats_daily",
        "api_rate_limits",
        "sync_runs",
        "product_feeds",
        "product_sets",
        "product_catalogs",
        "custom_conversions",
        "ads_pixels",
        "custom_audiences",
        "ads",
        "adsets",
        "ad_creatives",
        "campaigns",
        "ad_accounts",
        "businesses",
    ]:
        op.drop_table(tbl)
