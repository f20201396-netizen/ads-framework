"""
Row parsers: convert raw Meta API dicts into column dicts ready for upsert.

Rules:
  - Scalar numbers come back as strings from the API — coerce explicitly.
  - JSONB fields (actions, targeting, …) are passed as-is.
  - Every parser adds raw=<full_dict> so no field is ever lost.
  - datetime strings are left as strings; PostgreSQL TIMESTAMPTZ accepts ISO 8601.
"""

import hashlib
import json
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Type coercion micro-helpers
# ---------------------------------------------------------------------------

def _dt(v) -> datetime | None:
    """Parse a Meta API timestamp string → timezone-aware datetime.

    Meta returns ISO 8601 strings in two formats:
      '2026-02-16T19:53:45+0000'   (UTC offset without colon)
      '2023-02-10T21:09:15+0530'   (IST offset without colon)
    Python 3.11+ fromisoformat() handles both. Falls back to UTC NOW on error.
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(v))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None

def _i(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _f(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _b(v) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("true", "1", "yes")


def _s(v) -> str | None:
    """Coerce to str — handles Meta returning budget/bid fields as int or str."""
    if v is None:
        return None
    return str(v)


def _act(v: str | None) -> str | None:
    """Normalise Meta account ID — always ensure 'act_' prefix."""
    if v is None:
        return None
    s = str(v)
    return s if s.startswith("act_") else f"act_{s}"


# ---------------------------------------------------------------------------
# Dimension parsers
# ---------------------------------------------------------------------------

def parse_business(raw: dict) -> dict:
    return {
        "id": raw["id"],
        "name": raw.get("name"),
        "verification_status": raw.get("verification_status"),
        "timezone_id": str(raw["timezone_id"]) if raw.get("timezone_id") else None,
        "vertical": raw.get("vertical"),
        "primary_page": raw.get("primary_page"),
        "created_time": _dt(raw.get("created_time")),
        "raw": raw,
    }


def parse_ad_account(raw: dict, is_client: bool = False) -> dict:
    return {
        "id": raw["id"],
        "account_id": raw.get("account_id"),
        "business_id": raw.get("business", {}).get("id") if isinstance(raw.get("business"), dict) else None,
        "name": raw.get("name"),
        "account_status": _i(raw.get("account_status")),
        "age": _f(raw.get("age")),
        "currency": raw.get("currency"),
        "timezone_id": _i(raw.get("timezone_id")),
        "timezone_name": raw.get("timezone_name"),
        "timezone_offset_hours_utc": _f(raw.get("timezone_offset_hours_utc")),
        "business_city": raw.get("business_city"),
        "business_country_code": raw.get("business_country_code"),
        "business_name": raw.get("business_name"),
        "business_state": raw.get("business_state"),
        "business_street": raw.get("business_street"),
        "business_street2": raw.get("business_street2"),
        "business_zip": raw.get("business_zip"),
        "disable_reason": _i(raw.get("disable_reason")),
        "spend_cap": raw.get("spend_cap"),
        "amount_spent": raw.get("amount_spent"),
        "balance": raw.get("balance"),
        "min_campaign_group_spend_cap": raw.get("min_campaign_group_spend_cap"),
        "min_daily_budget": _i(raw.get("min_daily_budget")),
        "is_personal": _b(raw.get("is_personal")),
        "is_prepay_account": _b(raw.get("is_prepay_account")),
        "is_tax_id_required": _b(raw.get("is_tax_id_required")),
        "is_direct_deals_enabled": _b(raw.get("is_direct_deals_enabled")),
        "is_in_3ds_authorization_enabled_market": _b(raw.get("is_in_3ds_authorization_enabled_market")),
        "is_notifications_enabled": _b(raw.get("is_notifications_enabled")),
        "is_attribution_spec_system_default": _b(raw.get("is_attribution_spec_system_default")),
        "is_client_account": is_client,
        "offsite_pixels_tos_accepted": _b(raw.get("offsite_pixels_tos_accepted")),
        "io_number": raw.get("io_number"),
        "tax_id": raw.get("tax_id"),
        "tax_id_status": _i(raw.get("tax_id_status")),
        "tax_id_type": raw.get("tax_id_type"),
        "fb_entity": _i(raw.get("fb_entity")),
        "end_advertiser": raw.get("end_advertiser"),
        "end_advertiser_name": raw.get("end_advertiser_name"),
        "media_agency": raw.get("media_agency"),
        "partner": raw.get("partner"),
        "funding_source": raw.get("funding_source"),
        "user_access_expire_time": _dt(raw.get("user_access_expire_time")),
        "created_time": _dt(raw.get("created_time")),
        # JSONB
        "rf_spec": raw.get("rf_spec"),
        "funding_source_details": raw.get("funding_source_details"),
        "capabilities": raw.get("capabilities"),
        "failed_delivery_checks": raw.get("failed_delivery_checks"),
        "tos_accepted": raw.get("tos_accepted"),
        "user_tasks": raw.get("user_tasks"),
        "line_numbers": raw.get("line_numbers"),
        "agency_client_declaration": raw.get("agency_client_declaration"),
        "extended_credit_invoice_group": raw.get("extended_credit_invoice_group"),
        "raw": raw,
    }


def parse_campaign(raw: dict, account_id: str) -> dict:
    return {
        "id": raw["id"],
        "account_id": account_id,
        "name": raw.get("name"),
        "status": raw.get("status"),
        "effective_status": raw.get("effective_status"),
        "configured_status": raw.get("configured_status"),
        "objective": raw.get("objective"),
        "buying_type": raw.get("buying_type"),
        "bid_strategy": raw.get("bid_strategy"),
        "daily_budget": _s(raw.get("daily_budget")),
        "lifetime_budget": _s(raw.get("lifetime_budget")),
        "budget_remaining": _s(raw.get("budget_remaining")),
        "spend_cap": _s(raw.get("spend_cap")),
        "start_time": _dt(raw.get("start_time")),
        "stop_time": _dt(raw.get("stop_time")),
        "source_campaign_id": raw.get("source_campaign_id"),
        "is_skadnetwork_attribution": _b(raw.get("is_skadnetwork_attribution")),
        "smart_promotion_type": raw.get("smart_promotion_type"),
        "last_budget_toggling_time": _dt(raw.get("last_budget_toggling_time")),
        "can_use_spend_cap": _b(raw.get("can_use_spend_cap")),
        "can_create_brand_lift_study": _b(raw.get("can_create_brand_lift_study")),
        "created_time": _dt(raw.get("created_time")),
        "updated_time": _dt(raw.get("updated_time")),
        # JSONB
        "special_ad_categories": raw.get("special_ad_categories"),
        "special_ad_category_country": raw.get("special_ad_category_country"),
        "promoted_object": raw.get("promoted_object"),
        "pacing_type": raw.get("pacing_type"),
        "issues_info": raw.get("issues_info"),
        "recommendations": raw.get("recommendations"),
        "raw": raw,
    }


def parse_adset(raw: dict, account_id: str) -> dict:
    return {
        "id": raw["id"],
        "account_id": account_id,
        "campaign_id": raw.get("campaign_id"),
        "name": raw.get("name"),
        "status": raw.get("status"),
        "effective_status": raw.get("effective_status"),
        "configured_status": raw.get("configured_status"),
        "daily_budget": _s(raw.get("daily_budget")),
        "lifetime_budget": _s(raw.get("lifetime_budget")),
        "budget_remaining": _s(raw.get("budget_remaining")),
        "bid_amount": _s(raw.get("bid_amount")),
        "bid_strategy": raw.get("bid_strategy"),
        "billing_event": raw.get("billing_event"),
        "optimization_goal": raw.get("optimization_goal"),
        "optimization_sub_event": raw.get("optimization_sub_event"),
        "destination_type": raw.get("destination_type"),
        "use_new_app_click": _b(raw.get("use_new_app_click")),
        "rf_prediction_id": raw.get("rf_prediction_id"),
        "is_dynamic_creative": _b(raw.get("is_dynamic_creative")),
        "lifetime_min_spend_target": _s(raw.get("lifetime_min_spend_target")),
        "lifetime_spend_cap": _s(raw.get("lifetime_spend_cap")),
        "daily_min_spend_target": _s(raw.get("daily_min_spend_target")),
        "daily_spend_cap": _s(raw.get("daily_spend_cap")),
        "multi_optimization_goal_weight": raw.get("multi_optimization_goal_weight"),
        "start_time": _dt(raw.get("start_time")),
        "end_time": _dt(raw.get("end_time")),
        "created_time": _dt(raw.get("created_time")),
        "updated_time": _dt(raw.get("updated_time")),
        # JSONB
        "targeting": raw.get("targeting"),
        "targeting_optimization_types": raw.get("targeting_optimization_types"),
        "promoted_object": raw.get("promoted_object"),
        "attribution_spec": raw.get("attribution_spec"),
        "pacing_type": raw.get("pacing_type"),
        "frequency_control_specs": raw.get("frequency_control_specs"),
        "learning_stage_info": raw.get("learning_stage_info"),
        "issues_info": raw.get("issues_info"),
        "recommendations": raw.get("recommendations"),
        "raw": raw,
    }


def parse_ad(raw: dict, account_id: str) -> dict:
    creative_id = None
    creative = raw.get("creative")
    if isinstance(creative, dict):
        creative_id = creative.get("id")
    return {
        "id": raw["id"],
        "account_id": account_id,
        "adset_id": raw.get("adset_id"),
        "campaign_id": raw.get("campaign_id"),
        "creative_id": creative_id,
        "name": raw.get("name"),
        "status": raw.get("status"),
        "effective_status": raw.get("effective_status"),
        "configured_status": raw.get("configured_status"),
        "source_ad_id": raw.get("source_ad_id"),
        "preview_shareable_link": raw.get("preview_shareable_link"),
        "bid_amount": _s(raw.get("bid_amount")),
        "last_updated_by_app_id": raw.get("last_updated_by_app_id"),
        "engagement_audience": _b(raw.get("engagement_audience")),
        "demolink_hash": raw.get("demolink_hash"),
        "display_sequence": _i(raw.get("display_sequence")),
        "created_time": _dt(raw.get("created_time")),
        "updated_time": _dt(raw.get("updated_time")),
        # JSONB
        "tracking_specs": raw.get("tracking_specs"),
        "conversion_specs": raw.get("conversion_specs"),
        "issues_info": raw.get("issues_info"),
        "recommendations": raw.get("recommendations"),
        "ad_review_feedback": raw.get("ad_review_feedback"),
        "adlabels": raw.get("adlabels"),
        "targeting": raw.get("targeting"),
        "raw": raw,
    }


def parse_creative(raw: dict, account_id: str) -> dict:
    return {
        "id": raw["id"],
        "account_id": account_id,
        "name": raw.get("name"),
        "status": raw.get("status"),
        "object_type": raw.get("object_type"),
        "object_story_id": raw.get("object_story_id"),
        "title": raw.get("title"),
        "body": raw.get("body"),
        "image_url": raw.get("image_url"),
        "image_hash": raw.get("image_hash"),
        "video_id": raw.get("video_id"),
        "thumbnail_url": raw.get("thumbnail_url"),
        "call_to_action_type": raw.get("call_to_action_type"),
        "link_url": raw.get("link_url"),
        "link_destination_display_url": raw.get("link_destination_display_url"),
        "instagram_permalink_url": raw.get("instagram_permalink_url"),
        "effective_instagram_media_id": raw.get("effective_instagram_media_id"),
        "effective_object_story_id": raw.get("effective_object_story_id"),
        "url_tags": raw.get("url_tags"),
        "template_url": raw.get("template_url"),
        "product_set_id": raw.get("product_set_id"),
        "use_page_actor_override": _b(raw.get("use_page_actor_override")),
        "authorization_category": raw.get("authorization_category"),
        "branded_content_sponsor_page_id": raw.get("branded_content_sponsor_page_id"),
        "dynamic_ad_voice": raw.get("dynamic_ad_voice"),
        # JSONB
        "object_story_spec": raw.get("object_story_spec"),
        "asset_feed_spec": raw.get("asset_feed_spec"),
        "degrees_of_freedom_spec": raw.get("degrees_of_freedom_spec"),
        "contextual_multi_ads": raw.get("contextual_multi_ads"),
        "recommender_settings": raw.get("recommender_settings"),
        "platform_customizations": raw.get("platform_customizations"),
        "raw": raw,
    }


def parse_custom_audience(raw: dict, account_id: str) -> dict:
    return {
        "id": raw["id"],
        "account_id": account_id,
        "name": raw.get("name"),
        "description": raw.get("description"),
        "subtype": raw.get("subtype"),
        "approximate_count_lower_bound": _i(raw.get("approximate_count_lower_bound")),
        "approximate_count_upper_bound": _i(raw.get("approximate_count_upper_bound")),
        "customer_file_source": raw.get("customer_file_source"),
        "retention_days": _i(raw.get("retention_days")),
        "rule_aggregation": raw.get("rule_aggregation"),
        "time_created": _dt(raw.get("time_created")),
        "time_updated": _dt(raw.get("time_updated")),
        "time_content_updated": _dt(raw.get("time_content_updated")),
        "opt_out_link": raw.get("opt_out_link"),
        "is_value_based": _b(raw.get("is_value_based")),
        "pixel_id": raw.get("pixel_id"),
        "page_id": raw.get("page_id"),
        # JSONB
        "data_source": raw.get("data_source"),
        "delivery_status": raw.get("delivery_status"),
        "operation_status": raw.get("operation_status"),
        "permission_for_actions": raw.get("permission_for_actions"),
        "rule": raw.get("rule"),
        "lookalike_spec": raw.get("lookalike_spec"),
        "external_event_source": raw.get("external_event_source"),
        "sharing_status": raw.get("sharing_status"),
        "raw": raw,
    }


def parse_pixel(raw: dict, account_id: str) -> dict:
    return {
        "id": raw["id"],
        "account_id": account_id,
        "name": raw.get("name"),
        "code": raw.get("code"),
        "last_fired_time": _dt(raw.get("last_fired_time")),
        "is_created_by_business": _b(raw.get("is_created_by_business")),
        "is_unavailable": _b(raw.get("is_unavailable")),
        "data_use_setting": raw.get("data_use_setting"),
        "first_party_cookie_status": raw.get("first_party_cookie_status"),
        "enable_automatic_matching": _b(raw.get("enable_automatic_matching")),
        "can_proxy": _b(raw.get("can_proxy")),
        "creation_time": _dt(raw.get("creation_time")),
        # JSONB
        "automatic_matching_fields": raw.get("automatic_matching_fields"),
        "owner_business": raw.get("owner_business"),
        "owner_ad_account": raw.get("owner_ad_account"),
        "raw": raw,
    }


def parse_custom_conversion(raw: dict, account_id: str) -> dict:
    return {
        "id": raw["id"],
        "account_id": account_id,
        "name": raw.get("name"),
        "description": raw.get("description"),
        "custom_event_type": raw.get("custom_event_type"),
        "default_conversion_value": _f(raw.get("default_conversion_value")),
        "event_source_type": raw.get("event_source_type"),
        "aggregation_rule": raw.get("aggregation_rule"),
        "retention_days": _i(raw.get("retention_days")),
        "creation_time": _dt(raw.get("creation_time")),
        "first_fired_time": _dt(raw.get("first_fired_time")),
        "last_fired_time": _dt(raw.get("last_fired_time")),
        "is_archived": _b(raw.get("is_archived")),
        "is_unavailable": _b(raw.get("is_unavailable")),
        # JSONB
        "rule": raw.get("rule"),
        "pixel": raw.get("pixel"),
        "offline_conversion_data_set": raw.get("offline_conversion_data_set"),
        "data_sources": raw.get("data_sources"),
        "raw": raw,
    }


def parse_catalog(raw: dict, business_id: str) -> dict:
    return {
        "id": raw["id"],
        "business_id": business_id,
        "name": raw.get("name"),
        "product_count": _i(raw.get("product_count")),
        "vertical": raw.get("vertical"),
        "da_display_settings": raw.get("da_display_settings"),
        "business": raw.get("business"),
        "raw": raw,
    }


def parse_product_set(raw: dict, catalog_id: str) -> dict:
    return {
        "id": raw["id"],
        "catalog_id": catalog_id,
        "name": raw.get("name"),
        "product_count": _i(raw.get("product_count")),
        "auto_creation_url": raw.get("auto_creation_url"),
        "filter": raw.get("filter"),
        "raw": raw,
    }


def parse_product_feed(raw: dict, catalog_id: str) -> dict:
    return {
        "id": raw["id"],
        "catalog_id": catalog_id,
        "name": raw.get("name"),
        "file_name": raw.get("file_name"),
        "country": raw.get("country"),
        "deletion_enabled": _b(raw.get("deletion_enabled")),
        "schedule": raw.get("schedule"),
        "latest_upload": raw.get("latest_upload"),
        "update_schedule": raw.get("update_schedule"),
        "raw": raw,
    }


# ---------------------------------------------------------------------------
# Insight parsers
# ---------------------------------------------------------------------------

def parse_insight_ad(raw: dict, attribution_window: str) -> dict:
    from datetime import date as _date
    return {
        "ad_id": raw.get("ad_id"),
        "date": _date.fromisoformat(raw["date_start"]),
        "attribution_window": attribution_window,
        "account_id": _act(raw.get("account_id")),
        "campaign_id": raw.get("campaign_id"),
        "adset_id": raw.get("adset_id"),
        "account_name": raw.get("account_name"),
        "account_currency": raw.get("account_currency"),
        "campaign_name": raw.get("campaign_name"),
        "adset_name": raw.get("adset_name"),
        "ad_name": raw.get("ad_name"),
        "objective": raw.get("objective"),
        "buying_type": raw.get("buying_type"),
        "optimization_goal": raw.get("optimization_goal"),
        "attribution_setting": raw.get("attribution_setting"),
        "engagement_rate_ranking": raw.get("engagement_rate_ranking"),
        "quality_ranking": raw.get("quality_ranking"),
        "conversion_rate_ranking": raw.get("conversion_rate_ranking"),
        # Scalar metrics — API returns strings
        "impressions": _i(raw.get("impressions")),
        "reach": _i(raw.get("reach")),
        "frequency": _f(raw.get("frequency")),
        "spend": _f(raw.get("spend")),
        "cpm": _f(raw.get("cpm")),
        "cpc": _f(raw.get("cpc")),
        "cpp": _f(raw.get("cpp")),
        "ctr": _f(raw.get("ctr")),
        "clicks": _i(raw.get("clicks")),
        "unique_clicks": _i(raw.get("unique_clicks")),
        "inline_link_clicks": _i(raw.get("inline_link_clicks")),
        "inline_link_click_ctr": _f(raw.get("inline_link_click_ctr")),
        "unique_inline_link_clicks": _i(raw.get("unique_inline_link_clicks")),
        "unique_inline_link_click_ctr": _f(raw.get("unique_inline_link_click_ctr")),
        "outbound_clicks": _i(raw.get("outbound_clicks")),
        "unique_outbound_clicks": _i(raw.get("unique_outbound_clicks")),
        "outbound_clicks_ctr": _f(raw.get("outbound_clicks_ctr")),
        "cost_per_inline_link_click": _f(raw.get("cost_per_inline_link_click")),
        "cost_per_outbound_click": _f(raw.get("cost_per_outbound_click")),
        "cost_per_unique_outbound_click": _f(raw.get("cost_per_unique_outbound_click")),
        "social_spend": _f(raw.get("social_spend")),
        "canvas_avg_view_time": _f(raw.get("canvas_avg_view_time")),
        "canvas_avg_view_percent": _f(raw.get("canvas_avg_view_percent")),
        "instant_experience_clicks_to_open": _i(raw.get("instant_experience_clicks_to_open")),
        "instant_experience_clicks_to_start": _i(raw.get("instant_experience_clicks_to_start")),
        "full_view_impressions": _i(raw.get("full_view_impressions")),
        "full_view_reach": _i(raw.get("full_view_reach")),
        "estimated_ad_recall_rate": _f(raw.get("estimated_ad_recall_rate")),
        "estimated_ad_recallers": _i(raw.get("estimated_ad_recallers")),
        "cost_per_estimated_ad_recallers": _f(raw.get("cost_per_estimated_ad_recallers")),
        # JSONB — pass lists/dicts directly
        "actions": raw.get("actions"),
        "action_values": raw.get("action_values"),
        "unique_actions": raw.get("unique_actions"),
        "cost_per_action_type": raw.get("cost_per_action_type"),
        "cost_per_unique_action_type": raw.get("cost_per_unique_action_type"),
        "conversions": raw.get("conversions"),
        "conversion_values": raw.get("conversion_values"),
        "cost_per_conversion": raw.get("cost_per_conversion"),
        "purchase_roas": raw.get("purchase_roas"),
        "website_purchase_roas": raw.get("website_purchase_roas"),
        "mobile_app_purchase_roas": raw.get("mobile_app_purchase_roas"),
        "video_play_actions": raw.get("video_play_actions"),
        "video_p25_watched_actions": raw.get("video_p25_watched_actions"),
        "video_p50_watched_actions": raw.get("video_p50_watched_actions"),
        "video_p75_watched_actions": raw.get("video_p75_watched_actions"),
        "video_p95_watched_actions": raw.get("video_p95_watched_actions"),
        "video_p100_watched_actions": raw.get("video_p100_watched_actions"),
        "video_avg_time_watched_actions": raw.get("video_avg_time_watched_actions"),
        "video_thruplay_watched_actions": raw.get("video_thruplay_watched_actions"),
        "video_30_sec_watched_actions": raw.get("video_30_sec_watched_actions"),
        "video_continuous_2_sec_watched_actions": raw.get("video_continuous_2_sec_watched_actions"),
        "video_play_curve_actions": raw.get("video_play_curve_actions"),
        "catalog_segment_value": raw.get("catalog_segment_value"),
        "instant_experience_outbound_clicks": raw.get("instant_experience_outbound_clicks"),
    }


def parse_insight_breakdown(raw: dict, breakdown_type: str, attribution_window: str) -> dict:
    # Derive a stable hash for the breakdown key tuple (age+gender combo, etc.)
    breakdown_key = _extract_breakdown_key(raw, breakdown_type)
    key_hash = hashlib.md5(
        json.dumps(breakdown_key, sort_keys=True).encode()
    ).hexdigest()
    return {
        "ad_id": raw["ad_id"],
        "date": __import__("datetime").date.fromisoformat(raw["date_start"]),
        "attribution_window": attribution_window,
        "breakdown_type": breakdown_type,
        "breakdown_key_hash": key_hash,
        "breakdown_key": breakdown_key,
        "campaign_id": raw.get("campaign_id"),
        "adset_id": raw.get("adset_id"),
        "impressions": _i(raw.get("impressions")),
        "reach": _i(raw.get("reach")),
        "spend": _f(raw.get("spend")),
        "clicks": _i(raw.get("clicks")),
        "ctr": _f(raw.get("ctr")),
        "cpm": _f(raw.get("cpm")),
        "actions": raw.get("actions"),
        "action_values": raw.get("action_values"),
        "conversions": raw.get("conversions"),
        "conversion_values": raw.get("conversion_values"),
        "purchase_roas": raw.get("purchase_roas"),
    }


def parse_insight_level(raw: dict, level_id_col: str, level_id: str, attribution_window: str) -> dict:
    """Parser for adset/campaign/account level insights."""
    return {
        level_id_col: (_act(raw.get(level_id_col)) if level_id_col == "account_id" else raw.get(level_id_col)) or level_id,
        "date": __import__("datetime").date.fromisoformat(raw["date_start"]),
        "attribution_window": attribution_window,
        # Don't overwrite level_id_col when it IS account_id
        **({"account_id": _act(raw.get("account_id")) or level_id} if level_id_col != "account_id" else {}),
        **({"campaign_id": raw.get("campaign_id")} if level_id_col == "adset_id" else {}),
        "impressions": _i(raw.get("impressions")),
        "reach": _i(raw.get("reach")),
        "frequency": _f(raw.get("frequency")),
        "spend": _f(raw.get("spend")),
        "cpm": _f(raw.get("cpm")),
        "cpc": _f(raw.get("cpc")),
        "ctr": _f(raw.get("ctr")),
        "clicks": _i(raw.get("clicks")),
        "actions": raw.get("actions"),
        "action_values": raw.get("action_values"),
        "conversions": raw.get("conversions"),
        "purchase_roas": raw.get("purchase_roas"),
    }


# ---------------------------------------------------------------------------
# Breakdown key extraction
# ---------------------------------------------------------------------------

_BREAKDOWN_KEY_FIELDS = {
    "age": ["age"],
    "gender": ["gender"],
    "age,gender": ["age", "gender"],
    "country": ["country"],
    "region": ["region"],
    "dma": ["dma"],
    "impression_device": ["impression_device"],
    "publisher_platform": ["publisher_platform"],
    "platform_position": ["platform_position"],
    "device_platform": ["device_platform"],
    "publisher_platform,platform_position,impression_device": [
        "publisher_platform", "platform_position", "impression_device"
    ],
    "product_id": ["product_id"],
    "hourly_stats_aggregated_by_advertiser_time_zone": ["hourly_stats_aggregated_by_advertiser_time_zone"],
    "hourly_stats_aggregated_by_audience_time_zone": ["hourly_stats_aggregated_by_audience_time_zone"],
    "frequency_value": ["frequency_value"],
    "place_page_id": ["place_page_id"],
    "ad_format_asset": ["ad_format_asset"],
    "body_asset": ["body_asset"],
    "call_to_action_asset": ["call_to_action_asset"],
    "description_asset": ["description_asset"],
    "image_asset": ["image_asset"],
    "link_url_asset": ["link_url_asset"],
    "title_asset": ["title_asset"],
    "video_asset": ["video_asset"],
    "skan_campaign_id": ["skan_campaign_id"],
    "skan_conversion_id": ["skan_conversion_id"],
}


def _extract_breakdown_key(raw: dict, breakdown_type: str) -> dict:
    fields = _BREAKDOWN_KEY_FIELDS.get(breakdown_type, [breakdown_type])
    return {f: raw.get(f) for f in fields}
