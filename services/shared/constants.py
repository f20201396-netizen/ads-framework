"""
Field lists and enums extracted verbatim from scripts/meta-ads-full-fetch-curl.sh.
Every constant here has a 1-to-1 mapping with a --data-urlencode in the script.
"""

BASE_URL = "https://graph.facebook.com/v21.0"

# ---------------------------------------------------------------------------
# §1 — Business Manager + Ad Accounts
# ---------------------------------------------------------------------------

BUSINESSES_FIELDS = (
    "id,name,created_time,verification_status,timezone_id,primary_page,vertical"
)

AD_ACCOUNT_OWNED_FIELDS = (
    "id,account_id,name,account_status,currency,timezone_name,"
    "timezone_offset_hours_utc,business_country_code,disable_reason,"
    "spend_cap,amount_spent,balance,funding_source_details,"
    "min_campaign_group_spend_cap,min_daily_budget,capabilities,"
    "tax_id_status,business,owner,created_time"
)

AD_ACCOUNT_CLIENT_FIELDS = (
    "id,account_id,name,account_status,currency,timezone_name"
)

# §11 — full account metadata
AD_ACCOUNT_FULL_FIELDS = (
    "name,account_id,account_status,age,agency_client_declaration,balance,"
    "business,business_city,business_country_code,business_name,business_state,"
    "business_street,business_street2,business_zip,capabilities,created_time,"
    "currency,disable_reason,end_advertiser,end_advertiser_name,"
    "extended_credit_invoice_group,failed_delivery_checks,fb_entity,"
    "funding_source,funding_source_details,has_migrated_permissions,io_number,"
    "is_attribution_spec_system_default,is_direct_deals_enabled,"
    "is_in_3ds_authorization_enabled_market,is_notifications_enabled,"
    "is_personal,is_prepay_account,is_tax_id_required,line_numbers,"
    "media_agency,min_campaign_group_spend_cap,min_daily_budget,"
    "offsite_pixels_tos_accepted,owner,partner,rf_spec,spend_cap,tax_id,"
    "tax_id_status,tax_id_type,timezone_id,timezone_name,"
    "timezone_offset_hours_utc,tos_accepted,user_tasks,user_access_expire_time"
)

# ---------------------------------------------------------------------------
# §2 — Campaigns
# ---------------------------------------------------------------------------

CAMPAIGN_FIELDS = (
    "id,name,status,effective_status,configured_status,objective,buying_type,"
    "special_ad_categories,special_ad_category_country,bid_strategy,"
    "daily_budget,lifetime_budget,budget_remaining,spend_cap,start_time,"
    "stop_time,created_time,updated_time,source_campaign_id,"
    "is_skadnetwork_attribution,smart_promotion_type,promoted_object,"
    "pacing_type,issues_info,recommendations,last_budget_toggling_time,"
    "can_use_spend_cap,can_create_brand_lift_study"
)

# ---------------------------------------------------------------------------
# §3 — Ad Sets
# ---------------------------------------------------------------------------

ADSET_FIELDS = (
    "id,name,campaign_id,status,effective_status,configured_status,"
    "daily_budget,lifetime_budget,budget_remaining,bid_amount,bid_strategy,"
    "billing_event,optimization_goal,optimization_sub_event,targeting,"
    "targeting_optimization_types,promoted_object,attribution_spec,"
    "destination_type,start_time,end_time,created_time,updated_time,"
    "pacing_type,frequency_control_specs,use_new_app_click,rf_prediction_id,"
    "learning_stage_info,lifetime_min_spend_target,lifetime_spend_cap,"
    "daily_min_spend_target,daily_spend_cap,issues_info,recommendations,"
    "is_dynamic_creative,multi_optimization_goal_weight"
)

# ---------------------------------------------------------------------------
# §4 — Ads
# ---------------------------------------------------------------------------

AD_FIELDS = (
    "id,name,adset_id,campaign_id,status,effective_status,configured_status,"
    "creative{id},created_time,updated_time,"
    "source_ad_id,bid_amount,last_updated_by_app_id,engagement_audience,"
    "demolink_hash,display_sequence"
    # Omitted heavy JSONB fields that cause Meta HTTP 500 "reduce data":
    #   tracking_specs, conversion_specs, targeting — large JSONB per ad
    #   issues_info, recommendations, ad_review_feedback — JSONB arrays
    #   preview_shareable_link — not needed for analytics
    #   adlabels — rarely used
)

# ---------------------------------------------------------------------------
# §5 — Ad Creatives
# ---------------------------------------------------------------------------

AD_CREATIVE_FIELDS = (
    "id,name,status,object_type,object_story_id,title,body,"
    "image_url,image_hash,video_id,thumbnail_url,call_to_action_type,link_url,"
    "link_destination_display_url,instagram_permalink_url,"
    "effective_instagram_media_id,effective_object_story_id,url_tags,"
    "template_url,product_set_id,"
    "use_page_actor_override,authorization_category,"
    "branded_content_sponsor_page_id,contextual_multi_ads,dynamic_ad_voice,"
    "recommender_settings,platform_customizations"
    # Omitted heavy JSONB fields that cause Meta HTTP 500 "reduce data":
    #   asset_feed_spec, degrees_of_freedom_spec — DCO/Advantage+ creative specs
    #   object_story_spec — story/post spec, can be very large for video ads
    # None of these are needed for Phase 7 scoring. DB columns will be NULL.
)

# Preview formats from §5.2 loop
AD_PREVIEW_FORMATS = [
    "DESKTOP_FEED_STANDARD",
    "MOBILE_FEED_STANDARD",
    "MOBILE_FEED_BASIC",
    "INSTAGRAM_STANDARD",
    "INSTAGRAM_STORY",
    "INSTAGRAM_REELS",
    "FACEBOOK_REELS_MOBILE",
    "FACEBOOK_STORY_MOBILE",
    "AUDIENCE_NETWORK_OUTSTREAM_VIDEO",
    "MESSENGER_MOBILE_INBOX_MEDIA",
]

# ---------------------------------------------------------------------------
# §6 — Insights
# ---------------------------------------------------------------------------

# §6.1 — ad-level field set (trimmed to avoid Meta HTTP 500 "reduce data")
# Dropped: video_*, quality_ranking, estimated_ad_recall*, social_spend,
#          catalog_segment_value, canvas_avg_*, instant_experience_*,
#          full_view_*, cost_per_unique_*, unique_actions
INSIGHTS_AD_FIELDS = (
    "account_id,campaign_id,adset_id,ad_id,ad_name,objective,buying_type,"
    "impressions,reach,frequency,spend,cpm,cpc,ctr,clicks,unique_clicks,"
    "inline_link_clicks,inline_link_click_ctr,"
    "outbound_clicks,outbound_clicks_ctr,"
    "actions,action_values,cost_per_action_type,"
    "cost_per_inline_link_click,cost_per_outbound_click,"
    "conversions,purchase_roas,"
    "estimated_ad_recall_rate,estimated_ad_recallers,cost_per_estimated_ad_recallers,"
    "social_spend,canvas_avg_view_time,canvas_avg_view_percent,"
    "instant_experience_clicks_to_open,instant_experience_clicks_to_start,"
    "full_view_impressions,full_view_reach"
)

# §6.2 — breakdown-level field set (smaller, avoids unsupported field errors)
INSIGHTS_BREAKDOWN_FIELDS = (
    "campaign_id,adset_id,ad_id,impressions,reach,spend,clicks,ctr,cpm,"
    "actions,action_values,conversions,conversion_values,purchase_roas"
)

# §6.3 — adset/campaign/account level field set
# ID fields must be explicit so Meta includes them in the response
INSIGHTS_LEVEL_FIELDS = (
    "account_id,campaign_id,adset_id,"
    "impressions,reach,spend,clicks,ctr,cpm,actions,action_values,"
    "conversions,purchase_roas"
)

# Attribution windows — matches action_attribution_windows in §6.1 and §6.2
ACTION_ATTRIBUTION_WINDOWS = ["1d_click", "7d_click", "28d_click", "1d_view", "7d_view"]

# Action breakdowns — matches action_breakdowns in §6.1
ACTION_BREAKDOWNS = ["action_type", "action_target_id", "action_destination"]

# §6.2 breakdown loop — order matches the script exactly
INSIGHT_BREAKDOWNS = [
    "age",
    "gender",
    "age,gender",
    "country",
    "region",
    "dma",
    "impression_device",
    "publisher_platform",
    "platform_position",
    "device_platform",
    "publisher_platform,platform_position,impression_device",
    "product_id",
    "hourly_stats_aggregated_by_advertiser_time_zone",
    "hourly_stats_aggregated_by_audience_time_zone",
    "frequency_value",
    "place_page_id",
    "ad_format_asset",
    "body_asset",
    "call_to_action_asset",
    "description_asset",
    "image_asset",
    "link_url_asset",
    "title_asset",
    "video_asset",
    "skan_campaign_id",
    "skan_conversion_id",
]

# Insight levels for §6.3 loop
INSIGHT_LEVELS = ["adset", "campaign", "account"]

# ---------------------------------------------------------------------------
# §7 — Custom Audiences
# ---------------------------------------------------------------------------

CUSTOM_AUDIENCE_FIELDS = (
    "id,name,description,subtype,approximate_count_lower_bound,"
    "approximate_count_upper_bound,customer_file_source,data_source,"
    "delivery_status,operation_status,permission_for_actions,retention_days,"
    "rule,rule_aggregation,time_created,time_updated,time_content_updated,"
    "lookalike_spec,opt_out_link,is_value_based,external_event_source,"
    "pixel_id,page_id,sharing_status,account_id"
)

# ---------------------------------------------------------------------------
# §8 — Pixels, Custom Conversions
# ---------------------------------------------------------------------------

ADS_PIXEL_FIELDS = (
    "id,name,code,last_fired_time,is_created_by_business,is_unavailable,"
    "automatic_matching_fields,data_use_setting,first_party_cookie_status,"
    "enable_automatic_matching,can_proxy,owner_business,owner_ad_account,"
    "creation_time"
)

CUSTOM_CONVERSION_FIELDS = (
    "id,name,description,rule,custom_event_type,default_conversion_value,"
    "pixel,event_source_type,aggregation_rule,retention_days,creation_time,"
    "first_fired_time,last_fired_time,is_archived,is_unavailable,"
    "offline_conversion_data_set,data_sources"
)

# ---------------------------------------------------------------------------
# §9 — Catalogs
# ---------------------------------------------------------------------------

PRODUCT_CATALOG_FIELDS = "id,name,business,product_count,vertical,da_display_settings"

PRODUCT_SET_FIELDS = "id,name,filter,product_count,auto_creation_url"

PRODUCT_FEED_FIELDS = (
    "id,name,schedule,latest_upload,update_schedule,file_name,country,"
    "deletion_enabled"
)

# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

ALL_STATUSES = ["ACTIVE", "PAUSED", "DELETED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"]

ENTITY_STATUS_FILTER = (
    '[{"field":"effective_status","operator":"IN",'
    '"value":["ACTIVE","PAUSED","DELETED","ARCHIVED","IN_PROCESS","WITH_ISSUES"]}]'
)
