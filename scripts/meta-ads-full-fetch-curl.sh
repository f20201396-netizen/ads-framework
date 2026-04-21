#!/usr/bin/env bash
# =============================================================================
# META ADS — FULL DATA EXTRACTION via curl
# Graph API v21.0 — Marketing API
# =============================================================================
# Usage:
#   1. Fill in the env vars below (or export them in your shell)
#   2. chmod +x meta-ads-full-fetch-curl.sh
#   3. Run individual sections or the whole script
#
# Requires: curl, jq
# =============================================================================

# ---------- CONFIG ----------
export ACCESS_TOKEN="EAAB...YOUR_SYSTEM_USER_TOKEN"
export BUSINESS_ID="123456789012345"
export AD_ACCOUNT_ID="act_123456789012345"   # include the "act_" prefix
export API="https://graph.facebook.com/v21.0"
export SINCE="2024-01-01"
export UNTIL="$(date -u +%Y-%m-%d)"

# ---------- HELPER ----------
# paginate(): follows paging.next until exhausted and concatenates all data[]
paginate() {
  local url="$1"
  local out="[]"
  while [ -n "$url" ] && [ "$url" != "null" ]; do
    resp=$(curl -sS -G "$url" --data-urlencode "access_token=${ACCESS_TOKEN}")
    out=$(jq -s '.[0] + .[1].data' <(echo "$out") <(echo "$resp"))
    url=$(echo "$resp" | jq -r '.paging.next // empty')
  done
  echo "$out"
}

# =============================================================================
# 1. BUSINESS MANAGER + AD ACCOUNTS
# =============================================================================

# 1.1 List businesses you have access to
curl -sS -G "${API}/me/businesses" \
  --data-urlencode "fields=id,name,created_time,verification_status,timezone_id,primary_page,vertical" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 1.2 Owned ad accounts under a business
curl -sS -G "${API}/${BUSINESS_ID}/owned_ad_accounts" \
  --data-urlencode "fields=id,account_id,name,account_status,currency,timezone_name,timezone_offset_hours_utc,business_country_code,disable_reason,spend_cap,amount_spent,balance,funding_source_details,min_campaign_group_spend_cap,min_daily_budget,capabilities,tax_id_status,business,owner,created_time" \
  --data-urlencode "limit=200" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 1.3 Client ad accounts (agency setup)
curl -sS -G "${API}/${BUSINESS_ID}/client_ad_accounts" \
  --data-urlencode "fields=id,account_id,name,account_status,currency,timezone_name" \
  --data-urlencode "limit=200" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 2. CAMPAIGNS (all statuses incl. ARCHIVED/DELETED)
# =============================================================================

curl -sS -G "${API}/${AD_ACCOUNT_ID}/campaigns" \
  --data-urlencode 'fields=id,name,status,effective_status,configured_status,objective,buying_type,special_ad_categories,special_ad_category_country,bid_strategy,daily_budget,lifetime_budget,budget_remaining,spend_cap,start_time,stop_time,created_time,updated_time,source_campaign_id,is_skadnetwork_attribution,smart_promotion_type,promoted_object,pacing_type,issues_info,recommendations,last_budget_toggling_time,can_use_spend_cap,can_create_brand_lift_study' \
  --data-urlencode 'filtering=[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED","DELETED","ARCHIVED","IN_PROCESS","WITH_ISSUES"]}]' \
  --data-urlencode "limit=500" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 3. AD SETS
# =============================================================================

curl -sS -G "${API}/${AD_ACCOUNT_ID}/adsets" \
  --data-urlencode 'fields=id,name,campaign_id,status,effective_status,configured_status,daily_budget,lifetime_budget,budget_remaining,bid_amount,bid_strategy,billing_event,optimization_goal,optimization_sub_event,targeting,targeting_optimization_types,promoted_object,attribution_spec,destination_type,start_time,end_time,created_time,updated_time,pacing_type,frequency_control_specs,use_new_app_click,rf_prediction_id,learning_stage_info,lifetime_min_spend_target,lifetime_spend_cap,daily_min_spend_target,daily_spend_cap,issues_info,recommendations,is_dynamic_creative,multi_optimization_goal_weight' \
  --data-urlencode 'filtering=[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED","DELETED","ARCHIVED","IN_PROCESS","WITH_ISSUES"]}]' \
  --data-urlencode "limit=500" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 4. ADS
# =============================================================================

curl -sS -G "${API}/${AD_ACCOUNT_ID}/ads" \
  --data-urlencode 'fields=id,name,adset_id,campaign_id,status,effective_status,configured_status,creative{id},created_time,updated_time,tracking_specs,conversion_specs,source_ad_id,preview_shareable_link,issues_info,recommendations,bid_amount,targeting,last_updated_by_app_id,ad_review_feedback,engagement_audience,adlabels,demolink_hash,display_sequence' \
  --data-urlencode 'filtering=[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED","DELETED","ARCHIVED","IN_PROCESS","WITH_ISSUES"]}]' \
  --data-urlencode "limit=500" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 5. AD CREATIVES
# =============================================================================

# 5.1 List all creatives
curl -sS -G "${API}/${AD_ACCOUNT_ID}/adcreatives" \
  --data-urlencode 'fields=id,name,status,object_type,object_story_spec,object_story_id,title,body,image_url,image_hash,video_id,thumbnail_url,call_to_action_type,link_url,link_destination_display_url,instagram_permalink_url,effective_instagram_media_id,effective_object_story_id,url_tags,template_url,asset_feed_spec,degrees_of_freedom_spec,product_set_id,use_page_actor_override,authorization_category,branded_content_sponsor_page_id,contextual_multi_ads,dynamic_ad_voice,recommender_settings,platform_customizations' \
  --data-urlencode "limit=200" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 5.2 Previews — iterate one format at a time
#     Set CREATIVE_ID first, then loop through formats
export CREATIVE_ID="123456789"
for FORMAT in DESKTOP_FEED_STANDARD MOBILE_FEED_STANDARD MOBILE_FEED_BASIC \
              INSTAGRAM_STANDARD INSTAGRAM_STORY INSTAGRAM_REELS \
              FACEBOOK_REELS_MOBILE FACEBOOK_STORY_MOBILE \
              AUDIENCE_NETWORK_OUTSTREAM_VIDEO MESSENGER_MOBILE_INBOX_MEDIA; do
  echo "=== Preview: ${FORMAT} ==="
  curl -sS -G "${API}/${CREATIVE_ID}/previews" \
    --data-urlencode "ad_format=${FORMAT}" \
    --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .
done

# =============================================================================
# 6. INSIGHTS — daily, ad-level, full field set, all attribution windows
# =============================================================================

# 6.1 BASE — ad level, daily time_increment, no breakdown
curl -sS -G "${API}/${AD_ACCOUNT_ID}/insights" \
  --data-urlencode "level=ad" \
  --data-urlencode "time_increment=1" \
  --data-urlencode "time_range={\"since\":\"${SINCE}\",\"until\":\"${UNTIL}\"}" \
  --data-urlencode 'fields=account_id,account_name,account_currency,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,objective,buying_type,optimization_goal,impressions,reach,frequency,spend,cpm,cpc,cpp,ctr,clicks,unique_clicks,inline_link_clicks,inline_link_click_ctr,unique_inline_link_clicks,unique_inline_link_click_ctr,outbound_clicks,unique_outbound_clicks,outbound_clicks_ctr,actions,action_values,unique_actions,cost_per_action_type,cost_per_unique_action_type,cost_per_inline_link_click,cost_per_outbound_click,cost_per_unique_outbound_click,conversions,conversion_values,cost_per_conversion,purchase_roas,website_purchase_roas,mobile_app_purchase_roas,video_play_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions,video_avg_time_watched_actions,video_thruplay_watched_actions,video_30_sec_watched_actions,video_continuous_2_sec_watched_actions,video_play_curve_actions,cost_per_thruplay,engagement_rate_ranking,quality_ranking,conversion_rate_ranking,estimated_ad_recall_rate,estimated_ad_recallers,cost_per_estimated_ad_recallers,social_spend,catalog_segment_value,attribution_setting,canvas_avg_view_time,canvas_avg_view_percent,instant_experience_clicks_to_open,instant_experience_clicks_to_start,instant_experience_outbound_clicks,full_view_impressions,full_view_reach' \
  --data-urlencode 'action_attribution_windows=["1d_click","7d_click","28d_click","1d_view","7d_view"]' \
  --data-urlencode 'action_breakdowns=["action_type","action_target_id","action_destination"]' \
  --data-urlencode "limit=1000" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 6.2 BREAKDOWNS — run one curl per breakdown (some combos will 400; log and skip)
#     Loop pattern:
for BREAKDOWN in \
  "age" \
  "gender" \
  "age,gender" \
  "country" \
  "region" \
  "dma" \
  "impression_device" \
  "publisher_platform" \
  "platform_position" \
  "device_platform" \
  "publisher_platform,platform_position,impression_device" \
  "product_id" \
  "hourly_stats_aggregated_by_advertiser_time_zone" \
  "hourly_stats_aggregated_by_audience_time_zone" \
  "frequency_value" \
  "place_page_id" \
  "ad_format_asset" \
  "body_asset" \
  "call_to_action_asset" \
  "description_asset" \
  "image_asset" \
  "link_url_asset" \
  "title_asset" \
  "video_asset" \
  "skan_campaign_id" \
  "skan_conversion_id"; do
  echo "=== Breakdown: ${BREAKDOWN} ==="
  curl -sS -G "${API}/${AD_ACCOUNT_ID}/insights" \
    --data-urlencode "level=ad" \
    --data-urlencode "time_increment=1" \
    --data-urlencode "time_range={\"since\":\"${SINCE}\",\"until\":\"${UNTIL}\"}" \
    --data-urlencode "breakdowns=${BREAKDOWN}" \
    --data-urlencode "fields=campaign_id,adset_id,ad_id,impressions,reach,spend,clicks,ctr,cpm,actions,action_values,conversions,conversion_values,purchase_roas" \
    --data-urlencode 'action_attribution_windows=["1d_click","7d_click","28d_click","1d_view","7d_view"]' \
    --data-urlencode "limit=1000" \
    --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .
  sleep 1  # gentle pacing
done

# 6.3 Re-run at other levels
for LEVEL in adset campaign account; do
  echo "=== Level: ${LEVEL} ==="
  curl -sS -G "${API}/${AD_ACCOUNT_ID}/insights" \
    --data-urlencode "level=${LEVEL}" \
    --data-urlencode "time_increment=1" \
    --data-urlencode "time_range={\"since\":\"${SINCE}\",\"until\":\"${UNTIL}\"}" \
    --data-urlencode "fields=impressions,reach,spend,clicks,ctr,cpm,actions,action_values,conversions,purchase_roas" \
    --data-urlencode 'action_attribution_windows=["1d_click","7d_click","28d_click","1d_view","7d_view"]' \
    --data-urlencode "limit=1000" \
    --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .
done

# 6.4 ASYNC REPORTS — for > 90 days or > 100k rows
#     Step 1: create the job
REPORT_RUN=$(curl -sS -X POST "${API}/${AD_ACCOUNT_ID}/insights" \
  -d "level=ad" \
  -d "time_increment=1" \
  -d "time_range={\"since\":\"2023-01-01\",\"until\":\"${UNTIL}\"}" \
  -d 'fields=campaign_id,adset_id,ad_id,impressions,spend,actions' \
  -d "access_token=${ACCESS_TOKEN}")
REPORT_RUN_ID=$(echo "$REPORT_RUN" | jq -r '.report_run_id')
echo "Report run ID: ${REPORT_RUN_ID}"

#     Step 2: poll status
while true; do
  STATUS=$(curl -sS -G "${API}/${REPORT_RUN_ID}" \
    --data-urlencode "access_token=${ACCESS_TOKEN}" | jq -r '.async_status')
  echo "Status: ${STATUS}"
  [ "$STATUS" = "Job Completed" ] && break
  [ "$STATUS" = "Job Failed" ] && { echo "FAILED"; exit 1; }
  sleep 10
done

#     Step 3: fetch results
curl -sS -G "${API}/${REPORT_RUN_ID}/insights" \
  --data-urlencode "limit=1000" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 7. CUSTOM AUDIENCES + LOOKALIKES
# =============================================================================

curl -sS -G "${API}/${AD_ACCOUNT_ID}/customaudiences" \
  --data-urlencode 'fields=id,name,description,subtype,approximate_count_lower_bound,approximate_count_upper_bound,customer_file_source,data_source,delivery_status,operation_status,permission_for_actions,retention_days,rule,rule_aggregation,time_created,time_updated,time_content_updated,lookalike_spec,opt_out_link,is_value_based,external_event_source,pixel_id,page_id,sharing_status,account_id' \
  --data-urlencode "limit=200" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 8. PIXELS + CUSTOM CONVERSIONS
# =============================================================================

# 8.1 Pixels on the ad account
curl -sS -G "${API}/${AD_ACCOUNT_ID}/adspixels" \
  --data-urlencode 'fields=id,name,code,last_fired_time,is_created_by_business,is_unavailable,automatic_matching_fields,data_use_setting,first_party_cookie_status,enable_automatic_matching,can_proxy,owner_business,owner_ad_account,creation_time' \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 8.2 Pixel event stats (last 90 days)
export PIXEL_ID="1234567890"
export START_TS=$(date -u -d "90 days ago" +%s 2>/dev/null || date -u -v-90d +%s)
export END_TS=$(date -u +%s)
curl -sS -G "${API}/${PIXEL_ID}/stats" \
  --data-urlencode "aggregation=event" \
  --data-urlencode "start_time=${START_TS}" \
  --data-urlencode "end_time=${END_TS}" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 8.3 Custom conversions
curl -sS -G "${API}/${AD_ACCOUNT_ID}/customconversions" \
  --data-urlencode 'fields=id,name,description,rule,custom_event_type,default_conversion_value,pixel,event_source_type,aggregation_rule,retention_days,creation_time,first_fired_time,last_fired_time,is_archived,is_unavailable,offline_conversion_data_set,data_sources' \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 9. CATALOGS + PRODUCT SETS
# =============================================================================

# 9.1 Catalogs owned by the business
curl -sS -G "${API}/${BUSINESS_ID}/owned_product_catalogs" \
  --data-urlencode "fields=id,name,business,product_count,vertical,da_display_settings" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 9.2 Product sets under a catalog
export CATALOG_ID="987654321"
curl -sS -G "${API}/${CATALOG_ID}/product_sets" \
  --data-urlencode "fields=id,name,filter,product_count,auto_creation_url" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 9.3 Product feeds
curl -sS -G "${API}/${CATALOG_ID}/product_feeds" \
  --data-urlencode "fields=id,name,schedule,latest_upload,update_schedule,file_name,country,deletion_enabled" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 10. RECOMMENDATIONS + DELIVERY DIAGNOSTICS
# =============================================================================

# 10.1 Per-entity recommendations
export CAMPAIGN_ID="12345"
export ADSET_ID="67890"
export AD_ID="11223"

curl -sS -G "${API}/${CAMPAIGN_ID}" \
  --data-urlencode "fields=recommendations,issues_info" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

curl -sS -G "${API}/${ADSET_ID}" \
  --data-urlencode "fields=recommendations,issues_info,learning_stage_info" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

curl -sS -G "${API}/${AD_ID}" \
  --data-urlencode "fields=recommendations,issues_info,ad_review_feedback" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# 10.2 Delivery estimate (for a given targeting spec)
curl -sS -G "${API}/${AD_ACCOUNT_ID}/delivery_estimate" \
  --data-urlencode "optimization_goal=OFFSITE_CONVERSIONS" \
  --data-urlencode 'targeting_spec={"geo_locations":{"countries":["IN"]},"age_min":25,"age_max":55}' \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 11. ACCOUNT METADATA
# =============================================================================

curl -sS -G "${API}/${AD_ACCOUNT_ID}" \
  --data-urlencode 'fields=name,account_id,account_status,age,agency_client_declaration,balance,business,business_city,business_country_code,business_name,business_state,business_street,business_street2,business_zip,capabilities,created_time,currency,disable_reason,end_advertiser,end_advertiser_name,extended_credit_invoice_group,failed_delivery_checks,fb_entity,funding_source,funding_source_details,has_migrated_permissions,io_number,is_attribution_spec_system_default,is_direct_deals_enabled,is_in_3ds_authorization_enabled_market,is_notifications_enabled,is_personal,is_prepay_account,is_tax_id_required,line_numbers,media_agency,min_campaign_group_spend_cap,min_daily_budget,offsite_pixels_tos_accepted,owner,partner,rf_spec,spend_cap,tax_id,tax_id_status,tax_id_type,timezone_id,timezone_name,timezone_offset_hours_utc,tos_accepted,user_tasks,user_access_expire_time' \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

curl -sS -G "${API}/${AD_ACCOUNT_ID}/ads_volume" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

curl -sS -G "${API}/${AD_ACCOUNT_ID}/assigned_users" \
  --data-urlencode "fields=id,name,role,tasks" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" | jq .

# =============================================================================
# 12. BATCH REQUESTS — up to 50 sub-calls in one HTTP request
# =============================================================================

curl -sS -X POST "${API}/" \
  -d "access_token=${ACCESS_TOKEN}" \
  --data-urlencode 'batch=[
    {"method":"GET","relative_url":"act_123/campaigns?fields=id,name,status&limit=100"},
    {"method":"GET","relative_url":"act_123/adsets?fields=id,name,status&limit=100"},
    {"method":"GET","relative_url":"act_123/ads?fields=id,name,status&limit=100"}
  ]' | jq .

# =============================================================================
# 13. RATE-LIMIT INSPECTION — read the usage headers
# =============================================================================
# Add -D /dev/stderr (or -D headers.txt) to dump headers alongside the body

curl -sS -D /dev/stderr -G "${API}/${AD_ACCOUNT_ID}/campaigns" \
  --data-urlencode "fields=id,name" \
  --data-urlencode "limit=1" \
  --data-urlencode "access_token=${ACCESS_TOKEN}" 2>&1 | \
  grep -iE "^(x-business-use-case-usage|x-ad-account-usage|x-app-usage):"

# =============================================================================
# 14. USING paginate() HELPER — full auto-pagination example
# =============================================================================
# ALL_CAMPAIGNS=$(paginate "${API}/${AD_ACCOUNT_ID}/campaigns?fields=id,name,status&limit=500")
# echo "$ALL_CAMPAIGNS" | jq '. | length'

# =============================================================================
# END
# =============================================================================
