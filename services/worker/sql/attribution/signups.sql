-- Attribution signups ingestion
--
-- One row per user who signed up in [since, until).
-- Partition column: install_date = DATE(users.created_at)
--
-- MMP cutover (2026-06-18): for users who signed up on/after this date, attribution
-- comes from AppsFlyer (appsflyer_push_events, the 'Sign_Up_Success' event); for earlier
-- users it comes from Singular (user_additional_details). Hard cut on DATE(users.created_at)
-- — no Singular fallback for post-cut users. The numeric Meta ids live in AppsFlyer's
-- raw_payload JSONB (af_c_id / af_adset_id / af_ad_id); the af_* name columns hold display
-- names only. Meta traffic = media_source = 'Facebook Ads' (Instagram is the af_channel
-- sub-platform, not a separate media_source).
--
-- Parameters (substituted by Python before sending via EXTERNAL_QUERY):
--   {since}  TIMESTAMPTZ  lower bound (inclusive)
--   {until}  TIMESTAMPTZ  upper bound (exclusive)
--
-- Mandatory partition filter: u.created_at >= {since} AND u.created_at < {until}

WITH af AS (
    -- One AppsFlyer signup-attribution row per user (earliest Sign_Up_Success wins).
    -- Bounded by event_time so the scan stays cheap; the ±2 day margin absorbs the
    -- IST/UTC offset and any install→signup delay.
    SELECT DISTINCT ON (customer_user_id)
        customer_user_id,
        raw_payload->>'af_c_id'      AS af_campaign_id,
        raw_payload->>'af_adset_id'  AS af_adset_id,
        raw_payload->>'af_ad_id'     AS af_ad_id,
        campaign                     AS af_campaign_name,
        af_adset                     AS af_adset_name,
        af_ad                        AS af_ad_name,
        af_channel                   AS af_publisher_site,
        is_retargeting               AS af_is_retargeting,
        platform                     AS af_platform,
        os_version                   AS af_os_version,
        device_model                 AS af_device_model
    FROM appsflyer_push_events
    WHERE event_name = 'Sign_Up_Success'
      AND media_source = 'Facebook Ads'
      AND event_time >= '{since}'::timestamptz - INTERVAL '2 days'
      AND event_time <  '{until}'::timestamptz + INTERVAL '2 days'
    ORDER BY customer_user_id, event_time
)
SELECT
    -- PK: stable hash so upserts are idempotent
    'signup_' || u.id::text                                      AS id,
    u.id::bigint                                                 AS user_id,
    'signup'                                                     AS event_name,
    -- Prod stores naive timestamps in IST clock values, so we treat them as
    -- 'Asia/Kolkata' to produce a correct UTC TIMESTAMPTZ for event_time
    (u.created_at AT TIME ZONE 'Asia/Kolkata')                   AS event_time,
    DATE(u.created_at)                                           AS install_date,
    0                                                            AS days_since_signup,

    -- Attribution: AppsFlyer for signups on/after the cutover, Singular before.
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN 'Facebook' ELSE uad.network END                   AS network,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_publisher_site ELSE uad.partner_site END     AS publisher_site,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_campaign_id ELSE uad.tracker_campaign_id END  AS meta_campaign_id,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_adset_id ELSE uad.tracker_sub_campaign_id END AS meta_adset_id,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_ad_id ELSE uad.tracker_creative_id END        AS meta_creative_id,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_campaign_name ELSE uad.tracker_campaign_name END AS campaign_name,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN LOWER(TRIM(af.af_adset_name))
         ELSE LOWER(TRIM(uad.tracker_sub_campaign_name)) END      AS adset_name,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_ad_name ELSE uad.creative END                 AS creative_name,

    -- Revenue (NULL for signup events)
    NULL::numeric                                                AS revenue_inr,
    NULL::text                                                   AS plan_id,
    FALSE                                                        AS is_trial,
    FALSE                                                        AS is_first_payment,

    -- Attribution flags (Singular stores them as text '0'/'1')
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN COALESCE(af.af_is_retargeting, FALSE)
         ELSE (uad.is_reengagement = '1') END                     AS is_reattributed,
    -- Post-cut rows are filtered to paid Facebook Ads traffic, so organic/viewthrough = FALSE.
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN FALSE ELSE (uad.is_organic = '1') END               AS is_organic,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN FALSE ELSE (uad.is_viewthrough = '1') END           AS is_viewthrough,

    -- Device / geo
    -- user_devices.os is PRIMARY (true iOS/Android split); fall back to the MMP platform.
    COALESCE(
        CASE
            WHEN LOWER(ud.os) LIKE 'ios%' OR LOWER(ud.os) = 'ipados' THEN 'iOS'
            WHEN LOWER(ud.os) LIKE 'android%' THEN 'Android'
        END,
        CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
             THEN af.af_platform ELSE uad.platform END
    )                                                            AS platform,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_os_version ELSE uad.os_version END            AS os_version,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN NULL ELSE uad.device_brand END                      AS device_brand,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN af.af_device_model ELSE uad.device_model END        AS device_model,

    -- User quality tier (PAYMENT-P0, PAYMENT-P1, etc.) — lives on users table
    u.priority,

    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN 'appsflyer_push_events' ELSE 'user_additional_details' END AS source_table

FROM users u
LEFT JOIN af                       ON af.customer_user_id = u.id
LEFT JOIN user_additional_details uad ON uad.user_id = u.id
LEFT JOIN user_devices             ud  ON ud.user_id  = u.id
WHERE (u.created_at AT TIME ZONE 'Asia/Kolkata') >= '{since}'
  AND (u.created_at AT TIME ZONE 'Asia/Kolkata') <  '{until}'
  -- Mirror the marketing team's Metabase report user-side exclusions
  AND u.referred_by IS NULL
  AND u.user_interest IS NULL
  AND EXISTS (
        SELECT 1 FROM user_devices ud2
        WHERE ud2.user_id = u.id
          AND ud2.os IN ('android', 'Android Web')
      )
  -- Demat exclusion on the RESOLVED adset id (AppsFlyer post-cut, Singular before)
  AND NOT EXISTS (
        SELECT 1 FROM "Demat_Campaigns" dc
        WHERE dc."Adset ID" = CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
                                   THEN af.af_adset_id ELSE uad.tracker_sub_campaign_id END
          AND dc."Adset ID" IS NOT NULL
          AND TRIM(dc."Adset ID") <> ''
      )
  -- Meta filter: post-cut requires a Facebook Ads signup event; pre-cut uses Singular network.
  AND CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
           THEN af.customer_user_id IS NOT NULL
           ELSE (uad.network ILIKE '%Facebook%' OR uad.network ILIKE '%Instagram%') END
ORDER BY u.created_at
