-- Attribution signups ingestion
--
-- One row per user who signed up in [since, until).
-- Attribution columns come from user_additional_details (Singular install data).
-- Partition column: install_date = DATE(users.created_at)
--
-- Parameters (substituted by Python before sending via EXTERNAL_QUERY):
--   {since}  TIMESTAMPTZ  lower bound (inclusive)
--   {until}  TIMESTAMPTZ  upper bound (exclusive)
--
-- Mandatory partition filter: u.created_at >= {since} AND u.created_at < {until}

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

    -- Singular attribution (may be NULL for organic / unattributed users)
    uad.network,
    uad.partner_site                                             AS publisher_site,
    uad.tracker_campaign_id                                      AS meta_campaign_id,
    uad.tracker_sub_campaign_id                                  AS meta_adset_id,
    uad.tracker_creative_id                                      AS meta_creative_id,
    uad.tracker_campaign_name                                    AS campaign_name,
    LOWER(TRIM(uad.tracker_sub_campaign_name))                   AS adset_name,
    uad.creative                                                 AS creative_name,

    -- Revenue (NULL for signup events)
    NULL::numeric                                                AS revenue_inr,
    NULL::text                                                   AS plan_id,
    FALSE                                                        AS is_trial,
    FALSE                                                        AS is_first_payment,

    -- Attribution flags (stored as text '0'/'1' in prod DB)
    (uad.is_reengagement = '1')                                  AS is_reattributed,
    (uad.is_organic = '1')                                       AS is_organic,
    (uad.is_viewthrough = '1')                                   AS is_viewthrough,

    -- Device / geo
    -- user_devices.os is PRIMARY: Singular sets platform='Android' for all Facebook users
    -- regardless of actual device; ud.os gives the true iOS/Android split.
    -- Fall back to Singular uad.platform for users not in user_devices.
    COALESCE(
        CASE
            WHEN LOWER(ud.os) LIKE 'ios%' OR LOWER(ud.os) = 'ipados' THEN 'iOS'
            WHEN LOWER(ud.os) LIKE 'android%' THEN 'Android'
        END,
        uad.platform
    )                                                            AS platform,
    uad.os_version,
    uad.device_brand,
    uad.device_model,

    -- User quality tier (PAYMENT-P0, PAYMENT-P1, etc.) — lives on users table
    u.priority,

    'user_additional_details'                                    AS source_table

FROM users u
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
  AND NOT EXISTS (
        SELECT 1 FROM "Demat_Campaigns" dc
        WHERE dc."Adset ID" = uad.tracker_sub_campaign_id
          AND dc."Adset ID" IS NOT NULL
          AND TRIM(dc."Adset ID") <> ''
      )
  AND (uad.network ILIKE '%Facebook%' OR uad.network ILIKE '%Instagram%')
ORDER BY u.created_at
