-- Attribution conversions ingestion.
--
-- Definitions:
--   trial             = status='CHARGED' AND amount = 1 AND plan_id matches trial
--                       pattern (ILIKE '%trial%' OR IN known free-tier plans).
--   conversion        = status='CHARGED' AND amount > 50, first paid txn per user.
--   repeat_conversion = status='CHARGED' AND amount > 50, subsequent paid txns.
--   is_mandate        = order_id ILIKE '%md%' (NACH/e-mandate auto-debit; trial
--                       plan amount > 50 typically lands here).
--
-- Partition column: install_date = DATE(users.created_at)
--
-- MMP cutover (2026-06-18): a conversion's attribution is the user's SIGNUP attribution.
-- If the user signed up on/after the cutover (DATE(users.created_at) >= 2026-06-18) we read
-- it from AppsFlyer (appsflyer_push_events 'Sign_Up_Success'); otherwise from Singular
-- (user_additional_details). Driven by signup date, NOT payment date — so a post-cutover
-- payment for a pre-cutover user still reads Singular. Numeric Meta ids come from AppsFlyer's
-- raw_payload (af_c_id / af_adset_id / af_ad_id). Meta = media_source = 'Facebook Ads'.
--
-- Parameters (substituted by Python):
--   {since}  TIMESTAMPTZ  lower bound on payment_date (inclusive)
--   {until}  TIMESTAMPTZ  upper bound on payment_date (exclusive)

WITH ranked AS (
    SELECT
        uth.id,
        uth.user_id,
        uth.payment_date,
        uth.amount,
        uth.plan_id,
        uth.order_id,
        -- A real trial: amount = 1 with trial-pattern plan_id.
        -- amount > 50 on a trial-plan is the post-trial mandate charge (paid branch).
        (uth.amount = 1
         AND (uth.plan_id ILIKE '%trial%'
              OR uth.plan_id IN ('plan_000','plan_000_plus','plan_000_super'))
        )                                                AS is_real_trial,
        -- Rank only among amount > 50 transactions per user (matches Metabase
        -- first_payments CTE which filters amount > 50 before ranking).
        ROW_NUMBER() OVER (
            PARTITION BY uth.user_id, (uth.amount > 50)
            ORDER BY uth.payment_date
        )                                                AS paid_rank
    FROM user_transaction_history uth
    WHERE uth.status = 'CHARGED'
      AND (
            uth.amount > 50
            OR (uth.amount = 1
                AND (uth.plan_id ILIKE '%trial%'
                     OR uth.plan_id IN ('plan_000','plan_000_plus','plan_000_super')))
          )
),
af AS (
    -- One AppsFlyer signup-attribution row per user (earliest Sign_Up_Success wins).
    -- Anchored at the cutover (not the payment window): a converting user may have signed
    -- up weeks before paying, so we cover all post-cutover signup events up to {until}.
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
      AND event_time >= DATE '2026-06-18' - INTERVAL '1 day'
      AND event_time <  '{until}'::timestamptz + INTERVAL '2 days'
    ORDER BY customer_user_id, event_time
)
SELECT
    'txn_' || r.id::text                                         AS id,
    r.user_id::bigint                                            AS user_id,

    CASE
        WHEN r.is_real_trial                              THEN 'trial'
        WHEN r.paid_rank = 1                              THEN 'conversion'
        ELSE                                                   'repeat_conversion'
    END                                                          AS event_name,

    -- Prod stores naive timestamps in IST clock values; cast as Asia/Kolkata to
    -- produce a correct UTC TIMESTAMPTZ for event_time.
    (r.payment_date AT TIME ZONE 'Asia/Kolkata')                 AS event_time,
    DATE(u.created_at)                                           AS install_date,
    (DATE(r.payment_date) - DATE(u.created_at))::integer         AS days_since_signup,

    -- Attribution: AppsFlyer if the user signed up on/after the cutover, else Singular.
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

    r.amount                                                     AS revenue_inr,
    r.plan_id,
    r.is_real_trial                                              AS is_trial,
    (r.amount > 50 AND r.paid_rank = 1)                          AS is_first_payment,
    (r.order_id ILIKE '%md%')                                    AS is_mandate,

    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN COALESCE(af.af_is_retargeting, FALSE)
         ELSE (uad.is_reengagement = '1') END                     AS is_reattributed,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN FALSE ELSE (uad.is_organic = '1') END               AS is_organic,
    CASE WHEN DATE(u.created_at) >= DATE '2026-06-18'
         THEN FALSE ELSE (uad.is_viewthrough = '1') END           AS is_viewthrough,

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
    u.priority,

    'user_transaction_history'                                   AS source_table

FROM ranked r
JOIN users u ON u.id = r.user_id
LEFT JOIN af                       ON af.customer_user_id = r.user_id
LEFT JOIN user_additional_details uad ON uad.user_id = r.user_id
LEFT JOIN user_devices             ud  ON ud.user_id  = r.user_id
WHERE (r.payment_date AT TIME ZONE 'Asia/Kolkata') >= '{since}'
  AND (r.payment_date AT TIME ZONE 'Asia/Kolkata') <  '{until}'
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
ORDER BY r.payment_date
