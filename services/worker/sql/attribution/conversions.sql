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

    uad.network,
    uad.partner_site                                             AS publisher_site,
    uad.tracker_campaign_id                                      AS meta_campaign_id,
    uad.tracker_sub_campaign_id                                  AS meta_adset_id,
    uad.tracker_creative_id                                      AS meta_creative_id,
    uad.tracker_campaign_name                                    AS campaign_name,
    LOWER(TRIM(uad.tracker_sub_campaign_name))                   AS adset_name,
    uad.creative                                                 AS creative_name,

    r.amount                                                     AS revenue_inr,
    r.plan_id,
    r.is_real_trial                                              AS is_trial,
    (r.amount > 50 AND r.paid_rank = 1)                          AS is_first_payment,
    (r.order_id ILIKE '%md%')                                    AS is_mandate,

    (uad.is_reengagement = '1')                                  AS is_reattributed,
    (uad.is_organic = '1')                                       AS is_organic,
    (uad.is_viewthrough = '1')                                   AS is_viewthrough,

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
    u.priority,

    'user_transaction_history'                                   AS source_table

FROM ranked r
JOIN users u ON u.id = r.user_id
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
  AND NOT EXISTS (
        SELECT 1 FROM "Demat_Campaigns" dc
        WHERE dc."Adset ID" = uad.tracker_sub_campaign_id
          AND dc."Adset ID" IS NOT NULL
          AND TRIM(dc."Adset ID") <> ''
      )
  AND (uad.network ILIKE '%Facebook%' OR uad.network ILIKE '%Instagram%')
ORDER BY r.payment_date
