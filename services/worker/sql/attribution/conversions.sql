-- Attribution conversions ingestion
--
-- One row per charged transaction (trial or paid conversion).
-- Attribution columns inherited from user_additional_details (install-time Singular data).
-- Partition column: install_date = DATE(users.created_at)  [user signup date, not payment date]
--
-- Definitions:
--   trial      = status='CHARGED' AND amount=1 AND plan_id ILIKE '%trial%'
--   conversion = status='CHARGED' AND amount>50  AND ROW_NUMBER per user = 1
--   repeat_conversion = status='CHARGED' AND amount>50 AND ROW_NUMBER per user > 1
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
        -- first-payment flag across ALL statuses/amounts for the user
        ROW_NUMBER() OVER (
            PARTITION BY uth.user_id
            ORDER BY uth.payment_date
        ) AS user_payment_rank
    FROM user_transaction_history uth
    WHERE uth.status = 'CHARGED'
      AND (
            uth.amount > 50
            OR (uth.amount = 1 AND uth.plan_id ILIKE '%trial%')
          )
)
SELECT
    -- PK: transaction id is unique, so prefix avoids collision with signup IDs
    'txn_' || r.id::text                                         AS id,
    r.user_id::bigint                                            AS user_id,

    -- Event classification
    CASE
        WHEN r.amount = 1 AND r.plan_id ILIKE '%trial%' THEN 'trial'
        WHEN r.user_payment_rank = 1                     THEN 'conversion'
        ELSE                                                   'repeat_conversion'
    END                                                          AS event_name,

    r.payment_date                                               AS event_time,
    DATE(u.created_at)                                           AS install_date,
    (DATE(r.payment_date) - DATE(u.created_at))::integer         AS days_since_signup,

    -- Singular attribution (from install, not from payment row)
    uad.network,
    uad.partner_site                                             AS publisher_site,
    uad.tracker_campaign_id                                      AS meta_campaign_id,
    uad.tracker_sub_campaign_id                                  AS meta_adset_id,
    uad.tracker_creative_id                                      AS meta_creative_id,
    uad.tracker_campaign_name                                    AS campaign_name,
    LOWER(TRIM(uad.tracker_sub_campaign_name))                   AS adset_name,
    uad.creative                                                 AS creative_name,

    -- Revenue
    r.amount                                                     AS revenue_inr,
    r.plan_id,
    (r.amount = 1 AND r.plan_id ILIKE '%trial%')                 AS is_trial,
    (r.user_payment_rank = 1)                                    AS is_first_payment,
    (r.order_id ILIKE '%md%')                                    AS is_mandate,

    -- Attribution flags
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
    u.priority,

    'user_transaction_history'                                   AS source_table

FROM ranked r
JOIN users u ON u.id = r.user_id
LEFT JOIN user_additional_details uad ON uad.user_id = r.user_id
LEFT JOIN user_devices             ud  ON ud.user_id  = r.user_id
WHERE r.payment_date >= '{since}'
  AND r.payment_date <  '{until}'
ORDER BY r.payment_date
