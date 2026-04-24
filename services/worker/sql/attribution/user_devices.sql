-- Sync user_devices from prod Postgres
--
-- Returns one row per user_id for all users with user_id > {min_user_id}.
-- DISTINCT ON ensures one row per user (arbitrary device chosen when multiple exist).
--
-- Parameters (substituted by Python before EXTERNAL_QUERY):
--   {min_user_id}  BIGINT  watermark — max user_id already in local user_devices

SELECT DISTINCT ON (user_id)
    user_id,
    os
FROM user_devices
WHERE user_id > {min_user_id}
ORDER BY user_id
