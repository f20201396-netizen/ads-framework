-- Sync singular_campaign_metrics from prod Postgres
--
-- Aggregate daily campaign cost from Singular MMP, grouped to one row per
-- (date, source, campaign_name, os) to match the local table's unique key.
-- Each row in prod is at the creative level; we collapse to campaign level here.
--
-- Key prod columns:
--   start_date       → local date column
--   source           → network name ('Facebook', 'AdWords', …)
--   adn_campaign_name → local campaign_name
--   os               → 'Android', 'iOS', 'Web', 'Mixed'
--   adn_cost         → local cost (INR, summed across creatives)
--   adn_installs, adn_clicks, adn_impressions
--
-- Parameters (substituted by Python before EXTERNAL_QUERY):
--   {since}  DATE  start date (inclusive), e.g. '2026-01-01'
--   {until}  DATE  end date   (inclusive), e.g. '2026-04-24'

SELECT
    start_date                                  AS date,
    COALESCE(source, '')                        AS source,
    COALESCE(adn_campaign_name, '')             AS campaign_name,
    COALESCE(os, '')                            AS os,
    COALESCE(SUM(adn_cost), 0)                  AS cost,
    COALESCE(SUM(adn_installs::bigint), 0)      AS installs,
    COALESCE(SUM(adn_clicks::bigint), 0)        AS clicks,
    COALESCE(SUM(adn_impressions::bigint), 0)   AS impressions
FROM singular_campaign_metrics
WHERE start_date >= '{since}'
  AND start_date <= '{until}'
GROUP BY start_date, source, adn_campaign_name, os
ORDER BY start_date, source, adn_campaign_name, os
