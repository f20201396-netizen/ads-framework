/**
 * FetchAttribution.gs — single BQ EXTERNAL_QUERY against prod Postgres that
 * returns per-campaign attribution rollups bucketed into mature / mid / recent.
 *
 * Mirrors the `attr` CTE inside scripts/update_meta_dashboard.py
 * CAMPAIGN_LEVEL_SQL — every column the JS aggregator needs is produced here.
 *
 * Window definitions (must match Config.WINDOWS):
 *   mature : install_date <= today - 14
 *   mid    : today - 13 <= install_date <= today - 7
 *   recent : install_date >= today - 6
 *   overall: install_date >= today - 90 (the full attribution lookback)
 *
 * Filters: network='Facebook', is_reengagement <> '1',
 *          meta_campaign_id IS NOT NULL AND <> 'N/A'.
 */

const FetchAttribution = (function () {
  /**
   * Build the Postgres-side SQL that runs inside EXTERNAL_QUERY.
   * Returns one row per meta_campaign_id with overall + mature/mid/recent
   * aggregates as separate columns.
   */
  function buildInnerSql() {
    const W = Config.WINDOWS;
    return [
      "WITH params AS (",
      "  SELECT (CURRENT_DATE - INTERVAL '" + W.ATTR_LOOKBACK_DAYS + " days')::date AS since_date,",
      "         (CURRENT_DATE - INTERVAL '" + W.MATURE_END_DAYS    + " days')::date AS mature_end,",
      "         (CURRENT_DATE - INTERVAL '" + W.MID_START_DAYS     + " days')::date AS mid_start,",
      "         (CURRENT_DATE - INTERVAL '" + W.MID_END_DAYS       + " days')::date AS mid_end,",
      "         (CURRENT_DATE - INTERVAL '" + W.RECENT_START_DAYS  + " days')::date AS recent_start",
      "),",
      "sig AS (",
      "  SELECT u.id AS user_id,",
      "         'signup'::text AS event_name,",
      "         DATE(u.created_at) AS install_date,",
      "         0::int AS days_since_signup,",
      "         FALSE AS is_mandate,",
      "         0::numeric AS revenue_inr,",
      "         uad.tracker_campaign_id AS meta_campaign_id",
      "  FROM users u",
      "  JOIN user_additional_details uad ON uad.user_id = u.id",
      "  WHERE u.created_at >= (SELECT since_date FROM params)",
      "    AND uad.network = 'Facebook'",
      "    AND COALESCE(uad.is_reengagement,'0') <> '1'",
      "    AND uad.tracker_campaign_id IS NOT NULL",
      "    AND uad.tracker_campaign_id <> 'N/A'",
      "),",
      "ranked AS (",
      "  SELECT uth.user_id, uth.payment_date, uth.amount, uth.plan_id, uth.order_id,",
      "         ROW_NUMBER() OVER (PARTITION BY uth.user_id ORDER BY uth.payment_date) AS rn",
      "  FROM user_transaction_history uth",
      "  WHERE uth.status = 'CHARGED'",
      "    AND (uth.amount > 50 OR (uth.amount = 1 AND uth.plan_id ILIKE '%trial%'))",
      "),",
      "txn AS (",
      "  SELECT r.user_id,",
      "         CASE WHEN r.amount = 1 AND r.plan_id ILIKE '%trial%' THEN 'trial'",
      "              WHEN r.rn = 1 THEN 'conversion'",
      "              ELSE 'repeat_conversion' END AS event_name,",
      "         DATE(u.created_at) AS install_date,",
      "         (DATE(r.payment_date) - DATE(u.created_at))::int AS days_since_signup,",
      "         (r.order_id ILIKE '%md%') AS is_mandate,",
      "         r.amount::numeric AS revenue_inr,",
      "         uad.tracker_campaign_id AS meta_campaign_id",
      "  FROM ranked r",
      "  JOIN users u                          ON u.id        = r.user_id",
      "  LEFT JOIN user_additional_details uad ON uad.user_id = r.user_id",
      "  WHERE r.payment_date >= (SELECT since_date FROM params)",
      "    AND uad.network = 'Facebook'",
      "    AND COALESCE(uad.is_reengagement,'0') <> '1'",
      "    AND uad.tracker_campaign_id IS NOT NULL",
      "    AND uad.tracker_campaign_id <> 'N/A'",
      "),",
      "events AS (",
      "  SELECT user_id, event_name, install_date, days_since_signup, is_mandate, revenue_inr, meta_campaign_id FROM sig",
      "  UNION ALL",
      "  SELECT user_id, event_name, install_date, days_since_signup, is_mandate, revenue_inr, meta_campaign_id FROM txn",
      ")",
      "SELECT",
      "  meta_campaign_id,",
      // Overall (full 90d)
      "  COUNT(DISTINCT CASE WHEN event_name='signup' THEN user_id END)                                                                AS signups,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 THEN user_id END)            AS d0_conv,",
      "  COUNT(DISTINCT CASE WHEN event_name='trial' AND days_since_signup=0 THEN user_id END)                                          AS d0_trials,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 THEN revenue_inr ELSE 0 END)            AS d0_revenue,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=TRUE THEN user_id END) AS d6_mandate,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=FALSE THEN user_id END) AS d6_non_mandate,",
      "  COUNT(DISTINCT CASE WHEN event_name='trial' AND days_since_signup<=6 THEN user_id END)                                         AS d6_trials,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 THEN revenue_inr ELSE 0 END)           AS d6_revenue,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=TRUE THEN revenue_inr ELSE 0 END)  AS d6_mandate_revenue,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=FALSE THEN revenue_inr ELSE 0 END) AS d6_non_mandate_revenue,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') THEN revenue_inr ELSE 0 END)                                    AS total_revenue,",
      // Mature (install_date <= today - 14)
      "  COUNT(DISTINCT CASE WHEN event_name='signup' AND install_date <= (SELECT mature_end FROM params) THEN user_id END)             AS mature_signups,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 AND install_date <= (SELECT mature_end FROM params) THEN user_id END) AS mature_d0_conv,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 AND install_date <= (SELECT mature_end FROM params) THEN revenue_inr ELSE 0 END) AS mature_d0_revenue,",
      "  COUNT(DISTINCT CASE WHEN event_name='trial' AND days_since_signup=0 AND install_date <= (SELECT mature_end FROM params) THEN user_id END) AS mature_d0_trials,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=TRUE AND install_date <= (SELECT mature_end FROM params) THEN user_id END) AS mature_d6_mandate,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=FALSE AND install_date <= (SELECT mature_end FROM params) THEN user_id END) AS mature_d6_non_mandate,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND install_date <= (SELECT mature_end FROM params) THEN revenue_inr ELSE 0 END) AS mature_d6_revenue,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=TRUE  AND install_date <= (SELECT mature_end FROM params) THEN revenue_inr ELSE 0 END) AS mature_d6_mandate_revenue,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=FALSE AND install_date <= (SELECT mature_end FROM params) THEN revenue_inr ELSE 0 END) AS mature_d6_non_mandate_revenue,",
      // Mid
      "  COUNT(DISTINCT CASE WHEN event_name='signup' AND install_date BETWEEN (SELECT mid_start FROM params) AND (SELECT mid_end FROM params) THEN user_id END) AS mid_signups,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 AND install_date BETWEEN (SELECT mid_start FROM params) AND (SELECT mid_end FROM params) THEN user_id END) AS mid_d0_conv,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 AND install_date BETWEEN (SELECT mid_start FROM params) AND (SELECT mid_end FROM params) THEN revenue_inr ELSE 0 END) AS mid_d0_revenue,",
      "  COUNT(DISTINCT CASE WHEN event_name='trial' AND days_since_signup=0 AND install_date BETWEEN (SELECT mid_start FROM params) AND (SELECT mid_end FROM params) THEN user_id END) AS mid_d0_trials,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=TRUE  AND install_date BETWEEN (SELECT mid_start FROM params) AND (SELECT mid_end FROM params) THEN user_id END) AS mid_d6_mandate,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=FALSE AND install_date BETWEEN (SELECT mid_start FROM params) AND (SELECT mid_end FROM params) THEN user_id END) AS mid_d6_non_mandate,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND install_date BETWEEN (SELECT mid_start FROM params) AND (SELECT mid_end FROM params) THEN revenue_inr ELSE 0 END) AS mid_d6_revenue,",
      // Recent
      "  COUNT(DISTINCT CASE WHEN event_name='signup' AND install_date >= (SELECT recent_start FROM params) THEN user_id END) AS recent_signups,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 AND install_date >= (SELECT recent_start FROM params) THEN user_id END) AS recent_d0_conv,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup=0 AND install_date >= (SELECT recent_start FROM params) THEN revenue_inr ELSE 0 END) AS recent_d0_revenue,",
      "  COUNT(DISTINCT CASE WHEN event_name='trial' AND days_since_signup=0 AND install_date >= (SELECT recent_start FROM params) THEN user_id END) AS recent_d0_trials,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=TRUE  AND install_date >= (SELECT recent_start FROM params) THEN user_id END) AS recent_d6_mandate,",
      "  COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND is_mandate=FALSE AND install_date >= (SELECT recent_start FROM params) THEN user_id END) AS recent_d6_non_mandate,",
      "  SUM(CASE WHEN event_name IN ('conversion','repeat_conversion') AND days_since_signup<=6 AND install_date >= (SELECT recent_start FROM params) THEN revenue_inr ELSE 0 END) AS recent_d6_revenue",
      "FROM events",
      "GROUP BY meta_campaign_id"
    ].join('\n');
  }

  /**
   * Wraps the inner Postgres SQL in EXTERNAL_QUERY and returns the BQ
   * outer SQL. Escapes any single quotes in the inner SQL for the BQ
   * string literal.
   */
  function buildOuterSql() {
    const cfg = Config.getAll();
    // Use BQ triple-quoted string for the inner SQL — single quotes don't
    // need escaping. Inner SQL must not contain a triple-double-quote
    // sequence (it doesn't — we only use single quotes for string literals).
    const inner = buildInnerSql().replace(/\n/g, ' ');
    return 'SELECT * FROM EXTERNAL_QUERY("' + cfg.bqConnection + '", """' + inner + '""")';
  }

  /**
   * Runs the query and returns rows keyed by meta_campaign_id for fast lookup
   * from the aggregator.
   */
  function run() {
    const sql = buildOuterSql();
    const rows = BqClient.query(sql);
    const byCid = {};
    rows.forEach(function (r) {
      byCid[String(r.meta_campaign_id)] = r;
    });
    Logger.log('Attribution: ' + rows.length + ' campaigns rolled up.');
    return byCid;
  }

  return { run, buildInnerSql, buildOuterSql };
})();
