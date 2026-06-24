/**
 * Config.gs — secrets + constants for the Campaign Level PoC.
 *
 * Secrets live in PropertiesService (Script properties). Run setupSecrets()
 * once after first deploy, or open Project Settings → Script properties and
 * add them manually.
 */

const Config = (function () {
  const DEFAULTS = {
    BQ_PROJECT:    'univest-applications',
    BQ_CONNECTION: 'projects/univest-applications/locations/asia-south2/connections/univest_db',
    SHEET_ID:      '1dZ71WnbgY0k7zM-DB8EcudvdW0G005W3bsOocGoTQUs',
  };

  function get(key, opts) {
    opts = opts || {};
    const v = PropertiesService.getScriptProperties().getProperty(key);
    if (v) return v;
    if (DEFAULTS.hasOwnProperty(key)) return DEFAULTS[key];
    if (opts.required) throw new Error('Missing script property: ' + key);
    return null;
  }

  function getAll() {
    return {
      metaAccessToken: get('META_ACCESS_TOKEN', { required: true }),
      metaAppSecret:   get('META_APP_SECRET',   { required: true }),
      adAccountIds:    get('AD_ACCOUNT_IDS',    { required: true }).split(',').map(s => s.trim()).filter(Boolean),
      bqProject:       get('BQ_PROJECT'),
      bqConnection:    get('BQ_CONNECTION'),
      sheetId:         get('SHEET_ID'),
    };
  }

  /**
   * One-time setup. Run from the editor with values pasted into the body, OR
   * call from the editor after pasting them into Project Settings → Script
   * properties directly (preferred — avoids leaking secrets in source).
   */
  function setupSecrets(values) {
    if (!values) throw new Error('Pass an object with keys: META_ACCESS_TOKEN, META_APP_SECRET, AD_ACCOUNT_IDS');
    const props = PropertiesService.getScriptProperties();
    Object.keys(values).forEach(k => props.setProperty(k, String(values[k])));
    Logger.log('Stored ' + Object.keys(values).length + ' properties.');
  }

  // PoC tab names
  const TABS = {
    HIDDEN_SPEND_CAMPAIGN: '_spend_campaign',
    OUTPUT:                'Campaign Level — Apps Script Test',
  };

  // Meta API constants — mirror services/shared/constants.py
  const META = {
    API_VERSION:         'v20.0',
    BASE_URL:            'https://graph.facebook.com',
    ATTRIBUTION_WINDOW:  '7d_click',
    INSIGHT_FIELDS:      ['spend', 'impressions', 'clicks', 'campaign_id', 'campaign_name', 'date_start'],
    PAGE_DELAY_MS:       1000,
    RETRY_ERROR_CODES:   [1, 2, 4, 17, 32, 613, 80004],
    MAX_RETRIES:         8,
  };

  // Window boundaries for mature/mid/recent — must match Python scripts/update_meta_dashboard.py
  // Mature: date <= today - 7   (D6 fully complete)
  // Mid:    today-6 <= date <= today-3   (4-day window — D0 done, D6 partial)
  // Recent: date >= today - 2   (3-day window — today + 2 prior days)
  // Attribution lookback: 90 days
  const WINDOWS = {
    MATURE_END_DAYS:    7,
    MID_START_DAYS:     6,
    MID_END_DAYS:       3,
    RECENT_START_DAYS:  2,
    ATTR_LOOKBACK_DAYS: 90,
  };

  // Predicted D6 ROAS multiplier picker thresholds (per today's Python fix)
  const PRED = {
    MIN_TRIALS_FOR_CAMP_MULT: 10,
    GLOBAL_FALLBACK_REV_PER_TRIAL: 500,  // matches Python's q1 fallback default
  };

  // BQ cost cap (matches Python BQ_COST_CAP_BYTES)
  const BQ = {
    COST_CAP_BYTES: 5 * 1000 * 1000 * 1000,  // 5 GB
  };

  return { get, getAll, setupSecrets, TABS, META, WINDOWS, PRED, BQ };
})();
