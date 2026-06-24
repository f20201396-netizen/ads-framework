/**
 * MetaClient.gs — minimal Meta Marketing API client for the PoC.
 *
 * Only covers what we need for campaign-level insights: paginated GET
 * with appsecret_proof, retry on transient errors, and 1s page spacing.
 *
 * Mirrors services/shared/meta_client.py at a high level.
 */

const MetaClient = (function () {
  /** Compute appsecret_proof = HMAC-SHA256(access_token, app_secret) hex. */
  function appsecretProof() {
    const cfg = Config.getAll();
    const raw = Utilities.computeHmacSha256Signature(cfg.metaAccessToken, cfg.metaAppSecret);
    return raw.map(function (b) {
      const v = (b < 0 ? b + 256 : b).toString(16);
      return v.length === 1 ? '0' + v : v;
    }).join('');
  }

  function buildUrl(path, params) {
    const cfg = Config.getAll();
    const M   = Config.META;
    const base = M.BASE_URL + '/' + M.API_VERSION + path;
    const merged = Object.assign({
      access_token:    cfg.metaAccessToken,
      appsecret_proof: appsecretProof(),
    }, params || {});
    const qs = Object.keys(merged).map(function (k) {
      return encodeURIComponent(k) + '=' + encodeURIComponent(merged[k]);
    }).join('&');
    return base + '?' + qs;
  }

  /** Single GET with backoff. Returns parsed JSON body or throws. */
  function fetchOnce(url) {
    const M = Config.META;
    let lastErr;
    for (let attempt = 1; attempt <= M.MAX_RETRIES; attempt++) {
      const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      const code = resp.getResponseCode();
      const body = resp.getContentText();

      if (code === 200) {
        return JSON.parse(body);
      }

      // Parse Meta error envelope
      let metaCode = null;
      try {
        const j = JSON.parse(body);
        if (j && j.error && j.error.code != null) metaCode = j.error.code;
      } catch (e) { /* not JSON */ }

      const retryable =
        code === 429 ||
        (code >= 500 && code <= 599) ||
        (metaCode != null && M.RETRY_ERROR_CODES.indexOf(metaCode) >= 0);

      lastErr = 'HTTP ' + code + ' meta_code=' + metaCode + ' body=' + body.slice(0, 300);
      if (!retryable) throw new Error('Meta API non-retryable: ' + lastErr);

      // Exponential backoff: 4s, 8s, 16s ... capped at 120s
      const sleepMs = Math.min(4000 * Math.pow(2, attempt - 1), 120000);
      Logger.log('Meta retry ' + attempt + '/' + M.MAX_RETRIES + ' in ' + (sleepMs / 1000) + 's — ' + lastErr);
      Utilities.sleep(sleepMs);
    }
    throw new Error('Meta API exhausted retries: ' + lastErr);
  }

  /**
   * Paginated GET — follows `paging.next` cursors.
   * Yields all rows; sleeps PAGE_DELAY_MS between pages.
   *
   * @param {string} path   e.g. '/act_123/insights'
   * @param {object} params query params (without access_token / appsecret_proof)
   * @returns {Array<object>} flat array of all data rows
   */
  function getPaginated(path, params) {
    const M = Config.META;
    let url = buildUrl(path, params);
    const all = [];
    while (url) {
      const j = fetchOnce(url);
      if (j.data && j.data.length) all.push.apply(all, j.data);
      url = (j.paging && j.paging.next) ? j.paging.next : null;
      if (url) Utilities.sleep(M.PAGE_DELAY_MS);
    }
    return all;
  }

  /**
   * Fetch campaign-level insights for one ad account, one day.
   * Returns rows with: campaign_id, campaign_name, spend, impressions, clicks, date_start
   */
  function getCampaignInsights(accountId, dateISO) {
    const M = Config.META;
    // Meta accepts time_range as a JSON object — explicit bracket-form is safer than relying on URL-encoded JSON
    // which we've seen Meta silently misinterpret (returning the same day's data for many requested dates).
    const params = {
      level:                          'campaign',
      time_increment:                 1,
      'time_range[since]':            dateISO,
      'time_range[until]':            dateISO,
      fields:                         M.INSIGHT_FIELDS.join(','),
      action_attribution_windows:     '["' + M.ATTRIBUTION_WINDOW + '"]',
      limit:                          500,
    };
    return getPaginated('/' + accountId + '/insights', params);
  }

  return { fetchOnce, getPaginated, getCampaignInsights, buildUrl };
})();
