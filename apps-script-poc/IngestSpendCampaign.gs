/**
 * IngestSpendCampaign.gs — pull campaign-level spend from Meta and persist
 * to the hidden _spend_campaign tab.
 *
 * First run: 90 days. Steady state: last 3 days (re-upserts to catch lag).
 *
 * Mirrors scripts/sync.py steps 1 + 3 but only the campaign level — the PoC
 * scope is one tab.
 */

const IngestSpendCampaign = (function () {
  /**
   * Pull insights for the given number of trailing days.
   * Returns { totalRows, accountsTouched }.
   */
  function run(daysBack) {
    const cfg = Config.getAll();
    const days = typeof daysBack === 'number' ? daysBack : 3;
    const today = new Date();
    const dates = [];
    for (let i = days - 1; i >= 0; i--) {
      const d = new Date(today);
      d.setUTCDate(d.getUTCDate() - i);
      dates.push(Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd'));
    }

    const collected = [];
    cfg.adAccountIds.forEach(function (acctId) {
      dates.forEach(function (dt) {
        let rows;
        try {
          rows = MetaClient.getCampaignInsights(acctId, dt);
        } catch (e) {
          Logger.log('Skip ' + acctId + ' ' + dt + ': ' + e.message);
          return;
        }
        rows.forEach(function (raw) {
          // Trust raw.date_start (Meta's authoritative date) — not the requested dt.
          // If Meta returns the wrong date, this surfaces it loudly during diff.
          const rowDate = raw.date_start || dt;
          collected.push({
            campaign_id:   String(raw.campaign_id || ''),
            campaign_name: raw.campaign_name || '',
            date:          rowDate,
            spend:         _toNum(raw.spend),
            impressions:   _toInt(raw.impressions),
            clicks:        _toInt(raw.clicks),
          });
        });
        Logger.log('Meta ' + acctId + ' ' + dt + ': ' + rows.length + ' rows');
      });
    });

    const result = HiddenSpend.upsert(collected);
    Logger.log('Spend ingest done: ' + JSON.stringify(result));
    return { totalRows: collected.length, accounts: cfg.adAccountIds.length };
  }

  function _toNum(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
  function _toInt(v) { const n = parseInt(v, 10); return isNaN(n) ? 0 : n; }

  /** Cold-start helper — pull 90 days. Call from the editor once. */
  function coldStart() {
    return run(90);
  }

  return { run, coldStart };
})();
