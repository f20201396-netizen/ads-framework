/**
 * HiddenSpend.gs — persists campaign-level spend rows in a hidden sheet tab.
 *
 * Schema (columns):
 *   campaign_id | date | spend | impressions | clicks | campaign_name
 *
 * Primary key: (campaign_id, date). Upsert merges new rows over old ones.
 * Trim policy: keep last 95 days.
 */

const HiddenSpend = (function () {
  const HEADERS = ['campaign_id', 'date', 'spend', 'impressions', 'clicks', 'campaign_name'];

  function _getSheet() {
    const cfg = Config.getAll();
    const TAB = Config.TABS.HIDDEN_SPEND_CAMPAIGN;
    const ss = SpreadsheetApp.openById(cfg.sheetId);
    let sh = ss.getSheetByName(TAB);
    if (!sh) {
      sh = ss.insertSheet(TAB);
      sh.appendRow(HEADERS);
      sh.hideSheet();
    }
    return sh;
  }

  /** Read all rows as array of objects. */
  function readAll() {
    const sh = _getSheet();
    const lastRow = sh.getLastRow();
    if (lastRow < 2) return [];
    const lastCol = sh.getLastColumn();
    const values = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
    return values.map(function (r) {
      const o = {};
      for (let i = 0; i < HEADERS.length; i++) o[HEADERS[i]] = r[i];
      // Normalize date column to ISO string
      if (o.date instanceof Date) {
        o.date = Utilities.formatDate(o.date, 'UTC', 'yyyy-MM-dd');
      }
      return o;
    });
  }

  /**
   * Upsert rows by (campaign_id, date) primary key.
   * Trims to keep only rows where date >= today - 95.
   *
   * @param {Array<{campaign_id, date, spend, impressions, clicks, campaign_name}>} newRows
   */
  function upsert(newRows) {
    const sh = _getSheet();
    const existing = readAll();
    const byKey = {};
    existing.forEach(function (r) { byKey[r.campaign_id + '|' + r.date] = r; });
    newRows.forEach(function (r) { byKey[r.campaign_id + '|' + r.date] = r; });

    const cutoff = _daysAgoIso(95);
    const kept = Object.keys(byKey)
      .map(function (k) { return byKey[k]; })
      .filter(function (r) { return r.date >= cutoff; })
      .sort(function (a, b) {
        if (a.date !== b.date) return a.date < b.date ? -1 : 1;
        return String(a.campaign_id).localeCompare(String(b.campaign_id));
      });

    // Rewrite the whole tab — simpler and atomic for ~5k rows.
    sh.clear();
    const rows = [HEADERS].concat(kept.map(function (r) {
      return HEADERS.map(function (h) { return r[h] == null ? '' : r[h]; });
    }));
    sh.getRange(1, 1, rows.length, HEADERS.length).setValues(rows);
    return { total: kept.length, written: newRows.length };
  }

  function _daysAgoIso(n) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - n);
    return Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd');
  }

  return { readAll, upsert, HEADERS };
})();
