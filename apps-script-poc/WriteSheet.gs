/**
 * WriteSheet.gs — write the Campaign Level test tab.
 *
 * PoC: numbers-first, no fancy formatting (group headers / color blocks /
 * conditional formatting from the Python version are intentionally skipped).
 *
 * Columns mirror the Python output column order in
 * scripts/update_meta_dashboard.py write_campaign_level_sheet, so the two
 * tabs can be diffed side by side.
 */

const WriteSheet = (function () {

  // Column order — must match Python write_campaign_level_sheet (lines ~2755-2822).
  // For the PoC we include the most important columns. Each entry is [header, rowKey].
  const COLUMNS = [
    ['Campaign',                  'campaign_name'],
    // MTD media
    ['Spend',                     'spend'],
    ['Impressions',               'impressions'],
    ['Clicks',                    'clicks'],
    ['CTR %',                     'ctr'],
    ['CPM',                       'cpm'],
    ['CPC',                       'cpc'],
    ['First Date',                'first_date'],
    ['Last Date',                 'last_date'],
    ['Status',                    'status'],
    // Overall attribution
    ['Signups',                   'signups'],
    ['D0 Conv',                   'd0_conv'],
    ['D0 Trials',                 'd0_trials'],
    ['D0 CAC',                    'd0_cac'],
    ['D0 Trial Cost',             'd0_trial_cost'],
    ['D0 ROAS',                   'd0_roas'],
    ['D6 Mandate',                'd6_mandate'],
    ['D6 Non-Mandate',            'd6_non_mandate'],
    ['D6 Trials',                 'd6_trials'],
    ['D6 ROAS',                   'd6_roas'],
    ['D6 Mandate ROAS',           'd6_mandate_roas'],
    ['D6 Non-Mandate ROAS',       'd6_non_mandate_roas'],
    ['D6 CAC',                    'd6_cac'],
    ['LTV',                       'ltv_inr'],
    ['CAC',                       'cac_inr'],
    // Mature
    ['Mature Spend',              'mature_spend'],
    ['Mature Imp',                'mature_impressions'],
    ['Mature Clicks',             'mature_clicks'],
    ['Mature CTR',                'mature_ctr'],
    ['Mature CPM',                'mature_cpm'],
    ['Mature CPC',                'mature_cpc'],
    ['Mature Signups',            'mature_signups'],
    ['Mature D0 Conv',            'mature_d0_conv'],
    ['Mature D0 CAC',             'mature_d0_cac'],
    ['Mature D0 ROAS',            'mature_d0_roas'],
    ['Mature D0 Trials',          'mature_d0_trials'],
    ['Mature D0 Trial Cost',      'mature_d0_trial_cost'],
    ['Mature D6 CAC',             'mature_d6_cac'],
    ['Mature D6 ROAS',            'mature_d6_roas'],
    ['Mature D6 Mandate ROAS',    'mature_d6_mandate_roas'],
    ['Mature D6 Non-Mandate ROAS','mature_d6_non_mandate_roas'],
    // Mid
    ['Mid Spend',                 'mid_spend'],
    ['Mid D0 Conv',               'mid_d0_conv'],
    ['Mid D0 CAC',                'mid_d0_cac'],
    ['Mid D0 ROAS',               'mid_d0_roas'],
    ['Mid D0 Trials',             'mid_d0_trials'],
    ['Mid D0 Trial Cost',         'mid_d0_trial_cost'],
    ['Mid D6 CAC',                'mid_d6_cac'],
    ['Mid D6 ROAS',               'mid_d6_roas'],
    ['Mid Grade',                 '_mid_grade'],
    // Recent
    ['Recent Spend',              'recent_spend'],
    ['Recent D0 Conv',            'recent_d0_conv'],
    ['Recent D0 CAC',             'recent_d0_cac'],
    ['Recent D0 ROAS',            'recent_d0_roas'],
    ['Recent D0 Trials',          'recent_d0_trials'],
    ['Recent D0 Trial Cost',      'recent_d0_trial_cost'],
    ['Pred D6 ROAS',              '_recent_pred_d6_roas'],
    ['Pred Rev/Trial',            '_pred_rev_per_trial'],
    ['Pred Source',               '_pred_mult_source'],
    ['Recent Grade',              '_recent_grade'],
    // Scoring
    ['Score',                     '_score'],
    ['Grade',                     '_grade'],
    ['Suggestion',                '_suggestion'],
  ];

  function write(rows) {
    const cfg = Config.getAll();
    const TAB = Config.TABS.OUTPUT;
    const ss = SpreadsheetApp.openById(cfg.sheetId);
    let sh = ss.getSheetByName(TAB);
    if (!sh) sh = ss.insertSheet(TAB);
    sh.clear();

    const header = COLUMNS.map(function (c) { return c[0]; });
    const data = rows.map(function (r) {
      return COLUMNS.map(function (c) {
        const v = r[c[1]];
        return v == null ? '' : v;
      });
    });

    const out = [header].concat(data);
    sh.getRange(1, 1, out.length, header.length).setValues(out);
    sh.setFrozenRows(1);

    // Footer
    const now = Utilities.formatDate(new Date(), 'Asia/Kolkata', "dd MMM yyyy, HH:mm 'IST'");
    sh.getRange(out.length + 2, 1).setValue('Last updated: ' + now);
    sh.getRange(out.length + 2, 2).setValue(rows.length + ' campaigns');

    Logger.log('Wrote ' + rows.length + ' campaigns to "' + Config.TABS.OUTPUT + '"');
    return rows.length;
  }

  return { write, COLUMNS };
})();
