/**
 * Aggregate.gs — JS port of build_campaign_data + CAMPAIGN_LEVEL_SQL +
 * compute_ad_scores(use_pred_for_recent=True) + _compute_period_grades.
 *
 * Source of truth: scripts/update_meta_dashboard.py
 *
 * Input:
 *   - HiddenSpend.readAll() → per-day campaign spend rows
 *   - FetchAttribution.run() → per-campaign rollup keyed by meta_campaign_id
 *
 * Output: array of row objects, one per campaign, with every column the
 * Python Campaign Level tab produces (minus visual formatting).
 */

const Aggregate = (function () {
  function buildCampaignLevel() {
    const W    = Config.WINDOWS;
    const PRED = Config.PRED;
    const spend   = HiddenSpend.readAll();
    const attrMap = FetchAttribution.run();

    const today    = _today();
    const matureEnd   = _addDays(today, -W.MATURE_END_DAYS);   // <= mature_end → mature
    const midStart    = _addDays(today, -W.MID_START_DAYS);
    const midEnd      = _addDays(today, -W.MID_END_DAYS);
    const recentStart = _addDays(today, -W.RECENT_START_DAYS);
    const attrSince   = _addDays(today, -W.ATTR_LOOKBACK_DAYS);

    // Group spend by campaign
    const byCid = {};
    spend.forEach(function (s) {
      if (!s.campaign_id) return;
      if (s.date < attrSince) return;
      const cid = String(s.campaign_id);
      if (!byCid[cid]) {
        byCid[cid] = {
          campaign_id:   cid,
          campaign_name: s.campaign_name || '',
          rows:          [],
        };
      }
      // Keep latest name (campaign rename safe)
      if (s.campaign_name) byCid[cid].campaign_name = s.campaign_name;
      byCid[cid].rows.push(s);
    });

    // Compute per-campaign media metrics + window slices
    const rows = [];
    Object.keys(byCid).forEach(function (cid) {
      const g = byCid[cid];
      if (_isIosOrRetargetName(g.campaign_name)) return;

      const a = attrMap[cid] || {};
      const r = _buildRow(g, a, { matureEnd: matureEnd, midStart: midStart, midEnd: midEnd, recentStart: recentStart });
      rows.push(r);
    });

    // Predicted D6 ROAS multiplier — per-campaign rev/trial picker
    _applyPredRoas(rows);

    // Grades — mature (main _grade) + mid/recent
    _applyPercentileGrades(rows, MID_WEIGHTS,    'mid_spend',    '_mid_grade');
    _applyPercentileGrades(rows, RECENT_WEIGHTS, 'recent_spend', '_recent_grade');
    _applyMatureGrades(rows);

    // Sort by spend desc (matches Python ORDER BY m.spend DESC)
    rows.sort(function (a, b) { return (b.spend || 0) - (a.spend || 0); });
    return rows;
  }

  function _buildRow(g, a, w) {
    let spend=0, imp=0, clk=0;
    let mat_s=0, mat_i=0, mat_c=0;
    let mid_s=0, mid_i=0, mid_c=0;
    let rec_s=0, rec_i=0, rec_c=0;
    let firstDate = null, lastDate = null;

    g.rows.forEach(function (s) {
      const sp = +s.spend || 0;
      if (sp <= 0) return;
      spend += sp; imp += +s.impressions || 0; clk += +s.clicks || 0;
      if (firstDate == null || s.date < firstDate) firstDate = s.date;
      if (lastDate  == null || s.date > lastDate)  lastDate  = s.date;

      if (s.date <= w.matureEnd) {
        mat_s += sp; mat_i += +s.impressions || 0; mat_c += +s.clicks || 0;
      }
      if (s.date >= w.midStart && s.date <= w.midEnd) {
        mid_s += sp; mid_i += +s.impressions || 0; mid_c += +s.clicks || 0;
      }
      if (s.date >= w.recentStart) {
        rec_s += sp; rec_i += +s.impressions || 0; rec_c += +s.clicks || 0;
      }
    });

    const r = {
      campaign_id:   g.campaign_id,
      campaign_name: g.campaign_name,
      // overall media
      spend:       _r0(spend),
      impressions: imp,
      clicks:      clk,
      ctr:         imp > 0 ? _r3(clk * 100 / imp) : null,
      cpm:         imp > 0 ? _r1(spend * 1000 / imp) : null,
      cpc:         clk > 0 ? _r1(spend / clk) : null,
      first_date:  firstDate,
      last_date:   lastDate,
      status:      '',  // not in PoC scope — would need a Meta status pull

      // overall attribution
      signups:        +a.signups || 0,
      d0_conv:        +a.d0_conv || 0,
      d0_trials:      +a.d0_trials || 0,
      d0_cac:         _div0(spend, a.d0_conv),
      d0_trial_cost:  _div0(spend, a.d0_trials),
      d0_roas:        _ratio(a.d0_revenue, spend),
      d6_mandate:     +a.d6_mandate || 0,
      d6_non_mandate: +a.d6_non_mandate || 0,
      d6_trials:      +a.d6_trials || 0,
      d6_roas:            _ratio(a.d6_revenue, spend),
      d6_mandate_roas:    _ratio(a.d6_mandate_revenue, spend),
      d6_non_mandate_roas:_ratio(a.d6_non_mandate_revenue, spend),
      d6_cac:         _div0(spend, (+a.d6_mandate || 0) + (+a.d6_non_mandate || 0)),
      ltv_inr:        a.signups > 0 ? _r0((+a.total_revenue || 0) / a.signups) : null,
      cac_inr:        _div0(spend, a.signups),

      // mature
      mature_spend:       _r0(mat_s),
      mature_impressions: mat_i,
      mature_clicks:      mat_c,
      mature_ctr: mat_i > 0 ? _r3(mat_c * 100 / mat_i) : null,
      mature_cpm: mat_i > 0 ? _r1(mat_s * 1000 / mat_i) : null,
      mature_cpc: mat_c > 0 ? _r1(mat_s / mat_c) : null,
      mature_signups: +a.mature_signups || 0,
      mature_d0_conv: +a.mature_d0_conv || 0,
      mature_d0_cac:  _div0(mat_s, a.mature_d0_conv),
      mature_d0_roas: _ratio(a.mature_d0_revenue, mat_s),
      mature_d6_cac:  _div0(mat_s, (+a.mature_d6_mandate || 0) + (+a.mature_d6_non_mandate || 0)),
      mature_d6_roas: _ratio(a.mature_d6_revenue, mat_s),
      mature_d6_mandate_roas:    _ratio(a.mature_d6_mandate_revenue, mat_s),
      mature_d6_non_mandate_roas:_ratio(a.mature_d6_non_mandate_revenue, mat_s),
      mature_d0_trials:     +a.mature_d0_trials || 0,
      mature_d0_trial_cost: _div0(mat_s, a.mature_d0_trials),
      mature_d6_revenue:    +a.mature_d6_revenue || 0,

      // mid
      mid_spend: _r0(mid_s),
      mid_impressions: mid_i,
      mid_clicks:      mid_c,
      mid_ctr: mid_i > 0 ? _r3(mid_c * 100 / mid_i) : null,
      mid_cpm: mid_i > 0 ? _r1(mid_s * 1000 / mid_i) : null,
      mid_cpc: mid_c > 0 ? _r1(mid_s / mid_c) : null,
      mid_signups: +a.mid_signups || 0,
      mid_d0_conv: +a.mid_d0_conv || 0,
      mid_d0_cac:  _div0(mid_s, a.mid_d0_conv),
      mid_d0_roas: _ratio(a.mid_d0_revenue, mid_s),
      mid_d6_cac:  _div0(mid_s, (+a.mid_d6_mandate || 0) + (+a.mid_d6_non_mandate || 0)),
      mid_d6_roas: _ratio(a.mid_d6_revenue, mid_s),
      mid_d0_trials:     +a.mid_d0_trials || 0,
      mid_d0_trial_cost: _div0(mid_s, a.mid_d0_trials),

      // recent
      recent_spend:       _r0(rec_s),
      recent_impressions: rec_i,
      recent_clicks:      rec_c,
      recent_ctr: rec_i > 0 ? _r3(rec_c * 100 / rec_i) : null,
      recent_cpm: rec_i > 0 ? _r1(rec_s * 1000 / rec_i) : null,
      recent_cpc: rec_c > 0 ? _r1(rec_s / rec_c) : null,
      recent_signups: +a.recent_signups || 0,
      recent_d0_conv: +a.recent_d0_conv || 0,
      recent_d0_cac:  _div0(rec_s, a.recent_d0_conv),
      recent_d0_roas: _ratio(a.recent_d0_revenue, rec_s),
      recent_d6_cac:  _div0(rec_s, (+a.recent_d6_mandate || 0) + (+a.recent_d6_non_mandate || 0)),
      recent_d6_roas: _ratio(a.recent_d6_revenue, rec_s),
      recent_d0_trials:     +a.recent_d0_trials || 0,
      recent_d0_trial_cost: _div0(rec_s, a.recent_d0_trials),

      // raw figures for pred-D6-ROAS picker
      overall_d6_revenue: +a.d6_revenue || 0,
      overall_d0_trials:  +a.d0_trials || 0,
    };
    return r;
  }

  /**
   * Predicted D6 ROAS — mirrors build_campaign_data() picker logic exactly.
   * Priority: campaign mature → campaign overall → global fallback.
   */
  function _applyPredRoas(rows) {
    const PRED = Config.PRED;
    // Global fallback: pooled median rev/trial from mature rows.
    // For PoC simplicity, compute pooled (sum revenue / sum trials) across all
    // campaigns with ≥10 mature trials. Falls back to Config.PRED.GLOBAL_FALLBACK_REV_PER_TRIAL.
    let totRev = 0, totTrl = 0;
    rows.forEach(function (r) {
      if ((r.mature_d0_trials || 0) >= PRED.MIN_TRIALS_FOR_CAMP_MULT) {
        totRev += +r.mature_d6_revenue || 0;
        totTrl += +r.mature_d0_trials  || 0;
      }
    });
    const globalRpt = totTrl > 0 ? (totRev / totTrl) : PRED.GLOBAL_FALLBACK_REV_PER_TRIAL;
    Logger.log('Pred D6 ROAS global fallback rev/trial = ₹' + Math.round(globalRpt));

    rows.forEach(function (r) {
      const rtc = +r.recent_d0_trial_cost;
      if (!rtc || rtc <= 0) {
        r._recent_pred_d6_roas = null;
        r._pred_rev_per_trial  = null;
        r._pred_mult_source    = null;
        return;
      }
      const mTrials = +r.mature_d0_trials || 0;
      const mRev    = +r.mature_d6_revenue || 0;
      const oTrials = +r.overall_d0_trials || 0;
      const oRev    = +r.overall_d6_revenue || 0;
      let rpt, src;
      if (mTrials >= PRED.MIN_TRIALS_FOR_CAMP_MULT && mRev > 0) {
        rpt = mRev / mTrials; src = 'mature';
      } else if (oTrials >= PRED.MIN_TRIALS_FOR_CAMP_MULT && oRev > 0) {
        rpt = oRev / oTrials; src = 'overall';
      } else {
        rpt = globalRpt; src = 'global';
      }
      r._pred_rev_per_trial  = Math.round(rpt);
      r._pred_mult_source    = src;
      r._recent_pred_d6_roas = _r3(rpt / rtc);
    });
  }

  // ── Scoring weights (mirror compute_ad_scores) ─────────────────────────────
  const MATURE_WEIGHTS = [
    ['mature_d6_mandate_roas',     0.35, 'higher'],
    ['mature_d0_trial_cost',       0.25, 'lower'],
    ['mature_d0_cac',              0.20, 'lower'],
    ['mature_d6_non_mandate_roas', 0.10, 'higher'],
    ['mature_d6_cac',              0.10, 'lower'],
  ];
  const MID_WEIGHTS = [
    ['mid_d6_roas',       0.40, 'higher'],
    ['mid_d0_trial_cost', 0.35, 'lower'],
    ['mid_d0_cac',        0.25, 'lower'],
  ];
  // Recent uses _recent_pred_d6_roas — matches today's use_pred_for_recent=True
  const RECENT_WEIGHTS = [
    ['_recent_pred_d6_roas', 0.40, 'higher'],
    ['recent_d0_trial_cost', 0.35, 'lower'],
    ['recent_d0_cac',        0.25, 'lower'],
  ];

  function _percentileRank(val, sortedVals) {
    if (!sortedVals.length) return 0;
    let lt = 0;
    for (let i = 0; i < sortedVals.length; i++) {
      if (sortedVals[i] < val) lt++; else break;
    }
    return lt / sortedVals.length;
  }

  function _applyPercentileGrades(rows, weights, spendKey, gradeKey) {
    const dists = {};
    weights.forEach(function (w) {
      const k = w[0];
      const vals = [];
      rows.forEach(function (r) {
        const v = r[k];
        if (v != null && !isNaN(v)) vals.push(+v);
      });
      vals.sort(function (a, b) { return a - b; });
      dists[k] = vals;
    });

    rows.forEach(function (r) {
      const spend = +r[spendKey] || 0;
      let totalW = 0, sumS = 0;
      weights.forEach(function (w) {
        const k = w[0], wt = w[1], dir = w[2];
        const v = r[k];
        let s;
        if (v != null && !isNaN(v)) {
          const pr = _percentileRank(+v, dists[k]);
          s = dir === 'lower' ? (1 - pr) : pr;
        } else if (spend > 0) {
          s = 0;
        } else {
          return;
        }
        sumS += s * wt;
        totalW += wt;
      });
      if (totalW < 0.10) {
        r[gradeKey] = null;
        return;
      }
      const score = (sumS / totalW) * 100;
      r[gradeKey] = _scoreToGrade(score);
    });
  }

  function _applyMatureGrades(rows) {
    // Same percentile method as compute_ad_scores's mature scoring
    const today = new Date();
    const dists = {};
    MATURE_WEIGHTS.forEach(function (w) {
      const k = w[0];
      const vals = [];
      rows.forEach(function (r) {
        const v = r[k];
        if (v != null && !isNaN(v)) vals.push(+v);
      });
      vals.sort(function (a, b) { return a - b; });
      dists[k] = vals;
    });

    rows.forEach(function (r) {
      const fd = r.first_date ? new Date(r.first_date) : null;
      const ageDays = fd ? Math.floor((today - fd) / 86400000) : 999;
      if (ageDays < 3) {
        r._score = null;
        r._grade = 'FULL IMMATURE';
        r._suggestion = 'Only ' + ageDays + 'd of data — too early to evaluate';
        return;
      }
      const isPartial = ageDays < 7;
      const matureSpend = +r.mature_spend || 0;

      let totalW = 0, sumS = 0;
      MATURE_WEIGHTS.forEach(function (w) {
        const k = w[0], wt = w[1], dir = w[2];
        const v = r[k];
        let s;
        if (v != null && !isNaN(v)) {
          const pr = _percentileRank(+v, dists[k]);
          s = dir === 'lower' ? (1 - pr) : pr;
        } else if (matureSpend > 0) {
          s = 0;
        } else {
          return;
        }
        sumS += s * wt;
        totalW += wt;
      });
      if (totalW < 0.10) {
        r._score = null;
        r._grade = isPartial ? 'PARTIAL IMMATURE' : 'NO DATA';
        r._suggestion = isPartial
          ? 'Only ' + ageDays + 'd — partial data, revisit after day 7'
          : 'Insufficient mature data for scoring';
        return;
      }
      const score = (sumS / totalW) * 100;
      r._score = Math.round(score * 10) / 10;
      const baseGrade = _scoreToGrade(score);

      if (isPartial) {
        r._grade = 'PARTIAL IMMATURE';
        r._suggestion = 'D' + ageDays + ' — early signal: ' + baseGrade + ' (score ' + r._score + ') — revisit after day 7';
      } else {
        r._grade = baseGrade;
        r._suggestion = '';  // category/INEFFICIENT logic deliberately skipped in PoC
      }
    });
  }

  function _scoreToGrade(score) {
    if (score >= 75) return 'TOP PERFORMER';
    if (score >= 55) return 'GOOD';
    if (score >= 35) return 'AVERAGE';
    if (score >= 20) return 'UNDERPERFORMING';
    return 'POOR';
  }

  function _isIosOrRetargetName(name) {
    if (!name) return false;
    const nl = name.toLowerCase();
    return nl.indexOf('ios') >= 0 || nl.indexOf('retarget') >= 0;
  }

  // ── Helpers ───────────────────────────────────────────────────────────────
  function _ratio(num, den) {
    const n = +num || 0, d = +den || 0;
    return (d > 0 && n > 0) ? _r3(n / d) : null;
  }
  function _div0(num, den) {
    const n = +num || 0, d = +den || 0;
    return (d > 0 && n > 0) ? _r0(n / d) : null;
  }
  function _r0(x) { return Math.round(+x); }
  function _r1(x) { return Math.round(+x * 10) / 10; }
  function _r3(x) { return Math.round(+x * 1000) / 1000; }

  function _today() {
    return Utilities.formatDate(new Date(), 'UTC', 'yyyy-MM-dd');
  }
  function _addDays(iso, n) {
    const d = new Date(iso + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + n);
    return Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd');
  }

  return { buildCampaignLevel };
})();
