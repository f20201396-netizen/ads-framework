"""
Univest Meta Ads — Google Sheets Live Dashboard
================================================
Creates / updates a Google Sheet with key Meta Ads performance metrics.

Usage:
    python scripts/update_meta_dashboard.py
    python scripts/update_meta_dashboard.py --sheet-id <existing_id>

First run: creates a new sheet and prints the URL.
Subsequent runs: updates the existing sheet in-place.

Auth: uses the GCP service account key (same one used for BigQuery).
      The service account needs Google Sheets API enabled.
      Sheet is automatically shared with SHARE_WITH email below.

Targets: edit the TARGETS dict to update targets.
"""

import argparse
import json
import os
import smtplib
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = "/Users/macbook/Downloads/google-json-key.json"
SHARE_WITH           = None   # set to "you@gmail.com" to auto-share on first run
DB_DSN               = "postgresql://macbook@localhost/meta_ads"
SHEET_NAME           = "Univest Meta Ads Dashboard"

# Edit targets here
TARGETS = {
    "d0_trial_cost":     1_500,    # ₹ per trial
    "d0_cac":            50_000,   # ₹ per D0 conversion
    "d0_conv_pct":       22.0,     # %
    "d0_conv_abs":       1_400,    # count
    "monthly_cac":       None,
    "roas_blended":      3.20,
    "roas_meta":         3.30,
    "sub_rev_mtd_cr":    52.0,     # ₹ Cr
    "new_user_cac":      None,
}

# ── DB helpers ────────────────────────────────────────────────────────────────
def db_conn():
    return psycopg2.connect(DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)

def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()

def q1(conn, sql, params=None):
    rows = q(conn, sql, params)
    return rows[0] if rows else {}

# ── Date helpers ──────────────────────────────────────────────────────────────
today      = date.today()
mtd_start  = today.replace(day=1)
lm_start   = (mtd_start - timedelta(days=1)).replace(day=1)
lm_same    = lm_start + timedelta(days=(today - mtd_start).days)  # same-day LM

# ── SQL ───────────────────────────────────────────────────────────────────────
# Spend from insights_daily (Meta only, 7d_click window)
SPEND_SQL = """
SELECT
    COALESCE(SUM(CASE WHEN date = %(today)s THEN spend END), 0)               AS today_spend,
    COALESCE(SUM(CASE WHEN date >= %(mtd)s   THEN spend END), 0)              AS mtd_spend,
    COALESCE(SUM(CASE WHEN date >= %(lm)s AND date <= %(lm_same)s
                      THEN spend END), 0)                                      AS lm_spend,
    COALESCE(SUM(CASE WHEN date = %(lm_same)s THEN spend END), 0)             AS lm_today_spend
FROM insights_daily
WHERE attribution_window = '7d_click'
"""

# Attribution metrics from attribution_events
ATTR_SQL = """
SELECT
    -- MTD signups
    COUNT(DISTINCT CASE WHEN event_name = 'signup'
                         AND install_date >= %(mtd)s THEN user_id END)        AS mtd_signups,

    -- LM signups (same period)
    COUNT(DISTINCT CASE WHEN event_name = 'signup'
                         AND install_date >= %(lm)s
                         AND install_date <= %(lm_same)s THEN user_id END)    AS lm_signups,

    -- MTD D0 conversions
    COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                         AND days_since_signup = 0
                         AND install_date >= %(mtd)s THEN user_id END)        AS mtd_d0_conv,

    -- LM D0 conversions
    COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                         AND days_since_signup = 0
                         AND install_date >= %(lm)s
                         AND install_date <= %(lm_same)s THEN user_id END)    AS lm_d0_conv,

    -- MTD D0 trials
    COUNT(DISTINCT CASE WHEN event_name = 'trial'
                         AND days_since_signup = 0
                         AND install_date >= %(mtd)s THEN user_id END)        AS mtd_d0_trials,

    -- LM D0 trials
    COUNT(DISTINCT CASE WHEN event_name = 'trial'
                         AND days_since_signup = 0
                         AND install_date >= %(lm)s
                         AND install_date <= %(lm_same)s THEN user_id END)    AS lm_d0_trials,

    -- MTD D0 revenue
    COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                       AND days_since_signup = 0
                       AND install_date >= %(mtd)s THEN revenue_inr END), 0)  AS mtd_d0_revenue,

    -- LM D0 revenue
    COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                       AND days_since_signup = 0
                       AND install_date >= %(lm)s
                       AND install_date <= %(lm_same)s
                      THEN revenue_inr END), 0)                                AS lm_d0_revenue,

    -- MTD D6 total conversions
    COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                         AND days_since_signup <= 6
                         AND install_date >= %(mtd)s THEN user_id END)         AS mtd_d6_conv,

    -- LM D6 total conversions
    COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                         AND days_since_signup <= 6
                         AND install_date >= %(lm)s
                         AND install_date <= %(lm_same)s THEN user_id END)     AS lm_d6_conv,

    -- MTD total revenue from NEW installs this month (cohort view)
    COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                       AND install_date >= %(mtd)s THEN revenue_inr END), 0)  AS mtd_total_revenue,

    -- LM total revenue from LM new installs (cohort view)
    COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                       AND install_date >= %(lm)s
                       AND install_date <= %(lm_same)s
                      THEN revenue_inr END), 0)                                AS lm_total_revenue,

    -- MTD revenue from ALL Facebook users who paid this period (blended, for ROAS)
    COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                       AND DATE(event_time) >= %(mtd)s
                       AND DATE(event_time) <= %(today)s THEN revenue_inr END), 0) AS mtd_period_revenue,

    -- LM revenue from ALL Facebook users who paid LM same period (blended, for ROAS)
    COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                       AND DATE(event_time) >= %(lm)s
                       AND DATE(event_time) <= %(lm_same)s THEN revenue_inr END), 0) AS lm_period_revenue

FROM attribution_events
WHERE network = 'Facebook'
  AND is_reattributed = FALSE
"""

params = {
    "today":   today,
    "mtd":     mtd_start,
    "lm":      lm_start,
    "lm_same": lm_same,
}

# ── Formatting ────────────────────────────────────────────────────────────────
def inr(v, cr=False):
    if v is None: return "—"
    f = float(v)
    if cr: return f"₹{f/1e7:.1f}Cr"
    return f"₹{int(f):,}"

def pct(v, d=1):
    if v is None: return "—"
    return f"{float(v):.{d}f}%"

def vs(current, target, higher_is_better=True):
    if target is None or current is None: return "—"
    c, t = float(current), float(target)
    if t == 0: return "—"
    diff_pct = (c - t) / t * 100
    # For lower-is-better (e.g. CAC, trial cost): being below target is GOOD → show as positive
    if not higher_is_better:
        diff_pct = -diff_pct
    sign = "+" if diff_pct >= 0 else ""
    return f"{sign}{diff_pct:.1f}%"

def vs_lm(current, last, higher_is_better=True):
    if last is None or current is None or float(last) == 0: return "—"
    diff_pct = (float(current) - float(last)) / float(last) * 100
    if not higher_is_better:
        diff_pct = -diff_pct
    sign = "+" if diff_pct >= 0 else ""
    return f"{sign}{diff_pct:.1f}%"

def health_score(metrics: dict) -> tuple[int, str]:
    """0-100 health score. Lower-is-better uses target/current ratio."""
    scores = []
    # D0 Trial Cost — lower is better
    if metrics.get("d0_trial_cost") and TARGETS["d0_trial_cost"]:
        ratio = TARGETS["d0_trial_cost"] / float(metrics["d0_trial_cost"])
        scores.append(min(ratio * 100, 100))
    # D0 CAC — lower is better
    if metrics.get("d0_cac") and TARGETS["d0_cac"]:
        ratio = TARGETS["d0_cac"] / float(metrics["d0_cac"])
        scores.append(min(ratio * 100, 100))
    # D0 Conversions % — higher is better
    if metrics.get("d0_conv_pct") and TARGETS["d0_conv_pct"]:
        ratio = float(metrics["d0_conv_pct"]) / TARGETS["d0_conv_pct"]
        scores.append(min(ratio * 100, 100))
    # D0 Conversions (absolute) — higher is better
    if metrics.get("d0_conv_abs") and TARGETS["d0_conv_abs"]:
        ratio = float(metrics["d0_conv_abs"]) / TARGETS["d0_conv_abs"]
        scores.append(min(ratio * 100, 100))
    # ROAS — higher is better
    if metrics.get("roas_meta") and TARGETS["roas_meta"]:
        ratio = float(metrics["roas_meta"]) / TARGETS["roas_meta"]
        scores.append(min(ratio * 100, 100))
    if not scores:
        return 50, "WATCH"
    score = int(sum(scores) / len(scores))
    zone = "ON TRACK" if score >= 70 else ("WATCH" if score >= 45 else "OFF TRACK")
    return score, zone

# ── Sheet builder ─────────────────────────────────────────────────────────────
def build_data(conn) -> dict:
    spend = q1(conn, SPEND_SQL, params)
    attr  = q1(conn, ATTR_SQL,  params)

    mtd_spend    = float(spend.get("mtd_spend")    or 0)
    lm_spend     = float(spend.get("lm_spend")     or 0)
    today_spend  = float(spend.get("today_spend")  or 0)

    mtd_signups  = int(attr.get("mtd_signups")  or 0)
    lm_signups   = int(attr.get("lm_signups")   or 0)
    mtd_d0_conv  = int(attr.get("mtd_d0_conv")  or 0)
    lm_d0_conv   = int(attr.get("lm_d0_conv")   or 0)
    mtd_d0_trial = int(attr.get("mtd_d0_trials") or 0)
    lm_d0_trial  = int(attr.get("lm_d0_trials") or 0)
    mtd_d0_rev   = float(attr.get("mtd_d0_revenue")   or 0)
    lm_d0_rev    = float(attr.get("lm_d0_revenue")    or 0)
    mtd_tot_rev       = float(attr.get("mtd_total_revenue")   or 0)
    lm_tot_rev        = float(attr.get("lm_total_revenue")    or 0)
    mtd_period_rev    = float(attr.get("mtd_period_revenue")  or 0)
    lm_period_rev     = float(attr.get("lm_period_revenue")   or 0)
    mtd_d6_conv  = int(attr.get("mtd_d6_conv") or 0)
    lm_d6_conv   = int(attr.get("lm_d6_conv")  or 0)

    d0_trial_cost  = (mtd_spend / mtd_d0_trial) if mtd_d0_trial else None
    lm_trial_cost  = (lm_spend  / lm_d0_trial)  if lm_d0_trial  else None
    d0_cac         = (mtd_spend / mtd_d0_conv)  if mtd_d0_conv  else None
    lm_d0_cac      = (lm_spend  / lm_d0_conv)   if lm_d0_conv   else None
    d0_conv_pct    = (mtd_d0_conv / mtd_signups * 100) if mtd_signups else None
    lm_conv_pct    = (lm_d0_conv  / lm_signups  * 100) if lm_signups  else None
    monthly_cac    = (mtd_spend / mtd_d6_conv)  if mtd_d6_conv  else None  # spend / D6 conversions
    lm_cac         = (lm_spend  / lm_d6_conv)   if lm_d6_conv   else None
    # M0 ROAS: revenue from this month's signups / total spend
    roas_meta      = (mtd_tot_rev / mtd_spend)  if mtd_spend else None
    lm_roas_meta   = (lm_tot_rev  / lm_spend)   if lm_spend  else None

    return {
        "d0_trial_cost":  d0_trial_cost,
        "lm_trial_cost":  lm_trial_cost,
        "d0_cac":         d0_cac,
        "lm_d0_cac":      lm_d0_cac,
        "d0_conv_pct":    d0_conv_pct,
        "lm_conv_pct":    lm_conv_pct,
        "d0_conv_abs":    mtd_d0_conv,
        "lm_d0_conv_abs": lm_d0_conv,
        "monthly_cac":    monthly_cac,
        "lm_cac":         lm_cac,
        "roas_meta":      roas_meta,
        "lm_roas_meta":   lm_roas_meta,
        "mtd_spend":      mtd_spend,
        "lm_spend":       lm_spend,
        "today_spend":    today_spend,
        "mtd_signups":    mtd_signups,
        "lm_signups":     lm_signups,
        "mtd_d0_rev":     mtd_d0_rev,
        "mtd_tot_rev":    mtd_tot_rev,
        "lm_tot_rev":     lm_tot_rev,
    }


def write_sheet(sh, data: dict):
    import gspread
    from gspread.utils import rowcol_to_a1
    from gspread_formatting import (
        BooleanCondition, BooleanRule, CellFormat, Color, ConditionalFormatRule,
        GridRange, NumberFormat, TextFormat, batch_updater, cellFormat, format_cell_range,
        get_conditional_format_rules, set_frozen,
    )

    ws = sh.sheet1
    ws.clear()
    ws.update_title("Dashboard")

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    hs, zone = health_score(data)

    m = data
    T = TARGETS

    def fmt_roas(v): return f"{float(v):.2f}x" if v else "—"
    def fmt_inr(v):  return inr(v)
    def fmt_cr(v):   return inr(v, cr=True)

    # ── Build rows ────────────────────────────────────────────────────────────
    header = ["Metric", "Unit", "MTD / Today", "Target",
              f"Last Month\n(same period)", "vs Target", "vs Last Month", "", "Score", "Zone"]

    def section(name):
        return [name, "", "", "", "", "", "", "", "", ""]

    def row(metric, unit, current, target, last, current_raw=None, target_raw=None, last_raw=None, higher_is_better=True):
        vt = vs(current_raw, target_raw, higher_is_better) if (current_raw is not None and target_raw is not None) else "—"
        vl = vs_lm(current_raw, last_raw, higher_is_better) if (current_raw is not None and last_raw is not None) else "—"
        return [metric, unit, current or "—", target or "—", last or "—", vt, vl, "", "", ""]

    rows = [
        header,
        [],
        section("Core Funnel Metrics — Meta"),
        row("D0 Trial Cost (New User Trial)", "₹",
            inr(m["d0_trial_cost"]), inr(T["d0_trial_cost"]), inr(m["lm_trial_cost"]),
            m["d0_trial_cost"], T["d0_trial_cost"], m["lm_trial_cost"], higher_is_better=False),
        row("D0 CAC (Cost per D0 Conversion)", "₹",
            inr(m["d0_cac"]), inr(T["d0_cac"]), inr(m["lm_d0_cac"]),
            m["d0_cac"], T["d0_cac"], m["lm_d0_cac"], higher_is_better=False),
        row("D0 Conversions %", "%",
            pct(m["d0_conv_pct"]), pct(T["d0_conv_pct"]), pct(m["lm_conv_pct"]),
            m["d0_conv_pct"], T["d0_conv_pct"], m["lm_conv_pct"]),
        row("D0 Conversions (Absolute)", "count",
            f"{m['d0_conv_abs']:,}", f"{T['d0_conv_abs']:,}" if T["d0_conv_abs"] else "—",
            f"{m['lm_d0_conv_abs']:,}",
            m["d0_conv_abs"], T["d0_conv_abs"], m["lm_d0_conv_abs"]),
        row("Monthly CAC (D6 Conv)", "₹",
            inr(m["monthly_cac"]), inr(T["monthly_cac"]), inr(m["lm_cac"]),
            m["monthly_cac"], T["monthly_cac"], m["lm_cac"], higher_is_better=False),
        [],
        section("M0 ROAS — Channel-wise"),
        row("M0 ROAS — Meta", "x",
            fmt_roas(m["roas_meta"]), fmt_roas(T["roas_meta"]), fmt_roas(m["lm_roas_meta"]),
            m["roas_meta"], T["roas_meta"], m["lm_roas_meta"]),
        [],
        section("Revenue & New User Economics"),
        row("Subscription Revenue MTD", "₹Cr",
            fmt_cr(m["mtd_tot_rev"]), fmt_cr(T["sub_rev_mtd_cr"] * 1e7) if T["sub_rev_mtd_cr"] else "—",
            fmt_cr(m["lm_tot_rev"]),
            m["mtd_tot_rev"], T["sub_rev_mtd_cr"] * 1e7 if T["sub_rev_mtd_cr"] else None, m["lm_tot_rev"]),
        row("New User Signups MTD", "count",
            f"{m['mtd_signups']:,}", "—", f"{m['lm_signups']:,}",
            m["mtd_signups"], None, m["lm_signups"]),
        row("New User CAC", "₹",
            inr(m["monthly_cac"]), inr(T["new_user_cac"]), inr(m["lm_cac"]),
            m["monthly_cac"], T["new_user_cac"], m["lm_cac"], higher_is_better=False),
        [],
        section("Meta Spend"),
        row("Meta Spend MTD", "₹",
            inr(m["mtd_spend"]), "—", inr(m["lm_spend"]),
            m["mtd_spend"], None, m["lm_spend"]),
        row("Meta Spend Today", "₹",
            inr(m["today_spend"]), "—", "—", m["today_spend"], None, None),
        [],
        ["Last updated:", now_str, "", "", "", "", "", "", "", ""],
    ]

    # Health score goes in column I/J of rows 1-3
    rows[0][8] = "Score"
    rows[0][9] = "Zone"

    ws.update("A1", rows)

    # Put health score in a visible spot
    ws.update("I3", [[hs]])
    ws.update("J3", [[zone]])

    # ── Formatting via batch requests ─────────────────────────────────────────
    body = {
        "requests": [
            # Header row — dark bg
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                                   "fontSize": 10},
                    "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP",
                }},
                "fields": "userEnteredFormat",
            }},
            # Section rows — accent bg
            *[{"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": r-1, "endRowIndex": r,
                          "startColumnIndex": 0, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.059, "green": 0.204, "blue": 0.376},
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                                   "fontSize": 9},
                }},
                "fields": "userEnteredFormat",
            }} for r in [3, 10, 13, 18]],  # section rows (1-indexed)
            # Health score cell
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 3,
                          "startColumnIndex": 8, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True, "fontSize": 28,
                                   "foregroundColor": {"red": 0.102, "green": 0.478, "blue": 0.224}},
                    "horizontalAlignment": "CENTER",
                }},
                "fields": "userEnteredFormat",
            }},
            # Zone cell
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 3,
                          "startColumnIndex": 9, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.839, "green": 0.933, "blue": 0.847},
                    "textFormat": {"bold": True, "fontSize": 10,
                                   "foregroundColor": {"red": 0.102, "green": 0.478, "blue": 0.224}},
                    "horizontalAlignment": "CENTER",
                }},
                "fields": "userEnteredFormat",
            }},
            # Freeze header row
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }},
            # Column widths
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 280}, "fields": "pixelSize",
            }},
            *[{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": i, "endIndex": i+1},
                "properties": {"pixelSize": 120}, "fields": "pixelSize",
            }} for i in range(1, 8)],
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 8, "endIndex": 10},
                "properties": {"pixelSize": 100}, "fields": "pixelSize",
            }},
            # Alternating row bg for data rows
            {"addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                                "endRowIndex": len(rows), "startColumnIndex": 0, "endColumnIndex": 8}],
                    "booleanRule": {
                        "condition": {"type": "CUSTOM_FORMULA",
                                      "values": [{"userEnteredValue": "=ISEVEN(ROW())"}]},
                        "format": {"backgroundColor": {"red": 0.957, "green": 0.965, "blue": 0.976}},
                    },
                },
                "index": 0,
            }},
        ]
    }
    sh.batch_update(body)
    print(f"  Sheet updated: {len(rows)} rows written.")


# ── Ad-level SQL ─────────────────────────────────────────────────────────────
AD_LEVEL_SQL = """
WITH media AS (
    SELECT
        i.ad_id,
        MAX(i.ad_name)       AS ad_name,
        MAX(i.campaign_id)   AS campaign_id,
        MAX(i.adset_id)      AS adset_id,
        ROUND(SUM(i.spend)::numeric, 0)                                   AS spend,
        SUM(i.impressions)                                                 AS impressions,
        SUM(i.clicks)                                                      AS clicks,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.clicks)::numeric * 100 / SUM(i.impressions), 3) END AS ctr,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.spend)::numeric * 1000 / SUM(i.impressions), 1) END AS cpm,
        CASE WHEN SUM(i.clicks) > 0
             THEN ROUND(SUM(i.spend)::numeric / SUM(i.clicks), 1) END     AS cpc,
        MIN(i.date) AS first_date,
        MAX(i.date) AS last_date
    FROM insights_daily i
    WHERE i.attribution_window = '7d_click'
      AND i.date >= %(mtd)s
      AND i.spend > 0
    GROUP BY i.ad_id
),
attr AS (
    SELECT
        ae.meta_creative_id                                               AS ad_id,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                            THEN ae.user_id END)                          AS signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                            THEN ae.user_id END)                          AS d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                            THEN ae.user_id END)                          AS d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                 THEN ae.revenue_inr ELSE 0 END)                          AS d0_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                            THEN ae.user_id END)                          AS d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                            THEN ae.user_id END)                          AS d6_non_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup <= 6
                            THEN ae.user_id END)                          AS d6_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                 THEN ae.revenue_inr ELSE 0 END)                          AS total_revenue
    FROM attribution_events ae
    WHERE ae.network = 'Facebook'
      AND ae.is_reattributed = FALSE
      AND ae.meta_creative_id IS NOT NULL
      AND ae.meta_creative_id <> 'N/A'
      AND ae.install_date >= %(attr_since)s
    GROUP BY ae.meta_creative_id
)
SELECT
    m.ad_id,
    m.ad_name,
    c.name  AS campaign_name,
    s.name  AS adset_name,
    m.spend,
    m.impressions,
    m.clicks,
    m.ctr,
    m.cpm,
    m.cpc,
    m.first_date,
    m.last_date,
    COALESCE(at.signups,        0)                                        AS signups,
    COALESCE(at.d0_conv,        0)                                        AS d0_conv,
    COALESCE(at.d0_trials,      0)                                        AS d0_trials,
    CASE WHEN m.spend > 0 AND COALESCE(at.d0_conv, 0) > 0
         THEN ROUND(m.spend::numeric / at.d0_conv, 0) END                 AS d0_cac,
    CASE WHEN m.spend > 0 AND COALESCE(at.d0_trials, 0) > 0
         THEN ROUND(m.spend::numeric / at.d0_trials, 0) END               AS d0_trial_cost,
    CASE WHEN m.spend > 0 AND COALESCE(at.d0_revenue, 0) > 0
         THEN ROUND(at.d0_revenue::numeric / m.spend, 3) END              AS d0_roas,
    COALESCE(at.d6_mandate,     0)                                        AS d6_mandate,
    COALESCE(at.d6_non_mandate, 0)                                        AS d6_non_mandate,
    COALESCE(at.d6_trials,      0)                                        AS d6_trials,
    a.effective_status                                                     AS status,
    CASE WHEN m.spend > 0 AND COALESCE(at.d6_revenue, 0) > 0
         THEN ROUND(at.d6_revenue::numeric / m.spend, 3) END              AS d6_roas,
    CASE WHEN m.spend > 0
              AND (COALESCE(at.d6_mandate, 0) + COALESCE(at.d6_non_mandate, 0)) > 0
         THEN ROUND(m.spend::numeric /
                    (COALESCE(at.d6_mandate, 0) + COALESCE(at.d6_non_mandate, 0)),
                    0) END                                                 AS d6_cac,
    CASE WHEN COALESCE(at.signups, 0) > 0
         THEN ROUND(at.total_revenue::numeric / at.signups, 0) END        AS ltv_inr,
    CASE WHEN m.spend > 0 AND COALESCE(at.signups, 0) > 0
         THEN ROUND(m.spend::numeric / at.signups, 0) END                 AS cac_inr
FROM media m
LEFT JOIN campaigns c ON c.id = m.campaign_id
LEFT JOIN adsets s    ON s.id = m.adset_id
LEFT JOIN ads a       ON a.id = m.ad_id::text
LEFT JOIN attr at     ON at.ad_id = m.ad_id::text
ORDER BY c.name, m.spend DESC NULLS LAST
"""

ATTR_SINCE_AD = date.today().replace(day=1) - timedelta(days=60)  # 2 months back for D6 coverage


def build_ad_data(conn) -> list:
    params_ad = {"mtd": mtd_start, "attr_since": ATTR_SINCE_AD}
    rows = q(conn, AD_LEVEL_SQL, params_ad)
    compute_ad_scores(rows)
    return rows


def compute_ad_scores(rows: list) -> None:
    """
    Adds _score, _grade, _suggestion keys to each row dict in-place.

    Weights  : D6 CAC 60%  |  D6 ROAS 20%  |  D0 Trial Cost 10%  |  D0 CAC 10%
    Method   : percentile rank across all ads for each metric; weighted average.
    IMMATURE : ad's first_date is < 6 days ago — not enough data to evaluate.

    For lower-is-better metrics a NULL value (zero conversions despite spend)
    is treated as worst-case (score=0) rather than excluded, so ads with no
    D6 conversions are correctly penalised.
    """
    WEIGHTS = [
        ('d6_cac',        0.60, 'lower'),
        ('d6_roas',       0.20, 'higher'),
        ('d0_trial_cost', 0.10, 'lower'),
        ('d0_cac',        0.10, 'lower'),
    ]

    # Sorted value lists per metric (non-NULL only) — used for percentile ranking
    metric_vals: dict[str, list[float]] = {
        key: sorted(float(r[key]) for r in rows if r.get(key) is not None)
        for key, _, _ in WEIGHTS
    }

    def pct_rank(val: float, sorted_vals: list[float]) -> float:
        """Fraction of values strictly below val (0 = best for lower-is-better)."""
        if not sorted_vals:
            return 0.0
        return sum(1 for v in sorted_vals if v < val) / len(sorted_vals)

    ACTIONS = {
        'TOP PERFORMER':   'Scale — all key metrics above peers',
        'GOOD':            'Grow spend 15-20% — above average performance',
        'AVERAGE':         'Run creative A/B test to improve rank',
        'UNDERPERFORMING': 'Cut budget 30% and test new creative',
        'POOR':            'Pause or overhaul — underperforms across metrics',
        'INEFFICIENT CAT 1': 'KILL immediately — POOR grade burning >₹10k, highest waste',
        'INEFFICIENT CAT 2': 'Pause within 24h — POOR grade, ₹5-10k spend, escalating waste',
        'INEFFICIENT CAT 3': 'Cut budget 50% now — underperforming at >₹10k, diminishing returns',
        'OPPORTUNITY':       'Scale aggressively — test campaign proving out at >₹10k, graduate to evergreen',
    }
    WEAK_NOTES = {
        'd6_cac':        'D6 CAC above peers → tighten audience or swap creative',
        'd6_roas':       'D6 ROAS below peers → refresh creative or review bid cap',
        'd0_trial_cost': 'Trial cost above peers → test hook or intro-offer angle',
        'd0_cac':        'D0 CAC above peers → review creative-to-intent alignment',
    }

    def _is_test_campaign(name: str) -> bool:
        if not name:
            return False
        nl = name.lower()
        return 'test' in nl or 'experiment' in nl or 'pilot' in nl

    for r in rows:
        fd = r.get('first_date')
        age_days = (today - fd).days if isinstance(fd, date) else 999

        # ── Maturity cohorts: D0-D2 = FULL IMMATURE, D3-D6 = PARTIAL IMMATURE ──
        if age_days < 3:
            r['_score']      = None
            r['_grade']      = 'FULL IMMATURE'
            r['_suggestion'] = f'Only {age_days}d of data — too early to evaluate'
            continue

        is_partial = age_days < 7  # D3-D6

        spend = float(r.get('spend') or 0)
        metric_scores: dict[str, tuple[float, float]] = {}   # key → (0-1 score, weight)

        for key, weight, direction in WEIGHTS:
            val = r.get(key)
            if val is not None:
                pr = pct_rank(float(val), metric_vals[key])
                s  = (1.0 - pr) if direction == 'lower' else pr
            elif spend > 0:
                # Metric is NULL despite active spend = zero conversions = worst rank
                s = 0.0
            else:
                continue
            metric_scores[key] = (s, weight)

        total_w = sum(w for _, w in metric_scores.values())
        if total_w < 0.10:
            r['_score']      = None
            r['_grade']      = 'PARTIAL IMMATURE' if is_partial else 'NO DATA'
            r['_suggestion'] = f'Only {age_days}d — partial data, revisit after day 7' if is_partial else 'Insufficient metric data for scoring'
            continue

        score = sum(s * w for s, w in metric_scores.values()) / total_w * 100
        r['_score'] = round(score, 1)

        if   score >= 75: base_grade = 'TOP PERFORMER'
        elif score >= 55: base_grade = 'GOOD'
        elif score >= 35: base_grade = 'AVERAGE'
        elif score >= 20: base_grade = 'UNDERPERFORMING'
        else:             base_grade = 'POOR'

        # ── Partial immature: score computed but flagged ──
        if is_partial:
            r['_grade'] = 'PARTIAL IMMATURE'
            r['_suggestion'] = f'D{age_days} — early signal: {base_grade} (score {r["_score"]}) — revisit after day 7'
            continue

        # ── Spend-based overlay categories (ACTIVE ads only) ──
        status      = (r.get('status') or '').upper()
        camp_name   = r.get('campaign_name') or ''
        is_active   = status == 'ACTIVE'

        grade = base_grade  # default

        if is_active and base_grade == 'POOR' and spend > 10_000:
            grade = 'INEFFICIENT CAT 1'
        elif is_active and base_grade == 'POOR' and spend >= 5_000:
            grade = 'INEFFICIENT CAT 2'
        elif is_active and base_grade == 'UNDERPERFORMING' and spend > 10_000:
            grade = 'INEFFICIENT CAT 3'
        elif _is_test_campaign(camp_name) and base_grade in ('AVERAGE', 'GOOD', 'TOP PERFORMER') and spend > 10_000:
            grade = 'OPPORTUNITY'

        r['_grade'] = grade

        if grade in ('INEFFICIENT CAT 1', 'INEFFICIENT CAT 2', 'INEFFICIENT CAT 3', 'OPPORTUNITY'):
            weakest = min(metric_scores, key=lambda k: metric_scores[k][0])
            r['_suggestion'] = f"{ACTIONS[grade]} | Spend ₹{int(spend):,} | {WEAK_NOTES[weakest]}"
        elif grade == 'TOP PERFORMER':
            r['_suggestion'] = ACTIONS['TOP PERFORMER']
        else:
            weakest = min(metric_scores, key=lambda k: metric_scores[k][0])
            r['_suggestion'] = f"{ACTIONS[grade]} | Key drag: {WEAK_NOTES[weakest]}"


# ── DoD Trial Cost SQL ────────────────────────────────────────────────────────
DOD_SQL = """
WITH spend AS (
    SELECT date, SUM(spend) AS spend
    FROM insights_daily
    WHERE attribution_window = '7d_click'
      AND date >= %(since)s
    GROUP BY date
),
trials AS (
    SELECT install_date AS date, COUNT(DISTINCT user_id) AS d0_trials
    FROM attribution_events
    WHERE network = 'Facebook'
      AND is_reattributed = FALSE
      AND event_name = 'trial'
      AND days_since_signup = 0
      AND install_date >= %(since)s
    GROUP BY install_date
)
SELECT
    s.date,
    ROUND(s.spend)                                                   AS spend,
    COALESCE(t.d0_trials, 0)                                         AS d0_trials,
    CASE WHEN COALESCE(t.d0_trials, 0) > 0
         THEN ROUND(s.spend / t.d0_trials) END                       AS trial_cost
FROM spend s
LEFT JOIN trials t ON t.date = s.date
ORDER BY s.date
"""


def build_dod_data(conn) -> list:
    # This month + last month for comparison
    lm_start_for_dod = (mtd_start - timedelta(days=1)).replace(day=1)
    return q(conn, DOD_SQL, {"since": lm_start_for_dod})


# ── Platform ROAS SQL ─────────────────────────────────────────────────────────
# M0 ROAS = Revenue from that month's signups paid within the same calendar month
#           divided by Singular per-OS spend for the same period.
# "Same calendar month" = event_time falls within [since, until], same bounds as install_date.

PLATFORM_ROAS_SQL = """
WITH resolved AS (
    -- Resolve true device platform: user_devices.os is PRIMARY.
    -- Singular sets platform='Android' even for iOS Facebook users (campaign-level),
    -- so user_devices.os gives the true iOS/Android split.
    -- iOS attribution note: Apple ATT prevents Singular from attributing most iOS
    -- Facebook users — they land with network=NULL.  We include ALL iOS installs
    -- (regardless of network) so the numerator is comparable to Singular iOS spend.
    -- Android attribution via Singular works correctly, so we filter network='Facebook'.
    SELECT
        ae.*,
        COALESCE(
            CASE
                WHEN LOWER(ud.os) LIKE 'ios%%' OR LOWER(ud.os) = 'ipados' THEN 'iOS'
                WHEN LOWER(ud.os) LIKE 'android%%'                         THEN 'Android'
            END,
            ae.platform
        ) AS resolved_platform
    FROM attribution_events ae
    LEFT JOIN user_devices ud ON ud.user_id = ae.user_id
    WHERE ae.is_reattributed = FALSE
      AND ae.install_date >= %(since)s
      AND ae.install_date <= %(until)s
),
conv AS (
    SELECT
        resolved_platform                                                   AS platform,
        COUNT(DISTINCT CASE WHEN event_name = 'signup'
                            THEN user_id END)                              AS signups,
        COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                             AND days_since_signup = 0
                            THEN user_id END)                              AS d0_conv,
        COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                             AND days_since_signup <= 6
                            THEN user_id END)                              AS d6_conv,
        COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                             AND DATE(event_time) >= %(since)s
                             AND DATE(event_time) <= %(until)s
                            THEN user_id END)                              AS m0_conv,
        COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                           AND DATE(event_time) >= %(since)s
                           AND DATE(event_time) <= %(until)s
                          THEN revenue_inr END), 0)                        AS m0_revenue,
        COALESCE(SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                          THEN revenue_inr END), 0)                        AS total_revenue
    FROM resolved
    WHERE (
        -- Android: Singular attribution works — include only Meta-attributed installs
        (resolved_platform = 'Android' AND network = 'Facebook')
        OR
        -- iOS: ATT prevents Meta attribution — include all iOS installs so revenue
        -- is comparable to Singular's iOS spend denominator
        (resolved_platform = 'iOS')
    )
    GROUP BY 1
),
platform_spend AS (
    -- Direct per-OS spend from Singular MMP.
    SELECT
        os AS platform,
        COALESCE(SUM(cost), 0) AS spend
    FROM singular_campaign_metrics
    WHERE source = 'Facebook'
      AND os IN ('Android', 'iOS')
      AND date >= %(since)s
      AND date <= %(until)s
    GROUP BY os
)
SELECT
    c.platform,
    c.signups,
    c.d0_conv,
    c.d6_conv,
    c.m0_conv,
    ROUND(c.m0_revenue::numeric, 0)                                 AS m0_revenue,
    ROUND(c.total_revenue::numeric, 0)                              AS total_revenue,
    COALESCE(ROUND(ps.spend::numeric, 0), 0)                        AS allocated_spend,
    CASE WHEN COALESCE(ps.spend, 0) > 0
         THEN ROUND(c.m0_revenue / ps.spend, 3)
    END                                                             AS m0_roas,
    CASE WHEN c.signups > 0
         THEN ROUND(c.m0_conv * 100.0 / c.signups, 2)
    END                                                             AS m0_conv_pct
FROM conv c
LEFT JOIN platform_spend ps ON ps.platform = c.platform
ORDER BY c.signups DESC
"""


def build_platform_roas_data(conn) -> list:
    """Returns per-month platform ROAS for Jan–Apr 2026."""
    months = [
        ("Jan 2026", date(2026, 1, 1),  date(2026, 1, 31)),
        ("Feb 2026", date(2026, 2, 1),  date(2026, 2, 28)),
        ("Mar 2026", date(2026, 3, 1),  date(2026, 3, 31)),
        ("Apr 2026", date(2026, 4, 1),  today),
    ]
    result = []
    for label, since, until in months:
        rows = q(conn, PLATFORM_ROAS_SQL, {"since": since, "until": until})
        result.append({"month": label, "since": since, "until": until, "rows": rows})
    return result


def write_dod_sheet(sh, rows: list):
    """Write 'DoD — Trial Cost' tab with a line chart comparing this month vs last month."""
    try:
        ws = sh.worksheet("DoD — Trial Cost")
        ws.clear()
    except Exception:
        ws = sh.add_worksheet("DoD — Trial Cost", rows=100, cols=20)

    # Ensure sheet is wide enough to hold the chart to the right of the data
    sh.batch_update({"requests": [{"updateSheetProperties": {
        "properties": {"sheetId": ws.id,
                       "gridProperties": {"rowCount": 100, "columnCount": 20}},
        "fields": "gridProperties.rowCount,gridProperties.columnCount",
    }}]})

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    # Split into this month / last month
    this_month_rows = [r for r in rows if r["date"] >= mtd_start]
    last_month_rows = [r for r in rows if r["date"] < mtd_start]

    # Build a side-by-side table: day_of_month | LM date | LM trial cost | MTD date | MTD trial cost
    max_days = max(len(this_month_rows), len(last_month_rows))

    headers = ["Day", "Last Month Date", "LM Trial Cost ₹", "LM Spend ₹", "LM Trials",
               "", "This Month Date", "MTD Trial Cost ₹", "MTD Spend ₹", "MTD Trials"]

    data_rows = [headers]
    for i in range(max_days):
        lm = last_month_rows[i] if i < len(last_month_rows) else {}
        tm = this_month_rows[i]  if i < len(this_month_rows)  else {}
        data_rows.append([
            i + 1,
            str(lm.get("date", "")) if lm else "",
            int(lm["trial_cost"]) if lm and lm.get("trial_cost") else "",
            int(lm["spend"])      if lm and lm.get("spend")      else "",
            int(lm["d0_trials"])  if lm and lm.get("d0_trials")  else "",
            "",
            str(tm.get("date", "")) if tm else "",
            int(tm["trial_cost"]) if tm and tm.get("trial_cost") else "",
            int(tm["spend"])      if tm and tm.get("spend")      else "",
            int(tm["d0_trials"])  if tm and tm.get("d0_trials")  else "",
        ])

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}"])

    ws.update(values=data_rows, range_name="A1")

    n_data = max_days  # number of data rows (excl header)

    # ── Formatting + chart ────────────────────────────────────────────────────
    body = {
        "requests": [
            # Header row
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                    "textFormat": {"bold": True,
                                   "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                                   "fontSize": 9},
                    "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP",
                }},
                "fields": "userEnteredFormat",
            }},
            # Freeze header
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id,
                               "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }},
            # Column widths
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 50}, "fields": "pixelSize",
            }},
            *[{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": i, "endIndex": i + 1},
                "properties": {"pixelSize": 130}, "fields": "pixelSize",
            }} for i in range(1, 10)],
            # Chart: line chart of trial cost, this month vs last month
            {"addChart": {
                "chart": {
                    "spec": {
                        "title": f"Daily Trial Cost — {today.strftime('%B %Y')} vs Last Month",
                        "titleTextFormat": {"bold": True, "fontSize": 13},
                        "basicChart": {
                            "chartType": "LINE",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Day of Month"},
                                {"position": "LEFT_AXIS",   "title": "Trial Cost (₹)"},
                            ],
                            "domains": [{
                                "domain": {"sourceRange": {"sources": [{
                                    "sheetId": ws.id,
                                    "startRowIndex": 1, "endRowIndex": 1 + n_data,
                                    "startColumnIndex": 0, "endColumnIndex": 1,
                                }]}},
                            }],
                            "series": [
                                # Last month trial cost (col C, index 2)
                                {
                                    "series": {"sourceRange": {"sources": [{
                                        "sheetId": ws.id,
                                        "startRowIndex": 1, "endRowIndex": 1 + n_data,
                                        "startColumnIndex": 2, "endColumnIndex": 3,
                                    }]}},
                                    "targetAxis": "LEFT_AXIS",
                                    "color": {"red": 0.6, "green": 0.6, "blue": 0.6},
                                    "lineStyle": {"type": "MEDIUM_DASHED"},
                                },
                                # This month trial cost (col H, index 7)
                                {
                                    "series": {"sourceRange": {"sources": [{
                                        "sheetId": ws.id,
                                        "startRowIndex": 1, "endRowIndex": 1 + n_data,
                                        "startColumnIndex": 7, "endColumnIndex": 8,
                                    }]}},
                                    "targetAxis": "LEFT_AXIS",
                                    "color": {"red": 0.102, "green": 0.204, "blue": 0.376},
                                },
                            ],
                            "headerCount": 0,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": ws.id, "rowIndex": 1, "columnIndex": 10},
                            "widthPixels": 700,
                            "heightPixels": 420,
                        }
                    },
                }
            }},
        ]
    }
    sh.batch_update(body)
    print(f"  DoD tab: {len(this_month_rows)} days this month, {len(last_month_rows)} days last month.")


def write_ad_level_sheet(sh, rows: list):
    # Get or create "Ad Level — Meta" tab
    try:
        ws = sh.worksheet("Ad Level — Meta")
        ws.clear()
    except Exception:
        ws = sh.add_worksheet("Ad Level — Meta", rows=2000, cols=30)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    headers = [
        "Ad Name", "Campaign", "Adset",
        "Spend (MTD)", "Impressions", "Clicks", "CTR %", "CPM ₹", "CPC ₹",
        "First Date", "Last Date", "Status",
        "Signups", "D0 Conv", "D0 Trials", "D0 CAC ₹", "D0 Trial Cost ₹", "D0 ROAS",
        "D6 Mandate", "D6 Non-Mdt", "D6 Trials", "D6 ROAS", "D6 CAC ₹",
        "LTV ₹", "CAC ₹",
        "Score", "Grade", "Suggestion",
    ]
    # Column indices (0-based) — computed from headers so insertion order doesn't matter
    IDX_STATUS     = headers.index("Status")
    IDX_SCORE      = headers.index("Score")
    IDX_GRADE      = headers.index("Grade")
    IDX_SUGGESTION = headers.index("Suggestion")

    def _v(v):  return "" if v is None else v
    def _i(v):  return "" if v is None else int(float(v))
    def _f2(v): return "" if v is None else round(float(v), 2)
    def _f3(v): return "" if v is None else round(float(v), 3)

    data_rows = [headers]
    for r in rows:
        data_rows.append([
            r["ad_name"] or "",
            r["campaign_name"] or "",
            r["adset_name"] or "",
            _i(r["spend"]),
            _i(r["impressions"]),
            _i(r["clicks"]),
            _f3(r["ctr"]),
            _f2(r["cpm"]),
            _f2(r["cpc"]),
            str(r["first_date"]) if r["first_date"] else "",
            str(r["last_date"])  if r["last_date"]  else "",
            r.get("status") or "",
            _i(r["signups"]),
            _i(r["d0_conv"]),
            _i(r["d0_trials"]),
            _i(r["d0_cac"]),
            _i(r["d0_trial_cost"]),
            _f3(r["d0_roas"]),
            _i(r["d6_mandate"]),
            _i(r["d6_non_mandate"]),
            _i(r["d6_trials"]),
            _f3(r["d6_roas"]),
            _i(r.get("d6_cac")),
            _i(r["ltv_inr"]),
            _i(r["cac_inr"]),
            r.get("_score", "") if r.get("_score") is not None else "",
            r.get("_grade", ""),
            r.get("_suggestion", ""),
        ])

    # Add footer
    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(rows)} ads"])

    ws.update(values=data_rows, range_name="A1")

    # Formatting
    body = {
        "requests": [
            # Header row
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": len(headers)},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                    "textFormat": {"bold": True,
                                   "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                                   "fontSize": 9},
                    "horizontalAlignment": "CENTER",
                    "wrapStrategy": "WRAP",
                }},
                "fields": "userEnteredFormat",
            }},
            # Freeze header + first 3 cols
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id,
                               "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 3}},
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }},
            # Ad name col width
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 260}, "fields": "pixelSize",
            }},
            # Campaign col
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 200}, "fields": "pixelSize",
            }},
            # Adset col
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 2, "endIndex": 3},
                "properties": {"pixelSize": 180}, "fields": "pixelSize",
            }},
            # Metric cols (indices 3 to IDX_SCORE-1) — narrow
            *[{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": i, "endIndex": i+1},
                "properties": {"pixelSize": 95}, "fields": "pixelSize",
            }} for i in range(3, IDX_SCORE)],
            # Score col
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": IDX_SCORE, "endIndex": IDX_SCORE + 1},
                "properties": {"pixelSize": 70}, "fields": "pixelSize",
            }},
            # Grade col
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": IDX_GRADE, "endIndex": IDX_GRADE + 1},
                "properties": {"pixelSize": 175}, "fields": "pixelSize",
            }},
            # Suggestion col — wide
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": IDX_SUGGESTION, "endIndex": IDX_SUGGESTION + 1},
                "properties": {"pixelSize": 380}, "fields": "pixelSize",
            }},
            # Alternating row shading (data columns only, not scoring)
            {"addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                                "endRowIndex": len(data_rows),
                                "startColumnIndex": 0, "endColumnIndex": IDX_SCORE}],
                    "booleanRule": {
                        "condition": {"type": "CUSTOM_FORMULA",
                                      "values": [{"userEnteredValue": "=ISEVEN(ROW())"}]},
                        "format": {"backgroundColor": {"red": 0.957, "green": 0.965, "blue": 0.976}},
                    },
                },
                "index": 0,
            }},
            # Status column — colour per value
            *[{"addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                                "endRowIndex": len(data_rows),
                                "startColumnIndex": IDX_STATUS, "endColumnIndex": IDX_STATUS + 1}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ",
                                      "values": [{"userEnteredValue": label}]},
                        "format": {"backgroundColor": bg,
                                   "textFormat": {"bold": True, "foregroundColor": fg}},
                    },
                },
                "index": idx + 1,
            }} for idx, (label, bg, fg) in enumerate([
                ("ACTIVE",       {"red": 0.714, "green": 0.882, "blue": 0.722},
                                 {"red": 0.0,   "green": 0.239, "blue": 0.086}),
                ("PAUSED",       {"red": 1.0,   "green": 0.898, "blue": 0.600},
                                 {"red": 0.4,   "green": 0.267, "blue": 0.0}),
                ("WITH_ISSUES",  {"red": 0.914, "green": 0.263, "blue": 0.208},
                                 {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("ADSET_PAUSED", {"red": 0.800, "green": 0.824, "blue": 0.855},
                                 {"red": 0.267, "green": 0.306, "blue": 0.365}),
                ("ARCHIVED",     {"red": 0.851, "green": 0.851, "blue": 0.851},
                                 {"red": 0.4,   "green": 0.4,   "blue": 0.4}),
            ])],
            # Grade column — colour per label
            *[{"addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                                "endRowIndex": len(data_rows),
                                "startColumnIndex": IDX_GRADE, "endColumnIndex": IDX_GRADE + 1}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ",
                                      "values": [{"userEnteredValue": label}]},
                        "format": {"backgroundColor": bg,
                                   "textFormat": {"bold": True, "foregroundColor": fg}},
                    },
                },
                "index": idx + 1,
            }} for idx, (label, bg, fg) in enumerate([
                ("TOP PERFORMER",   {"red": 0.137, "green": 0.612, "blue": 0.290},
                                    {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("GOOD",            {"red": 0.714, "green": 0.882, "blue": 0.722},
                                    {"red": 0.0,   "green": 0.239, "blue": 0.086}),
                ("AVERAGE",         {"red": 1.0,   "green": 0.898, "blue": 0.600},
                                    {"red": 0.4,   "green": 0.267, "blue": 0.0}),
                ("UNDERPERFORMING", {"red": 1.0,   "green": 0.639, "blue": 0.353},
                                    {"red": 0.525, "green": 0.161, "blue": 0.0}),
                ("POOR",            {"red": 0.914, "green": 0.263, "blue": 0.208},
                                    {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                # Inefficiency tiers — deep red / dark red / orange-red
                ("INEFFICIENT CAT 1", {"red": 0.545, "green": 0.0,   "blue": 0.0},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("INEFFICIENT CAT 2", {"red": 0.698, "green": 0.133, "blue": 0.133},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("INEFFICIENT CAT 3", {"red": 0.804, "green": 0.361, "blue": 0.361},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                # Opportunity — bright blue
                ("OPPORTUNITY",       {"red": 0.118, "green": 0.533, "blue": 0.898},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                # Maturity tiers
                ("FULL IMMATURE",   {"red": 0.800, "green": 0.824, "blue": 0.855},
                                    {"red": 0.267, "green": 0.306, "blue": 0.365}),
                ("PARTIAL IMMATURE", {"red": 0.878, "green": 0.890, "blue": 0.914},
                                     {"red": 0.400, "green": 0.420, "blue": 0.470}),
                ("NO DATA",         {"red": 0.910, "green": 0.910, "blue": 0.910},
                                    {"red": 0.4,   "green": 0.4,   "blue": 0.4}),
            ])],
        ]
    }
    sh.batch_update(body)
    print(f"  Ad Level tab: {len(rows)} ads written.")


def write_platform_roas_sheet(sh, platform_data: list):
    """
    Write 'Platform ROAS' tab — M0 ROAS by platform for each month.

    Layout: rows = Month × Platform, columns = metrics.
    Months: Jan 2026, Feb 2026, Mar 2026, Apr 2026 (MTD).
    """
    try:
        ws = sh.worksheet("Platform ROAS")
        ws.clear()
    except Exception:
        ws = sh.add_worksheet("Platform ROAS", rows=200, cols=20)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    def _roas(v):  return f"{float(v)*100:.1f}%"  if v else "—"
    def _inr(v):   return f"₹{int(float(v)):,}" if v else "—"
    def _pct(v):   return f"{float(v):.2f}%"    if v else "—"
    def _num(v):   return f"{int(float(v)):,}"   if v else "—"

    METRICS = [
        ("Signups",        "signups",        _num),
        ("D0 Conv",        "d0_conv",        _num),
        ("D6 Conv",        "d6_conv",        _num),
        ("M0 Conv",        "m0_conv",        _num),
        ("M0 Conv%",       "m0_conv_pct",    _pct),
        ("M0 Revenue",     "m0_revenue",     _inr),
        ("Alloc. Spend",   "allocated_spend",_inr),
        ("M0 ROAS",        "m0_roas",        _roas),
    ]

    header = ["Month", "Platform"] + [m[0] for m in METRICS]
    data_rows = [header]

    section_rows = []   # track row indices of month-header rows (for shading)
    prev_roas_by_platform: dict[str, float] = {}

    for month_entry in platform_data:
        month_label = month_entry["month"]
        rows        = month_entry["rows"]

        # Month section header (merged visual row)
        section_row_idx = len(data_rows)   # 0-based for Sheets API
        section_rows.append(section_row_idx)
        data_rows.append([month_label] + [""] * (len(header) - 1))

        if not rows:
            data_rows.append(["", "No data"] + ["—"] * len(METRICS))
            continue

        for r in rows:
            platform = r["platform"]
            row = [month_label, platform] + [fmt(r.get(key)) for _, key, fmt in METRICS]
            data_rows.append(row)

            # MoM delta row (only if previous month has data for this platform)
            cur_roas = float(r["m0_roas"]) if r.get("m0_roas") else None
            prev_roas = prev_roas_by_platform.get(platform)
            if cur_roas and prev_roas:
                diff = (cur_roas - prev_roas) / prev_roas * 100
                sign = "+" if diff >= 0 else ""
                data_rows.append(
                    ["", "  vs prev month"] + [""] * (len(METRICS) - 1) + [f"{sign}{diff:.1f}%"]
                )
            if cur_roas:
                prev_roas_by_platform[platform] = cur_roas

    data_rows.append([])
    data_rows.append([
        f"M0 = revenue from month's signups paid within same calendar month  |  Spend from Singular MMP per OS  |  Apr 2026 = MTD  |  Last updated: {now_str}"
    ])

    ws.update(values=data_rows, range_name="A1")

    n_cols = len(header)
    n_rows = len(data_rows)

    month_colors = [
        {"red": 0.102, "green": 0.204, "blue": 0.376},  # Jan — dark blue
        {"red": 0.059, "green": 0.204, "blue": 0.376},  # Feb
        {"red": 0.102, "green": 0.267, "blue": 0.376},  # Mar
        {"red": 0.059, "green": 0.267, "blue": 0.314},  # Apr
    ]

    requests = [
        # Header row
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": n_cols},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.063, "green": 0.063, "blue": 0.063},
                "textFormat": {"bold": True,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                               "fontSize": 9},
                "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP",
            }},
            "fields": "userEnteredFormat",
        }},
        # Freeze header + first 2 cols
        {"updateSheetProperties": {
            "properties": {"sheetId": ws.id,
                           "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 2}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }},
        # Month col width
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 100}, "fields": "pixelSize",
        }},
        # Platform col width
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 120}, "fields": "pixelSize",
        }},
        # Metric cols
        *[{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": 110}, "fields": "pixelSize",
        }} for i in range(2, n_cols)],
        # Section (month header) rows — accent colour per month
        *[{"repeatCell": {
            "range": {"sheetId": ws.id,
                      "startRowIndex": row_idx, "endRowIndex": row_idx + 1,
                      "startColumnIndex": 0, "endColumnIndex": n_cols},
            "cell": {"userEnteredFormat": {
                "backgroundColor": month_colors[i % len(month_colors)],
                "textFormat": {"bold": True,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                               "fontSize": 10},
            }},
            "fields": "userEnteredFormat",
        }} for i, row_idx in enumerate(section_rows)],
        # M0 ROAS column — bold
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": n_rows,
                      "startColumnIndex": n_cols - 1, "endColumnIndex": n_cols},
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat",
        }},
    ]

    sh.batch_update({"requests": requests})
    total_platforms = sum(len(m["rows"]) for m in platform_data)
    print(f"  Platform ROAS tab: {len(platform_data)} months, {total_platforms} platform-month rows written.")


def get_or_create_sheet(gc, sheet_id=None):
    import gspread
    if sheet_id:
        return gc.open_by_key(sheet_id)
    sh = gc.create(SHEET_NAME)
    print(f"Created new sheet: {sh.url}")
    if SHARE_WITH:
        sh.share(SHARE_WITH, perm_type="user", role="writer")
        print(f"Shared with {SHARE_WITH}")
    return sh


# ── Day-Level Ad Spend ─────────────���──────────────────────────────────────────
DAY_LEVEL_SQL = """
SELECT
    i.date,
    i.ad_id,
    i.ad_name,
    c.name                                                AS campaign_name,
    s.name                                                AS adset_name,
    ROUND(i.spend::numeric, 0)                            AS spend,
    i.impressions,
    i.clicks,
    CASE WHEN i.impressions > 0
         THEN ROUND(i.clicks::numeric * 100 / i.impressions, 3) END AS ctr,
    CASE WHEN i.impressions > 0
         THEN ROUND(i.spend::numeric * 1000 / i.impressions, 1) END AS cpm,
    CASE WHEN i.clicks > 0
         THEN ROUND(i.spend::numeric / i.clicks, 1) END  AS cpc
FROM insights_daily i
LEFT JOIN campaigns c ON c.id = i.campaign_id
LEFT JOIN adsets s    ON s.id = i.adset_id
WHERE i.attribution_window = '7d_click'
  AND i.date >= %(mtd)s
  AND i.spend > 0
ORDER BY i.date DESC, i.spend DESC
"""


def build_day_level_data(conn, ad_rows: list) -> list:
    """Fetch day-level spend and attach grade from scored ad_rows."""
    grade_map = {str(r["ad_id"]): r.get("_grade", "") for r in ad_rows if r.get("ad_id")}
    rows = q(conn, DAY_LEVEL_SQL, {"mtd": mtd_start})
    for r in rows:
        r["_grade"] = grade_map.get(str(r["ad_id"]), "")
    return rows


def write_day_level_sheet(sh, rows: list):
    """Write 'Day Level — Ads' tab with per-ad per-day spend."""
    try:
        ws = sh.worksheet("Day Level — Ads")
        ws.clear()
    except Exception:
        ws = sh.add_worksheet("Day Level — Ads", rows=max(len(rows) + 50, 6000), cols=15)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    headers = [
        "Date", "Ad ID", "Ad Name", "Campaign", "Adset",
        "Spend ₹", "Impressions", "Clicks", "CTR %", "CPM ₹", "CPC ₹",
        "Grade",
    ]
    IDX_GRADE_DL = headers.index("Grade")

    def _v(v):  return "" if v is None else v
    def _i(v):  return "" if v is None else int(float(v))
    def _f(v, d=2): return "" if v is None else round(float(v), d)

    data_rows = [headers]
    for r in rows:
        data_rows.append([
            str(r["date"]) if r["date"] else "",
            r.get("ad_id") or "",
            r.get("ad_name") or "",
            r.get("campaign_name") or "",
            r.get("adset_name") or "",
            _i(r["spend"]),
            _i(r["impressions"]),
            _i(r["clicks"]),
            _f(r["ctr"], 3),
            _f(r["cpm"], 1),
            _f(r["cpc"], 1),
            r.get("_grade", ""),
        ])

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(rows)} rows"])

    ws.update(values=data_rows, range_name="A1")

    # Formatting
    body = {"requests": [
        # Header row
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": len(headers)},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                "textFormat": {"bold": True,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                               "fontSize": 9},
                "horizontalAlignment": "CENTER",
                "wrapStrategy": "WRAP",
            }},
            "fields": "userEnteredFormat",
        }},
        # Freeze header + first 3 cols
        {"updateSheetProperties": {
            "properties": {"sheetId": ws.id,
                           "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 3}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }},
        # Column widths
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 100}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 140}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 2, "endIndex": 3},
            "properties": {"pixelSize": 260}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 3, "endIndex": 4},
            "properties": {"pixelSize": 200}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 4, "endIndex": 5},
            "properties": {"pixelSize": 180}, "fields": "pixelSize",
        }},
        *[{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": 95}, "fields": "pixelSize",
        }} for i in range(5, IDX_GRADE_DL)],
        # Grade column width
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": IDX_GRADE_DL, "endIndex": IDX_GRADE_DL + 1},
            "properties": {"pixelSize": 175}, "fields": "pixelSize",
        }},
        # Alternating row shading (data cols, not grade)
        {"addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                            "endRowIndex": len(data_rows),
                            "startColumnIndex": 0, "endColumnIndex": IDX_GRADE_DL}],
                "booleanRule": {
                    "condition": {"type": "CUSTOM_FORMULA",
                                  "values": [{"userEnteredValue": "=ISEVEN(ROW())"}]},
                    "format": {"backgroundColor": {"red": 0.957, "green": 0.965, "blue": 0.976}},
                },
            },
            "index": 0,
        }},
        # Grade column — colour per label
        *[{"addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                            "endRowIndex": len(data_rows),
                            "startColumnIndex": IDX_GRADE_DL, "endColumnIndex": IDX_GRADE_DL + 1}],
                "booleanRule": {
                    "condition": {"type": "TEXT_EQ",
                                  "values": [{"userEnteredValue": label}]},
                    "format": {"backgroundColor": bg,
                               "textFormat": {"bold": True, "foregroundColor": fg}},
                },
            },
            "index": idx + 1,
        }} for idx, (label, bg, fg) in enumerate([
            ("TOP PERFORMER",     {"red": 0.137, "green": 0.612, "blue": 0.290},
                                  {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("GOOD",              {"red": 0.714, "green": 0.882, "blue": 0.722},
                                  {"red": 0.0,   "green": 0.239, "blue": 0.086}),
            ("AVERAGE",           {"red": 1.0,   "green": 0.898, "blue": 0.600},
                                  {"red": 0.4,   "green": 0.267, "blue": 0.0}),
            ("UNDERPERFORMING",   {"red": 1.0,   "green": 0.639, "blue": 0.353},
                                  {"red": 0.525, "green": 0.161, "blue": 0.0}),
            ("POOR",              {"red": 0.914, "green": 0.263, "blue": 0.208},
                                  {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("INEFFICIENT CAT 1", {"red": 0.545, "green": 0.0,   "blue": 0.0},
                                  {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("INEFFICIENT CAT 2", {"red": 0.698, "green": 0.133, "blue": 0.133},
                                  {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("INEFFICIENT CAT 3", {"red": 0.804, "green": 0.361, "blue": 0.361},
                                  {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("OPPORTUNITY",       {"red": 0.118, "green": 0.533, "blue": 0.898},
                                  {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("FULL IMMATURE",     {"red": 0.800, "green": 0.824, "blue": 0.855},
                                  {"red": 0.267, "green": 0.306, "blue": 0.365}),
            ("PARTIAL IMMATURE",  {"red": 0.878, "green": 0.890, "blue": 0.914},
                                  {"red": 0.400, "green": 0.420, "blue": 0.470}),
            ("NO DATA",           {"red": 0.910, "green": 0.910, "blue": 0.910},
                                  {"red": 0.4,   "green": 0.4,   "blue": 0.4}),
        ])],
    ]}
    sh.batch_update(body)
    print(f"  Day Level tab: {len(rows)} rows written.")


# ── Grade Movement Tracking & Email ───────────────────────────────────────────
SNAPSHOT_FILE = Path(__file__).parent / ".grade_snapshot.json"
EMAIL_RECIPIENTS = [
    "pranit@univest.in",
    "ripal.vachher@univest.in",
    "anmol.gandhi@univest.in",
]
GMAIL_SENDER   = os.environ.get("GMAIL_SENDER", "")      # e.g. alerts@univest.in
GMAIL_APP_PASS = os.environ.get("GMAIL_APP_PASSWORD", "") # Gmail app password

ALL_GRADES = [
    "INEFFICIENT CAT 1", "INEFFICIENT CAT 2", "INEFFICIENT CAT 3",
    "POOR", "UNDERPERFORMING", "AVERAGE", "GOOD", "TOP PERFORMER",
    "OPPORTUNITY", "PARTIAL IMMATURE", "FULL IMMATURE", "NO DATA",
]


def _load_snapshot() -> dict:
    if SNAPSHOT_FILE.exists():
        return json.loads(SNAPSHOT_FILE.read_text())
    return {}


def _save_snapshot(ad_rows: list):
    snap = {}
    for r in ad_rows:
        ad_id = str(r.get("ad_id") or "")
        grade = r.get("_grade", "")
        if ad_id and grade:
            snap[ad_id] = {
                "grade": grade,
                "ad_name": r.get("ad_name") or "",
                "campaign": r.get("campaign_name") or "",
                "spend": float(r.get("spend") or 0),
            }
    SNAPSHOT_FILE.write_text(json.dumps(snap, indent=2))


def compute_grade_movements(ad_rows: list) -> dict:
    """
    Compare current grades with last snapshot.
    Returns: {
        "POOR → INEFFICIENT CAT 1": [{"ad_name": ..., "campaign": ..., "spend": ...}, ...],
        ...
    }
    """
    prev = _load_snapshot()
    if not prev:
        return {}

    movements: dict[str, list] = {}
    for r in ad_rows:
        ad_id = str(r.get("ad_id") or "")
        new_grade = r.get("_grade", "")
        if not ad_id or not new_grade:
            continue
        old = prev.get(ad_id)
        if not old:
            continue
        old_grade = old.get("grade", "")
        if old_grade and old_grade != new_grade:
            key = f"{old_grade} → {new_grade}"
            if key not in movements:
                movements[key] = []
            movements[key].append({
                "ad_name": r.get("ad_name") or "",
                "campaign": r.get("campaign_name") or "",
                "spend": float(r.get("spend") or 0),
            })

    return movements


def _build_movement_summary(movements: dict) -> dict:
    """Build a summary: {grade: {"in": count, "out": count}} for net flow."""
    summary: dict[str, dict[str, int]] = {g: {"in": 0, "out": 0} for g in ALL_GRADES}
    for transition, ads in movements.items():
        old_g, new_g = transition.split(" → ")
        count = len(ads)
        if old_g in summary:
            summary[old_g]["out"] += count
        if new_g in summary:
            summary[new_g]["in"] += count
    return summary


def build_movement_email_html(movements: dict) -> str:
    """Build an HTML email body for grade movements."""
    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    summary = _build_movement_summary(movements)

    html = f"""
    <html><body style="font-family: -apple-system, Arial, sans-serif; color: #1a1a2e; padding: 20px;">
    <h2 style="margin-bottom: 4px;">Ad Grade Movement Report</h2>
    <p style="color: #666; margin-top: 0;">{now_str}</p>

    <h3>Category Summary</h3>
    <table style="border-collapse: collapse; width: 100%; max-width: 650px;">
    <tr style="background: #1a3461; color: white;">
        <th style="padding: 8px 12px; text-align: left;">Category</th>
        <th style="padding: 8px 12px; text-align: center;">Moved In</th>
        <th style="padding: 8px 12px; text-align: center;">Moved Out</th>
        <th style="padding: 8px 12px; text-align: center;">Net</th>
    </tr>"""

    GRADE_COLORS = {
        "INEFFICIENT CAT 1": "#8b0000", "INEFFICIENT CAT 2": "#b22222",
        "INEFFICIENT CAT 3": "#cd5c5c", "POOR": "#e94335",
        "UNDERPERFORMING": "#ff7f50", "AVERAGE": "#e5b800",
        "GOOD": "#4caf50", "TOP PERFORMER": "#238b4a",
        "OPPORTUNITY": "#1e88e5",
        "PARTIAL IMMATURE": "#b0bec5", "FULL IMMATURE": "#90a4ae",
        "NO DATA": "#e0e0e0",
    }

    for grade in ALL_GRADES:
        s = summary.get(grade, {"in": 0, "out": 0})
        if s["in"] == 0 and s["out"] == 0:
            continue
        net = s["in"] - s["out"]
        net_str = f"+{net}" if net > 0 else str(net)
        net_color = "#238b4a" if net > 0 else "#e94335" if net < 0 else "#666"
        bg = "#f8f9fa" if ALL_GRADES.index(grade) % 2 == 0 else "#fff"
        gc = GRADE_COLORS.get(grade, "#333")
        html += f"""
    <tr style="background: {bg};">
        <td style="padding: 8px 12px;"><span style="color: {gc}; font-weight: bold;">{'●'} {grade}</span></td>
        <td style="padding: 8px 12px; text-align: center; color: #238b4a;">{s['in'] if s['in'] else '—'}</td>
        <td style="padding: 8px 12px; text-align: center; color: #e94335;">{s['out'] if s['out'] else '—'}</td>
        <td style="padding: 8px 12px; text-align: center; color: {net_color}; font-weight: bold;">{net_str}</td>
    </tr>"""

    html += "</table>"

    # Detail section: list transitions with top ads
    html += "<h3>Movement Details</h3>"
    for transition, ads in sorted(movements.items(), key=lambda x: -len(x[1])):
        total_spend = sum(a["spend"] for a in ads)
        html += f"""
    <div style="margin-bottom: 16px; padding: 12px; background: #f8f9fa; border-left: 4px solid #1a3461; border-radius: 4px;">
        <strong>{transition}</strong> — {len(ads)} ad{'s' if len(ads) != 1 else ''} (₹{int(total_spend):,} spend)
        <ul style="margin: 6px 0 0 0; padding-left: 20px; color: #444;">"""
        for a in sorted(ads, key=lambda x: -x["spend"])[:5]:
            html += f"""
            <li>{a['ad_name']} <span style="color: #888;">— {a['campaign'][:50]} — ₹{int(a['spend']):,}</span></li>"""
        if len(ads) > 5:
            html += f"""
            <li style="color: #888;">... and {len(ads) - 5} more</li>"""
        html += """
        </ul>
    </div>"""

    html += """
    <p style="color: #999; font-size: 12px; margin-top: 24px;">
        Sent automatically by Univest Ads Dashboard.
        <a href="https://docs.google.com/spreadsheets/d/1EBu7vZWGdLUVdL4I6a0J22soLIoXKWWIRRWTGk3BZ7s">Open Sheet</a>
    </p>
    </body></html>"""
    return html


def send_movement_email(movements: dict):
    """Send grade movement email via Gmail SMTP."""
    if not GMAIL_SENDER or not GMAIL_APP_PASS:
        print("  Email: skipped (GMAIL_SENDER / GMAIL_APP_PASSWORD not set)")
        return
    if not movements:
        print("  Email: skipped (no grade movements)")
        return

    total_moves = sum(len(ads) for ads in movements.values())
    html = build_movement_email_html(movements)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Ads Dashboard] {total_moves} ad{'s' if total_moves != 1 else ''} changed grade — {datetime.now().strftime('%d %b %H:%M')}"
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = ", ".join(EMAIL_RECIPIENTS)
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_APP_PASS)
            server.sendmail(GMAIL_SENDER, EMAIL_RECIPIENTS, msg.as_string())
        print(f"  Email: sent to {len(EMAIL_RECIPIENTS)} recipients ({total_moves} movements)")
    except Exception as exc:
        print(f"  Email: FAILED — {exc}")


def main():
    import gspread
    from google.oauth2.service_account import Credentials

    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet-id", default=os.environ.get("DASHBOARD_SHEET_ID", ""))
    args = parser.parse_args()

    print(f"Connecting to DB...")
    conn = db_conn()

    print("Fetching metrics...")
    data = build_data(conn)
    print("Fetching ad-level data...")
    ad_rows = build_ad_data(conn)
    print(f"  {len(ad_rows)} ads found.")
    print("Fetching DoD data...")
    dod_rows = build_dod_data(conn)
    print("Fetching platform ROAS data...")
    platform_data = build_platform_roas_data(conn)
    print("Fetching day-level ad spend...")
    day_rows = build_day_level_data(conn, ad_rows)
    print(f"  {len(day_rows)} day-level rows found.")
    conn.close()

    # Grade movement tracking
    print("Checking grade movements...")
    movements = compute_grade_movements(ad_rows)
    _save_snapshot(ad_rows)
    send_movement_email(movements)

    print("Connecting to Google Sheets...")
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    gc = gspread.authorize(creds)

    sh = get_or_create_sheet(gc, args.sheet_id or None)
    print(f"Writing to: {sh.url}")
    write_sheet(sh, data)
    write_ad_level_sheet(sh, ad_rows)
    write_dod_sheet(sh, dod_rows)
    write_platform_roas_sheet(sh, platform_data)
    write_day_level_sheet(sh, day_rows)
    print(f"\nDone. Open sheet: {sh.url}")
    print(f"Sheet ID (save for --sheet-id): {sh.id}")


if __name__ == "__main__":
    main()
