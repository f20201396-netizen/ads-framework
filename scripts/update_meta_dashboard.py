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
mature_end    = today - timedelta(days=7)   # 7+ days old = fully mature (D6 complete)
mid_start     = today - timedelta(days=6)   # \
mid_end       = today - timedelta(days=3)   #  4-day mid window (D0 done, D6 partial)
recent_start  = today - timedelta(days=2)   # today, yesterday, dby
# Ad × Date day-window boundaries (all relative to today)
dw_d0d2_start   = today - timedelta(days=2)    # d0-d2:   today, yday, dby
dw_d3d5_start   = today - timedelta(days=5)    # d3-d5
dw_d3d5_end     = today - timedelta(days=3)
dw_d6d8_start   = today - timedelta(days=8)    # d6-d8
dw_d6d8_end     = today - timedelta(days=6)
dw_d9d10_start  = today - timedelta(days=10)   # d9-d10
dw_d9d10_end    = today - timedelta(days=9)
dw_d11d13_start = today - timedelta(days=13)   # d11-d13
dw_d11d13_end   = today - timedelta(days=11)
dw_d14p_end     = today - timedelta(days=14)   # d14+: oldest

# ── SQL ───────────────────────────────────────────────────────────────────────
# Spend from insights_account_daily (authoritative account-level totals)
SPEND_SQL = """
SELECT
    COALESCE(SUM(CASE WHEN date = %(today)s THEN spend END), 0)               AS today_spend,
    COALESCE(SUM(CASE WHEN date >= %(mtd)s   THEN spend END), 0)              AS mtd_spend,
    COALESCE(SUM(CASE WHEN date >= %(lm)s AND date <= %(lm_same)s
                      THEN spend END), 0)                                      AS lm_spend,
    COALESCE(SUM(CASE WHEN date = %(lm_same)s THEN spend END), 0)             AS lm_today_spend
FROM insights_account_daily
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
    return f"₹{_inr_indian(int(f))}"


def _inr_indian(n) -> str:
    """Integer → Indian lakh-crore grouping: 100000 → '1,00,000', 11191997 → '1,11,91,997'.

    Standard Indian numbering: last 3 digits, then groups of 2 thousands separated by commas.
    """
    n = int(n)
    if n < 0:
        return '-' + _inr_indian(-n)
    s = str(n)
    if len(s) <= 3:
        return s
    last3 = s[-3:]
    rest = s[:-3]
    parts = []
    while len(rest) > 2:
        parts.insert(0, rest[-2:])
        rest = rest[:-2]
    if rest:
        parts.insert(0, rest)
    return ','.join(parts) + ',' + last3


# Google Sheets cell number formats. PERCENT type is reliable across locales —
# it auto-multiplies the cell value by 100 and appends '%'. Currency must be
# done as pre-formatted strings (Sheets ignores custom Indian grouping patterns
# when the spreadsheet locale is en_US, which is the default).
NUMFMT_PERCENT = {"type": "PERCENT", "pattern": '0.0%'}


def _inr_str(value, decimals: int = 0) -> str:
    """Format a numeric value as '₹3,30,76,874' / '₹170.5' (Indian grouping)."""
    if value is None or value == "" or value == 0:
        return "" if value in (None, "") else "₹0"
    f = float(value)
    intpart = int(f)
    sign = "-" if intpart < 0 else ""
    intpart_abs = abs(intpart)
    if decimals == 0:
        return f"{sign}₹{_inr_indian(intpart_abs)}"
    frac = abs(f) - intpart_abs
    frac_str = (f"{frac:.{decimals}f}")[1:]  # strip leading '0' from '0.5'
    return f"{sign}₹{_inr_indian(intpart_abs)}{frac_str}"


def _fmt_request(ws_id, col_idx, start_row, end_row, number_format):
    """Build a single repeatCell request that applies a number format to one column."""
    return {"repeatCell": {
        "range": {"sheetId": ws_id, "startRowIndex": start_row, "endRowIndex": end_row,
                  "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
        "cell": {"userEnteredFormat": {"numberFormat": number_format}},
        "fields": "userEnteredFormat.numberFormat",
    }}


def _auto_format_requests(ws_id, headers: list[str], start_row: int, end_row: int) -> list[dict]:
    """Apply PERCENT format to any column whose header contains 'ROAS'.

    Currency columns are handled by writing pre-formatted strings (see _inr_str)
    because Sheets ignores custom Indian-grouping patterns under en_US locale.
    """
    reqs = []
    for col_idx, h in enumerate(headers):
        if h and 'ROAS' in h:
            reqs.append(_fmt_request(ws_id, col_idx, start_row, end_row, NUMFMT_PERCENT))
    return reqs

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

    try:
        ws = sh.worksheet("Dashboard")
    except Exception:
        ws = sh.sheet1
        ws.update_title("Dashboard")
    ws.clear()

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
WITH first_dates AS (
    SELECT ad_id, MIN(date) AS first_date
    FROM insights_daily
    WHERE attribution_window = '7d_click' AND spend > 0
    GROUP BY ad_id
),
media AS (
    SELECT
        i.ad_id,
        MAX(i.ad_name)       AS ad_name,
        MAX(i.campaign_id)   AS campaign_id,
        MAX(i.adset_id)      AS adset_id,
        -- Full MTD
        ROUND(SUM(i.spend)::numeric, 0)                                   AS spend,
        SUM(i.impressions)                                                 AS impressions,
        SUM(i.clicks)                                                      AS clicks,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.clicks)::numeric * 100 / SUM(i.impressions), 3) END AS ctr,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.spend)::numeric * 1000 / SUM(i.impressions), 1) END AS cpm,
        CASE WHEN SUM(i.clicks) > 0
             THEN ROUND(SUM(i.spend)::numeric / SUM(i.clicks), 1) END     AS cpc,
        MAX(i.date) AS last_date,
        -- Mature (7+ days old, D6 complete — decides DNA)
        ROUND(COALESCE(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.spend END), 0)::numeric, 0)              AS mature_spend,
        COALESCE(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END), 0)                            AS mature_impressions,
        COALESCE(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END), 0)                                 AS mature_clicks,
        CASE WHEN SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END)::numeric * 100
                      / SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END), 3) END                 AS mature_ctr,
        CASE WHEN SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.spend END)::numeric * 1000
                      / SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END), 1) END                 AS mature_cpm,
        CASE WHEN SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END) > 0
             THEN ROUND(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.spend END)::numeric
                      / SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END), 1) END                      AS mature_cpc,
        -- Mid (days today-6 to today-3, D0 complete, D6 partial)
        ROUND(COALESCE(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.spend END), 0)::numeric, 0)  AS mid_spend,
        COALESCE(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END), 0)               AS mid_impressions,
        COALESCE(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END), 0)                    AS mid_clicks,
        CASE WHEN SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END)::numeric * 100
                      / SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END), 3) END    AS mid_ctr,
        CASE WHEN SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.spend END)::numeric * 1000
                      / SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END), 1) END    AS mid_cpm,
        CASE WHEN SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END) > 0
             THEN ROUND(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.spend END)::numeric
                      / SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END), 1) END         AS mid_cpc,
        -- Recent (today, yesterday, dby — last 3 days)
        ROUND(COALESCE(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.spend END), 0)::numeric, 0)             AS recent_spend,
        COALESCE(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END), 0)                          AS recent_impressions,
        COALESCE(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END), 0)                               AS recent_clicks,
        CASE WHEN SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END)::numeric * 100
                      / SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END), 3) END               AS recent_ctr,
        CASE WHEN SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.spend END)::numeric * 1000
                      / SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END), 1) END               AS recent_cpm,
        CASE WHEN SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END) > 0
             THEN ROUND(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.spend END)::numeric
                      / SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END), 1) END                    AS recent_cpc,
        -- Day-window spend (relative to today, for Ad × Date tab)
        ROUND(COALESCE(SUM(CASE WHEN i.date >= %(dw_d0d2_start)s                                       THEN i.spend END), 0)::numeric, 0) AS d0d2_spend,
        ROUND(COALESCE(SUM(CASE WHEN i.date BETWEEN %(dw_d3d5_start)s   AND %(dw_d3d5_end)s            THEN i.spend END), 0)::numeric, 0) AS d3d5_spend,
        ROUND(COALESCE(SUM(CASE WHEN i.date BETWEEN %(dw_d6d8_start)s   AND %(dw_d6d8_end)s            THEN i.spend END), 0)::numeric, 0) AS d6d8_spend,
        ROUND(COALESCE(SUM(CASE WHEN i.date BETWEEN %(dw_d9d10_start)s  AND %(dw_d9d10_end)s           THEN i.spend END), 0)::numeric, 0) AS d9d10_spend,
        ROUND(COALESCE(SUM(CASE WHEN i.date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s          THEN i.spend END), 0)::numeric, 0) AS d11d13_spend,
        ROUND(COALESCE(SUM(CASE WHEN i.date <= %(dw_d14p_end)s                                         THEN i.spend END), 0)::numeric, 0) AS d14p_spend
    FROM insights_daily i
    WHERE i.attribution_window = '7d_click'
      AND i.spend > 0
      AND i.date >= %(attr_since)s
    GROUP BY i.ad_id
),
attr AS (
    SELECT
        ae.meta_creative_id                                               AS ad_id,
        -- Full period
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
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = TRUE
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = FALSE
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6_non_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                 THEN ae.revenue_inr ELSE 0 END)                          AS total_revenue,
        -- Mature attribution (7+ days old, D6 complete)
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d0_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d0_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d6_non_mandate,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = TRUE
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = FALSE
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d0_trials,
        -- Mid attribution (days today-6 to today-3, D0 complete, D6 partial)
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d0_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d0_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d6_non_mandate,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = TRUE
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = FALSE
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d0_trials,
        -- Recent attribution (today, yesterday, dby)
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d0_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                  AND ae.install_date >= %(recent_start)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS recent_d0_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d6_non_mandate,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date >= %(recent_start)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS recent_d6_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d0_trials,
        -- Day-window attribution (relative to today, for Ad × Date tab)
        -- d0-d2: today, yesterday, dby
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date >= %(dw_d0d2_start)s
                            THEN ae.user_id END)                          AS d0d2_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date >= %(dw_d0d2_start)s
                            THEN ae.user_id END)                          AS d0d2_d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date >= %(dw_d0d2_start)s
                            THEN ae.user_id END)                          AS d0d2_d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date >= %(dw_d0d2_start)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d0d2_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = TRUE
                  AND ae.install_date >= %(dw_d0d2_start)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d0d2_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = FALSE
                  AND ae.install_date >= %(dw_d0d2_start)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d0d2_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.install_date >= %(dw_d0d2_start)s
                            THEN ae.user_id END)                          AS d0d2_d6_conv,
        -- d3-d5
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date BETWEEN %(dw_d3d5_start)s AND %(dw_d3d5_end)s
                            THEN ae.user_id END)                          AS d3d5_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d3d5_start)s AND %(dw_d3d5_end)s
                            THEN ae.user_id END)                          AS d3d5_d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d3d5_start)s AND %(dw_d3d5_end)s
                            THEN ae.user_id END)                          AS d3d5_d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date BETWEEN %(dw_d3d5_start)s AND %(dw_d3d5_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d3d5_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = TRUE
                  AND ae.install_date BETWEEN %(dw_d3d5_start)s AND %(dw_d3d5_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d3d5_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = FALSE
                  AND ae.install_date BETWEEN %(dw_d3d5_start)s AND %(dw_d3d5_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d3d5_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.install_date BETWEEN %(dw_d3d5_start)s AND %(dw_d3d5_end)s
                            THEN ae.user_id END)                          AS d3d5_d6_conv,
        -- d6-d8
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date BETWEEN %(dw_d6d8_start)s AND %(dw_d6d8_end)s
                            THEN ae.user_id END)                          AS d6d8_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d6d8_start)s AND %(dw_d6d8_end)s
                            THEN ae.user_id END)                          AS d6d8_d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d6d8_start)s AND %(dw_d6d8_end)s
                            THEN ae.user_id END)                          AS d6d8_d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date BETWEEN %(dw_d6d8_start)s AND %(dw_d6d8_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6d8_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = TRUE
                  AND ae.install_date BETWEEN %(dw_d6d8_start)s AND %(dw_d6d8_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6d8_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = FALSE
                  AND ae.install_date BETWEEN %(dw_d6d8_start)s AND %(dw_d6d8_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6d8_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.install_date BETWEEN %(dw_d6d8_start)s AND %(dw_d6d8_end)s
                            THEN ae.user_id END)                          AS d6d8_d6_conv,
        -- d9-d10
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date BETWEEN %(dw_d9d10_start)s AND %(dw_d9d10_end)s
                            THEN ae.user_id END)                          AS d9d10_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d9d10_start)s AND %(dw_d9d10_end)s
                            THEN ae.user_id END)                          AS d9d10_d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d9d10_start)s AND %(dw_d9d10_end)s
                            THEN ae.user_id END)                          AS d9d10_d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date BETWEEN %(dw_d9d10_start)s AND %(dw_d9d10_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d9d10_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = TRUE
                  AND ae.install_date BETWEEN %(dw_d9d10_start)s AND %(dw_d9d10_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d9d10_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = FALSE
                  AND ae.install_date BETWEEN %(dw_d9d10_start)s AND %(dw_d9d10_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d9d10_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.install_date BETWEEN %(dw_d9d10_start)s AND %(dw_d9d10_end)s
                            THEN ae.user_id END)                          AS d9d10_d6_conv,
        -- d11-d13
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s
                            THEN ae.user_id END)                          AS d11d13_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s
                            THEN ae.user_id END)                          AS d11d13_d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s
                            THEN ae.user_id END)                          AS d11d13_d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d11d13_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = TRUE
                  AND ae.install_date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d11d13_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = FALSE
                  AND ae.install_date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d11d13_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.install_date BETWEEN %(dw_d11d13_start)s AND %(dw_d11d13_end)s
                            THEN ae.user_id END)                          AS d11d13_d6_conv,
        -- d14+
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date <= %(dw_d14p_end)s
                            THEN ae.user_id END)                          AS d14p_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date <= %(dw_d14p_end)s
                            THEN ae.user_id END)                          AS d14p_d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date <= %(dw_d14p_end)s
                            THEN ae.user_id END)                          AS d14p_d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date <= %(dw_d14p_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d14p_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = TRUE
                  AND ae.install_date <= %(dw_d14p_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d14p_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 AND ae.is_mandate = FALSE
                  AND ae.install_date <= %(dw_d14p_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS d14p_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.install_date <= %(dw_d14p_end)s
                            THEN ae.user_id END)                          AS d14p_d6_conv
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
    fd.first_date,
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
    CASE WHEN m.spend > 0 AND COALESCE(at.d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d6_mandate_revenue::numeric / m.spend, 3) END      AS d6_mandate_roas,
    CASE WHEN m.spend > 0 AND COALESCE(at.d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d6_non_mandate_revenue::numeric / m.spend, 3) END  AS d6_non_mandate_roas,
    CASE WHEN m.spend > 0
              AND (COALESCE(at.d6_mandate, 0) + COALESCE(at.d6_non_mandate, 0)) > 0
         THEN ROUND(m.spend::numeric /
                    (COALESCE(at.d6_mandate, 0) + COALESCE(at.d6_non_mandate, 0)),
                    0) END                                                 AS d6_cac,
    CASE WHEN COALESCE(at.signups, 0) > 0
         THEN ROUND(at.total_revenue::numeric / at.signups, 0) END        AS ltv_inr,
    CASE WHEN m.spend > 0 AND COALESCE(at.signups, 0) > 0
         THEN ROUND(m.spend::numeric / at.signups, 0) END                 AS cac_inr,
    -- Mature media
    m.mature_spend, m.mature_impressions, m.mature_clicks, m.mature_ctr, m.mature_cpm, m.mature_cpc,
    -- Mature attribution derived
    COALESCE(at.mature_signups, 0)                                                    AS mature_signups,
    COALESCE(at.mature_d0_conv, 0)                                                    AS mature_d0_conv,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d0_conv, 0) > 0
         THEN ROUND(m.mature_spend::numeric / at.mature_d0_conv, 0) END               AS mature_d0_cac,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d0_revenue, 0) > 0
         THEN ROUND(at.mature_d0_revenue::numeric / m.mature_spend, 3) END            AS mature_d0_roas,
    CASE WHEN m.mature_spend > 0
              AND (COALESCE(at.mature_d6_mandate, 0) + COALESCE(at.mature_d6_non_mandate, 0)) > 0
         THEN ROUND(m.mature_spend::numeric /
                    (at.mature_d6_mandate + at.mature_d6_non_mandate), 0) END         AS mature_d6_cac,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d6_revenue, 0) > 0
         THEN ROUND(at.mature_d6_revenue::numeric / m.mature_spend, 3) END            AS mature_d6_roas,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.mature_d6_mandate_revenue::numeric / m.mature_spend, 3) END    AS mature_d6_mandate_roas,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.mature_d6_non_mandate_revenue::numeric / m.mature_spend, 3) END AS mature_d6_non_mandate_roas,
    COALESCE(at.mature_d0_trials, 0)                                                  AS mature_d0_trials,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d0_trials, 0) > 0
         THEN ROUND(m.mature_spend::numeric / at.mature_d0_trials, 0) END             AS mature_d0_trial_cost,
    -- Mid media
    m.mid_spend, m.mid_impressions, m.mid_clicks, m.mid_ctr, m.mid_cpm, m.mid_cpc,
    -- Mid attribution derived
    COALESCE(at.mid_signups, 0)                                                       AS mid_signups,
    COALESCE(at.mid_d0_conv, 0)                                                       AS mid_d0_conv,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d0_conv, 0) > 0
         THEN ROUND(m.mid_spend::numeric / at.mid_d0_conv, 0) END                     AS mid_d0_cac,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d0_revenue, 0) > 0
         THEN ROUND(at.mid_d0_revenue::numeric / m.mid_spend, 3) END                  AS mid_d0_roas,
    CASE WHEN m.mid_spend > 0
              AND (COALESCE(at.mid_d6_mandate, 0) + COALESCE(at.mid_d6_non_mandate, 0)) > 0
         THEN ROUND(m.mid_spend::numeric /
                    (at.mid_d6_mandate + at.mid_d6_non_mandate), 0) END               AS mid_d6_cac,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d6_revenue, 0) > 0
         THEN ROUND(at.mid_d6_revenue::numeric / m.mid_spend, 3) END                  AS mid_d6_roas,
    COALESCE(at.mid_d0_trials, 0)                                                     AS mid_d0_trials,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d0_trials, 0) > 0
         THEN ROUND(m.mid_spend::numeric / at.mid_d0_trials, 0) END                   AS mid_d0_trial_cost,
    -- Recent media (today, yesterday, dby)
    m.recent_spend, m.recent_impressions, m.recent_clicks, m.recent_ctr, m.recent_cpm, m.recent_cpc,
    -- Recent attribution derived
    COALESCE(at.recent_signups, 0)                                                    AS recent_signups,
    COALESCE(at.recent_d0_conv, 0)                                                    AS recent_d0_conv,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d0_conv, 0) > 0
         THEN ROUND(m.recent_spend::numeric / at.recent_d0_conv, 0) END               AS recent_d0_cac,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d0_revenue, 0) > 0
         THEN ROUND(at.recent_d0_revenue::numeric / m.recent_spend, 3) END            AS recent_d0_roas,
    CASE WHEN m.recent_spend > 0
              AND (COALESCE(at.recent_d6_mandate, 0) + COALESCE(at.recent_d6_non_mandate, 0)) > 0
         THEN ROUND(m.recent_spend::numeric /
                    (at.recent_d6_mandate + at.recent_d6_non_mandate), 0) END         AS recent_d6_cac,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d6_revenue, 0) > 0
         THEN ROUND(at.recent_d6_revenue::numeric / m.recent_spend, 3) END            AS recent_d6_roas,
    COALESCE(at.recent_d0_trials, 0)                                                  AS recent_d0_trials,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d0_trials, 0) > 0
         THEN ROUND(m.recent_spend::numeric / at.recent_d0_trials, 0) END             AS recent_d0_trial_cost,
    -- Ad-age day-window derived (for Ad × Date tab)
    m.d0d2_spend,
    COALESCE(at.d0d2_signups, 0)                                                       AS d0d2_signups,
    COALESCE(at.d0d2_d0_conv, 0)                                                       AS d0d2_d0_conv,
    COALESCE(at.d0d2_d0_trials, 0)                                                     AS d0d2_d0_trials,
    COALESCE(at.d0d2_d6_conv, 0)                                                       AS d0d2_d6_conv,
    CASE WHEN m.d0d2_spend > 0 AND COALESCE(at.d0d2_d0_conv, 0) > 0
         THEN ROUND(m.d0d2_spend::numeric / at.d0d2_d0_conv, 0) END                   AS d0d2_d0_cac,
    CASE WHEN m.d0d2_spend > 0 AND COALESCE(at.d0d2_d0_trials, 0) > 0
         THEN ROUND(m.d0d2_spend::numeric / at.d0d2_d0_trials, 0) END                 AS d0d2_d0_trial_cost,
    CASE WHEN m.d0d2_spend > 0 AND COALESCE(at.d0d2_d6_conv, 0) > 0
         THEN ROUND(m.d0d2_spend::numeric / at.d0d2_d6_conv, 0) END                   AS d0d2_d6_cac,
    CASE WHEN m.d0d2_spend > 0 AND COALESCE(at.d0d2_d6_revenue, 0) > 0
         THEN ROUND(at.d0d2_d6_revenue::numeric / m.d0d2_spend, 3) END                AS d0d2_d6_roas,
    CASE WHEN m.d0d2_spend > 0 AND COALESCE(at.d0d2_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d0d2_d6_mandate_revenue::numeric / m.d0d2_spend, 3) END        AS d0d2_d6_mandate_roas,
    CASE WHEN m.d0d2_spend > 0 AND COALESCE(at.d0d2_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d0d2_d6_non_mandate_revenue::numeric / m.d0d2_spend, 3) END    AS d0d2_d6_non_mandate_roas,
    m.d3d5_spend,
    COALESCE(at.d3d5_signups, 0)                                                       AS d3d5_signups,
    COALESCE(at.d3d5_d0_conv, 0)                                                       AS d3d5_d0_conv,
    COALESCE(at.d3d5_d0_trials, 0)                                                     AS d3d5_d0_trials,
    COALESCE(at.d3d5_d6_conv, 0)                                                       AS d3d5_d6_conv,
    CASE WHEN m.d3d5_spend > 0 AND COALESCE(at.d3d5_d0_conv, 0) > 0
         THEN ROUND(m.d3d5_spend::numeric / at.d3d5_d0_conv, 0) END                   AS d3d5_d0_cac,
    CASE WHEN m.d3d5_spend > 0 AND COALESCE(at.d3d5_d0_trials, 0) > 0
         THEN ROUND(m.d3d5_spend::numeric / at.d3d5_d0_trials, 0) END                 AS d3d5_d0_trial_cost,
    CASE WHEN m.d3d5_spend > 0 AND COALESCE(at.d3d5_d6_conv, 0) > 0
         THEN ROUND(m.d3d5_spend::numeric / at.d3d5_d6_conv, 0) END                   AS d3d5_d6_cac,
    CASE WHEN m.d3d5_spend > 0 AND COALESCE(at.d3d5_d6_revenue, 0) > 0
         THEN ROUND(at.d3d5_d6_revenue::numeric / m.d3d5_spend, 3) END                AS d3d5_d6_roas,
    CASE WHEN m.d3d5_spend > 0 AND COALESCE(at.d3d5_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d3d5_d6_mandate_revenue::numeric / m.d3d5_spend, 3) END        AS d3d5_d6_mandate_roas,
    CASE WHEN m.d3d5_spend > 0 AND COALESCE(at.d3d5_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d3d5_d6_non_mandate_revenue::numeric / m.d3d5_spend, 3) END    AS d3d5_d6_non_mandate_roas,
    m.d6d8_spend,
    COALESCE(at.d6d8_signups, 0)                                                       AS d6d8_signups,
    COALESCE(at.d6d8_d0_conv, 0)                                                       AS d6d8_d0_conv,
    COALESCE(at.d6d8_d0_trials, 0)                                                     AS d6d8_d0_trials,
    COALESCE(at.d6d8_d6_conv, 0)                                                       AS d6d8_d6_conv,
    CASE WHEN m.d6d8_spend > 0 AND COALESCE(at.d6d8_d0_conv, 0) > 0
         THEN ROUND(m.d6d8_spend::numeric / at.d6d8_d0_conv, 0) END                   AS d6d8_d0_cac,
    CASE WHEN m.d6d8_spend > 0 AND COALESCE(at.d6d8_d0_trials, 0) > 0
         THEN ROUND(m.d6d8_spend::numeric / at.d6d8_d0_trials, 0) END                 AS d6d8_d0_trial_cost,
    CASE WHEN m.d6d8_spend > 0 AND COALESCE(at.d6d8_d6_conv, 0) > 0
         THEN ROUND(m.d6d8_spend::numeric / at.d6d8_d6_conv, 0) END                   AS d6d8_d6_cac,
    CASE WHEN m.d6d8_spend > 0 AND COALESCE(at.d6d8_d6_revenue, 0) > 0
         THEN ROUND(at.d6d8_d6_revenue::numeric / m.d6d8_spend, 3) END                AS d6d8_d6_roas,
    CASE WHEN m.d6d8_spend > 0 AND COALESCE(at.d6d8_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d6d8_d6_mandate_revenue::numeric / m.d6d8_spend, 3) END        AS d6d8_d6_mandate_roas,
    CASE WHEN m.d6d8_spend > 0 AND COALESCE(at.d6d8_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d6d8_d6_non_mandate_revenue::numeric / m.d6d8_spend, 3) END    AS d6d8_d6_non_mandate_roas,
    m.d9d10_spend,
    COALESCE(at.d9d10_signups, 0)                                                      AS d9d10_signups,
    COALESCE(at.d9d10_d0_conv, 0)                                                      AS d9d10_d0_conv,
    COALESCE(at.d9d10_d0_trials, 0)                                                    AS d9d10_d0_trials,
    COALESCE(at.d9d10_d6_conv, 0)                                                      AS d9d10_d6_conv,
    CASE WHEN m.d9d10_spend > 0 AND COALESCE(at.d9d10_d0_conv, 0) > 0
         THEN ROUND(m.d9d10_spend::numeric / at.d9d10_d0_conv, 0) END                 AS d9d10_d0_cac,
    CASE WHEN m.d9d10_spend > 0 AND COALESCE(at.d9d10_d0_trials, 0) > 0
         THEN ROUND(m.d9d10_spend::numeric / at.d9d10_d0_trials, 0) END               AS d9d10_d0_trial_cost,
    CASE WHEN m.d9d10_spend > 0 AND COALESCE(at.d9d10_d6_conv, 0) > 0
         THEN ROUND(m.d9d10_spend::numeric / at.d9d10_d6_conv, 0) END                 AS d9d10_d6_cac,
    CASE WHEN m.d9d10_spend > 0 AND COALESCE(at.d9d10_d6_revenue, 0) > 0
         THEN ROUND(at.d9d10_d6_revenue::numeric / m.d9d10_spend, 3) END              AS d9d10_d6_roas,
    CASE WHEN m.d9d10_spend > 0 AND COALESCE(at.d9d10_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d9d10_d6_mandate_revenue::numeric / m.d9d10_spend, 3) END      AS d9d10_d6_mandate_roas,
    CASE WHEN m.d9d10_spend > 0 AND COALESCE(at.d9d10_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d9d10_d6_non_mandate_revenue::numeric / m.d9d10_spend, 3) END  AS d9d10_d6_non_mandate_roas,
    m.d11d13_spend,
    COALESCE(at.d11d13_signups, 0)                                                     AS d11d13_signups,
    COALESCE(at.d11d13_d0_conv, 0)                                                     AS d11d13_d0_conv,
    COALESCE(at.d11d13_d0_trials, 0)                                                   AS d11d13_d0_trials,
    COALESCE(at.d11d13_d6_conv, 0)                                                     AS d11d13_d6_conv,
    CASE WHEN m.d11d13_spend > 0 AND COALESCE(at.d11d13_d0_conv, 0) > 0
         THEN ROUND(m.d11d13_spend::numeric / at.d11d13_d0_conv, 0) END               AS d11d13_d0_cac,
    CASE WHEN m.d11d13_spend > 0 AND COALESCE(at.d11d13_d0_trials, 0) > 0
         THEN ROUND(m.d11d13_spend::numeric / at.d11d13_d0_trials, 0) END             AS d11d13_d0_trial_cost,
    CASE WHEN m.d11d13_spend > 0 AND COALESCE(at.d11d13_d6_conv, 0) > 0
         THEN ROUND(m.d11d13_spend::numeric / at.d11d13_d6_conv, 0) END               AS d11d13_d6_cac,
    CASE WHEN m.d11d13_spend > 0 AND COALESCE(at.d11d13_d6_revenue, 0) > 0
         THEN ROUND(at.d11d13_d6_revenue::numeric / m.d11d13_spend, 3) END            AS d11d13_d6_roas,
    CASE WHEN m.d11d13_spend > 0 AND COALESCE(at.d11d13_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d11d13_d6_mandate_revenue::numeric / m.d11d13_spend, 3) END    AS d11d13_d6_mandate_roas,
    CASE WHEN m.d11d13_spend > 0 AND COALESCE(at.d11d13_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d11d13_d6_non_mandate_revenue::numeric / m.d11d13_spend, 3) END AS d11d13_d6_non_mandate_roas,
    m.d14p_spend,
    COALESCE(at.d14p_signups, 0)                                                       AS d14p_signups,
    COALESCE(at.d14p_d0_conv, 0)                                                       AS d14p_d0_conv,
    COALESCE(at.d14p_d0_trials, 0)                                                     AS d14p_d0_trials,
    COALESCE(at.d14p_d6_conv, 0)                                                       AS d14p_d6_conv,
    CASE WHEN m.d14p_spend > 0 AND COALESCE(at.d14p_d0_conv, 0) > 0
         THEN ROUND(m.d14p_spend::numeric / at.d14p_d0_conv, 0) END                   AS d14p_d0_cac,
    CASE WHEN m.d14p_spend > 0 AND COALESCE(at.d14p_d0_trials, 0) > 0
         THEN ROUND(m.d14p_spend::numeric / at.d14p_d0_trials, 0) END                 AS d14p_d0_trial_cost,
    CASE WHEN m.d14p_spend > 0 AND COALESCE(at.d14p_d6_conv, 0) > 0
         THEN ROUND(m.d14p_spend::numeric / at.d14p_d6_conv, 0) END                   AS d14p_d6_cac,
    CASE WHEN m.d14p_spend > 0 AND COALESCE(at.d14p_d6_revenue, 0) > 0
         THEN ROUND(at.d14p_d6_revenue::numeric / m.d14p_spend, 3) END                AS d14p_d6_roas,
    CASE WHEN m.d14p_spend > 0 AND COALESCE(at.d14p_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d14p_d6_mandate_revenue::numeric / m.d14p_spend, 3) END        AS d14p_d6_mandate_roas,
    CASE WHEN m.d14p_spend > 0 AND COALESCE(at.d14p_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d14p_d6_non_mandate_revenue::numeric / m.d14p_spend, 3) END    AS d14p_d6_non_mandate_roas,
    -- Raw revenue per day-window (used for actuals-based pred D6 ROAS benchmarking)
    COALESCE(at.d0d2_d6_revenue, 0)                                                    AS d0d2_d6_revenue,
    COALESCE(at.d0d2_d6_mandate_revenue, 0)                                            AS d0d2_d6_mandate_revenue,
    COALESCE(at.d0d2_d6_non_mandate_revenue, 0)                                        AS d0d2_d6_non_mandate_revenue,
    COALESCE(at.d3d5_d6_revenue, 0)                                                    AS d3d5_d6_revenue,
    COALESCE(at.d3d5_d6_mandate_revenue, 0)                                            AS d3d5_d6_mandate_revenue,
    COALESCE(at.d3d5_d6_non_mandate_revenue, 0)                                        AS d3d5_d6_non_mandate_revenue,
    COALESCE(at.d6d8_d6_revenue, 0)                                                    AS d6d8_d6_revenue,
    COALESCE(at.d6d8_d6_mandate_revenue, 0)                                            AS d6d8_d6_mandate_revenue,
    COALESCE(at.d6d8_d6_non_mandate_revenue, 0)                                        AS d6d8_d6_non_mandate_revenue,
    COALESCE(at.d9d10_d6_revenue, 0)                                                   AS d9d10_d6_revenue,
    COALESCE(at.d9d10_d6_mandate_revenue, 0)                                           AS d9d10_d6_mandate_revenue,
    COALESCE(at.d9d10_d6_non_mandate_revenue, 0)                                       AS d9d10_d6_non_mandate_revenue,
    COALESCE(at.d11d13_d6_revenue, 0)                                                  AS d11d13_d6_revenue,
    COALESCE(at.d11d13_d6_mandate_revenue, 0)                                          AS d11d13_d6_mandate_revenue,
    COALESCE(at.d11d13_d6_non_mandate_revenue, 0)                                      AS d11d13_d6_non_mandate_revenue,
    COALESCE(at.d14p_d6_revenue, 0)                                                    AS d14p_d6_revenue,
    COALESCE(at.d14p_d6_mandate_revenue, 0)                                            AS d14p_d6_mandate_revenue,
    COALESCE(at.d14p_d6_non_mandate_revenue, 0)                                        AS d14p_d6_non_mandate_revenue
FROM media m
LEFT JOIN first_dates fd ON fd.ad_id = m.ad_id
LEFT JOIN campaigns c ON c.id = m.campaign_id
LEFT JOIN adsets s    ON s.id = m.adset_id
LEFT JOIN ads a       ON a.id = m.ad_id::text
LEFT JOIN attr at     ON at.ad_id = m.ad_id::text
ORDER BY m.spend DESC NULLS LAST
"""

ATTR_SINCE_AD = today - timedelta(days=365)  # 1 year back for overall attribution coverage


# ── Campaign-level SQL — mirrors AD_LEVEL_SQL but keyed on campaign_id ────────
CAMPAIGN_LEVEL_SQL = """
WITH first_dates AS (
    SELECT campaign_id, MIN(date) AS first_date
    FROM insights_campaign_daily
    WHERE attribution_window = '7d_click' AND spend > 0
    GROUP BY campaign_id
),
media AS (
    SELECT
        i.campaign_id,
        ROUND(SUM(i.spend)::numeric, 0)                                   AS spend,
        SUM(i.impressions)                                                 AS impressions,
        SUM(i.clicks)                                                      AS clicks,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.clicks)::numeric * 100 / SUM(i.impressions), 3) END AS ctr,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.spend)::numeric * 1000 / SUM(i.impressions), 1) END AS cpm,
        CASE WHEN SUM(i.clicks) > 0
             THEN ROUND(SUM(i.spend)::numeric / SUM(i.clicks), 1) END     AS cpc,
        MAX(i.date)                                                        AS last_date,
        -- Mature
        ROUND(COALESCE(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.spend END), 0)::numeric, 0)              AS mature_spend,
        COALESCE(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END), 0)                            AS mature_impressions,
        COALESCE(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END), 0)                                 AS mature_clicks,
        CASE WHEN SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END)::numeric * 100
                      / SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END), 3) END                 AS mature_ctr,
        CASE WHEN SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.spend END)::numeric * 1000
                      / SUM(CASE WHEN i.date <= %(mature_end)s THEN i.impressions END), 1) END                 AS mature_cpm,
        CASE WHEN SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END) > 0
             THEN ROUND(SUM(CASE WHEN i.date <= %(mature_end)s THEN i.spend END)::numeric
                      / SUM(CASE WHEN i.date <= %(mature_end)s THEN i.clicks END), 1) END                      AS mature_cpc,
        -- Mid
        ROUND(COALESCE(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.spend END), 0)::numeric, 0)  AS mid_spend,
        COALESCE(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END), 0)               AS mid_impressions,
        COALESCE(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END), 0)                    AS mid_clicks,
        CASE WHEN SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END)::numeric * 100
                      / SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END), 3) END    AS mid_ctr,
        CASE WHEN SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.spend END)::numeric * 1000
                      / SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.impressions END), 1) END    AS mid_cpm,
        CASE WHEN SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END) > 0
             THEN ROUND(SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.spend END)::numeric
                      / SUM(CASE WHEN i.date BETWEEN %(mid_start)s AND %(mid_end)s THEN i.clicks END), 1) END         AS mid_cpc,
        -- Recent
        ROUND(COALESCE(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.spend END), 0)::numeric, 0)             AS recent_spend,
        COALESCE(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END), 0)                          AS recent_impressions,
        COALESCE(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END), 0)                               AS recent_clicks,
        CASE WHEN SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END)::numeric * 100
                      / SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END), 3) END               AS recent_ctr,
        CASE WHEN SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END) > 0
             THEN ROUND(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.spend END)::numeric * 1000
                      / SUM(CASE WHEN i.date >= %(recent_start)s THEN i.impressions END), 1) END               AS recent_cpm,
        CASE WHEN SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END) > 0
             THEN ROUND(SUM(CASE WHEN i.date >= %(recent_start)s THEN i.spend END)::numeric
                      / SUM(CASE WHEN i.date >= %(recent_start)s THEN i.clicks END), 1) END                    AS recent_cpc
    FROM insights_campaign_daily i
    WHERE i.attribution_window = '7d_click'
      AND i.spend > 0
      AND i.date >= %(attr_since)s
    GROUP BY i.campaign_id
),
attr AS (
    SELECT
        ae.meta_campaign_id                                               AS campaign_id,
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
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = TRUE
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = FALSE
                 THEN ae.revenue_inr ELSE 0 END)                          AS d6_non_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                 THEN ae.revenue_inr ELSE 0 END)                          AS total_revenue,
        -- Mature
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d0_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d0_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d6_non_mandate,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = TRUE
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = FALSE
                  AND ae.install_date <= %(mature_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mature_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date <= %(mature_end)s
                            THEN ae.user_id END)                          AS mature_d0_trials,
        -- Mid
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d0_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d0_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d6_non_mandate,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = TRUE
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d6_mandate_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.is_mandate = FALSE
                  AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS mid_d6_non_mandate_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date BETWEEN %(mid_start)s AND %(mid_end)s
                            THEN ae.user_id END)                          AS mid_d0_trials,
        -- Recent
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d0_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                  AND ae.install_date >= %(recent_start)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS recent_d0_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d6_non_mandate,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                  AND ae.install_date >= %(recent_start)s
                 THEN ae.revenue_inr ELSE 0 END)                          AS recent_d6_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                             AND ae.install_date >= %(recent_start)s
                            THEN ae.user_id END)                          AS recent_d0_trials
    FROM attribution_events ae
    WHERE ae.network = 'Facebook'
      AND ae.is_reattributed = FALSE
      AND ae.meta_campaign_id IS NOT NULL
      AND ae.meta_campaign_id <> 'N/A'
      AND ae.install_date >= %(attr_since)s
    GROUP BY ae.meta_campaign_id
)
SELECT
    m.campaign_id,
    c.name                                                                AS campaign_name,
    m.spend, m.impressions, m.clicks, m.ctr, m.cpm, m.cpc,
    fd.first_date, m.last_date,
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
    c.effective_status                                                     AS status,
    CASE WHEN m.spend > 0 AND COALESCE(at.d6_revenue, 0) > 0
         THEN ROUND(at.d6_revenue::numeric / m.spend, 3) END              AS d6_roas,
    CASE WHEN m.spend > 0 AND COALESCE(at.d6_mandate_revenue, 0) > 0
         THEN ROUND(at.d6_mandate_revenue::numeric / m.spend, 3) END      AS d6_mandate_roas,
    CASE WHEN m.spend > 0 AND COALESCE(at.d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.d6_non_mandate_revenue::numeric / m.spend, 3) END  AS d6_non_mandate_roas,
    CASE WHEN m.spend > 0
              AND (COALESCE(at.d6_mandate, 0) + COALESCE(at.d6_non_mandate, 0)) > 0
         THEN ROUND(m.spend::numeric /
                    (COALESCE(at.d6_mandate, 0) + COALESCE(at.d6_non_mandate, 0)),
                    0) END                                                 AS d6_cac,
    CASE WHEN COALESCE(at.signups, 0) > 0
         THEN ROUND(at.total_revenue::numeric / at.signups, 0) END        AS ltv_inr,
    CASE WHEN m.spend > 0 AND COALESCE(at.signups, 0) > 0
         THEN ROUND(m.spend::numeric / at.signups, 0) END                 AS cac_inr,
    -- Mature
    m.mature_spend, m.mature_impressions, m.mature_clicks, m.mature_ctr, m.mature_cpm, m.mature_cpc,
    COALESCE(at.mature_signups, 0)                                                    AS mature_signups,
    COALESCE(at.mature_d0_conv, 0)                                                    AS mature_d0_conv,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d0_conv, 0) > 0
         THEN ROUND(m.mature_spend::numeric / at.mature_d0_conv, 0) END               AS mature_d0_cac,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d0_revenue, 0) > 0
         THEN ROUND(at.mature_d0_revenue::numeric / m.mature_spend, 3) END            AS mature_d0_roas,
    CASE WHEN m.mature_spend > 0
              AND (COALESCE(at.mature_d6_mandate, 0) + COALESCE(at.mature_d6_non_mandate, 0)) > 0
         THEN ROUND(m.mature_spend::numeric /
                    (at.mature_d6_mandate + at.mature_d6_non_mandate), 0) END         AS mature_d6_cac,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d6_revenue, 0) > 0
         THEN ROUND(at.mature_d6_revenue::numeric / m.mature_spend, 3) END            AS mature_d6_roas,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d6_mandate_revenue, 0) > 0
         THEN ROUND(at.mature_d6_mandate_revenue::numeric / m.mature_spend, 3) END    AS mature_d6_mandate_roas,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d6_non_mandate_revenue, 0) > 0
         THEN ROUND(at.mature_d6_non_mandate_revenue::numeric / m.mature_spend, 3) END AS mature_d6_non_mandate_roas,
    COALESCE(at.mature_d0_trials, 0)                                                  AS mature_d0_trials,
    CASE WHEN m.mature_spend > 0 AND COALESCE(at.mature_d0_trials, 0) > 0
         THEN ROUND(m.mature_spend::numeric / at.mature_d0_trials, 0) END             AS mature_d0_trial_cost,
    -- Mid
    m.mid_spend, m.mid_impressions, m.mid_clicks, m.mid_ctr, m.mid_cpm, m.mid_cpc,
    COALESCE(at.mid_signups, 0)                                                       AS mid_signups,
    COALESCE(at.mid_d0_conv, 0)                                                       AS mid_d0_conv,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d0_conv, 0) > 0
         THEN ROUND(m.mid_spend::numeric / at.mid_d0_conv, 0) END                     AS mid_d0_cac,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d0_revenue, 0) > 0
         THEN ROUND(at.mid_d0_revenue::numeric / m.mid_spend, 3) END                  AS mid_d0_roas,
    CASE WHEN m.mid_spend > 0
              AND (COALESCE(at.mid_d6_mandate, 0) + COALESCE(at.mid_d6_non_mandate, 0)) > 0
         THEN ROUND(m.mid_spend::numeric /
                    (at.mid_d6_mandate + at.mid_d6_non_mandate), 0) END               AS mid_d6_cac,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d6_revenue, 0) > 0
         THEN ROUND(at.mid_d6_revenue::numeric / m.mid_spend, 3) END                  AS mid_d6_roas,
    COALESCE(at.mid_d0_trials, 0)                                                     AS mid_d0_trials,
    CASE WHEN m.mid_spend > 0 AND COALESCE(at.mid_d0_trials, 0) > 0
         THEN ROUND(m.mid_spend::numeric / at.mid_d0_trials, 0) END                   AS mid_d0_trial_cost,
    -- Recent
    m.recent_spend, m.recent_impressions, m.recent_clicks, m.recent_ctr, m.recent_cpm, m.recent_cpc,
    COALESCE(at.recent_signups, 0)                                                    AS recent_signups,
    COALESCE(at.recent_d0_conv, 0)                                                    AS recent_d0_conv,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d0_conv, 0) > 0
         THEN ROUND(m.recent_spend::numeric / at.recent_d0_conv, 0) END               AS recent_d0_cac,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d0_revenue, 0) > 0
         THEN ROUND(at.recent_d0_revenue::numeric / m.recent_spend, 3) END            AS recent_d0_roas,
    CASE WHEN m.recent_spend > 0
              AND (COALESCE(at.recent_d6_mandate, 0) + COALESCE(at.recent_d6_non_mandate, 0)) > 0
         THEN ROUND(m.recent_spend::numeric /
                    (at.recent_d6_mandate + at.recent_d6_non_mandate), 0) END         AS recent_d6_cac,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d6_revenue, 0) > 0
         THEN ROUND(at.recent_d6_revenue::numeric / m.recent_spend, 3) END            AS recent_d6_roas,
    COALESCE(at.recent_d0_trials, 0)                                                  AS recent_d0_trials,
    CASE WHEN m.recent_spend > 0 AND COALESCE(at.recent_d0_trials, 0) > 0
         THEN ROUND(m.recent_spend::numeric / at.recent_d0_trials, 0) END             AS recent_d0_trial_cost,
    -- raw figures needed for per-campaign predicted D6 ROAS multiplier
    COALESCE(at.mature_d6_revenue, 0)                                                 AS mature_d6_revenue,
    COALESCE(at.mature_d6_mandate_revenue, 0)                                         AS mature_d6_mandate_revenue,
    COALESCE(at.mature_d6_non_mandate_revenue, 0)                                     AS mature_d6_non_mandate_revenue,
    COALESCE(at.mid_d6_mandate_revenue, 0)                                            AS mid_d6_mandate_revenue,
    COALESCE(at.mid_d6_non_mandate_revenue, 0)                                        AS mid_d6_non_mandate_revenue,
    COALESCE(at.d6_revenue, 0)                                                        AS overall_d6_revenue,
    COALESCE(at.d0_trials, 0)                                                         AS overall_d0_trials
FROM media m
LEFT JOIN first_dates fd ON fd.campaign_id = m.campaign_id
LEFT JOIN campaigns c    ON c.id = m.campaign_id::text
LEFT JOIN attr at        ON at.campaign_id = m.campaign_id::text
ORDER BY m.spend DESC NULLS LAST
"""

# Median D6/D0 revenue ratio from mature ads (7+ days, 1k+ spend, last 3 months)
D6_D0_MULTIPLIER_SQL = """
WITH ad_metrics AS (
    SELECT
        i.ad_id,
        COALESCE(SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                          AND ae.days_since_signup = 0 THEN ae.revenue_inr END), 0) AS d0_revenue,
        COALESCE(SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                          AND ae.days_since_signup <= 6 THEN ae.revenue_inr END), 0) AS d6_revenue
    FROM insights_daily i
    LEFT JOIN attribution_events ae
        ON ae.meta_creative_id = i.ad_id::text
        AND ae.network = 'Facebook'
        AND ae.is_reattributed = FALSE
        AND ae.install_date >= %(since)s
    WHERE i.attribution_window = '7d_click'
      AND i.date >= %(since)s
      AND i.spend > 0
    GROUP BY i.ad_id
    HAVING MAX(i.date) - MIN(i.date) >= 6
       AND SUM(i.spend) > 1000
)
SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
    ORDER BY CASE WHEN d0_revenue > 0 THEN d6_revenue / d0_revenue END
)::numeric, 3) AS median_multiplier
FROM ad_metrics
"""

TOTAL_REV_MULTIPLIER_SQL = """
WITH ad_metrics AS (
    SELECT
        i.ad_id,
        COALESCE(SUM(CASE WHEN ae.event_name = 'trial' AND ae.days_since_signup = 0
                         THEN 1 ELSE 0 END), 0)                                    AS d0_trials,
        COALESCE(SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                           AND ae.days_since_signup <= 6
                          THEN ae.revenue_inr ELSE 0 END), 0)                      AS d6_total_revenue
    FROM insights_daily i
    LEFT JOIN attribution_events ae
        ON ae.meta_creative_id = i.ad_id::text
        AND ae.network = 'Facebook'
        AND ae.is_reattributed = FALSE
        AND ae.install_date >= %(since)s
    WHERE i.attribution_window = '7d_click'
      AND i.date >= %(since)s
      AND i.date <= %(mature_end)s
      AND i.spend > 0
    GROUP BY i.ad_id
    HAVING MAX(i.date) - MIN(i.date) >= 6
       AND SUM(i.spend) > 1000
)
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (
    ORDER BY CASE WHEN d0_trials > 0 AND d6_total_revenue > 0
                  THEN d6_total_revenue / d0_trials END
)::numeric AS median_total_rev_per_trial
FROM ad_metrics
"""


def build_ad_data(conn) -> list:
    params_ad = {
        "attr_since": ATTR_SINCE_AD,
        "mature_end": mature_end,
        "mid_start":  mid_start,
        "mid_end":    mid_end,
        "recent_start": recent_start,
        "dw_d0d2_start":   dw_d0d2_start,
        "dw_d3d5_start":   dw_d3d5_start,
        "dw_d3d5_end":     dw_d3d5_end,
        "dw_d6d8_start":   dw_d6d8_start,
        "dw_d6d8_end":     dw_d6d8_end,
        "dw_d9d10_start":  dw_d9d10_start,
        "dw_d9d10_end":    dw_d9d10_end,
        "dw_d11d13_start": dw_d11d13_start,
        "dw_d11d13_end":   dw_d11d13_end,
        "dw_d14p_end":     dw_d14p_end,
    }
    rows = q(conn, AD_LEVEL_SQL, params_ad)

    mult_since = (today - timedelta(days=90)).isoformat()

    # Total D6 revenue per trial (mandate + non-mandate combined) from mature ads
    total_mult_row = q1(conn, TOTAL_REV_MULTIPLIER_SQL, {
        "since": mult_since, "mature_end": mature_end
    })
    total_rev_per_trial = float(total_mult_row.get("median_total_rev_per_trial") or 500.0)
    print(f"  Total rev/trial: ₹{int(total_rev_per_trial)}")

    # Pred D6 ROAS for d0-d2 window — mandate-split, benchmark from actual revenue/spend
    # pooled across all non-d0-d2 day windows (d3-d5 .. d14+). Pred:
    #   pred_mandate     = avg_mandate_roas     × (d0d2_d0_trial_cost / avg_d0_trial_cost)
    #   pred_non_mandate = avg_non_mandate_roas × (d0d2_d0_cac        / avg_d0_cac)
    AGG_PREFIXES = ["d3d5", "d6d8", "d9d10", "d11d13", "d14p"]

    # Global fallback: pool actual totals across every row × every non-d0-d2 window
    _g_spend = _g_trials = _g_conv = _g_mdtrev = _g_nmrev = 0.0
    for r in rows:
        for p in AGG_PREFIXES:
            _g_spend  += float(r.get(f"{p}_spend") or 0)
            _g_trials += float(r.get(f"{p}_d0_trials") or 0)
            _g_conv   += float(r.get(f"{p}_d0_conv") or 0)
            _g_mdtrev += float(r.get(f"{p}_d6_mandate_revenue") or 0)
            _g_nmrev  += float(r.get(f"{p}_d6_non_mandate_revenue") or 0)
    g_mandate_roas = (_g_mdtrev / _g_spend) if (_g_spend and _g_mdtrev) else None
    g_non_mdt_roas = (_g_nmrev  / _g_spend) if (_g_spend and _g_nmrev)  else None
    g_trial_cost   = (_g_spend / _g_trials) if _g_trials else None
    g_d0_cac       = (_g_spend / _g_conv)   if _g_conv   else None

    for r in rows:
        rtc = r.get("d0d2_d0_trial_cost") or r.get("recent_d0_trial_cost")
        rcc = r.get("d0d2_d0_cac")        or r.get("recent_d0_cac")
        if not rtc or float(rtc) <= 0:
            r["_recent_pred_d6_roas"]    = None
            r["_pred_mandate_roas"]      = None
            r["_pred_non_mandate_roas"]  = None
            continue
        rec_tc  = float(rtc)
        rec_cac = float(rcc) if rcc and float(rcc) > 0 else 0

        # Per-ad benchmark: pool ACTUAL revenue and spend across non-d0-d2 windows
        a_spend = a_trials = a_conv = a_mdtrev = a_nmrev = 0.0
        for p in AGG_PREFIXES:
            a_spend  += float(r.get(f"{p}_spend") or 0)
            a_trials += float(r.get(f"{p}_d0_trials") or 0)
            a_conv   += float(r.get(f"{p}_d0_conv") or 0)
            a_mdtrev += float(r.get(f"{p}_d6_mandate_revenue") or 0)
            a_nmrev  += float(r.get(f"{p}_d6_non_mandate_revenue") or 0)
        # If the ad has ANY non-d0-d2 history, trust its own ratios (even 0s).
        # Only fall back to global when the ad has zero non-d0-d2 spend.
        if a_spend > 0:
            bench_mandate = a_mdtrev / a_spend
            bench_non_mdt = a_nmrev  / a_spend
        else:
            bench_mandate = g_mandate_roas
            bench_non_mdt = g_non_mdt_roas
        bench_tc  = (a_spend / a_trials) if a_trials else g_trial_cost
        bench_cac = (a_spend / a_conv)   if a_conv   else g_d0_cac

        # Pred = bench_roas × (bench_metric / d0d2_metric)
        # Higher d0-d2 trial cost / CAC than history → ratio < 1 → lower pred ROAS.
        pred_mandate = (bench_mandate * (bench_tc / rec_tc)
                        if (bench_mandate and bench_tc and rec_tc) else 0.0)
        # Non-mandate needs a CAC signal; mandate-only reference when d0-d2 has no conversions.
        if rec_cac > 0 and bench_cac:
            pred_non = (bench_non_mdt or 0) * (bench_cac / rec_cac)
        else:
            pred_non = 0.0

        total = pred_mandate + pred_non
        r["_pred_mandate_roas"]     = round(pred_mandate, 3) if pred_mandate else 0.0
        r["_pred_non_mandate_roas"] = round(pred_non, 3)     if pred_non     else 0.0
        r["_recent_pred_d6_roas"]   = round(total, 3) if total > 0 else None

    compute_ad_scores(rows)
    return rows


def build_campaign_data(conn, total_rev_per_trial: float | None = None) -> list:
    """Fetch and score campaign-level rows. Mirrors build_ad_data shape."""
    rows = q(conn, CAMPAIGN_LEVEL_SQL, {
        "attr_since":   ATTR_SINCE_AD,
        "mature_end":   mature_end,
        "mid_start":    mid_start,
        "mid_end":      mid_end,
        "recent_start": recent_start,
    })

    # Exclude iOS / Retarget campaigns
    rows = [r for r in rows if not _is_ios_or_retarget_name(r.get("campaign_name") or "")]

    # Pred D6 ROAS for the recent window — per-campaign rev/trial multiplier.
    # Priority: campaign's own mature rev/trial → campaign's overall rev/trial → global fallback.
    # Requires a minimum trial sample so noisy small denominators don't blow up the prediction.
    if total_rev_per_trial is None:
        mult_since = (today - timedelta(days=90)).isoformat()
        tot_row = q1(conn, TOTAL_REV_MULTIPLIER_SQL, {"since": mult_since, "mature_end": mature_end})
        total_rev_per_trial = float(tot_row.get("median_total_rev_per_trial") or 500.0)

    MIN_TRIALS_FOR_CAMP_MULT = 10

    # Pred D6 ROAS — benchmark from pooled actual revenue/spend across all
    # non-d0-d2 timeframes for this campaign (mature ≥7d + mid 3-6d). Formula:
    #   pred_mandate     = avg_mandate_roas     × (d0d2_d0_trial_cost / avg_d0_trial_cost)
    #   pred_non_mandate = avg_non_mandate_roas × (d0d2_d0_cac        / avg_d0_cac)
    # Fallback when campaign has no non-d0-d2 data: overall (full period) actuals,
    # then global rev/trial single-multiplier.
    for r in rows:
        rtc = r.get("recent_d0_trial_cost")
        rcc = r.get("recent_d0_cac")
        if not rtc or float(rtc) <= 0:
            r["_recent_pred_d6_roas"] = None
            r["_pred_mandate_roas"]   = None
            r["_pred_non_mandate_roas"] = None
            r["_pred_rev_per_trial"]  = None
            r["_pred_mult_source"]    = None
            continue

        rec_tc  = float(rtc)
        rec_cac = float(rcc) if rcc and float(rcc) > 0 else 0

        # Pool actual numbers (not percentages) across mature + mid windows
        bench_spend  = float(r.get("mature_spend") or 0) + float(r.get("mid_spend") or 0)
        bench_trials = float(r.get("mature_d0_trials") or 0) + float(r.get("mid_d0_trials") or 0)
        bench_conv   = float(r.get("mature_d0_conv") or 0)   + float(r.get("mid_d0_conv") or 0)
        bench_mdtrev = float(r.get("mature_d6_mandate_revenue") or 0)     + float(r.get("mid_d6_mandate_revenue") or 0)
        bench_nmrev  = float(r.get("mature_d6_non_mandate_revenue") or 0) + float(r.get("mid_d6_non_mandate_revenue") or 0)

        def _split_from(bench_mandate_roas, bench_non_mandate_roas, bench_tc, bench_cac):
            """Pred = bench_roas × (bench_metric / d0d2_metric). Higher d0-d2 cost → lower pred.
            Non-mandate requires CAC signal; mandate-only reference when d0-d2 has no conversions."""
            if bench_tc <= 0 or rec_tc <= 0:
                return None, None
            pred_mandate = (bench_mandate_roas or 0) * (bench_tc / rec_tc)
            if rec_cac > 0 and bench_cac > 0:
                pred_non = (bench_non_mandate_roas or 0) * (bench_cac / rec_cac)
            else:
                pred_non = 0.0
            return pred_mandate, pred_non

        pred_mandate = pred_non = None
        source = None
        rpt = None  # rev/trial fallback path only

        # Path 1: pooled mature + mid (non-d0-d2) mandate-split
        if bench_trials >= MIN_TRIALS_FOR_CAMP_MULT and bench_spend > 0:
            bench_mandate_roas = bench_mdtrev / bench_spend if bench_mdtrev else 0
            bench_non_mdt_roas = bench_nmrev  / bench_spend if bench_nmrev  else 0
            bench_tc           = bench_spend / bench_trials
            bench_cac          = bench_spend / bench_conv if bench_conv else 0
            if bench_mandate_roas or bench_non_mdt_roas:
                pm, pn = _split_from(bench_mandate_roas, bench_non_mdt_roas, bench_tc, bench_cac)
                if pm is not None:
                    pred_mandate, pred_non = pm, pn
                    source = "non-d0d2 (split)"

        # Path 2: overall (full-period) mandate-split — fallback when mature+mid is thin
        o_trials = float(r.get("overall_d0_trials") or 0)
        if pred_mandate is None and o_trials >= MIN_TRIALS_FOR_CAMP_MULT:
            pm, pn = _split_from(
                float(r.get("d6_mandate_roas") or 0),
                float(r.get("d6_non_mandate_roas") or 0),
                float(r.get("d0_trial_cost") or 0),
                float(r.get("d0_cac") or 0),
            )
            if pm is not None:
                pred_mandate, pred_non = pm, pn
                source = "overall (split)"

        # Path 3: rev/trial fallback (no mandate split available)
        if pred_mandate is None:
            o_rev = float(r.get("overall_d6_revenue") or 0)
            if bench_trials >= MIN_TRIALS_FOR_CAMP_MULT and (bench_mdtrev + bench_nmrev) > 0:
                rpt = (bench_mdtrev + bench_nmrev) / bench_trials; source = "non-d0d2 (rev/trial)"
            elif o_trials >= MIN_TRIALS_FOR_CAMP_MULT and o_rev > 0:
                rpt = o_rev / o_trials; source = "overall (rev/trial)"
            else:
                rpt = total_rev_per_trial; source = "global (rev/trial)"

        if pred_mandate is not None:
            total = (pred_mandate or 0) + (pred_non or 0)
            r["_pred_mandate_roas"]     = round(pred_mandate, 3) if pred_mandate else 0.0
            r["_pred_non_mandate_roas"] = round(pred_non, 3)     if pred_non     else 0.0
            r["_recent_pred_d6_roas"]   = round(total, 3) if total > 0 else None
            r["_pred_rev_per_trial"]    = None
        else:
            r["_pred_mandate_roas"]     = None
            r["_pred_non_mandate_roas"] = None
            r["_recent_pred_d6_roas"]   = round(rpt / rec_tc, 3) if rpt else None
            r["_pred_rev_per_trial"]    = round(rpt, 0) if rpt else None

        r["_pred_mult_source"] = source

    compute_ad_scores(rows, use_pred_for_recent=True)

    # Attach scaling decision per campaign (uses grades, ROAS, CAC, recent signal)
    for r in rows:
        d = _compute_decision(r)
        r["_decision_action"]            = d["action"]
        r["_decision_budget_change_pct"] = d["budget_change_pct"]
        r["_decision_suggested_daily"]   = d["suggested_daily"]
        r["_decision_reasoning"]         = d["reasoning"]

    return rows


def _compute_decision(r: dict) -> dict:
    """Per-campaign scaling decision: action + suggested daily budget + reasoning.

    Priority: RECENT signal first (pred D6 ROAS + recent grade) — that's the freshest
    truth. Mature grade is the secondary anchor for context.

    Reasoning string includes the actual drift multipliers (CAC / trial cost recent
    vs mature), so the recommendation is auditable: "CAC drift 2.3x → non-mandate
    ROAS halved" is the literal math behind the call.

    Output keys: action, budget_change_pct (int signed), suggested_daily (int ₹, raw —
    sheet writer applies GST), reasoning.
    """
    GST = 1.18
    grade        = (r.get("_grade") or "").upper()
    recent_grade = (r.get("_recent_grade") or "").upper()
    status       = (r.get("status") or "").upper()
    spend_total  = float(r.get("spend") or 0)
    recent_spend = float(r.get("recent_spend") or 0)
    # ROAS values are stored raw (= revenue / spend_excl_gst). Sheet displays them
    # GST-adjusted; reasoning should match what the user sees.
    mature_roas  = float(r.get("mature_d6_roas") or 0) / GST
    pred_roas    = float(r.get("_recent_pred_d6_roas") or 0) / GST
    pm_roas      = float(r.get("_pred_mandate_roas") or 0) / GST
    pn_roas      = float(r.get("_pred_non_mandate_roas") or 0) / GST
    mat_cac      = float(r.get("mature_d0_cac") or 0)
    mat_tc       = float(r.get("mature_d0_trial_cost") or 0)
    rec_cac      = float(r.get("recent_d0_cac") or 0)
    rec_tc       = float(r.get("recent_d0_trial_cost") or 0)

    daily_recent = recent_spend / 3.0 if recent_spend > 0 else 0
    cac_drift = (rec_cac / mat_cac) if mat_cac > 0 and rec_cac > 0 else None
    tc_drift  = (rec_tc  / mat_tc)  if mat_tc  > 0 and rec_tc  > 0 else None

    def _fmt_metrics():
        # ROAS shown as percentages (×100, no decimals — they're already noisy).
        bits = [f"D6 ROAS {mature_roas*100:.0f}%", f"Pred {pred_roas*100:.0f}%"]
        if pm_roas or pn_roas:
            bits.append(f"split mdt {pm_roas*100:.0f}%+non-mdt {pn_roas*100:.0f}%")
        # CAC / Trial cost going UP is bad (more spend per conversion / trial).
        # Going DOWN is good. Tag each drift with worse/better/flat so the reader
        # doesn't have to mentally translate the direction.
        def _drift_label(mult, threshold=0.05):
            if mult > 1 + threshold:  return f"worse {mult:.2f}x"
            if mult < 1 - threshold:  return f"better {mult:.2f}x"
            return f"flat {mult:.2f}x"
        if cac_drift is not None:
            bits.append(f"CAC {_drift_label(cac_drift)}")
        if tc_drift is not None:
            bits.append(f"Trial $ {_drift_label(tc_drift)}")
        return " | ".join(bits)

    metrics_str = _fmt_metrics()

    def _result(action, pct, daily, reason):
        return {"action": action, "budget_change_pct": pct,
                "suggested_daily": int(round(daily)), "reasoning": reason}

    # Quality gate for SCALE decisions — even when grades say "scale", refuse if
    # the absolute pred ROAS is too low or unit economics are drifting wrong.
    # CAC and trial cost going up is a negative signal: less revenue per ₹ spent.
    MIN_PRED_TO_SCALE = 0.30
    MAX_CAC_DRIFT_TO_SCALE = 1.30
    MAX_TC_DRIFT_TO_SCALE  = 1.30
    def _scale_blocked():
        reasons = []
        if pred_roas and pred_roas < MIN_PRED_TO_SCALE:
            reasons.append(f"pred D6 ROAS {pred_roas:.2f} < {MIN_PRED_TO_SCALE:.2f}")
        if cac_drift is not None and cac_drift > MAX_CAC_DRIFT_TO_SCALE:
            reasons.append(f"CAC {cac_drift:.2f}x mature (worsening)")
        if tc_drift is not None and tc_drift > MAX_TC_DRIFT_TO_SCALE:
            reasons.append(f"trial cost {tc_drift:.2f}x mature (worsening)")
        return reasons

    def _scale_or_hold(action, pct, multiplier, scale_reason):
        """Return SCALE if quality gate passes, otherwise downgrade to HOLD with
        an explanation that names the failing signal(s)."""
        blocked = _scale_blocked()
        if blocked:
            return _result("HOLD", 0, daily_recent,
                           f"Would scale ({scale_reason}) but blocked: " + "; ".join(blocked) + f" ({metrics_str}).")
        return _result(action, pct, daily_recent * multiplier, f"{scale_reason} ({metrics_str}).")

    # Not active — skip decision
    if status and status not in ("ACTIVE",):
        return _result("NO ACTION", 0, 0, f"Campaign is {status.lower()}.")

    # Immature mature grade → hold
    if grade in ("FULL IMMATURE", "PARTIAL IMMATURE", "NO DATA", ""):
        return _result("HOLD", 0, daily_recent,
                       f"Too new to evaluate ({grade or 'no data'}) — revisit after day 7.")

    # ── Recent signal overrides — these catch campaigns whose recent performance
    # is materially worse than their mature reputation suggests. The user explicitly
    # flagged that scale-down recommendations were missing — these branches add them.

    # Recent POOR → always cut (severity scales with mature grade + total spend)
    if recent_grade == "POOR":
        if grade in ("TOP PERFORMER", "GOOD"):
            return _result("CUT 30%", -30, daily_recent * 0.7,
                           f"Was {grade}, recent POOR ({metrics_str}). Slow the burn, find the leak.")
        if grade == "AVERAGE":
            return _result("CUT 50%", -50, daily_recent * 0.5,
                           f"AVG mature + recent POOR ({metrics_str}). Cut -50%.")
        if spend_total > 50000:
            return _result("PAUSE", -100, 0,
                           f"{grade} burning ₹{_inr_indian(int(spend_total*1.18))}, recent POOR ({metrics_str}). Pause.")
        return _result("CUT 50%", -50, daily_recent * 0.5,
                       f"{grade} + recent POOR ({metrics_str}). Cut -50%.")

    # Recent UNDERPERFORMING → trim
    if recent_grade == "UNDERPERFORMING":
        if grade in ("TOP PERFORMER", "GOOD"):
            return _result("CUT 15%", -15, daily_recent * 0.85,
                           f"Was {grade}, recent UNDER ({metrics_str}). Trim while you fix the leak.")
        return _result("CUT 30%", -30, daily_recent * 0.7,
                       f"Recent UNDER ({metrics_str}). Cut -30% and refresh creative.")

    # Hard threshold guard: even if grades look fine, very low predicted ROAS at
    # meaningful spend is a cut signal (catches drift the rank-based grades miss).
    if pred_roas > 0 and pred_roas < 0.10 and daily_recent * 1.18 > 5000:
        return _result("CUT 30%", -30, daily_recent * 0.7,
                       f"Pred D6 ROAS only {pred_roas:.2f} ({metrics_str}). Cut -30%.")

    # ── Mature-grade-driven branches (recent is at least AVG or OK) ─────────────

    # Mature POOR
    if grade == "POOR":
        if spend_total > 50000:
            return _result("PAUSE", -100, 0,
                           f"POOR mature, ₹{_inr_indian(int(spend_total*1.18))} sunk ({metrics_str}). Pause.")
        return _result("CUT 50%", -50, daily_recent * 0.5,
                       f"POOR mature ({metrics_str}). Cut -50%.")

    # Mature UNDERPERFORMING
    if grade == "UNDERPERFORMING":
        return _result("CUT 30%", -30, daily_recent * 0.7,
                       f"Mature UNDER ({metrics_str}). Cut -30%, refresh creative.")

    # Scale paths — both mature and recent are strong, AND quality gate passes
    if grade == "TOP PERFORMER" and recent_grade in ("TOP PERFORMER", "GOOD"):
        return _scale_or_hold("SCALE +30%", 30, 1.30,
                              f"Top performer, recent {recent_grade}")
    if grade == "GOOD" and recent_grade in ("TOP PERFORMER", "GOOD"):
        return _scale_or_hold("SCALE +15%", 15, 1.15,
                              f"Good performer, recent {recent_grade}")

    # Special INEFFICIENT / OPPORTUNITY / RECOVERING grade overrides
    if grade.startswith("INEFFICIENT CAT 1"):
        return _result("PAUSE", -100, 0,
                       f"INEFFICIENT CAT 1 ({metrics_str}). Pause immediately.")
    if grade.startswith("INEFFICIENT CAT 2"):
        return _result("CUT 70%", -70, daily_recent * 0.3,
                       f"INEFFICIENT CAT 2 ({metrics_str}). Cut -70%, evaluate within 24h.")
    if grade.startswith("INEFFICIENT CAT 3"):
        return _result("CUT 50%", -50, daily_recent * 0.5,
                       f"INEFFICIENT CAT 3 ({metrics_str}). Diminishing returns, cut -50%.")
    if grade == "OPPORTUNITY":
        return _scale_or_hold("SCALE +25%", 25, 1.25,
                              "OPPORTUNITY: test proving out — graduate to evergreen")
    if grade == "RECOVERING":
        return _result("HOLD", 0, daily_recent,
                       f"RECOVERING — mature weak but recent improving ({metrics_str}). Hold 3 more days.")

    # AVERAGE mature with recent OK
    if grade == "AVERAGE":
        return _result("HOLD", 0, daily_recent,
                       f"AVG mature ({metrics_str}). Run creative A/B test.")

    # Fallback (mature TOP/GOOD with recent AVG / no clear signal)
    return _result("HOLD", 0, daily_recent,
                   f"Mixed signal: {grade} mature + {recent_grade or 'n/a'} recent ({metrics_str}).")


def _is_ios_or_retarget_name(campaign_name: str) -> bool:
    if not campaign_name:
        return False
    nl = campaign_name.lower()
    return "ios" in nl or "retarget" in nl


def build_ad_x_date_data(ad_rows: list) -> list:
    """Aggregate per-ad rows into 8 sub-rows per ad by ad-age day windows.

    Windows (days since first spend): d0-d2, d3-d5, d6-d8, d9-d10, d11-d13, d14+
    plus a d3-d14 aggregate row (bold in sheet).

    Predicted ROAS uses each ad's own d14+ period as the maturity benchmark:
      - Mandate component: bench_mandate_roas × (bench_d0_trial_cost / slot_d0_trial_cost)
      - Non-mandate component: bench_non_mdt_roas × (bench_d0_cac / slot_d0_cac)
      - pred_d6_roas = mandate + non-mandate components

    Excludes iOS and Retarget campaigns.
    """
    PERIODS = [
        ("d0-d2",   "d0d2"),
        ("d3-d5",   "d3d5"),
        ("d6-d8",   "d6d8"),
        ("d9-d10",  "d9d10"),
        ("d11-d13", "d11d13"),
        ("d14+",    "d14p"),
    ]
    AGG_PREFIXES = ["d3d5", "d6d8", "d9d10", "d11d13", "d14p"]

    filtered = [r for r in ad_rows if not _is_ios_or_retarget_name(r.get("campaign_name") or "")]

    def _f(v):
        return float(v) if v is not None else 0.0

    def _blank_slot():
        return {
            "spend": 0.0, "signups": 0, "d0_conv": 0, "d0_trials": 0, "d6_conv": 0,
            "d6_revenue": 0.0, "d6_mandate_revenue": 0.0, "d6_non_mandate_revenue": 0.0,
        }

    # Toggle: set False to disable fatigue adjustment and rollback to unadjusted prediction
    APPLY_FATIGUE_ADJUSTMENT = False

    # Group by (campaign_name, adset_name, ad_name) and accumulate raw counters
    groups: dict[tuple[str, str, str], dict] = {}
    for r in filtered:
        key = (r.get("campaign_name") or "", r.get("adset_name") or "", r.get("ad_name") or "")
        g = groups.setdefault(key, {"_statuses": [], "_recent_cpm_num": 0.0, "_recent_cpm_den": 0.0,
                                     "_mature_cpm_num": 0.0, "_mature_cpm_den": 0.0})
        g["_statuses"].append(((r.get("status") or "").upper(), _f(r.get("spend"))))
        # Spend-weighted CPM accumulation for fatigue detection
        r_spend = _f(r.get("recent_spend"))
        m_spend = _f(r.get("mature_spend"))
        g["_recent_cpm_num"] += _f(r.get("recent_cpm") or 0) * r_spend
        g["_recent_cpm_den"] += r_spend
        g["_mature_cpm_num"] += _f(r.get("mature_cpm") or 0) * m_spend
        g["_mature_cpm_den"] += m_spend
        for _, prefix in PERIODS:
            slot = g.setdefault(prefix, _blank_slot())
            spend = _f(r.get(f"{prefix}_spend"))
            slot["spend"]                  += spend
            slot["signups"]                += int(_f(r.get(f"{prefix}_signups")))
            slot["d0_conv"]                += int(_f(r.get(f"{prefix}_d0_conv")))
            slot["d0_trials"]              += int(_f(r.get(f"{prefix}_d0_trials")))
            slot["d6_conv"]                += int(_f(r.get(f"{prefix}_d6_conv")))
            # Use raw revenue directly from SQL (no rounding loss vs roas × spend recovery)
            slot["d6_revenue"]             += _f(r.get(f"{prefix}_d6_revenue"))
            slot["d6_mandate_revenue"]     += _f(r.get(f"{prefix}_d6_mandate_revenue"))
            slot["d6_non_mandate_revenue"] += _f(r.get(f"{prefix}_d6_non_mandate_revenue"))

    # Compute aggregate status + total spend
    for g in groups.values():
        statuses = g["_statuses"]
        if any(s == "ACTIVE" for s, _ in statuses):
            g["_agg_status"] = "ACTIVE"
        else:
            g["_agg_status"] = max(statuses, key=lambda x: x[1])[0] if statuses else ""
        g["_total_spend"] = sum(g[p[1]]["spend"] for p in PERIODS)

    # Keep ads with spend in the last 13 days (d0-d13 windows), regardless of current status
    def _has_recent_spend(g):
        return any(g[p]["spend"] > 0 for p in ["d0d2", "d3d5", "d6d8", "d9d10", "d11d13"])
    groups = {k: g for k, g in groups.items() if _has_recent_spend(g)}

    # Account-wide benchmarks: pool actual revenue / spend / trials / conv across
    # all groups × all non-d0-d2 timeframes (d3-d5 .. d14+). Used as fallback for
    # ads with no historical data in those windows. We compute the ratio from the
    # pooled totals — never from an average of per-group ratios.
    _tot_spend = _tot_trials = _tot_conv = _tot_mdtrev = _tot_nmrev = 0.0
    for _g in groups.values():
        for _p in AGG_PREFIXES:
            _s = _g[_p]
            _tot_spend  += _s["spend"]
            _tot_trials += _s["d0_trials"]
            _tot_conv   += _s["d0_conv"]
            _tot_mdtrev += _s["d6_mandate_revenue"]
            _tot_nmrev  += _s["d6_non_mandate_revenue"]
    global_bench_mandate_roas  = (_tot_mdtrev / _tot_spend) if (_tot_spend and _tot_mdtrev) else None
    global_bench_non_mdt_roas  = (_tot_nmrev  / _tot_spend) if (_tot_spend and _tot_nmrev)  else None
    global_bench_trial_cost    = (_tot_spend / _tot_trials) if _tot_trials else None
    global_bench_d0_cac        = (_tot_spend / _tot_conv)   if _tot_conv   else None

    # Pre-compute campaign and (campaign, adset) total spend for sorting
    camp_spend: dict[str, float] = {}
    adset_spend: dict[tuple[str, str], float] = {}
    for (campaign_name, adset_name, _ad_name), g in groups.items():
        camp_spend[campaign_name] = camp_spend.get(campaign_name, 0.0) + g["_total_spend"]
        adset_spend[(campaign_name, adset_name)] = adset_spend.get((campaign_name, adset_name), 0.0) + g["_total_spend"]

    # Per-campaign and per-adset d0-d2 spend for sorting (highest first)
    camp_d0d2: dict[str, float] = {}
    adset_d0d2: dict[tuple[str, str], float] = {}
    for (campaign_name, adset_name, _ad_name), g in groups.items():
        d0d2 = g["d0d2"]["spend"]
        camp_d0d2[campaign_name] = camp_d0d2.get(campaign_name, 0.0) + d0d2
        adset_d0d2[(campaign_name, adset_name)] = adset_d0d2.get((campaign_name, adset_name), 0.0) + d0d2

    def _make_row(campaign_name, adset_name, ad_name, status, g, period_label, s, is_agg=False):
        spend = s["spend"]
        d6_conv = s["d6_conv"]
        d0_conv = s["d0_conv"]
        d0_trials = s["d0_trials"]
        return {
            "campaign_name":       campaign_name,
            "adset_name":          adset_name,
            "ad_name":             ad_name,
            "status":              status,
            "_camp_spend":         camp_spend[campaign_name],
            "_adset_spend":        adset_spend[(campaign_name, adset_name)],
            "_ad_spend":           g["_total_spend"],
            "_camp_d0d2_spend":    camp_d0d2[campaign_name],
            "_adset_d0d2_spend":   adset_d0d2[(campaign_name, adset_name)],
            "_d0d2_spend":         g["d0d2"]["spend"],
            "period":              period_label,
            "_is_agg":             is_agg,
            "spend":               round(spend) if spend else None,
            "signups":             s["signups"] or None,
            "d0_conv":             d0_conv or None,
            "d0_cac":              round(spend / d0_conv)    if spend and d0_conv    else None,
            "d0_trials":           d0_trials or None,
            "d0_trial_cost":       round(spend / d0_trials)  if spend and d0_trials  else None,
            "d6_cac":              round(spend / d6_conv)    if spend and d6_conv    else None,
            "d6_roas":             round(s["d6_revenue"] / spend, 3)             if spend and s["d6_revenue"]             else None,
            "d6_mandate_roas":     round(s["d6_mandate_revenue"] / spend, 3)     if spend and s["d6_mandate_revenue"]     else None,
            "d6_non_mandate_roas": round(s["d6_non_mandate_revenue"] / spend, 3) if spend and s["d6_non_mandate_revenue"] else None,
            "pred_d6_roas":        s.get("pred_d6_roas"),
        }

    out = []
    for (campaign_name, adset_name, ad_name), g in groups.items():
        status = g["_agg_status"]

        # Benchmark = spend-weighted average across all non-d0-d2 timeframes
        # (d3-d5 .. d14+). Falls back to account-wide median when ad has no
        # historical data in those windows.
        agg_spend  = sum(g[p]["spend"]                  for p in AGG_PREFIXES)
        agg_trials = sum(g[p]["d0_trials"]              for p in AGG_PREFIXES)
        agg_conv   = sum(g[p]["d0_conv"]                for p in AGG_PREFIXES)
        agg_mdtrev = sum(g[p]["d6_mandate_revenue"]     for p in AGG_PREFIXES)
        agg_nmrev  = sum(g[p]["d6_non_mandate_revenue"] for p in AGG_PREFIXES)

        # If the ad has ANY non-d0-d2 spend history, use its own ratios — even
        # when mandate or non-mandate revenue is 0 (don't inflate predictions
        # by leaking a global average into an ad that has shown zero of that revenue type).
        if agg_spend > 0:
            bench_mandate_roas = agg_mdtrev / agg_spend
            bench_non_mdt_roas = agg_nmrev / agg_spend
        else:
            bench_mandate_roas = global_bench_mandate_roas
            bench_non_mdt_roas = global_bench_non_mdt_roas
        bench_trial_cost = (agg_spend / agg_trials if agg_trials else global_bench_trial_cost)
        bench_d0_cac     = (agg_spend / agg_conv   if agg_conv   else global_bench_d0_cac)

        # Fatigue adjustment: recent CPM / mature CPM → scales bench ROAS down if ad is fatiguing
        if APPLY_FATIGUE_ADJUSTMENT:
            recent_cpm = g["_recent_cpm_num"] / g["_recent_cpm_den"] if g["_recent_cpm_den"] else None
            mature_cpm = g["_mature_cpm_num"] / g["_mature_cpm_den"] if g["_mature_cpm_den"] else None
            fatigue_factor = (recent_cpm / mature_cpm
                              if (recent_cpm and mature_cpm and mature_cpm > 0) else 1.0)
            bench_mandate_roas = bench_mandate_roas / fatigue_factor if bench_mandate_roas else bench_mandate_roas
            bench_non_mdt_roas = bench_non_mdt_roas / fatigue_factor if bench_non_mdt_roas else bench_non_mdt_roas

        for _, prefix in PERIODS:
            s = g[prefix]
            spend = s["spend"]
            if prefix == "d0d2" and spend:
                # Only predict if there are actual d0-d2 trials — otherwise we'd fall back to
                # global benchmarks and all trialless ads get the same meaningless pred ROAS
                if s["d0_trials"] > 0:
                    slot_trial_cost = spend / s["d0_trials"]
                    slot_d0_cac     = (spend / s["d0_conv"]) if s["d0_conv"] else 0
                    # pred_mandate = avg_mandate_roas × (avg_trial_cost / d0d2_trial_cost)
                    pred_mandate = (bench_mandate_roas * (bench_trial_cost / slot_trial_cost)
                                    if (bench_mandate_roas and bench_trial_cost and slot_trial_cost) else 0.0)
                    # Non-mandate prediction needs a CAC signal. If d0-d2 has no conversions,
                    # we can't extrapolate non-mandate ROAS — reference mandate-only.
                    if slot_d0_cac > 0 and bench_d0_cac:
                        pred_non_mdt = (bench_non_mdt_roas or 0) * (bench_d0_cac / slot_d0_cac)
                    else:
                        pred_non_mdt = 0.0
                    s["pred_d6_roas"] = round(pred_mandate + pred_non_mdt, 3) if (pred_mandate + pred_non_mdt) > 0 else None
                else:
                    s["pred_d6_roas"] = None
            else:
                s["pred_d6_roas"] = None

        # Build 6 period rows
        for period_label, prefix in PERIODS:
            out.append(_make_row(campaign_name, adset_name, ad_name, status, g, period_label, g[prefix]))

        # Build d3-d14 aggregate row
        agg: dict = _blank_slot()
        for prefix in AGG_PREFIXES:
            s = g[prefix]
            agg["spend"]                  += s["spend"]
            agg["signups"]                += s["signups"]
            agg["d0_conv"]                += s["d0_conv"]
            agg["d0_trials"]              += s["d0_trials"]
            agg["d6_conv"]                += s["d6_conv"]
            agg["d6_revenue"]             += s["d6_revenue"]
            agg["d6_mandate_revenue"]     += s["d6_mandate_revenue"]
            agg["d6_non_mandate_revenue"] += s["d6_non_mandate_revenue"]
        agg["pred_d6_roas"] = None  # aggregate shows actual d6_roas, not predicted
        out.append(_make_row(campaign_name, adset_name, ad_name, status, g, "d3-d14 (Agg)", agg, is_agg=True))

    return sorted(out, key=lambda x: (
        # Group by campaign → adset → ad, each level sorted by d0-d2 spend desc
        -x["_camp_d0d2_spend"], x["campaign_name"],
        -x["_adset_d0d2_spend"], x["adset_name"],
        -x["_d0d2_spend"], x["ad_name"],
        # Period ordering keeps each ad's 7 rows in stable sequence
        {"d0-d2": 0, "d3-d5": 1, "d6-d8": 2, "d9-d10": 3, "d11-d13": 4, "d14+": 5, "d3-d14 (Agg)": 6}.get(x["period"], 99),
    ))


def _score_to_grade(score: float) -> str:
    if   score >= 75: return 'TOP PERFORMER'
    elif score >= 55: return 'GOOD'
    elif score >= 35: return 'AVERAGE'
    elif score >= 20: return 'UNDERPERFORMING'
    else:             return 'POOR'


def _compute_period_grades(rows: list, metric_map: list[tuple], spend_key: str, grade_key: str) -> None:
    """Compute a percentile-based grade for one time period and store it on each row.

    metric_map: [(row_key, weight, direction), ...] — same shape as WEIGHTS.
    spend_key:  the row key for spend in this period (e.g. 'spend', 'l3d_spend').
    grade_key:  where to store the result (e.g. '_grade', '_l3d_grade').
    """
    metric_vals: dict[str, list[float]] = {
        key: sorted(float(r[key]) for r in rows if r.get(key) is not None)
        for key, _, _ in metric_map
    }

    def pct_rank(val: float, sorted_vals: list[float]) -> float:
        if not sorted_vals:
            return 0.0
        return sum(1 for v in sorted_vals if v < val) / len(sorted_vals)

    for r in rows:
        spend = float(r.get(spend_key) or 0)
        metric_scores: dict[str, tuple[float, float]] = {}

        for key, weight, direction in metric_map:
            val = r.get(key)
            if val is not None:
                pr = pct_rank(float(val), metric_vals[key])
                s  = (1.0 - pr) if direction == 'lower' else pr
            elif spend > 0:
                s = 0.0
            else:
                continue
            metric_scores[key] = (s, weight)

        total_w = sum(w for _, w in metric_scores.values())
        if total_w < 0.10:
            r[grade_key] = None
            continue

        score = sum(s * w for s, w in metric_scores.values()) / total_w * 100
        r[grade_key] = _score_to_grade(score)


def compute_ad_scores(rows: list, use_pred_for_recent: bool = False) -> None:
    """
    Adds _score, _grade, _suggestion keys to each row dict in-place.
    Also computes _mid_grade and _recent_grade for period-level grading.

    Main scoring uses mature window (7+ days old, D6 complete), split by mandate type.
    Method   : percentile rank across all ads for each metric; weighted average.
    IMMATURE : ad's first_date is < 6 days ago — not enough data to evaluate.

    For lower-is-better metrics a NULL value (zero conversions despite spend)
    is treated as worst-case (score=0) rather than excluded, so ads with no
    D6 conversions are correctly penalised.

    use_pred_for_recent: when True, grade the recent window off predicted D6
    ROAS instead of actual recent_d6_roas (which is mostly NULL because D6 has
    not yet elapsed for recent-window cohorts). Used for campaign-level rows.
    """
    MATURE_WEIGHTS = [
        ('mature_d6_mandate_roas',     0.35, 'higher'),
        ('mature_d0_trial_cost',       0.25, 'lower'),
        ('mature_d0_cac',              0.20, 'lower'),
        ('mature_d6_non_mandate_roas', 0.10, 'higher'),
        ('mature_d6_cac',              0.10, 'lower'),
    ]
    MID_WEIGHTS = [
        ('mid_d6_roas',       0.40, 'higher'),
        ('mid_d0_trial_cost', 0.35, 'lower'),
        ('mid_d0_cac',        0.25, 'lower'),
    ]
    recent_roas_key = '_recent_pred_d6_roas' if use_pred_for_recent else 'recent_d6_roas'
    RECENT_WEIGHTS = [
        (recent_roas_key,        0.40, 'higher'),
        ('recent_d0_trial_cost', 0.35, 'lower'),
        ('recent_d0_cac',        0.25, 'lower'),
    ]

    # Mid and recent period grades
    _compute_period_grades(rows, MID_WEIGHTS,    'mid_spend',    '_mid_grade')
    _compute_period_grades(rows, RECENT_WEIGHTS, 'recent_spend', '_recent_grade')

    # Build metric distributions across all rows for mature scoring
    metric_vals: dict[str, list[float]] = {
        key: sorted(float(r[key]) for r in rows if r.get(key) is not None)
        for key, _, _ in MATURE_WEIGHTS
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
        'OPPORTUNITY':   'Scale aggressively — test campaign proving out at >₹10k, graduate to evergreen',
        'RECOVERING':    'Hold spend — mature data weak but recent signal improving, review after 3 more days',
    }

    def _is_test_campaign(name: str) -> bool:
        if not name:
            return False
        nl = name.lower()
        return 'test' in nl or 'experiment' in nl or 'pilot' in nl

    for r in rows:
        fd = r.get('first_date')
        age_days = (today - fd).days if isinstance(fd, date) else 999

        if age_days < 3:
            r['_score']      = None
            r['_grade']      = 'FULL IMMATURE'
            r['_suggestion'] = f'Only {age_days}d of data — too early to evaluate'
            continue

        is_partial = age_days < 7  # D3-D6

        mature_spend = float(r.get('mature_spend') or 0)
        spend        = float(r.get('spend') or 0)  # overall for INEFFICIENT threshold

        metric_scores: dict[str, tuple[float, float]] = {}

        for key, weight, direction in MATURE_WEIGHTS:
            val = r.get(key)
            if val is not None:
                pr = pct_rank(float(val), metric_vals[key])
                s  = (1.0 - pr) if direction == 'lower' else pr
            elif mature_spend > 0:
                s = 0.0
            else:
                continue
            metric_scores[key] = (s, weight)

        total_w = sum(w for _, w in metric_scores.values())
        if total_w < 0.10:
            r['_score']      = None
            r['_grade']      = 'PARTIAL IMMATURE' if is_partial else 'NO DATA'
            r['_suggestion'] = f'Only {age_days}d — partial data, revisit after day 7' if is_partial else 'Insufficient mature data for scoring'
            continue

        score = sum(s * w for s, w in metric_scores.values()) / total_w * 100
        r['_score'] = round(score, 1)

        base_grade = _score_to_grade(score)

        if is_partial:
            r['_grade'] = 'PARTIAL IMMATURE'
            r['_suggestion'] = f'D{age_days} — early signal: {base_grade} (score {r["_score"]}) — revisit after day 7'
            continue

        status    = (r.get('status') or '').upper()
        camp_name = r.get('campaign_name') or ''
        is_active = status == 'ACTIVE'

        grade = base_grade

        _CAT1_RAW = 10_000 / 1.18  # ₹10k GST-inclusive in raw spend terms
        _CAT2_RAW =  5_000 / 1.18  # ₹5k  GST-inclusive in raw spend terms

        recent_grade = r.get('_recent_grade')
        _POOR_GRADES = {'POOR', None}  # None = no recent data, treat as unrecovered

        if is_active and base_grade == 'POOR' and spend > _CAT1_RAW:
            # Soften if recent window shows recovery (not POOR)
            if recent_grade not in _POOR_GRADES:
                grade = 'RECOVERING'  # historically poor but recent signal improving
            else:
                grade = 'INEFFICIENT CAT 1'
        elif is_active and base_grade == 'POOR' and spend >= _CAT2_RAW:
            grade = 'INEFFICIENT CAT 2'
        elif is_active and base_grade == 'UNDERPERFORMING' and spend > _CAT1_RAW:
            grade = 'INEFFICIENT CAT 3'
        elif _is_test_campaign(camp_name) and base_grade in ('AVERAGE', 'GOOD', 'TOP PERFORMER') and spend > _CAT1_RAW:
            grade = 'OPPORTUNITY'

        r['_grade'] = grade

        primary_metric = 'mature_d6_mandate_roas'
        WEAK_NOTES_MATURE = {
            'mature_d6_cac':              'D6 CAC above peers',
            'mature_d6_roas':             'D6 ROAS below peers',
            'mature_d6_mandate_roas':     'D6 Mandate ROAS below peers',
            'mature_d6_non_mandate_roas': 'D6 Non-Mandate ROAS below peers',
            'mature_d0_trial_cost':       'Trial cost above peers',
            'mature_d0_cac':              'D0 CAC above peers',
        }

        if grade in ('INEFFICIENT CAT 1', 'INEFFICIENT CAT 2', 'INEFFICIENT CAT 3', 'OPPORTUNITY', 'RECOVERING'):
            weakest = min(metric_scores, key=lambda k: metric_scores[k][0])
            spend_gst = int(round(spend * 1.18))
            r['_suggestion'] = f"{ACTIONS[grade]} | Spend ₹{spend_gst:,} | {WEAK_NOTES_MATURE.get(weakest, weakest)}"
        elif grade == 'TOP PERFORMER':
            r['_suggestion'] = ACTIONS['TOP PERFORMER']
        else:
            weakest = min(metric_scores, key=lambda k: metric_scores[k][0])
            r['_suggestion'] = f"{ACTIONS[grade]} | Key drag: {WEAK_NOTES_MATURE.get(weakest, weakest)}"


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
            # Currency / ROAS number formats
            *_auto_format_requests(ws.id, headers, 1, len(data_rows)),
        ]
    }
    sh.batch_update(body)
    print(f"  DoD tab: {len(this_month_rows)} days this month, {len(last_month_rows)} days last month.")


def write_ad_level_sheet(sh, rows: list):
    # Get or create "Ad Level — Meta" tab — delete & recreate to clear merges
    try:
        old_ws = sh.worksheet("Ad Level — Meta")
        sh.del_worksheet(old_ws)
    except Exception:
        pass
    ws = sh.add_worksheet("Ad Level — Meta", rows=max(len(rows) + 50, 2000), cols=55)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    mature_label = f"Mature (up to {mature_end.strftime('%d %b')} — D6 complete)"
    mid_label    = f"Mid ({mid_start.strftime('%d %b')}–{mid_end.strftime('%d %b')}, D0 done)"
    recent_label = f"Recent ({recent_start.strftime('%d %b')}–{today.strftime('%d %b')}, today+yday+dby)"

    # -- Row 0: group headers (merged) --
    # -- Row 1: metric headers --
    mtd_headers = [
        "Spend (Overall)", "Impressions", "Clicks", "CTR %", "CPM ₹", "CPC ₹",
        "First Date", "Last Date", "Status",
        "Signups", "D0 Conv", "D0 Trials", "D0 CAC ₹", "D0 Trial Cost ₹", "D0 ROAS",
        "D6 Mandate", "D6 Non-Mdt", "D6 Trials",
        "D6 ROAS", "D6 Mandate ROAS", "D6 Non-Mdt ROAS", "D6 CAC ₹",
        "LTV ₹", "CAC ₹",
    ]
    mature_headers = [
        "Spend", "Impressions", "Clicks", "CTR %", "CPM ₹", "CPC ₹",
        "Signups", "D0 Conv", "D0 CAC ₹", "D0 ROAS", "D0 Trials", "D0 Trial Cost ₹",
        "D6 CAC ₹", "D6 ROAS", "D6 Mandate ROAS", "D6 Non-Mdt ROAS",
    ]
    mid_headers = [
        "Spend", "D0 Conv", "D0 CAC ₹", "D0 ROAS", "D0 Trials", "D0 Trial Cost ₹",
        "D6 CAC ₹", "D6 ROAS", "Mid Grade",
    ]
    recent_headers = [
        "Spend", "D0 Conv", "D0 CAC ₹", "D0 ROAS", "D0 Trials", "D0 Trial Cost ₹",
        "Pred D6 ROAS", "Recent Grade",
    ]
    identity_headers = ["Ad Name", "Campaign", "Adset"]
    scoring_headers = ["Score", "Grade", "Suggestion"]

    headers = identity_headers + mtd_headers + mature_headers + mid_headers + recent_headers + scoring_headers

    N_ID      = len(identity_headers)
    N_MTD     = len(mtd_headers)
    N_MATURE  = len(mature_headers)
    N_MID     = len(mid_headers)
    N_RECENT  = len(recent_headers)

    IDX_MTD_START    = N_ID
    IDX_MATURE_START = N_ID + N_MTD
    IDX_MID_START    = IDX_MATURE_START + N_MATURE
    IDX_RECENT_START = IDX_MID_START + N_MID
    IDX_SCORE_START  = IDX_RECENT_START + N_RECENT

    IDX_STATUS     = N_ID + mtd_headers.index("Status")
    IDX_SCORE      = IDX_SCORE_START
    IDX_GRADE      = IDX_SCORE_START + 1
    IDX_SUGGESTION = IDX_SCORE_START + 2

    # Build group header row (row 0)
    group_row = [""] * len(headers)
    group_row[IDX_MTD_START]    = "Overall"
    group_row[IDX_MATURE_START] = mature_label
    group_row[IDX_MID_START]    = mid_label
    group_row[IDX_RECENT_START] = recent_label

    GST = 1.18
    def _v(v):   return "" if v is None else v
    def _i(v):   return "" if v is None else int(float(v))
    def _f1(v):  return "" if v is None else round(float(v), 1)   # CTR
    def _sp(v):  return "" if v is None else _inr_str(float(v) * GST, 0)         # spend / CAC / cost (×GST, ₹ Indian format)
    def _ro(v):  return "" if v is None else round(float(v) / GST, 3)            # ROAS (÷GST)
    def _pm(v):  return "" if v is None else _inr_str(float(v) * GST, 1)         # CPM / CPC (×GST, ₹ Indian format)

    data_rows = [group_row, headers]
    for r in rows:
        data_rows.append([
            r["ad_name"] or "",
            r["campaign_name"] or "",
            r["adset_name"] or "",
            # MTD media
            _sp(r["spend"]),
            _i(r["impressions"]),
            _i(r["clicks"]),
            _f1(r["ctr"]),
            _pm(r["cpm"]),
            _pm(r["cpc"]),
            str(r["first_date"]) if r["first_date"] else "",
            str(r["last_date"])  if r["last_date"]  else "",
            r.get("status") or "",
            # Overall attribution
            _i(r["signups"]),
            _i(r["d0_conv"]),
            _i(r["d0_trials"]),
            _sp(r["d0_cac"]),
            _sp(r["d0_trial_cost"]),
            _ro(r["d0_roas"]),
            _i(r["d6_mandate"]),
            _i(r["d6_non_mandate"]),
            _i(r["d6_trials"]),
            _ro(r["d6_roas"]),
            _ro(r.get("d6_mandate_roas")),
            _ro(r.get("d6_non_mandate_roas")),
            _sp(r.get("d6_cac")),
            _i(r["ltv_inr"]),
            _sp(r["cac_inr"]),
            # Mature media
            _sp(r.get("mature_spend")),
            _i(r.get("mature_impressions")),
            _i(r.get("mature_clicks")),
            _f1(r.get("mature_ctr")),
            _pm(r.get("mature_cpm")),
            _pm(r.get("mature_cpc")),
            # Mature attribution
            _i(r.get("mature_signups")),
            _i(r.get("mature_d0_conv")),
            _sp(r.get("mature_d0_cac")),
            _ro(r.get("mature_d0_roas")),
            _i(r.get("mature_d0_trials")),
            _sp(r.get("mature_d0_trial_cost")),
            _sp(r.get("mature_d6_cac")),
            _ro(r.get("mature_d6_roas")),
            _ro(r.get("mature_d6_mandate_roas")),
            _ro(r.get("mature_d6_non_mandate_roas")),
            # Mid
            _sp(r.get("mid_spend")),
            _i(r.get("mid_d0_conv")),
            _sp(r.get("mid_d0_cac")),
            _ro(r.get("mid_d0_roas")),
            _i(r.get("mid_d0_trials")),
            _sp(r.get("mid_d0_trial_cost")),
            _sp(r.get("mid_d6_cac")),
            _ro(r.get("mid_d6_roas")),
            r.get("_mid_grade") or "",
            # Recent
            _sp(r.get("recent_spend")),
            _i(r.get("recent_d0_conv")),
            _sp(r.get("recent_d0_cac")),
            _ro(r.get("recent_d0_roas")),
            _i(r.get("recent_d0_trials")),
            _sp(r.get("recent_d0_trial_cost")),
            _ro(r.get("_recent_pred_d6_roas")),
            r.get("_recent_grade") or "",
            # Scoring
            r.get("_score", "") if r.get("_score") is not None else "",
            r.get("_grade", ""),
            r.get("_suggestion", ""),
        ])

    # Add footer
    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(rows)} ads"])

    ws.update(values=data_rows, range_name="A1")

    # Formatting
    DATA_START_ROW = 2  # data starts after group header + metric header

    # Group header colours
    COL_MTD    = {"red": 0.102, "green": 0.204, "blue": 0.376}  # dark blue
    COL_MATURE = {"red": 0.067, "green": 0.392, "blue": 0.176}  # forest green
    COL_MID    = {"red": 0.345, "green": 0.376, "blue": 0.471}  # grey-blue
    COL_RECENT = {"red": 0.502, "green": 0.314, "blue": 0.063}  # amber-brown

    body = {
        "requests": [
            # -- Group header row (row 0) — merge cells for each group --
            {"mergeCells": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_MTD_START, "endColumnIndex": IDX_MATURE_START},
                "mergeType": "MERGE_ALL",
            }},
            {"mergeCells": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_MATURE_START, "endColumnIndex": IDX_MID_START},
                "mergeType": "MERGE_ALL",
            }},
            {"mergeCells": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_MID_START, "endColumnIndex": IDX_RECENT_START},
                "mergeType": "MERGE_ALL",
            }},
            {"mergeCells": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_RECENT_START, "endColumnIndex": IDX_SCORE_START},
                "mergeType": "MERGE_ALL",
            }},
            # Group header — MTD style
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_MTD_START, "endColumnIndex": IDX_MATURE_START},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": COL_MTD,
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat",
            }},
            # Group header — Mature style
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_MATURE_START, "endColumnIndex": IDX_MID_START},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": COL_MATURE,
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat",
            }},
            # Group header — Mid style
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_MID_START, "endColumnIndex": IDX_RECENT_START},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": COL_MID,
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat",
            }},
            # Group header — Recent style
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": IDX_RECENT_START, "endColumnIndex": IDX_SCORE_START},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": COL_RECENT,
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat",
            }},
            # -- Metric header row (row 1) --
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2,
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
            # Mature metric header — forest green tint
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2,
                          "startColumnIndex": IDX_MATURE_START, "endColumnIndex": IDX_MID_START},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.047, "green": 0.275, "blue": 0.122},
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                    "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP",
                }},
                "fields": "userEnteredFormat",
            }},
            # Mid metric header — grey-blue tint
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2,
                          "startColumnIndex": IDX_MID_START, "endColumnIndex": IDX_RECENT_START},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.267, "green": 0.298, "blue": 0.388},
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                    "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP",
                }},
                "fields": "userEnteredFormat",
            }},
            # Recent metric header — amber-brown tint
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2,
                          "startColumnIndex": IDX_RECENT_START, "endColumnIndex": IDX_SCORE_START},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.380, "green": 0.235, "blue": 0.047},
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                    "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP",
                }},
                "fields": "userEnteredFormat",
            }},
            # Freeze 2 header rows + first 3 cols
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id,
                               "gridProperties": {"frozenRowCount": 2, "frozenColumnCount": 3}},
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }},
            # -- Column widths --
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 260}, "fields": "pixelSize",
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 200}, "fields": "pixelSize",
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 2, "endIndex": 3},
                "properties": {"pixelSize": 180}, "fields": "pixelSize",
            }},
            # MTD metric cols
            *[{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": i, "endIndex": i+1},
                "properties": {"pixelSize": 95}, "fields": "pixelSize",
            }} for i in range(IDX_MTD_START, IDX_MATURE_START)],
            # Mature metric cols — slightly narrower
            *[{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": i, "endIndex": i+1},
                "properties": {"pixelSize": 85}, "fields": "pixelSize",
            }} for i in range(IDX_MATURE_START, IDX_MID_START)],
            # Mid metric cols
            *[{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": i, "endIndex": i+1},
                "properties": {"pixelSize": 85}, "fields": "pixelSize",
            }} for i in range(IDX_MID_START, IDX_RECENT_START)],
            # Recent metric cols
            *[{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": i, "endIndex": i+1},
                "properties": {"pixelSize": 85}, "fields": "pixelSize",
            }} for i in range(IDX_RECENT_START, IDX_SCORE_START)],
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
            # Suggestion col
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": IDX_SUGGESTION, "endIndex": IDX_SUGGESTION + 1},
                "properties": {"pixelSize": 380}, "fields": "pixelSize",
            }},
            # Alternating row shading
            {"addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START_ROW,
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
                    "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START_ROW,
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
                    "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START_ROW,
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
                ("INEFFICIENT CAT 1", {"red": 0.545, "green": 0.0,   "blue": 0.0},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("INEFFICIENT CAT 2", {"red": 0.698, "green": 0.133, "blue": 0.133},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("INEFFICIENT CAT 3", {"red": 0.804, "green": 0.361, "blue": 0.361},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("OPPORTUNITY",       {"red": 0.118, "green": 0.533, "blue": 0.898},
                                      {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
                ("FULL IMMATURE",   {"red": 0.800, "green": 0.824, "blue": 0.855},
                                    {"red": 0.267, "green": 0.306, "blue": 0.365}),
                ("PARTIAL IMMATURE", {"red": 0.878, "green": 0.890, "blue": 0.914},
                                     {"red": 0.400, "green": 0.420, "blue": 0.470}),
                ("NO DATA",         {"red": 0.910, "green": 0.910, "blue": 0.910},
                                    {"red": 0.4,   "green": 0.4,   "blue": 0.4}),
            ])],
            # Currency and ROAS number formats (auto-detected from header names)
            *_auto_format_requests(ws.id, headers, DATA_START_ROW, len(data_rows)),
        ]
    }
    sh.batch_update(body)
    print(f"  Ad Level tab: {len(rows)} ads written.")


def write_campaign_level_sheet(sh, rows: list):
    """Write 'Campaign Level — Meta' tab — mirrors Ad Level structure (1 identity col)."""
    try:
        old_ws = sh.worksheet("Campaign Level — Meta")
        sh.del_worksheet(old_ws)
    except Exception:
        pass
    ws = sh.add_worksheet("Campaign Level — Meta", rows=max(len(rows) + 50, 1000), cols=55)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    mature_label = f"Mature (up to {mature_end.strftime('%d %b')} — D6 complete)"
    mid_label    = f"Mid ({mid_start.strftime('%d %b')}–{mid_end.strftime('%d %b')}, D0 done)"
    recent_label = f"Recent ({recent_start.strftime('%d %b')}–{today.strftime('%d %b')}, today+yday+dby)"

    mtd_headers = [
        "Spend (Overall)", "Impressions", "Clicks", "CTR %", "CPM ₹", "CPC ₹",
        "First Date", "Last Date", "Status",
        "Signups", "D0 Conv", "D0 Trials", "D0 CAC ₹", "D0 Trial Cost ₹", "D0 ROAS",
        "D6 Mandate", "D6 Non-Mdt", "D6 Trials",
        "D6 ROAS", "D6 Mandate ROAS", "D6 Non-Mdt ROAS", "D6 CAC ₹",
        "LTV ₹", "CAC ₹",
    ]
    mature_headers = [
        "Spend", "Impressions", "Clicks", "CTR %", "CPM ₹", "CPC ₹",
        "Signups", "D0 Conv", "D0 CAC ₹", "D0 ROAS", "D0 Trials", "D0 Trial Cost ₹",
        "D6 CAC ₹", "D6 ROAS", "D6 Mandate ROAS", "D6 Non-Mdt ROAS",
    ]
    mid_headers = [
        "Spend", "D0 Conv", "D0 CAC ₹", "D0 ROAS", "D0 Trials", "D0 Trial Cost ₹",
        "D6 CAC ₹", "D6 ROAS", "Mid Grade",
    ]
    recent_headers = [
        "Spend", "D0 Conv", "D0 CAC ₹", "D0 ROAS", "D0 Trials", "D0 Trial Cost ₹",
        "Pred D6 ROAS", "Recent Grade",
    ]
    identity_headers = ["Campaign"]
    scoring_headers  = ["Score", "Grade", "Suggestion"]
    # Decision block — synthesised from grade + recent signal + ROAS/CAC
    decision_headers = ["Action", "Budget Δ%", "Suggested Daily ₹", "Reasoning"]

    headers = identity_headers + mtd_headers + mature_headers + mid_headers + recent_headers + scoring_headers + decision_headers

    N_ID      = len(identity_headers)
    N_MTD     = len(mtd_headers)
    N_MATURE  = len(mature_headers)
    N_MID     = len(mid_headers)
    N_RECENT  = len(recent_headers)
    N_SCORE   = len(scoring_headers)
    N_DEC     = len(decision_headers)

    IDX_MTD_START      = N_ID
    IDX_MATURE_START   = N_ID + N_MTD
    IDX_MID_START      = IDX_MATURE_START + N_MATURE
    IDX_RECENT_START   = IDX_MID_START + N_MID
    IDX_SCORE_START    = IDX_RECENT_START + N_RECENT
    IDX_DECISION_START = IDX_SCORE_START + N_SCORE

    IDX_STATUS     = N_ID + mtd_headers.index("Status")
    IDX_SCORE      = IDX_SCORE_START
    IDX_GRADE      = IDX_SCORE_START + 1
    IDX_SUGGESTION = IDX_SCORE_START + 2

    group_row = [""] * len(headers)
    group_row[IDX_MTD_START]      = "Overall"
    group_row[IDX_MATURE_START]   = mature_label
    group_row[IDX_MID_START]      = mid_label
    group_row[IDX_RECENT_START]   = recent_label
    group_row[IDX_DECISION_START] = "Decision"

    GST = 1.18
    def _v(v):   return "" if v is None else v
    def _i(v):   return "" if v is None else int(float(v))
    def _f1(v):  return "" if v is None else round(float(v), 1)
    def _sp(v):  return "" if v is None else _inr_str(float(v) * GST, 0)
    def _ro(v):  return "" if v is None else round(float(v) / GST, 3)
    def _pm(v):  return "" if v is None else _inr_str(float(v) * GST, 1)

    DATA_START_ROW = 2  # group header at row 0, metric header at row 1, data from row 2

    data_rows = [group_row, headers]
    for r in rows:
        data_rows.append([
            r.get("campaign_name") or "",
            # MTD media
            _sp(r.get("spend")),
            _i(r.get("impressions")),
            _i(r.get("clicks")),
            _f1(r.get("ctr")),
            _pm(r.get("cpm")),
            _pm(r.get("cpc")),
            str(r["first_date"]) if r.get("first_date") else "",
            str(r["last_date"])  if r.get("last_date")  else "",
            r.get("status") or "",
            # Overall attribution
            _i(r.get("signups")),
            _i(r.get("d0_conv")),
            _i(r.get("d0_trials")),
            _sp(r.get("d0_cac")),
            _sp(r.get("d0_trial_cost")),
            _ro(r.get("d0_roas")),
            _i(r.get("d6_mandate")),
            _i(r.get("d6_non_mandate")),
            _i(r.get("d6_trials")),
            _ro(r.get("d6_roas")),
            _ro(r.get("d6_mandate_roas")),
            _ro(r.get("d6_non_mandate_roas")),
            _sp(r.get("d6_cac")),
            _i(r.get("ltv_inr")),
            _sp(r.get("cac_inr")),
            # Mature
            _sp(r.get("mature_spend")),
            _i(r.get("mature_impressions")),
            _i(r.get("mature_clicks")),
            _f1(r.get("mature_ctr")),
            _pm(r.get("mature_cpm")),
            _pm(r.get("mature_cpc")),
            _i(r.get("mature_signups")),
            _i(r.get("mature_d0_conv")),
            _sp(r.get("mature_d0_cac")),
            _ro(r.get("mature_d0_roas")),
            _i(r.get("mature_d0_trials")),
            _sp(r.get("mature_d0_trial_cost")),
            _sp(r.get("mature_d6_cac")),
            _ro(r.get("mature_d6_roas")),
            _ro(r.get("mature_d6_mandate_roas")),
            _ro(r.get("mature_d6_non_mandate_roas")),
            # Mid
            _sp(r.get("mid_spend")),
            _i(r.get("mid_d0_conv")),
            _sp(r.get("mid_d0_cac")),
            _ro(r.get("mid_d0_roas")),
            _i(r.get("mid_d0_trials")),
            _sp(r.get("mid_d0_trial_cost")),
            _sp(r.get("mid_d6_cac")),
            _ro(r.get("mid_d6_roas")),
            r.get("_mid_grade") or "",
            # Recent
            _sp(r.get("recent_spend")),
            _i(r.get("recent_d0_conv")),
            _sp(r.get("recent_d0_cac")),
            _ro(r.get("recent_d0_roas")),
            _i(r.get("recent_d0_trials")),
            _sp(r.get("recent_d0_trial_cost")),
            _ro(r.get("_recent_pred_d6_roas")),
            r.get("_recent_grade") or "",
            # Scoring
            r.get("_score", "") if r.get("_score") is not None else "",
            r.get("_grade", ""),
            r.get("_suggestion", ""),
            # Decision
            r.get("_decision_action", ""),
            (f"{int(r.get('_decision_budget_change_pct', 0)):+d}%"
                if r.get('_decision_budget_change_pct') is not None else ""),
            _inr_str((r.get("_decision_suggested_daily") or 0) * GST, 0),
            r.get("_decision_reasoning", ""),
        ])

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(rows)} campaigns"])

    ws.update(values=data_rows, range_name="A1")

    COL_MTD      = {"red": 0.102, "green": 0.204, "blue": 0.376}
    COL_MATURE   = {"red": 0.067, "green": 0.392, "blue": 0.176}
    COL_MID      = {"red": 0.345, "green": 0.376, "blue": 0.471}
    COL_RECENT   = {"red": 0.502, "green": 0.314, "blue": 0.063}
    COL_DECISION = {"red": 0.376, "green": 0.122, "blue": 0.439}  # purple — stands out from the period colors

    GR_S, GR_E = 0, 1   # group header row
    HR_S, HR_E = 1, 2   # metric header row

    body = {"requests": [
        # Group header merges + colors
        {"mergeCells": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_MTD_START, "endColumnIndex": IDX_MATURE_START},
                        "mergeType": "MERGE_ALL"}},
        {"mergeCells": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_MATURE_START, "endColumnIndex": IDX_MID_START},
                        "mergeType": "MERGE_ALL"}},
        {"mergeCells": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_MID_START, "endColumnIndex": IDX_RECENT_START},
                        "mergeType": "MERGE_ALL"}},
        {"mergeCells": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_RECENT_START, "endColumnIndex": IDX_SCORE_START},
                        "mergeType": "MERGE_ALL"}},
        {"mergeCells": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_DECISION_START, "endColumnIndex": IDX_DECISION_START + N_DEC},
                        "mergeType": "MERGE_ALL"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_DECISION_START, "endColumnIndex": IDX_DECISION_START + N_DEC},
                        "cell": {"userEnteredFormat": {"backgroundColor": COL_DECISION,
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                                  "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_MTD_START, "endColumnIndex": IDX_MATURE_START},
                        "cell": {"userEnteredFormat": {"backgroundColor": COL_MTD,
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                                  "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_MATURE_START, "endColumnIndex": IDX_MID_START},
                        "cell": {"userEnteredFormat": {"backgroundColor": COL_MATURE,
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                                  "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_MID_START, "endColumnIndex": IDX_RECENT_START},
                        "cell": {"userEnteredFormat": {"backgroundColor": COL_MID,
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                                  "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": GR_S, "endRowIndex": GR_E,
                                   "startColumnIndex": IDX_RECENT_START, "endColumnIndex": IDX_SCORE_START},
                        "cell": {"userEnteredFormat": {"backgroundColor": COL_RECENT,
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                                  "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
                        "fields": "userEnteredFormat"}},
        # Metric header row
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": HR_S, "endRowIndex": HR_E,
                                   "startColumnIndex": 0, "endColumnIndex": len(headers)},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                                  "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": HR_S, "endRowIndex": HR_E,
                                   "startColumnIndex": IDX_MATURE_START, "endColumnIndex": IDX_MID_START},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.047, "green": 0.275, "blue": 0.122},
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                                  "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": HR_S, "endRowIndex": HR_E,
                                   "startColumnIndex": IDX_MID_START, "endColumnIndex": IDX_RECENT_START},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.267, "green": 0.298, "blue": 0.388},
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                                  "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": HR_S, "endRowIndex": HR_E,
                                   "startColumnIndex": IDX_RECENT_START, "endColumnIndex": IDX_SCORE_START},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.380, "green": 0.235, "blue": 0.047},
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                                  "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat"}},
        # Freeze 2 header rows + Campaign column
        {"updateSheetProperties": {"properties": {"sheetId": ws.id,
                                    "gridProperties": {"frozenRowCount": 2, "frozenColumnCount": N_ID}},
                                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
        # Column widths
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 0, "endIndex": 1},
                                       "properties": {"pixelSize": 320}, "fields": "pixelSize"}},
        *[{"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                          "startIndex": i, "endIndex": i+1},
                                          "properties": {"pixelSize": 95}, "fields": "pixelSize"}}
          for i in range(IDX_MTD_START, IDX_MATURE_START)],
        *[{"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                          "startIndex": i, "endIndex": i+1},
                                          "properties": {"pixelSize": 85}, "fields": "pixelSize"}}
          for i in range(IDX_MATURE_START, IDX_SCORE_START)],
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": IDX_SCORE, "endIndex": IDX_SCORE + 1},
                                       "properties": {"pixelSize": 70}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": IDX_GRADE, "endIndex": IDX_GRADE + 1},
                                       "properties": {"pixelSize": 175}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": IDX_SUGGESTION, "endIndex": IDX_SUGGESTION + 1},
                                       "properties": {"pixelSize": 380}, "fields": "pixelSize"}},
        # Decision column widths: Action / Budget Δ% / Suggested Daily ₹ / Reasoning
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": IDX_DECISION_START, "endIndex": IDX_DECISION_START + 1},
                                       "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": IDX_DECISION_START + 1, "endIndex": IDX_DECISION_START + 2},
                                       "properties": {"pixelSize": 90}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": IDX_DECISION_START + 2, "endIndex": IDX_DECISION_START + 3},
                                       "properties": {"pixelSize": 130}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": IDX_DECISION_START + 3, "endIndex": IDX_DECISION_START + 4},
                                       "properties": {"pixelSize": 480}, "fields": "pixelSize"}},
        # Decision metric-header row color
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": HR_S, "endRowIndex": HR_E,
                                   "startColumnIndex": IDX_DECISION_START, "endColumnIndex": IDX_DECISION_START + N_DEC},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.286, "green": 0.094, "blue": 0.337},
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9},
                                  "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat"}},
        # Decision wrap for reasoning column
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": DATA_START_ROW, "endRowIndex": len(data_rows),
                                   "startColumnIndex": IDX_DECISION_START + 3, "endColumnIndex": IDX_DECISION_START + 4},
                        "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"}},
                        "fields": "userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment"}},
        # Currency and ROAS number formats (auto-detected from headers)
        *_auto_format_requests(ws.id, headers, DATA_START_ROW, len(data_rows)),
        # Alternating row shading
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START_ROW, "endRowIndex": len(data_rows),
                        "startColumnIndex": 0, "endColumnIndex": IDX_SCORE}],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA",
                              "values": [{"userEnteredValue": "=ISEVEN(ROW())"}]},
                              "format": {"backgroundColor": {"red": 0.957, "green": 0.965, "blue": 0.976}}},
        }, "index": 0}},
        # Status column conditional formatting
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START_ROW, "endRowIndex": len(data_rows),
                        "startColumnIndex": IDX_STATUS, "endColumnIndex": IDX_STATUS + 1}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                             "format": {"backgroundColor": bg, "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx + 1}} for idx, (label, bg, fg) in enumerate([
            ("ACTIVE",       {"red": 0.714, "green": 0.882, "blue": 0.722}, {"red": 0.0,   "green": 0.239, "blue": 0.086}),
            ("PAUSED",       {"red": 1.0,   "green": 0.898, "blue": 0.600}, {"red": 0.4,   "green": 0.267, "blue": 0.0}),
            ("WITH_ISSUES",  {"red": 0.914, "green": 0.263, "blue": 0.208}, {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("ARCHIVED",     {"red": 0.851, "green": 0.851, "blue": 0.851}, {"red": 0.4,   "green": 0.4,   "blue": 0.4}),
        ])],
        # Grade column conditional formatting
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START_ROW, "endRowIndex": len(data_rows),
                        "startColumnIndex": IDX_GRADE, "endColumnIndex": IDX_GRADE + 1}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                             "format": {"backgroundColor": bg, "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx + 5}} for idx, (label, bg, fg) in enumerate([
            ("TOP PERFORMER",     {"red": 0.137, "green": 0.612, "blue": 0.290}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("GOOD",              {"red": 0.714, "green": 0.882, "blue": 0.722}, {"red": 0.0, "green": 0.239, "blue": 0.086}),
            ("AVERAGE",           {"red": 1.0,   "green": 0.898, "blue": 0.600}, {"red": 0.4, "green": 0.267, "blue": 0.0}),
            ("UNDERPERFORMING",   {"red": 1.0,   "green": 0.639, "blue": 0.353}, {"red": 0.525, "green": 0.161, "blue": 0.0}),
            ("POOR",              {"red": 0.914, "green": 0.263, "blue": 0.208}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("INEFFICIENT CAT 1", {"red": 0.545, "green": 0.0,   "blue": 0.0},   {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("INEFFICIENT CAT 2", {"red": 0.698, "green": 0.133, "blue": 0.133}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("INEFFICIENT CAT 3", {"red": 0.804, "green": 0.361, "blue": 0.361}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("OPPORTUNITY",       {"red": 0.118, "green": 0.533, "blue": 0.898}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("FULL IMMATURE",     {"red": 0.800, "green": 0.824, "blue": 0.855}, {"red": 0.267, "green": 0.306, "blue": 0.365}),
            ("PARTIAL IMMATURE",  {"red": 0.878, "green": 0.890, "blue": 0.914}, {"red": 0.400, "green": 0.420, "blue": 0.470}),
            ("NO DATA",           {"red": 0.910, "green": 0.910, "blue": 0.910}, {"red": 0.4, "green": 0.4, "blue": 0.4}),
        ])],
    ]}
    sh.batch_update(body)
    print(f"  Campaign Level tab: {len(rows)} campaigns written.")


def compute_adxdate_scores(rows: list) -> None:
    """Grade each Ad × Date tier row independently. Stores '_grade' on each row in-place.

    Each period's metrics are ranked within that period's distribution across all ads,
    so d0-d2 trial costs are compared against d0-d2 trial costs only, etc.
    Agg rows ('d3-d14 (Agg)') are skipped.
    """
    PERIOD_WEIGHTS = {
        "d0-d2":   None,  # handled separately using pred_d6_roas
        "d3-d5":   [("mandate_roas", 0.30, "higher"), ("trial_cost", 0.35, "lower"),
                    ("d0_cac", 0.25, "lower"), ("non_mandate_roas", 0.10, "higher")],
        "d6-d8":   [("mandate_roas", 0.35, "higher"), ("trial_cost", 0.25, "lower"),
                    ("d0_cac", 0.20, "lower"), ("non_mandate_roas", 0.10, "higher"),
                    ("d6_cac", 0.10, "lower")],
        "d9-d10":  [("mandate_roas", 0.35, "higher"), ("trial_cost", 0.25, "lower"),
                    ("d0_cac", 0.20, "lower"), ("non_mandate_roas", 0.10, "higher"),
                    ("d6_cac", 0.10, "lower")],
        "d11-d13": [("mandate_roas", 0.35, "higher"), ("trial_cost", 0.25, "lower"),
                    ("d0_cac", 0.20, "lower"), ("non_mandate_roas", 0.10, "higher"),
                    ("d6_cac", 0.10, "lower")],
        "d14+":    [("mandate_roas", 0.35, "higher"), ("trial_cost", 0.25, "lower"),
                    ("d0_cac", 0.20, "lower"), ("non_mandate_roas", 0.10, "higher"),
                    ("d6_cac", 0.10, "lower")],
    }

    def _pct_rank(val: float, sorted_vals: list[float]) -> float:
        if not sorted_vals:
            return 0.0
        return sum(1 for v in sorted_vals if v < val) / len(sorted_vals)

    def _derive(r: dict) -> dict:
        sp = float(r.get("spend") or 0)
        trials = float(r.get("d0_trials") or 0)
        conv   = float(r.get("d0_conv") or 0)
        d6_rev = float(r.get("d6_revenue") or 0)
        mdt_rev = float(r.get("d6_mandate_revenue") or 0)
        nmdt_rev = float(r.get("d6_non_mandate_revenue") or 0)
        d6_conv  = float(r.get("d6_conv") or 0)
        return {
            "mandate_roas":     (mdt_rev  / sp)    if sp > 0 else None,
            "non_mandate_roas": (nmdt_rev / sp)    if sp > 0 else None,
            "trial_cost":       (sp / trials)      if trials > 0 else None,
            "d0_cac":           (sp / conv)        if conv > 0 else None,
            "d6_cac":           (sp / d6_conv)     if d6_conv > 0 else None,
        }

    # Build d14+ total d6_roas distribution as benchmark for d0-d2 pred_d6_roas comparison
    d14p_total_roas = sorted(
        float(r.get("d6_roas") or 0)
        for r in rows
        if r.get("period") == "d14+" and float(r.get("d6_roas") or 0) > 0
    )

    # Group rows by period; compute derived metrics and build distributions
    by_period: dict[str, list[dict]] = {}
    for r in rows:
        p = r.get("period", "")
        if p == "d3-d14 (Agg)":
            r["_grade"] = ""
            continue
        by_period.setdefault(p, []).append(r)
        r["_derived"] = _derive(r)

    for period, period_rows in by_period.items():
        weights = PERIOD_WEIGHTS.get(period)

        if period == "d0-d2":
            # d0-d2: grade purely on pred_d6_roas vs mature (d14+) total ROAS distribution
            for r in period_rows:
                pred = r.get("pred_d6_roas")
                if pred is None or not d14p_total_roas:
                    r["_grade"] = ""
                    continue
                pr = _pct_rank(float(pred), d14p_total_roas)
                base = _score_to_grade(pr * 100)
                r["_grade"] = f"EARLY SIGNAL: {base}"
            continue

        if weights is None:
            for r in period_rows:
                r["_grade"] = ""
            continue

        # Build per-metric distribution for this period
        metric_keys = [k for k, _, _ in weights]
        dists: dict[str, list[float]] = {
            k: sorted(float(r["_derived"][k]) for r in period_rows
                      if r.get("_derived", {}).get(k) is not None)
            for k in metric_keys
        }

        for r in period_rows:
            sp = float(r.get("spend") or 0)
            if sp <= 0:
                r["_grade"] = ""
                continue
            d = r.get("_derived", {})
            mscores: list[tuple[float, float]] = []
            for key, w, direction in weights:
                val = d.get(key)
                if val is not None:
                    pr = _pct_rank(float(val), dists[key])
                    s  = (1.0 - pr) if direction == "lower" else pr
                elif sp > 0:
                    s = 0.0
                else:
                    continue
                mscores.append((s, w))
            if not mscores:
                r["_grade"] = ""
                continue
            total_w = sum(w for _, w in mscores)
            score = sum(s * w for s, w in mscores) / total_w * 100
            r["_grade"] = _score_to_grade(score)

    # Clean up temp key
    for r in rows:
        r.pop("_derived", None)


def write_ad_x_date_sheet(sh, rows: list):
    """Write 'Ad × Date — Meta' tab — 8 sub-rows per ad (6 day-age windows + d3-d14 agg)."""
    try:
        old_ws = sh.worksheet("Ad × Date — Meta")
        sh.del_worksheet(old_ws)
    except Exception:
        pass
    compute_adxdate_scores(rows)

    n_ads = len(rows) // 8 if rows else 0
    ws = sh.add_worksheet("Ad × Date — Meta", rows=max(len(rows) + 50, 1000), cols=21)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    headers = [
        "Campaign", "Adset", "Ad Name", "Status", "Period",
        "Spend ₹", "Signups", "D0 Conv", "D0 CAC ₹",
        "D0 Trials", "D0 Trial Cost ₹",
        "D6 CAC ₹", "D6 ROAS", "D6 Mandate ROAS", "D6 Non-Mdt ROAS",
        "Pred D6 ROAS", "Grade",
    ]
    N_COLS = len(headers)
    IDX_STATUS = headers.index("Status")
    IDX_PERIOD = headers.index("Period")
    IDX_DATA_START = headers.index("Spend ₹")

    GST = 1.18
    def _i(v):  return "" if v is None else int(float(v))
    def _sp(v): return "" if v is None else _inr_str(float(v) * GST, 0)
    def _ro(v): return "" if v is None else round(float(v) / GST, 3)

    data_rows = [headers]
    for r in rows:
        data_rows.append([
            r["campaign_name"],
            r["adset_name"],
            r["ad_name"],
            r["status"],
            r["period"],
            _sp(r["spend"]),
            _i(r["signups"]),
            _i(r["d0_conv"]),
            _sp(r["d0_cac"]),
            _i(r["d0_trials"]),
            _sp(r["d0_trial_cost"]),
            _sp(r["d6_cac"]),
            _ro(r["d6_roas"]),
            _ro(r["d6_mandate_roas"]),
            _ro(r["d6_non_mandate_roas"]),
            _ro(r["pred_d6_roas"]),
            r.get("_grade", ""),
        ])

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(rows)} rows ({n_ads} ads × 8 rows)"])

    ws.update(values=data_rows, range_name="A1")

    DATA_END_ROW = 1 + len(rows)

    # Period colour palette
    PERIOD_COLORS = [
        # (label, background, text)
        ("d0-d2",       {"red": 0.992, "green": 0.906, "blue": 0.776}, {"red": 0.502, "green": 0.314, "blue": 0.063}),
        ("d3-d5",       {"red": 0.878, "green": 0.890, "blue": 0.914}, {"red": 0.267, "green": 0.298, "blue": 0.388}),
        ("d6-d8",       {"red": 0.878, "green": 0.890, "blue": 0.914}, {"red": 0.267, "green": 0.298, "blue": 0.388}),
        ("d9-d10",      {"red": 0.878, "green": 0.890, "blue": 0.914}, {"red": 0.267, "green": 0.298, "blue": 0.388}),
        ("d11-d13",     {"red": 0.878, "green": 0.890, "blue": 0.914}, {"red": 0.267, "green": 0.298, "blue": 0.388}),
        ("d14+",        {"red": 0.847, "green": 0.918, "blue": 0.827}, {"red": 0.067, "green": 0.392, "blue": 0.176}),
        ("d3-d14 (Agg)",{"red": 0.925, "green": 0.925, "blue": 0.925}, {"red": 0.2,   "green": 0.2,   "blue": 0.2}),
    ]

    period_format_rules = [
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": DATA_END_ROW,
                        "startColumnIndex": IDX_PERIOD, "endColumnIndex": IDX_PERIOD + 1}],
            "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                "format": {"backgroundColor": bg, "textFormat": {"bold": True, "foregroundColor": fg}},
            },
        }, "index": idx}} for idx, (label, bg, fg) in enumerate(PERIOD_COLORS)
    ]

    # Bold entire row when period = "d3-d14 (Agg)" using CUSTOM_FORMULA
    agg_col_letter = chr(ord("A") + IDX_PERIOD)  # e.g. "E"
    agg_bold_rule = {"addConditionalFormatRule": {"rule": {
        "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": DATA_END_ROW,
                    "startColumnIndex": 0, "endColumnIndex": N_COLS}],
        "booleanRule": {
            "condition": {"type": "CUSTOM_FORMULA",
                          "values": [{"userEnteredValue": f'=${agg_col_letter}2="d3-d14 (Agg)"'}]},
            "format": {"textFormat": {"bold": True}},
        },
    }, "index": len(PERIOD_COLORS)}}

    body = {"requests": [
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                                   "startColumnIndex": 0, "endColumnIndex": N_COLS},
                        "cell": {"userEnteredFormat": {
                            "backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                            "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat"}},
        {"updateSheetProperties": {"properties": {"sheetId": ws.id,
                                    "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 4}},
                                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 0, "endIndex": 1},
                                       "properties": {"pixelSize": 240}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 1, "endIndex": 2},
                                       "properties": {"pixelSize": 220}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 2, "endIndex": 3},
                                       "properties": {"pixelSize": 280}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 3, "endIndex": 4},
                                       "properties": {"pixelSize": 110}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 4, "endIndex": 5},
                                       "properties": {"pixelSize": 80}, "fields": "pixelSize"}},
        *[{"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                          "startIndex": i, "endIndex": i+1},
                                          "properties": {"pixelSize": 95}, "fields": "pixelSize"}}
          for i in range(IDX_DATA_START, N_COLS)],
        # Status conditional formatting
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": DATA_END_ROW,
                        "startColumnIndex": IDX_STATUS, "endColumnIndex": IDX_STATUS + 1}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                             "format": {"backgroundColor": bg, "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx + len(PERIOD_COLORS) + 1}} for idx, (label, bg, fg) in enumerate([
            ("ACTIVE",       {"red": 0.714, "green": 0.882, "blue": 0.722}, {"red": 0.0,   "green": 0.239, "blue": 0.086}),
            ("PAUSED",       {"red": 1.0,   "green": 0.898, "blue": 0.600}, {"red": 0.4,   "green": 0.267, "blue": 0.0}),
            ("WITH_ISSUES",  {"red": 0.914, "green": 0.263, "blue": 0.208}, {"red": 1.0,   "green": 1.0,   "blue": 1.0}),
            ("ADSET_PAUSED", {"red": 0.800, "green": 0.824, "blue": 0.855}, {"red": 0.267, "green": 0.306, "blue": 0.365}),
            ("ARCHIVED",     {"red": 0.851, "green": 0.851, "blue": 0.851}, {"red": 0.4,   "green": 0.4,   "blue": 0.4}),
        ])],
        *period_format_rules,
        agg_bold_rule,
        # Grade column — wider to fit "EARLY SIGNAL: TOP PERFORMER"
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                        "startIndex": N_COLS - 1, "endIndex": N_COLS},
                                        "properties": {"pixelSize": 175}, "fields": "pixelSize"}},
        # Grade conditional formatting
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": DATA_END_ROW,
                        "startColumnIndex": N_COLS - 1, "endColumnIndex": N_COLS}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                             "format": {"backgroundColor": bg, "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx + len(PERIOD_COLORS) + 6}} for idx, (label, bg, fg) in enumerate([
            ("TOP PERFORMER",   {"red": 0.420, "green": 0.659, "blue": 0.302}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("GOOD",            {"red": 0.714, "green": 0.882, "blue": 0.722}, {"red": 0.0, "green": 0.239, "blue": 0.086}),
            ("AVERAGE",         {"red": 1.0,   "green": 0.949, "blue": 0.800}, {"red": 0.4, "green": 0.310, "blue": 0.043}),
            ("UNDERPERFORMING", {"red": 0.980, "green": 0.737, "blue": 0.624}, {"red": 0.6, "green": 0.165, "blue": 0.067}),
            ("POOR",            {"red": 0.918, "green": 0.263, "blue": 0.208}, {"red": 1.0, "green": 1.0,   "blue": 1.0}),
        ])],
        # EARLY SIGNAL grades — use a muted teal to distinguish from confirmed grades
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": DATA_END_ROW,
                        "startColumnIndex": N_COLS - 1, "endColumnIndex": N_COLS}],
            "booleanRule": {"condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "EARLY SIGNAL"}]},
                             "format": {"backgroundColor": {"red": 0.800, "green": 0.902, "blue": 0.918},
                                        "textFormat": {"italic": True, "foregroundColor": {"red": 0.063, "green": 0.353, "blue": 0.424}}}},
        }, "index": len(PERIOD_COLORS) + 11}}],
        {"setBasicFilter": {"filter": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": DATA_END_ROW + 1,
                       "startColumnIndex": 0, "endColumnIndex": N_COLS},
        }}},
        # Currency / ROAS number formats
        *_auto_format_requests(ws.id, headers, 1, DATA_END_ROW + 1),
    ]}
    sh.batch_update(body)
    print(f"  Ad × Date tab: {len(rows)} rows written ({n_ads} ads × 8 rows).")


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


def _is_ios_or_retarget(row: dict) -> bool:
    camp = (row.get('campaign_name') or '').lower()
    adset = (row.get('adset_name') or '').lower()
    combined = camp + ' ' + adset
    if 'ios' in combined:
        return True
    return any(kw in combined for kw in ['retarget', 'remarketing', 'remarket', 'retgt', 'rtgt', 'rtrgt', 'bot', 'bof'])


def _is_android(row: dict) -> bool:
    camp = (row.get('campaign_name') or '').lower()
    return 'android' in camp


def write_inefficient_sheet(sh, rows: list):
    """Write 'Action Required' — lean single-row-per-ad view.

    Filters: Android-only, currently ACTIVE, non-iOS/retarget, grade in
    INEFFICIENT CAT 1/2/3 or POOR. Sorted by spend desc.

    Layout is deliberately compact: identity (3) + category/action (2) +
    overall numbers (6) + recent signal (3) = 14 cols. The full 4-period
    matrix lives in the Ad Level tab.
    """
    TARGET_GRADES = {'INEFFICIENT CAT 1', 'INEFFICIENT CAT 2', 'INEFFICIENT CAT 3', 'POOR'}
    filtered = [
        r for r in rows
        if r.get('_grade') in TARGET_GRADES
        and _is_android(r)
        and not _is_ios_or_retarget(r)
        and (r.get('status') or '').upper() == 'ACTIVE'
    ]
    filtered.sort(key=lambda r: -(float(r.get('spend') or 0)))

    try:
        old_ws = sh.worksheet("Action Required")
        sh.del_worksheet(old_ws)
    except Exception:
        pass

    headers = [
        "Campaign", "Adset", "Ad",
        "Category", "Action",
        "Spend ₹", "D0 Trials", "D0 Trial Cost ₹", "D0 CAC ₹", "D6 CAC ₹",
        "D6 Mandate ROAS", "D6 Non-Mdt ROAS",
        "Recent Grade", "Recent D6 ROAS", "Pred D6 ROAS",
    ]
    NUM_COLS = len(headers)
    IDX_CATEGORY      = headers.index("Category")
    IDX_ACTION        = headers.index("Action")
    IDX_RECENT_GRADE  = headers.index("Recent Grade")

    ws = sh.add_worksheet("Action Required", rows=max(len(filtered) + 50, 500), cols=NUM_COLS)
    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    GST = 1.18
    def _i(v):  return "" if v is None else int(float(v))
    def _ic(v): return "" if v is None else _inr_str(float(v) * GST, 0)  # ₹ GST-inclusive
    def _ro(v): return "" if v is None else round(float(v) / GST, 3)     # ROAS ÷ GST

    data_rows = [headers]
    for r in filtered:
        # Pull the per-grade suggestion string the score logic already built
        action = r.get("_suggestion") or ""
        # Trim noisy "Spend ₹..." chunk from the suggestion (we show Spend in its own column)
        if " | Spend " in action:
            head, _, rest = action.partition(" | Spend ")
            tail = rest.partition(" | ")[2]
            action = f"{head} | {tail}" if tail else head
        data_rows.append([
            r.get("campaign_name") or "",
            r.get("adset_name") or "",
            r.get("ad_name") or "",
            r.get("_grade", ""),
            action,
            _ic(r.get("spend")),
            _i(r.get("d0_trials")),
            _ic(r.get("d0_trial_cost")),
            _ic(r.get("d0_cac")),
            _ic(r.get("d6_cac")),
            _ro(r.get("d6_mandate_roas")),
            _ro(r.get("d6_non_mandate_roas")),
            r.get("_recent_grade") or "",
            _ro(r.get("recent_d6_roas")),
            _ro(r.get("_recent_pred_d6_roas")),
        ])

    # Summary row
    total_spend = sum(float(r.get('spend') or 0) for r in filtered)
    data_rows.append([])
    data_rows.append([
        f"Total: {len(filtered)} ads", "", "", "", "",
        _inr_str(total_spend * GST, 0),
    ])
    data_rows.append([f"Last updated: {now_str}"])

    ws.update(values=data_rows, range_name="A1")

    GRADE_COLOURS = [
        ("INEFFICIENT CAT 1", {"red": 0.545, "green": 0.0,   "blue": 0.0},   {"red": 1.0, "green": 1.0, "blue": 1.0}),
        ("INEFFICIENT CAT 2", {"red": 0.698, "green": 0.133, "blue": 0.133}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
        ("INEFFICIENT CAT 3", {"red": 0.804, "green": 0.361, "blue": 0.361}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
        ("POOR",              {"red": 0.914, "green": 0.263, "blue": 0.208}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
        ("UNDERPERFORMING",   {"red": 0.984, "green": 0.737, "blue": 0.016}, {"red": 0.2, "green": 0.2, "blue": 0.2}),
        ("AVERAGE",           {"red": 1.0,   "green": 0.843, "blue": 0.0},   {"red": 0.2, "green": 0.2, "blue": 0.2}),
        ("GOOD",              {"red": 0.565, "green": 0.792, "blue": 0.376}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
        ("TOP PERFORMER",     {"red": 0.102, "green": 0.478, "blue": 0.224}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
    ]

    # Conditional format rules for Category and Recent Grade columns
    cat_col_rules = []
    rule_idx = 1
    for col_idx in (IDX_CATEGORY, IDX_RECENT_GRADE):
        for label, bg, fg in GRADE_COLOURS:
            cat_col_rules.append({"addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                                "endRowIndex": len(data_rows),
                                "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_CONTAINS",
                                      "values": [{"userEnteredValue": label}]},
                        "format": {"backgroundColor": bg,
                                   "textFormat": {"bold": True, "foregroundColor": fg}},
                    },
                },
                "index": rule_idx,
            }})
            rule_idx += 1

    body = {"requests": [
        # Header row (single)
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": NUM_COLS},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                               "fontSize": 10},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "wrapStrategy": "WRAP",
            }},
            "fields": "userEnteredFormat",
        }},
        # Header row height (wraps "D6 Non-Mdt ROAS" etc.)
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 60}, "fields": "pixelSize",
        }},
        # Freeze header + first 3 identity columns
        {"updateSheetProperties": {
            "properties": {"sheetId": ws.id,
                           "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 3}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }},
        # Column widths
        {"updateDimensionProperties": {  # Campaign
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 240}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Adset
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 200}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Ad
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 2, "endIndex": 3},
            "properties": {"pixelSize": 260}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Category
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": IDX_CATEGORY, "endIndex": IDX_CATEGORY + 1},
            "properties": {"pixelSize": 155}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Action — wide
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": IDX_ACTION, "endIndex": IDX_ACTION + 1},
            "properties": {"pixelSize": 420}, "fields": "pixelSize",
        }},
        # Numeric columns (spend through pred roas) — uniform width 110
        *[{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": 110}, "fields": "pixelSize",
        }} for i in range(IDX_ACTION + 1, NUM_COLS) if i != IDX_RECENT_GRADE],
        {"updateDimensionProperties": {  # Recent Grade — wider for label
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": IDX_RECENT_GRADE, "endIndex": IDX_RECENT_GRADE + 1},
            "properties": {"pixelSize": 155}, "fields": "pixelSize",
        }},
        # Wrap Action column text
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1,
                      "endRowIndex": len(data_rows),
                      "startColumnIndex": IDX_ACTION, "endColumnIndex": IDX_ACTION + 1},
            "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP",
                                            "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment",
        }},
        # Center identity-grade cells vertically
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1,
                      "endRowIndex": len(data_rows),
                      "startColumnIndex": IDX_CATEGORY, "endColumnIndex": IDX_CATEGORY + 1},
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER",
                                            "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
        }},
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1,
                      "endRowIndex": len(data_rows),
                      "startColumnIndex": IDX_RECENT_GRADE, "endColumnIndex": IDX_RECENT_GRADE + 1},
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER",
                                            "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
        }},
        # Alternating row shading
        {"addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                            "endRowIndex": len(data_rows),
                            "startColumnIndex": 0, "endColumnIndex": NUM_COLS}],
                "booleanRule": {
                    "condition": {"type": "CUSTOM_FORMULA",
                                  "values": [{"userEnteredValue": "=ISEVEN(ROW())"}]},
                    "format": {"backgroundColor": {"red": 0.957, "green": 0.965, "blue": 0.976}},
                },
            },
            "index": 0,
        }},
        *cat_col_rules,
        # ROAS columns to percent
        *_auto_format_requests(ws.id, headers, 1, len(data_rows)),
    ]}
    sh.batch_update(body)
    print(f"  Action Required tab: {len(filtered)} ads written (₹{_inr_indian(int(total_spend * GST))} spend).")


def write_executive_summary_sheet(sh, campaign_rows: list, ad_rows: list):
    """Write 'Executive Summary' tab — per-campaign ad-grade breakdown + decision.

    One row per campaign: total/active ads, ads-by-grade counts (TOP/GOOD/AVG/UNDER/POOR/IMMATURE),
    campaign-level grade, recent grade, scaling decision, suggested daily budget, reasoning.
    Sorted by total campaign spend desc. Totals row at the bottom.
    """
    tab_name = "Executive Summary"
    try:
        old_ws = sh.worksheet(tab_name)
        sh.del_worksheet(old_ws)
    except Exception:
        pass
    ws = sh.add_worksheet(tab_name, rows=max(len(campaign_rows) + 50, 200), cols=20)

    from collections import defaultdict
    GST = 1.18

    # Group ads by campaign_name → grade counts. iOS/Retarget already excluded from ad_rows
    # because build_ad_data uses the same filter in _is_ios_or_retarget_name.
    by_camp = defaultdict(lambda: {"top":0,"good":0,"avg":0,"under":0,"poor":0,"imm":0,
                                   "ineff":0,"opp":0,"recovering":0,"nodata":0,
                                   "total":0,"active":0,"spend":0.0})
    for ad in ad_rows:
        cn = ad.get("campaign_name") or ""
        if not cn or _is_ios_or_retarget_name(cn): continue
        g = (ad.get("_grade") or "NO DATA").upper()
        b = by_camp[cn]
        b["total"] += 1
        if (ad.get("status") or "").upper() == "ACTIVE":
            b["active"] += 1
        b["spend"] += float(ad.get("spend") or 0)
        if   g == "TOP PERFORMER":     b["top"]   += 1
        elif g == "GOOD":              b["good"]  += 1
        elif g == "AVERAGE":           b["avg"]   += 1
        elif g == "UNDERPERFORMING":   b["under"] += 1
        elif g == "POOR":              b["poor"]  += 1
        elif g.startswith("INEFFICIENT"): b["ineff"] += 1
        elif g == "OPPORTUNITY":       b["opp"]   += 1
        elif g == "RECOVERING":        b["recovering"] += 1
        elif g in ("FULL IMMATURE","PARTIAL IMMATURE"): b["imm"] += 1
        else:                          b["nodata"] += 1

    # Map campaign-level metadata for decision + grade columns
    camp_meta = {c.get("campaign_name") or "": c for c in campaign_rows}

    # Build summary rows sorted by spend desc; only include campaigns we have ads for.
    summary = []
    for cn, b in by_camp.items():
        c = camp_meta.get(cn) or {}
        summary.append({
            "campaign":       cn,
            "total":          b["total"],
            "active":         b["active"],
            "spend":          b["spend"],
            "camp_grade":     c.get("_grade") or "",
            "recent_grade":   c.get("_recent_grade") or "",
            "top":            b["top"],
            "good":           b["good"],
            "avg":            b["avg"],
            "under":          b["under"],
            "poor":           b["poor"],
            "ineff":          b["ineff"],
            "imm":            b["imm"],
            "action":         c.get("_decision_action") or "",
            "budget_change":  c.get("_decision_budget_change_pct"),
            "suggested":      c.get("_decision_suggested_daily") or 0,
            "reasoning":      c.get("_decision_reasoning") or "",
        })
    summary.sort(key=lambda x: -x["spend"])

    headers = [
        "Campaign", "Total Ads", "Active Ads", "Spend ₹",
        "Campaign Grade", "Recent Grade",
        "TOP", "GOOD", "AVG", "UNDER", "POOR", "INEFF", "IMMATURE",
        "Action", "Budget Δ%", "Suggested Daily ₹", "Reasoning",
    ]

    data_rows = [headers]
    for r in summary:
        data_rows.append([
            r["campaign"],
            r["total"],
            r["active"],
            _inr_str(r["spend"] * GST, 0),
            r["camp_grade"],
            r["recent_grade"],
            r["top"], r["good"], r["avg"], r["under"], r["poor"], r["ineff"], r["imm"],
            r["action"],
            (f"{int(r['budget_change']):+d}%" if r["budget_change"] is not None else ""),
            _inr_str((r["suggested"] or 0) * GST, 0),
            r["reasoning"],
        ])

    # Totals row
    tot = {
        "total":  sum(r["total"]  for r in summary),
        "active": sum(r["active"] for r in summary),
        "spend":  sum(r["spend"]  for r in summary),
        "top":    sum(r["top"]    for r in summary),
        "good":   sum(r["good"]   for r in summary),
        "avg":    sum(r["avg"]    for r in summary),
        "under":  sum(r["under"]  for r in summary),
        "poor":   sum(r["poor"]   for r in summary),
        "ineff":  sum(r["ineff"]  for r in summary),
        "imm":    sum(r["imm"]    for r in summary),
    }
    data_rows.append([])
    data_rows.append([
        f"TOTAL ({len(summary)} campaigns)",
        tot["total"], tot["active"],
        _inr_str(tot["spend"] * GST, 0),
        "", "",
        tot["top"], tot["good"], tot["avg"], tot["under"], tot["poor"], tot["ineff"], tot["imm"],
        "", "", "", "",
    ])

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}"])

    ws.update(values=data_rows, range_name="A1")

    # Formatting
    HDR_BG = {"red": 0.102, "green": 0.204, "blue": 0.376}
    body = {"requests": [
        # Header row
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                                   "startColumnIndex": 0, "endColumnIndex": len(headers)},
                        "cell": {"userEnteredFormat": {"backgroundColor": HDR_BG,
                                  "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                                  "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat"}},
        # Freeze
        {"updateSheetProperties": {"properties": {"sheetId": ws.id,
                                    "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1}},
                                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
        # Column widths: Campaign wide, count cols narrow, Reasoning very wide
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 0, "endIndex": 1},
                                       "properties": {"pixelSize": 360}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 1, "endIndex": 4},
                                       "properties": {"pixelSize": 90}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 4, "endIndex": 6},
                                       "properties": {"pixelSize": 150}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 6, "endIndex": 13},
                                       "properties": {"pixelSize": 65}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 13, "endIndex": 14},
                                       "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 14, "endIndex": 15},
                                       "properties": {"pixelSize": 90}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 15, "endIndex": 16},
                                       "properties": {"pixelSize": 140}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 16, "endIndex": 17},
                                       "properties": {"pixelSize": 520}, "fields": "pixelSize"}},
        # Wrap reasoning column
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(data_rows),
                                   "startColumnIndex": 16, "endColumnIndex": 17},
                        "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"}},
                        "fields": "userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment"}},
        # Currency cells are pre-formatted Indian-style strings (see _inr_str above) —
        # no number-format needed.
        # Alternating shading
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(summary) + 1,
                        "startColumnIndex": 0, "endColumnIndex": len(headers)}],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA",
                              "values": [{"userEnteredValue": "=ISEVEN(ROW())"}]},
                              "format": {"backgroundColor": {"red": 0.957, "green": 0.965, "blue": 0.976}}},
        }, "index": 0}},
        # Color the grade columns
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(summary) + 1,
                        "startColumnIndex": 4, "endColumnIndex": 6}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                             "format": {"backgroundColor": bg, "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx + 1}} for idx, (label, bg, fg) in enumerate([
            ("TOP PERFORMER",     {"red": 0.137, "green": 0.612, "blue": 0.290}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("GOOD",              {"red": 0.714, "green": 0.882, "blue": 0.722}, {"red": 0.0, "green": 0.239, "blue": 0.086}),
            ("AVERAGE",           {"red": 1.0,   "green": 0.898, "blue": 0.600}, {"red": 0.4, "green": 0.267, "blue": 0.0}),
            ("UNDERPERFORMING",   {"red": 1.0,   "green": 0.639, "blue": 0.353}, {"red": 0.525, "green": 0.161, "blue": 0.0}),
            ("POOR",              {"red": 0.914, "green": 0.263, "blue": 0.208}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
        ])],
        # Color the Action column
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(summary) + 1,
                        "startColumnIndex": 13, "endColumnIndex": 14}],
            "booleanRule": {"condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": substr}]},
                             "format": {"backgroundColor": bg, "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx + 10}} for idx, (substr, bg, fg) in enumerate([
            ("SCALE",   {"red": 0.137, "green": 0.612, "blue": 0.290}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("PAUSE",   {"red": 0.545, "green": 0.0,   "blue": 0.0},   {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("CUT",     {"red": 0.914, "green": 0.263, "blue": 0.208}, {"red": 1.0, "green": 1.0, "blue": 1.0}),
            ("HOLD",    {"red": 0.878, "green": 0.890, "blue": 0.914}, {"red": 0.4, "green": 0.4, "blue": 0.4}),
        ])],
        # Totals row bold
        {"repeatCell": {"range": {"sheetId": ws.id,
                                   "startRowIndex": len(summary) + 2, "endRowIndex": len(summary) + 3,
                                   "startColumnIndex": 0, "endColumnIndex": len(headers)},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.851, "green": 0.882, "blue": 0.949},
                                  "textFormat": {"bold": True}}},
                        "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.bold"}},
    ]}
    sh.batch_update(body)
    print(f"  Executive Summary tab: {len(summary)} campaigns written.")


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
WITH attr AS (
    SELECT
        ae.meta_creative_id                                                       AS ad_id,
        ae.install_date                                                           AS date,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                            THEN ae.user_id END)                                  AS signups,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                            THEN ae.user_id END)                                  AS d0_trials,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                            THEN ae.user_id END)                                  AS d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                            THEN ae.user_id END)                                  AS d6_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                 THEN ae.revenue_inr ELSE 0 END)                                  AS d0_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                 THEN ae.revenue_inr ELSE 0 END)                                  AS d6_revenue
    FROM attribution_events ae
    WHERE ae.install_date >= %(mtd)s
      AND ae.meta_creative_id IS NOT NULL
    GROUP BY ae.meta_creative_id, ae.install_date
)
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
         THEN ROUND(i.spend::numeric / i.clicks, 1) END  AS cpc,
    COALESCE(at.signups,    0)                            AS signups,
    COALESCE(at.d0_trials,  0)                            AS d0_trials,
    COALESCE(at.d0_conv,    0)                            AS d0_conv,
    COALESCE(at.d6_conv,    0)                            AS d6_conv,
    COALESCE(at.d0_revenue, 0)                            AS d0_revenue,
    COALESCE(at.d6_revenue, 0)                            AS d6_revenue,
    CASE WHEN at.signups   > 0
         THEN ROUND(i.spend::numeric / at.signups,   0) END                AS cac,
    CASE WHEN at.d0_trials > 0
         THEN ROUND(i.spend::numeric / at.d0_trials, 0) END                AS d0_trial_cost,
    CASE WHEN at.d0_conv   > 0
         THEN ROUND(i.spend::numeric / at.d0_conv,   0) END                AS d0_cac,
    CASE WHEN at.signups   > 0
         THEN ROUND(at.d0_conv::numeric * 100 / at.signups, 2) END         AS d0_conv_pct,
    CASE WHEN i.spend > 0
         THEN ROUND(at.d0_revenue::numeric / i.spend, 3) END               AS d0_roas,
    CASE WHEN i.spend > 0
         THEN ROUND(at.d6_revenue::numeric / i.spend, 3) END               AS d6_roas
FROM insights_daily i
LEFT JOIN campaigns c ON c.id = i.campaign_id
LEFT JOIN adsets s    ON s.id = i.adset_id
LEFT JOIN attr at     ON at.ad_id = i.ad_id AND at.date = i.date
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
    # Delete-then-recreate to drop accumulated conditional format rules
    # (ws.clear() retains them and slows API calls after many refreshes).
    try:
        old_ws = sh.worksheet("Day Level — Ads")
        sh.del_worksheet(old_ws)
    except Exception:
        pass
    ws = sh.add_worksheet("Day Level — Ads", rows=max(len(rows) + 50, 6000), cols=30)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    headers = [
        "Date", "Ad ID", "Ad Name", "Campaign", "Adset",
        "Spend ₹", "Impressions", "Clicks", "CTR %", "CPM ₹", "CPC ₹",
        "Signups", "D0 Trials", "D0 Conv", "D6 Conv",
        "D0 Revenue ₹", "D6 Revenue ₹",
        "CAC ₹", "D0 Trial Cost ₹", "D0 CAC ₹", "D0 Conv %",
        "D0 ROAS", "D6 ROAS",
        "Grade",
    ]
    IDX_GRADE_DL = headers.index("Grade")

    GST = 1.18
    def _sp(v): return "" if v is None else _inr_str(float(v) * GST, 0)
    def _pm(v): return "" if v is None else _inr_str(float(v) * GST, 1)
    def _ro(v): return "" if v is None else round(float(v) / GST, 3)
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
            _sp(r["spend"]),
            _i(r["impressions"]),
            _i(r["clicks"]),
            _f(r["ctr"], 3),
            _pm(r["cpm"]),
            _pm(r["cpc"]),
            _i(r.get("signups")),
            _i(r.get("d0_trials")),
            _i(r.get("d0_conv")),
            _i(r.get("d6_conv")),
            _sp(r.get("d0_revenue")),
            _sp(r.get("d6_revenue")),
            _sp(r.get("cac")),
            _sp(r.get("d0_trial_cost")),
            _sp(r.get("d0_cac")),
            _f(r.get("d0_conv_pct"), 2),
            _ro(r.get("d0_roas")),
            _ro(r.get("d6_roas")),
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
        # Currency / ROAS number formats
        *_auto_format_requests(ws.id, headers, 1, len(data_rows)),
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


# ── Creative Pipeline tab ────────────────────────────────────────────────────
TEST_CAMPAIGN_NAME = "Test4-Campaign_FB_MOF_Manual-App_Android_Pro-Sub_Pan-India_200326"


def _extract_creative_base(ad_name: str) -> tuple[str | None, int | None]:
    """Strip the trailing _DDMMYY date suffix from an ad name.

    Returns (base_creative_name, go_live_year) or (None, None) if it doesn't match.
    Example: 'FB_MOF_Static_Lambu-Chat_V0_280426' → ('FB_MOF_Static_Lambu-Chat_V0', 2026)
    """
    import re
    m = re.match(r"^(.+)_(\d{2})(\d{2})(\d{2})$", ad_name or "")
    if not m:
        return None, None
    return m.group(1), 2000 + int(m.group(4))


def build_creative_pipeline_data(ad_rows: list) -> tuple[list, list[str]]:
    """Build the Creative Pipeline view.

    Rows: ads in the TEST campaign passing filters (spend > ₹12k, D6 ROAS > 22%,
    go-live year >= 2026). Columns: identity + one column per active campaign
    (excluding the test campaign) with Yes/No indicating whether the same
    creative base name has been promoted to that campaign.

    Filters are applied on the DISPLAY values (post-GST spend; ROAS shown to user),
    matching the user's mental model.
    """
    GST = 1.18
    MIN_SPEND_DISPLAY = 12_000.0       # ₹12,000 (post-GST)
    MIN_D6_ROAS_DISPLAY = 0.22         # 22% (post-GST, i.e. value as shown in sheet)
    MIN_YEAR = 2026

    # Convert display thresholds to raw (pre-GST spend, raw ROAS).
    min_spend_raw   = MIN_SPEND_DISPLAY / GST
    min_d6_roas_raw = MIN_D6_ROAS_DISPLAY * GST

    # Map every base-creative-name → set of campaign names that ran it,
    # and collect the set of currently-ACTIVE campaign names (excluding the
    # test campaign so we don't get a redundant column for it).
    base_to_campaigns: dict[str, set[str]] = {}
    active_campaigns: set[str] = set()
    for r in ad_rows:
        ad_name = r.get("ad_name") or ""
        campaign_name = r.get("campaign_name") or ""
        status = (r.get("status") or "").upper()
        base, _year = _extract_creative_base(ad_name)
        if base is None:
            continue
        base_to_campaigns.setdefault(base, set()).add(campaign_name)
        if status == "ACTIVE" and campaign_name and campaign_name != TEST_CAMPAIGN_NAME:
            active_campaigns.add(campaign_name)

    # Filter to test-campaign ads that pass the spend/ROAS/year gate.
    qualifying = []
    for r in ad_rows:
        if (r.get("campaign_name") or "") != TEST_CAMPAIGN_NAME:
            continue
        ad_name = r.get("ad_name") or ""
        base, year = _extract_creative_base(ad_name)
        if base is None or year is None or year < MIN_YEAR:
            continue
        spend = float(r.get("spend") or 0)
        d6_roas = float(r.get("d6_roas") or 0)
        if spend < min_spend_raw or d6_roas < min_d6_roas_raw:
            continue
        qualifying.append({
            "ad_name":       ad_name,
            "base":          base,
            "spend":         spend,
            "d6_roas":       d6_roas,
            "status":        r.get("status") or "",
            "adset_name":    r.get("adset_name") or "",
        })

    # Sort active campaign columns alphabetically for a stable view.
    active_list = sorted(active_campaigns)

    # Decorate each qualifying row with Yes/No per active campaign.
    out = []
    for tr in qualifying:
        row = dict(tr)
        promotions = base_to_campaigns.get(tr["base"], set())
        for camp in active_list:
            row[camp] = "Yes" if camp in promotions else "No"
        row["_promo_count"] = sum(1 for c in active_list if c in promotions)
        out.append(row)

    # Sort by spend desc so the most-tested creatives show first.
    out.sort(key=lambda x: -x["spend"])
    return out, active_list


def write_creative_pipeline_sheet(sh, rows: list, active_campaigns: list[str]):
    """Write the 'Creative Pipeline' tab."""
    try:
        old = sh.worksheet("Creative Pipeline")
        sh.del_worksheet(old)
    except Exception:
        pass
    n_cols = max(len(active_campaigns) + 8, 15)
    ws = sh.add_worksheet("Creative Pipeline", rows=max(len(rows) + 50, 200), cols=n_cols)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    GST = 1.18
    def _sp(v): return "" if v is None else _inr_str(float(v) * GST, 0)
    def _ro(v): return "" if v is None else round(float(v) / GST, 3)

    fixed = ["Ad Name", "Base Creative", "Adset", "Status",
             "Spend ₹", "D6 ROAS", "Promoted To"]
    headers = fixed + list(active_campaigns)
    IDX_PROMO_COUNT = headers.index("Promoted To")
    FIRST_CAMP_COL = len(fixed)

    data_rows = [headers]
    for r in rows:
        d = [
            r["ad_name"],
            r["base"],
            r.get("adset_name", ""),
            r.get("status", ""),
            _sp(r["spend"]),
            _ro(r["d6_roas"]),
            r.get("_promo_count", 0),
        ]
        for c in active_campaigns:
            d.append(r.get(c, "No"))
        data_rows.append(d)

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}",
                      f"{len(rows)} creatives passing spend > ₹12k, D6 ROAS > 22%, year ≥ 2026"])

    ws.update(values=data_rows, range_name="A1")

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
                "verticalAlignment": "MIDDLE",
                "wrapStrategy": "WRAP",
            }},
            "fields": "userEnteredFormat",
        }},
        # Freeze header + first 2 columns (Ad Name, Base Creative)
        {"updateSheetProperties": {
            "properties": {"sheetId": ws.id,
                           "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 2}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }},
        # Header row height
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 70}, "fields": "pixelSize",
        }},
        # Column widths — Ad Name, Base Creative wider
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 0, "endIndex": 2},
            "properties": {"pixelSize": 280}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 2, "endIndex": 3},
            "properties": {"pixelSize": 230}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 3, "endIndex": FIRST_CAMP_COL},
            "properties": {"pixelSize": 90}, "fields": "pixelSize",
        }},
        # Campaign columns — narrow Yes/No
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": FIRST_CAMP_COL, "endIndex": len(headers)},
            "properties": {"pixelSize": 70}, "fields": "pixelSize",
        }},
        # Center-align Yes/No cells (conditional format only supports color/bold,
        # so alignment must be a static cell format applied to the whole range).
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1,
                      "endRowIndex": len(data_rows),
                      "startColumnIndex": FIRST_CAMP_COL, "endColumnIndex": len(headers)},
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat.horizontalAlignment",
        }},
        # Conditional format: Yes = green
        {"addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                            "endRowIndex": len(data_rows),
                            "startColumnIndex": FIRST_CAMP_COL, "endColumnIndex": len(headers)}],
                "booleanRule": {
                    "condition": {"type": "TEXT_EQ",
                                  "values": [{"userEnteredValue": "Yes"}]},
                    "format": {"backgroundColor": {"red": 0.714, "green": 0.882, "blue": 0.722},
                               "textFormat": {"bold": True,
                                              "foregroundColor": {"red": 0.0, "green": 0.239, "blue": 0.086}}},
                },
            },
            "index": 0,
        }},
        # Conditional format: No = light grey, dim text
        {"addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                            "endRowIndex": len(data_rows),
                            "startColumnIndex": FIRST_CAMP_COL, "endColumnIndex": len(headers)}],
                "booleanRule": {
                    "condition": {"type": "TEXT_EQ",
                                  "values": [{"userEnteredValue": "No"}]},
                    "format": {"backgroundColor": {"red": 0.97, "green": 0.97, "blue": 0.97},
                               "textFormat": {"foregroundColor": {"red": 0.55, "green": 0.55, "blue": 0.55}}},
                },
            },
            "index": 1,
        }},
        # ROAS column as percent
        *_auto_format_requests(ws.id, headers, 1, len(data_rows)),
    ]}
    sh.batch_update(body)
    print(f"  Creative Pipeline tab: {len(rows)} creatives × {len(active_campaigns)} campaigns.")


# ── Change Log tab (Meta account audit log) ──────────────────────────────────
CHANGE_LOG_SQL = """
SELECT
    event_time,
    event_type,
    COALESCE(translated_event_type, event_type) AS pretty_event,
    actor_name,
    object_type,
    object_name,
    object_id,
    extra_data
FROM meta_change_log
WHERE account_id = ANY(%(accounts)s)
ORDER BY event_time DESC
LIMIT 5000
"""


def build_change_log_data(conn) -> list:
    from services.shared.config import settings
    return q(conn, CHANGE_LOG_SQL, {"accounts": list(settings.ad_account_id_list)})


def _format_extra(raw) -> str:
    """Render extra_data JSON as a compact 'old → new' string when possible."""
    if not raw:
        return ""
    import json
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return raw
    if not isinstance(raw, dict):
        return str(raw)
    old = raw.get("old_value") or raw.get("old")
    new = raw.get("new_value") or raw.get("new")
    if old is not None or new is not None:
        return f"{old} → {new}"
    return json.dumps(raw, separators=(",", ":"))[:200]


def write_change_log_sheet(sh, rows: list):
    """Write the 'Change Log' tab — most recent Meta audit events on top."""
    try:
        old = sh.worksheet("Change Log")
        sh.del_worksheet(old)
    except Exception:
        pass
    ws = sh.add_worksheet("Change Log", rows=max(len(rows) + 50, 200), cols=10)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    headers = ["Time", "Event", "Actor", "Object Type", "Object", "Object ID", "Details"]

    data_rows = [headers]
    for r in rows:
        et = r.get("event_time")
        time_str = et.strftime("%d %b %Y, %H:%M") if et else ""
        data_rows.append([
            time_str,
            r.get("pretty_event") or r.get("event_type") or "",
            r.get("actor_name") or "",
            r.get("object_type") or "",
            r.get("object_name") or "",
            str(r.get("object_id") or ""),
            _format_extra(r.get("extra_data")),
        ])

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(rows)} events"])

    ws.update(values=data_rows, range_name="A1")

    body = {"requests": [
        # Header
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
        # Freeze header
        {"updateSheetProperties": {
            "properties": {"sheetId": ws.id,
                           "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }},
        # Column widths
        {"updateDimensionProperties": {  # Time
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 130}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Event
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 220}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Actor
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 2, "endIndex": 3},
            "properties": {"pixelSize": 160}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Object Type
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 3, "endIndex": 4},
            "properties": {"pixelSize": 100}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Object
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 4, "endIndex": 5},
            "properties": {"pixelSize": 280}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Object ID
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 5, "endIndex": 6},
            "properties": {"pixelSize": 150}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {  # Details
            "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                      "startIndex": 6, "endIndex": 7},
            "properties": {"pixelSize": 360}, "fields": "pixelSize",
        }},
        # Alternating row shading
        {"addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 1,
                            "endRowIndex": len(data_rows),
                            "startColumnIndex": 0, "endColumnIndex": len(headers)}],
                "booleanRule": {
                    "condition": {"type": "CUSTOM_FORMULA",
                                  "values": [{"userEnteredValue": "=ISEVEN(ROW())"}]},
                    "format": {"backgroundColor": {"red": 0.957, "green": 0.965, "blue": 0.976}},
                },
            },
            "index": 0,
        }},
    ]}
    sh.batch_update(body)
    print(f"  Change Log tab: {len(rows)} events written.")


def main():
    import gspread
    from google.oauth2.service_account import Credentials

    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet-id", default=os.environ.get("DASHBOARD_SHEET_ID", ""))
    args = parser.parse_args()

    print(f"Connecting to DB...")
    conn = db_conn()

    print("Fetching ad-level data...")
    ad_rows = build_ad_data(conn)
    print(f"  {len(ad_rows)} ads found.")
    print("Fetching campaign-level data...")
    campaign_rows = build_campaign_data(conn)
    print(f"  {len(campaign_rows)} campaigns found.")
    print("Building Ad × Date pivot...")
    ad_x_date_rows = build_ad_x_date_data(ad_rows)
    print(f"  {len(ad_x_date_rows)} ad×date rows ({len(ad_x_date_rows)//8} ads × 8 rows).")
    print("Fetching day-level ad spend...")
    day_rows = build_day_level_data(conn, ad_rows)
    print(f"  {len(day_rows)} day-level rows found.")
    print("Fetching change log...")
    change_log_rows = build_change_log_data(conn)
    print(f"  {len(change_log_rows)} change-log events found.")
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
    # Remove deprecated tabs if present (no-op if already gone).
    for _stale in ("Dashboard", "Platform ROAS", "DoD", "DoD Trial Cost", "Executive Summary"):
        try:
            sh.del_worksheet(sh.worksheet(_stale))
            print(f"  Deleted stale tab: {_stale}")
        except Exception:
            pass
    write_ad_level_sheet(sh, ad_rows)
    write_campaign_level_sheet(sh, campaign_rows)
    write_ad_x_date_sheet(sh, ad_x_date_rows)
    write_day_level_sheet(sh, day_rows)
    write_inefficient_sheet(sh, ad_rows)
    pipeline_rows, pipeline_campaigns = build_creative_pipeline_data(ad_rows)
    write_creative_pipeline_sheet(sh, pipeline_rows, pipeline_campaigns)
    write_change_log_sheet(sh, change_log_rows)
    print(f"\nDone. Open sheet: {sh.url}")
    print(f"Sheet ID (save for --sheet-id): {sh.id}")


if __name__ == "__main__":
    main()
