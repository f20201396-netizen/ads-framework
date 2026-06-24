#!/usr/bin/env python3
"""Build the Test Creative Pipeline tab from Test4-campaign ad data.

Two tabs are written into the target spreadsheet:

1. "Pipeline Daily" (hidden) — one row per (ad, date) with primitive metrics.
   Insights and attribution are aggregated per install_date so they share the
   date axis. This is the source of truth for all metric formulas in the main
   tab.

2. "Test Creative Pipeline" — the user-facing tab, 58 columns per the CSV
   layout. Identity columns (S.No, Creative Name, Date - Go Live, Week, Year)
   are values. Start date (col J) and End Date (col K) are pre-filled but
   editable — change them and the metric cells recompute via SUMIFS against
   the hidden daily tab. Category and Live? auto-derive too. Manual columns
   (Refined name, Link, Next Steps, Himanshu Rating, Test Performance,
   Remarks) are left blank for human editing.

Usage:
    .venv/bin/python3 scripts/test_creative_pipeline.py
    .venv/bin/python3 scripts/test_creative_pipeline.py --sheet-id <ID>
"""

import argparse
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import gspread
import psycopg2
import psycopg2.extras
from google.oauth2.service_account import Credentials

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = "/Users/macbook/Downloads/univest-applications-d51f19bb3ffc.json"
DB_DSN = "postgresql://macbook@localhost/meta_ads"
DEFAULT_SHEET_ID = "1QX2pa5NTi-Sl0TwSxNFjov4t-wpdOjNoSE1S6dpwSKw"
MAIN_TAB = "Test Creative Pipeline"
DAILY_TAB = "Pipeline Daily"
TEST_CAMPAIGN_NAME = "Test4-Campaign_FB_MOF_Manual-App_Android_Pro-Sub_Pan-India_200326"
GST = 1.18
GST_S = "1.18"  # string form for embedding in formulas


# ── SQL ───────────────────────────────────────────────────────────────────────
# Per-ad metadata for the main-tab identity columns. Spend/conversion totals
# live in the daily tab, not here.
IDENTITY_SQL = """
WITH lifetime AS (
    SELECT
        i.ad_id,
        MAX(i.ad_name) AS ad_name,
        MIN(i.date) AS first_date,
        MAX(i.date) AS last_date
    FROM insights_daily i
    JOIN campaigns c ON c.id = i.campaign_id
    WHERE i.attribution_window = '7d_click' AND i.spend > 0
      AND c.name = %(test_campaign_name)s
    GROUP BY i.ad_id
)
SELECT
    l.ad_id,
    l.ad_name,
    l.first_date,
    l.last_date,
    a.effective_status
FROM lifetime l
LEFT JOIN ads a ON a.id = l.ad_id
ORDER BY l.first_date ASC, l.ad_id ASC
"""

# Per-(ad, date) daily aggregates. insights gives media + video; attribution
# gives signup/conv/revenue grouped by install_date. Full-outer-joined so a
# date with spend-but-no-conversions OR conversions-but-no-spend still
# shows up.
DAILY_SQL = """
WITH ins AS (
    SELECT
        i.ad_id,
        MAX(i.ad_name) AS ad_name,
        i.date,
        SUM(i.spend)::numeric         AS spend,
        SUM(i.impressions)            AS impressions,
        SUM(i.clicks)                 AS clicks,
        -- Installs (mobile_app_install action) + video metrics from JSONB.
        COALESCE(SUM(COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                 CASE WHEN jsonb_typeof(i.actions)='array' THEN i.actions ELSE '[]'::jsonb END) a
            WHERE a->>'action_type' = 'mobile_app_install'), 0)), 0) AS installs,
        COALESCE(SUM(COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                 CASE WHEN jsonb_typeof(i.video_thruplay_watched_actions)='array' THEN i.video_thruplay_watched_actions ELSE '[]'::jsonb END) a), 0)), 0) AS thruplays,
        COALESCE(SUM(COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                 CASE WHEN jsonb_typeof(i.video_continuous_2_sec_watched_actions)='array' THEN i.video_continuous_2_sec_watched_actions ELSE '[]'::jsonb END) a), 0)), 0) AS three_sec,
        COALESCE(SUM(COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                 CASE WHEN jsonb_typeof(i.video_p100_watched_actions)='array' THEN i.video_p100_watched_actions ELSE '[]'::jsonb END) a), 0)), 0) AS full_plays
    FROM insights_daily i
    JOIN campaigns c ON c.id = i.campaign_id
    WHERE i.attribution_window = '7d_click' AND i.spend > 0
      AND c.name = %(test_campaign_name)s
    GROUP BY i.ad_id, i.date
),
attr AS (
    SELECT
        ae.meta_creative_id AS ad_id,
        ae.install_date AS date,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup' THEN ae.user_id END) AS signups,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup' AND ae.is_mandate = TRUE
                             THEN ae.user_id END) AS p0p1,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial' AND ae.days_since_signup = 0
                             THEN ae.user_id END) AS d0_trials,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0 THEN ae.user_id END) AS d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 2 THEN ae.user_id END) AS d2_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6 THEN ae.user_id END) AS d6_conv,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 2 THEN ae.revenue_inr ELSE 0 END)::numeric AS d2_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6 THEN ae.revenue_inr ELSE 0 END)::numeric AS d6_revenue,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                 THEN ae.revenue_inr ELSE 0 END)::numeric AS total_revenue,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'conversion'
                             THEN ae.user_id END) AS new_user_conv
    FROM attribution_events ae
    WHERE ae.network = 'Facebook'
      AND ae.is_reattributed = FALSE
      AND ae.meta_creative_id IS NOT NULL
      AND ae.meta_creative_id <> 'N/A'
      AND ae.install_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY ae.meta_creative_id, ae.install_date
)
SELECT
    COALESCE(ins.ad_id, attr.ad_id)               AS ad_id,
    COALESCE(ins.ad_name, '')                     AS ad_name,
    COALESCE(ins.date, attr.date)                 AS date,
    COALESCE(ins.spend, 0)                        AS spend,
    COALESCE(ins.impressions, 0)                  AS impressions,
    COALESCE(ins.clicks, 0)                       AS clicks,
    COALESCE(ins.installs, 0)                     AS installs,
    COALESCE(ins.thruplays, 0)                    AS thruplays,
    COALESCE(ins.three_sec, 0)                    AS three_sec,
    COALESCE(ins.full_plays, 0)                   AS full_plays,
    COALESCE(attr.signups, 0)                     AS signups,
    COALESCE(attr.p0p1, 0)                        AS p0p1,
    COALESCE(attr.d0_trials, 0)                   AS d0_trials,
    COALESCE(attr.d0_conv, 0)                     AS d0_conv,
    COALESCE(attr.d2_conv, 0)                     AS d2_conv,
    COALESCE(attr.d6_conv, 0)                     AS d6_conv,
    COALESCE(attr.d2_revenue, 0)                  AS d2_revenue,
    COALESCE(attr.d6_revenue, 0)                  AS d6_revenue,
    COALESCE(attr.total_revenue, 0)               AS total_revenue,
    COALESCE(attr.new_user_conv, 0)               AS new_user_conv
FROM ins
FULL OUTER JOIN attr USING (ad_id, date)
-- Only include ad_ids that exist in the insights side (i.e., are Test4 ads).
-- Attribution-only rows from non-Test4 ads would otherwise leak in.
WHERE ins.ad_name IS NOT NULL OR attr.ad_id IN (SELECT ad_id FROM ins)
ORDER BY ad_id, date
"""


# ── Helpers ───────────────────────────────────────────────────────────────────
def q(conn, sql: str, params: dict | None = None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or {})
        return list(cur.fetchall())


_DATE_SUFFIX_RE = re.compile(r"_\d{6}$")


def creative_type(ad_name: str) -> str:
    """Parse Video/Static/Carousel from the FB_MOF_<type>_... naming convention."""
    if not ad_name:
        return ""
    parts = ad_name.split("_")
    if len(parts) >= 3:
        t = parts[2].lower()
        if t in {"video", "static", "carousel", "image"}:
            return t.title()
    return ""


def live_status(effective_status: str | None) -> str:
    s = (effective_status or "").upper()
    if s == "ACTIVE":
        return "Live"
    if s in {"PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED"}:
        return "Paused"
    return "Not Live"


# ── Column layout (main tab) ──────────────────────────────────────────────────
# Each entry: (header, kind). Kind is one of:
#   "value"     — written as a static value from identity row
#   "formula"   — written as a Sheets formula (interactive)
#   "blank"     — left empty for manual editing
HEADERS = [
    ("S.No",                              "value"),
    ("Type",                              "value"),
    ("Live?",                             "value"),
    ("Live? (adset)",                     "blank"),
    ("Category",                          "formula"),
    ("Refined creative name",             "blank"),
    ("Creative Name",                     "value"),    # G — lookup key
    ("Link",                              "blank"),
    ("Date - Go Live",                    "value"),
    ("Start date",                        "value"),    # J — editable, drives formulas
    ("End Date",                          "value"),    # K — editable, drives formulas
    ("Matured Date (static)",             "value"),    # L — fixed cutoff
    ("Spent (15k min)",                   "formula"),  # M
    ("Matured spends",                    "formula"),  # N
    ("Impr.",                             "formula"),  # O
    ("CPM",                               "formula"),  # P
    ("Clicks",                            "formula"),  # Q
    ("CTR",                               "formula"),  # R
    ("Installs",                          "formula"),  # S
    ("CPI",                               "formula"),  # T
    ("Signups",                           "formula"),  # U
    ("Signup Cost (800)",                 "formula"),  # V
    ("Signup%",                           "formula"),  # W
    ("P0P1",                              "formula"),  # X
    ("P0P1%",                             "formula"),  # Y
    ("P0P1 Cost (2400)",                  "formula"),  # Z
    ("D0_Trials",                         "formula"),  # AA
    ("D0 Trial Cost",                     "formula"),  # AB
    ("D0",                                "formula"),  # AC
    ("D0 CAC",                            "formula"),  # AD
    ("D6",                                "formula"),  # AE
    ("D6 CAC",                            "formula"),  # AF
    ("D6 revenue(overall)",               "formula"),  # AG
    ("D6 ROAS (overall)",                 "formula"),  # AH
    ("New User Conversions",              "formula"),  # AI
    ("New User CAC",                      "formula"),  # AJ
    ("Overall revenue",                   "formula"),  # AK
    ("Overall ROAS",                      "formula"),  # AL
    ("Signups Matured",                   "formula"),  # AM
    ("Signup Cost Matured",               "formula"),  # AN
    ("D2 matured",                        "formula"),  # AO
    ("D2 CAC matured",                    "formula"),  # AP
    ("D2 revenue overall (matured)",      "formula"),  # AQ
    ("D2 ROAS overall (matured)",         "formula"),  # AR
    ("Overall revenue matured",           "formula"),  # AS
    ("Overall ROAS matured",              "formula"),  # AT
    ("ThruPlays",                         "formula"),  # AU
    ("3-Second Video Views",              "formula"),  # AV
    ("Hook%",                             "formula"),  # AW
    ("Hold%",                             "formula"),  # AX
    ("Full video play",                   "formula"),  # AY
    ("Next Steps",                        "blank"),
    ("Himanshu Rating",                   "blank"),
    ("",                                  "blank"),
    ("Test Performance",                  "blank"),
    ("Remarks",                           "blank"),
    ("Week",                              "value"),
    ("Year",                              "value"),
]

# Daily-tab column letters used by the formulas.
D = {
    "ad_name":       "A",
    "date":          "B",
    "spend":         "C",
    "impressions":   "D",
    "clicks":        "E",
    "installs":      "F",
    "thruplays":     "G",
    "three_sec":     "H",
    "full_plays":    "I",
    "signups":       "J",
    "p0p1":          "K",
    "d0_trials":     "L",
    "d0_conv":       "M",
    "d2_conv":       "N",
    "d6_conv":       "O",
    "d2_revenue":    "P",
    "d6_revenue":    "Q",
    "total_revenue": "R",
    "new_user_conv": "S",
}


def col_letter(n: int) -> str:
    """0-indexed column number → A1 letter."""
    s, n = "", n + 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# Pre-compute the column letter for each main-tab field once.
COL = {}
for idx, (header, _) in enumerate(HEADERS):
    COL[header] = col_letter(idx)


def sumifs(metric_col: str, row: int, end_cap_col: str | None = None) -> str:
    """SUMIFS pulling `metric_col` from the Pipeline Daily tab for the row's
    creative + Start/End date range. If `end_cap_col` is given, the end date
    is capped at the cell in that column (used for matured aggregates)."""
    name = f"$G{row}"
    start = f"$J{row}"
    end = f"$K{row}" if end_cap_col is None else f"MIN($K{row},${end_cap_col}{row})"
    return (
        f"SUMIFS('{DAILY_TAB}'!${metric_col}:${metric_col},"
        f"'{DAILY_TAB}'!$A:$A,{name},"
        f"'{DAILY_TAB}'!$B:$B,\">=\"&{start},"
        f"'{DAILY_TAB}'!$B:$B,\"<=\"&{end})"
    )


def build_formulas(row: int) -> dict[str, str]:
    """Return {col_letter: formula} for the row's metric cells."""
    f: dict[str, str] = {}

    # Media — raw spend × GST for display
    f[COL["Spent (15k min)"]]  = f"=IFERROR({sumifs(D['spend'], row)}*{GST_S}, \"\")"
    f[COL["Matured spends"]]   = f"=IFERROR({sumifs(D['spend'], row, end_cap_col='L')}*{GST_S}, \"\")"
    f[COL["Impr."]]            = f"=IFERROR({sumifs(D['impressions'], row)}, \"\")"
    f[COL["CPM"]]              = f"=IFERROR(${COL['Spent (15k min)']}{row}*1000/${COL['Impr.']}{row}, \"\")"
    f[COL["Clicks"]]           = f"=IFERROR({sumifs(D['clicks'], row)}, \"\")"
    f[COL["CTR"]]              = f"=IFERROR(${COL['Clicks']}{row}/${COL['Impr.']}{row}, \"\")"
    f[COL["Installs"]]         = f"=IFERROR({sumifs(D['installs'], row)}, \"\")"
    f[COL["CPI"]]              = f"=IFERROR(${COL['Spent (15k min)']}{row}/${COL['Installs']}{row}, \"\")"

    # Acquisition
    f[COL["Signups"]]            = f"=IFERROR({sumifs(D['signups'], row)}, \"\")"
    f[COL["Signup Cost (800)"]]  = f"=IFERROR(${COL['Spent (15k min)']}{row}/${COL['Signups']}{row}, \"\")"
    f[COL["Signup%"]]            = f"=IFERROR(${COL['Signups']}{row}/${COL['Installs']}{row}, \"\")"
    f[COL["P0P1"]]               = f"=IFERROR({sumifs(D['p0p1'], row)}, \"\")"
    f[COL["P0P1%"]]              = f"=IFERROR(${COL['P0P1']}{row}/${COL['Signups']}{row}, \"\")"
    f[COL["P0P1 Cost (2400)"]]   = f"=IFERROR(${COL['Spent (15k min)']}{row}/${COL['P0P1']}{row}, \"\")"

    # Funnel
    f[COL["D0_Trials"]]      = f"=IFERROR({sumifs(D['d0_trials'], row)}, \"\")"
    f[COL["D0 Trial Cost"]]  = f"=IFERROR(${COL['Spent (15k min)']}{row}/${COL['D0_Trials']}{row}, \"\")"
    f[COL["D0"]]             = f"=IFERROR({sumifs(D['d0_conv'], row)}, \"\")"
    f[COL["D0 CAC"]]         = f"=IFERROR(${COL['Spent (15k min)']}{row}/${COL['D0']}{row}, \"\")"
    f[COL["D6"]]             = f"=IFERROR({sumifs(D['d6_conv'], row)}, \"\")"
    f[COL["D6 CAC"]]         = f"=IFERROR(${COL['Spent (15k min)']}{row}/${COL['D6']}{row}, \"\")"
    f[COL["D6 revenue(overall)"]] = f"=IFERROR({sumifs(D['d6_revenue'], row)}*{GST_S}, \"\")"
    f[COL["D6 ROAS (overall)"]]   = f"=IFERROR(${COL['D6 revenue(overall)']}{row}/${COL['Spent (15k min)']}{row}, \"\")"
    f[COL["New User Conversions"]]= f"=IFERROR({sumifs(D['new_user_conv'], row)}, \"\")"
    f[COL["New User CAC"]]        = f"=IFERROR(${COL['Spent (15k min)']}{row}/${COL['New User Conversions']}{row}, \"\")"
    f[COL["Overall revenue"]]     = f"=IFERROR({sumifs(D['total_revenue'], row)}*{GST_S}, \"\")"
    f[COL["Overall ROAS"]]        = f"=IFERROR(${COL['Overall revenue']}{row}/${COL['Spent (15k min)']}{row}, \"\")"

    # Matured cohort (end-capped at Matured Date in col L)
    f[COL["Signups Matured"]]              = f"=IFERROR({sumifs(D['signups'], row, end_cap_col='L')}, \"\")"
    f[COL["Signup Cost Matured"]]          = f"=IFERROR(${COL['Matured spends']}{row}/${COL['Signups Matured']}{row}, \"\")"
    f[COL["D2 matured"]]                   = f"=IFERROR({sumifs(D['d2_conv'], row, end_cap_col='L')}, \"\")"
    f[COL["D2 CAC matured"]]               = f"=IFERROR(${COL['Matured spends']}{row}/${COL['D2 matured']}{row}, \"\")"
    f[COL["D2 revenue overall (matured)"]] = f"=IFERROR({sumifs(D['d2_revenue'], row, end_cap_col='L')}*{GST_S}, \"\")"
    f[COL["D2 ROAS overall (matured)"]]    = f"=IFERROR(${COL['D2 revenue overall (matured)']}{row}/${COL['Matured spends']}{row}, \"\")"
    f[COL["Overall revenue matured"]]      = f"=IFERROR({sumifs(D['total_revenue'], row, end_cap_col='L')}*{GST_S}, \"\")"
    f[COL["Overall ROAS matured"]]         = f"=IFERROR(${COL['Overall revenue matured']}{row}/${COL['Matured spends']}{row}, \"\")"

    # Video
    f[COL["ThruPlays"]]            = f"=IFERROR({sumifs(D['thruplays'], row)}, \"\")"
    f[COL["3-Second Video Views"]] = f"=IFERROR({sumifs(D['three_sec'], row)}, \"\")"
    f[COL["Hook%"]]                = f"=IFERROR(${COL['3-Second Video Views']}{row}/${COL['Impr.']}{row}, \"\")"
    f[COL["Hold%"]]                = f"=IFERROR(${COL['ThruPlays']}{row}/${COL['3-Second Video Views']}{row}, \"\")"
    f[COL["Full video play"]]      = f"=IFERROR({sumifs(D['full_plays'], row)}/${COL['Impr.']}{row}, \"\")"

    # Category — derived from current Spent + D6 ROAS, so updates with date range
    spent = f"${COL['Spent (15k min)']}{row}"
    roas  = f"${COL['D6 ROAS (overall)']}{row}"
    f[COL["Category"]] = (
        f"=IFERROR(IF(OR({spent}=\"\",{spent}<=0),\"Keep testing\","
        f"IF(AND({spent}>=50000,{roas}>=0.30),\"Cat 1\","
        f"IF(AND({spent}>=30000,{roas}>=0.25),\"Cat 2\","
        f"IF(AND({spent}>=12000,{roas}>=0.22),\"Cat 3\","
        f"IF(AND({spent}>=12000,OR({roas}=\"\",{roas}<0.15)),\"Loser\","
        f"\"Keep testing\"))))),\"Keep testing\")"
    )
    return f


# ── Writers ───────────────────────────────────────────────────────────────────
def write_daily_tab(sh, daily_rows: list[dict]) -> int:
    """Replace the Pipeline Daily tab with one row per (ad, date). Returns
    the new worksheet's sheetId."""
    try:
        sh.del_worksheet(sh.worksheet(DAILY_TAB))
    except Exception:
        pass
    ws = sh.add_worksheet(DAILY_TAB, rows=max(len(daily_rows) + 50, 1000), cols=20)

    header = ["ad_name", "date", "spend", "impressions", "clicks", "installs",
              "thruplays", "three_sec", "full_plays", "signups", "p0p1",
              "d0_trials", "d0_conv", "d2_conv", "d6_conv", "d2_revenue",
              "d6_revenue", "total_revenue", "new_user_conv"]
    out = [header]
    for r in daily_rows:
        d = r.get("date")
        out.append([
            r.get("ad_name") or "",
            d.strftime("%Y-%m-%d") if d else "",
            float(r.get("spend") or 0),
            int(r.get("impressions") or 0),
            int(r.get("clicks") or 0),
            int(r.get("installs") or 0),
            int(r.get("thruplays") or 0),
            int(r.get("three_sec") or 0),
            int(r.get("full_plays") or 0),
            int(r.get("signups") or 0),
            int(r.get("p0p1") or 0),
            int(r.get("d0_trials") or 0),
            int(r.get("d0_conv") or 0),
            int(r.get("d2_conv") or 0),
            int(r.get("d6_conv") or 0),
            float(r.get("d2_revenue") or 0),
            float(r.get("d6_revenue") or 0),
            float(r.get("total_revenue") or 0),
            int(r.get("new_user_conv") or 0),
        ])
    ws.update(values=out, range_name="A1", value_input_option="USER_ENTERED")

    # Hide the tab so it doesn't clutter the spreadsheet's tab bar.
    sh.batch_update({"requests": [{
        "updateSheetProperties": {
            "properties": {"sheetId": ws.id, "hidden": True},
            "fields": "hidden",
        }
    }]})
    print(f"  Pipeline Daily tab: {len(daily_rows)} rows written (hidden).")
    return ws.id


def write_main_tab(sh, identity_rows: list[dict]) -> None:
    """Replace the user-facing main tab with one row per ad (identity + formulas)."""
    try:
        sh.del_worksheet(sh.worksheet(MAIN_TAB))
    except Exception:
        pass
    ws = sh.add_worksheet(MAIN_TAB,
                          rows=max(len(identity_rows) + 50, 600),
                          cols=len(HEADERS) + 2)
    n_cols = len(HEADERS)
    mature_end = date.today() - timedelta(days=7)
    mature_end_s = mature_end.strftime("%Y-%m-%d")
    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    # Group-header row (matches the CSV's "Test Metrics (14k in 2 days)" label)
    group_row = [""] * n_cols
    group_row[9] = "Test Metrics (14k in 2 days)"

    # Header row
    header_row = [h for h, _ in HEADERS]

    data_rows = [group_row, header_row]
    for idx, r in enumerate(identity_rows, start=1):
        sheet_row = 2 + idx  # 1-indexed, accounting for the two header rows
        ad_name = r.get("ad_name") or ""
        first_date = r.get("first_date")
        last_date = r.get("last_date")

        formulas = build_formulas(sheet_row)

        row = [""] * n_cols
        for ci, (header, kind) in enumerate(HEADERS):
            if header == "S.No":
                row[ci] = idx
            elif header == "Type":
                row[ci] = creative_type(ad_name)
            elif header == "Live?":
                row[ci] = live_status(r.get("effective_status"))
            elif header == "Creative Name":
                row[ci] = ad_name
            elif header == "Date - Go Live":
                row[ci] = first_date.strftime("%Y-%m-%d") if first_date else ""
            elif header == "Start date":
                row[ci] = first_date.strftime("%Y-%m-%d") if first_date else ""
            elif header == "End Date":
                row[ci] = last_date.strftime("%Y-%m-%d") if last_date else ""
            elif header == "Matured Date (static)":
                row[ci] = mature_end_s
            elif header == "Week":
                row[ci] = first_date.isocalendar()[1] if first_date else ""
            elif header == "Year":
                row[ci] = first_date.isocalendar()[0] if first_date else ""
            elif kind == "formula":
                col = col_letter(ci)
                row[ci] = formulas.get(col, "")
        data_rows.append(row)

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(identity_rows)} Test4 creatives"])

    ws.update(values=data_rows, range_name="A1", value_input_option="USER_ENTERED")

    # ── Formatting ────────────────────────────────────────────────────────────
    HEADER_ROW = 1
    DATA_START = 2
    DATA_END = 2 + len(identity_rows)

    # Identify column indices for number formatting.
    pct_cols = {"CTR", "Signup%", "P0P1%", "D6 ROAS (overall)", "Overall ROAS",
                "D2 ROAS overall (matured)", "Overall ROAS matured",
                "Hook%", "Hold%", "Full video play"}
    int_cols = {"Spent (15k min)", "Matured spends", "Impr.", "CPM", "Clicks",
                "Installs", "CPI", "Signups", "Signup Cost (800)", "P0P1",
                "P0P1 Cost (2400)", "D0_Trials", "D0 Trial Cost", "D0", "D0 CAC",
                "D6", "D6 CAC", "D6 revenue(overall)", "New User Conversions",
                "New User CAC", "Overall revenue", "Signups Matured",
                "Signup Cost Matured", "D2 matured", "D2 CAC matured",
                "D2 revenue overall (matured)", "Overall revenue matured",
                "ThruPlays", "3-Second Video Views"}

    def fmt_request(col_idx: int, pattern: str, ntype: str = "NUMBER"):
        return {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": DATA_START, "endRowIndex": DATA_END,
                      "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": ntype, "pattern": pattern}}},
            "fields": "userEnteredFormat.numberFormat",
        }}

    fmt_reqs = []
    for ci, (header, _) in enumerate(HEADERS):
        if header in pct_cols:
            fmt_reqs.append(fmt_request(ci, "0.0%", "PERCENT"))
        elif header in int_cols:
            fmt_reqs.append(fmt_request(ci, "0", "NUMBER"))

    body = {"requests": [
        # Group-header row colour
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                                   "startColumnIndex": 0, "endColumnIndex": n_cols},
                         "cell": {"userEnteredFormat": {
                             "backgroundColor": {"red": 0.886, "green": 0.910, "blue": 0.941},
                             "textFormat": {"bold": True, "fontSize": 9,
                                            "foregroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3}},
                             "horizontalAlignment": "CENTER"}},
                         "fields": "userEnteredFormat"}},
        # Metric-header row
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": HEADER_ROW, "endRowIndex": HEADER_ROW + 1,
                                   "startColumnIndex": 0, "endColumnIndex": n_cols},
                         "cell": {"userEnteredFormat": {
                             "backgroundColor": {"red": 0.102, "green": 0.204, "blue": 0.376},
                             "textFormat": {"bold": True, "fontSize": 10,
                                             "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                             "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
                         "fields": "userEnteredFormat"}},
        # Highlight the two editable date columns so users know to click them.
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": DATA_START, "endRowIndex": DATA_END,
                                   "startColumnIndex": 9, "endColumnIndex": 11},
                         "cell": {"userEnteredFormat": {
                             "backgroundColor": {"red": 1.0, "green": 0.961, "blue": 0.820},
                             "textFormat": {"bold": True}}},
                         "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"}},
        # Freeze 2 header rows + first 7 columns (S.No through Link)
        {"updateSheetProperties": {"properties": {"sheetId": ws.id,
                                    "gridProperties": {"frozenRowCount": 2, "frozenColumnCount": 7}},
                                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
        # Category colour rules
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START, "endRowIndex": DATA_END,
                        "startColumnIndex": 4, "endColumnIndex": 5}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                             "format": {"backgroundColor": bg,
                                         "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx}} for idx, (label, bg, fg) in enumerate([
            ("Cat 1",        {"red": 0.275, "green": 0.553, "blue": 0.247}, {"red": 1, "green": 1, "blue": 1}),
            ("Cat 2",        {"red": 0.420, "green": 0.659, "blue": 0.302}, {"red": 1, "green": 1, "blue": 1}),
            ("Cat 3",        {"red": 0.847, "green": 0.918, "blue": 0.827}, {"red": 0.067, "green": 0.392, "blue": 0.176}),
            ("Loser",        {"red": 0.918, "green": 0.263, "blue": 0.208}, {"red": 1, "green": 1, "blue": 1}),
            ("Keep testing", {"red": 1.0,   "green": 0.949, "blue": 0.800}, {"red": 0.4, "green": 0.310, "blue": 0.043}),
        ])],
        # Live? colour rules
        *[{"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": ws.id, "startRowIndex": DATA_START, "endRowIndex": DATA_END,
                        "startColumnIndex": 2, "endColumnIndex": 3}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                             "format": {"backgroundColor": bg,
                                         "textFormat": {"bold": True, "foregroundColor": fg}}},
        }, "index": idx + 5}} for idx, (label, bg, fg) in enumerate([
            ("Live",     {"red": 0.714, "green": 0.882, "blue": 0.722}, {"red": 0.0, "green": 0.239, "blue": 0.086}),
            ("Paused",   {"red": 1.0,   "green": 0.898, "blue": 0.600}, {"red": 0.4, "green": 0.267, "blue": 0.0}),
            ("Not Live", {"red": 0.910, "green": 0.910, "blue": 0.910}, {"red": 0.4, "green": 0.4,   "blue": 0.4}),
        ])],
        # Column-level number formats
        *fmt_reqs,
        # Standard column filters on metric-header row
        {"setBasicFilter": {"filter": {
            "range": {"sheetId": ws.id, "startRowIndex": HEADER_ROW, "endRowIndex": DATA_END + 1,
                       "startColumnIndex": 0, "endColumnIndex": n_cols},
        }}},
        # Column widths
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 0, "endIndex": 1},
                                       "properties": {"pixelSize": 50}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS",
                                       "startIndex": 6, "endIndex": 7},
                                       "properties": {"pixelSize": 320}, "fields": "pixelSize"}},
    ]}
    sh.batch_update(body)

    # Top-right "Last refreshed" banner, same convention as the main dashboards.
    try:
        ws.spreadsheet.batch_update({"requests": [
            {"insertDimension": {
                "range": {"sheetId": ws.id, "dimension": "ROWS",
                          "startIndex": 0, "endIndex": 1},
                "inheritFromBefore": False,
            }},
            {"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": n_cols},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.961, "green": 0.961, "blue": 0.961},
                    "textFormat": {"italic": True, "fontSize": 10,
                                   "foregroundColor": {"red": 0.3, "green": 0.3, "blue": 0.3}},
                    "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment",
            }},
            {"updateCells": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": 1},
                "rows": [{"values": [{
                    "userEnteredValue": {"stringValue":
                        f"Last refreshed: {now_str}  •  Edit Start date (col J) / End Date (col K) per row to recompute metrics for that creative"},
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.961, "green": 0.961, "blue": 0.961},
                        "textFormat": {"italic": True, "fontSize": 10,
                                       "foregroundColor": {"red": 0.3, "green": 0.3, "blue": 0.3}},
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                    },
                }]}],
                "fields": "userEnteredValue,userEnteredFormat",
            }},
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id,
                               "gridProperties": {"frozenRowCount": 3}},
                "fields": "gridProperties.frozenRowCount",
            }},
        ]})
    except Exception:
        pass

    print(f"  Test Creative Pipeline tab: {len(identity_rows)} ads written.")


def main():
    parser = argparse.ArgumentParser(description="Build Test Creative Pipeline tab")
    parser.add_argument("--sheet-id", default=DEFAULT_SHEET_ID,
                        help="Google Sheet ID to write into (default: linked tracker)")
    args = parser.parse_args()

    print("Connecting to DB...")
    conn = psycopg2.connect(DB_DSN)
    print("Fetching identity rows...")
    identity_rows = q(conn, IDENTITY_SQL, {"test_campaign_name": TEST_CAMPAIGN_NAME})
    print(f"  {len(identity_rows)} Test4 ads found.")
    print("Fetching daily aggregates...")
    daily_rows = q(conn, DAILY_SQL, {"test_campaign_name": TEST_CAMPAIGN_NAME})
    conn.close()
    print(f"  {len(daily_rows)} ad×date daily rows.")

    print("Connecting to Google Sheets...")
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(args.sheet_id)
    print(f"Writing to: {sh.url}")
    write_daily_tab(sh, daily_rows)
    write_main_tab(sh, identity_rows)
    print(f"Done. Open sheet: {sh.url}")


if __name__ == "__main__":
    main()
