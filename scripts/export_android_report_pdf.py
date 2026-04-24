"""
Android Meta Ads — current active campaigns report.
Saves to ~/Downloads/univest_android_ads_report.pdf

Attribution source: Singular (attribution_events) — full D6 conversion data available.
Cohort: all active non-iOS, non-retargeting campaigns with spend since Apr 10.

Scoring (percentile-ranked within Android cohort):
  media_score = 0.35*CTR + 0.35*(1-CPM) + 0.30*(1-CPC)
  conv_score  = 0.50*D6% + 0.30*ROAS + 0.20*(1-CAC)
  composite   = 0.50*media + 0.50*conv
"""

import asyncio
import logging
import os
from collections import defaultdict
from datetime import date, datetime

try:
    import anthropic as _anthropic
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
    TableStyle,
)
from sqlalchemy import text

logging.basicConfig(level=logging.WARNING)

OUTPUT_PATH = "/Users/macbook/Downloads/univest_android_ads_report.pdf"
ATTR_SINCE  = date(2026, 4, 10)   # attribution window start

# ── Palette ───────────────────────────────────────────────────────────────────
C_DARK      = colors.HexColor("#1a1a2e")
C_PRIMARY   = colors.HexColor("#0f3460")
C_ACCENT    = colors.HexColor("#e94560")
C_LIGHT_BG  = colors.HexColor("#f4f6f9")
C_MID_BG    = colors.HexColor("#dde3ed")
C_WHITE     = colors.white
C_GREEN     = colors.HexColor("#1a7a4a")
C_GREEN_BG  = colors.HexColor("#d4edda")
C_YELLOW    = colors.HexColor("#856404")
C_YELLOW_BG = colors.HexColor("#fff3cd")
C_RED       = colors.HexColor("#721c24")
C_RED_BG    = colors.HexColor("#f8d7da")
C_GREY      = colors.HexColor("#666677")
C_ANDROID   = colors.HexColor("#3DDC84")   # Android green
C_ANDROID_D = colors.HexColor("#1a7a4a")   # dark android green for headers

# ── SQL ───────────────────────────────────────────────────────────────────────
QUERY = text("""
WITH
-- Active Android non-retargeting ads
cohort AS (
    SELECT DISTINCT
        a.id                 AS ad_id,
        a.name               AS ad_name,
        a.adset_id,
        a.campaign_id,
        a.effective_status,
        a.created_time::date AS created_date,
        c.name               AS campaign_name,
        c.daily_budget,
        c.lifetime_budget,
        s.name               AS adset_name
    FROM ads a
    JOIN campaigns c ON c.id = a.campaign_id
    JOIN adsets    s ON s.id = a.adset_id
    WHERE c.effective_status = 'ACTIVE'
      AND c.name NOT ILIKE '%ios%'
      AND c.name NOT ILIKE '%Retar%'
),

-- Media metrics (all available spend data)
media AS (
    SELECT
        i.ad_id,
        MIN(i.date)                               AS first_date,
        MAX(i.date)                               AS last_date,
        COUNT(DISTINCT i.date)                    AS days_active,
        ROUND(SUM(i.spend)::numeric, 0)           AS spend,
        SUM(i.impressions)                        AS impressions,
        SUM(i.clicks)                             AS clicks,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.clicks)::numeric * 100 / SUM(i.impressions), 3)
        END AS ctr,
        CASE WHEN SUM(i.impressions) > 0
             THEN ROUND(SUM(i.spend)::numeric * 1000 / SUM(i.impressions), 1)
        END AS cpm,
        CASE WHEN SUM(i.clicks) > 0
             THEN ROUND(SUM(i.spend)::numeric / SUM(i.clicks), 1)
        END AS cpc
    FROM insights_daily i
    WHERE i.attribution_window = '7d_click'
      AND i.spend > 0
    GROUP BY i.ad_id
),

-- Singular attribution metrics — all 8 user-requested metrics
attr AS (
    SELECT
        ae.meta_creative_id AS ad_id,

        -- Signups
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                            THEN ae.user_id END)                                       AS signups,

        -- D0 metrics
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup = 0
                            THEN ae.user_id END)                                       AS d0_conv,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup = 0
                            THEN ae.user_id END)                                       AS d0_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup = 0
                 THEN ae.revenue_inr ELSE 0 END)                                       AS d0_revenue,

        -- D6 metrics
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = TRUE
                            THEN ae.user_id END)                                       AS d6_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                             AND ae.is_mandate = FALSE
                            THEN ae.user_id END)                                       AS d6_non_mandate,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                             AND ae.days_since_signup <= 6
                            THEN ae.user_id END)                                       AS d6_trials,
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                  AND ae.days_since_signup <= 6
                 THEN ae.revenue_inr ELSE 0 END)                                       AS d6_revenue,

        -- Total revenue (for LTV)
        SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                 THEN ae.revenue_inr ELSE 0 END)                                       AS total_revenue

    FROM attribution_events ae
    WHERE ae.is_reattributed = FALSE
      AND ae.network = 'Facebook'
      AND ae.meta_creative_id IS NOT NULL
      AND ae.meta_creative_id <> 'N/A'
      AND ae.install_date >= :attr_since
    GROUP BY ae.meta_creative_id
),

-- Join
joined AS (
    SELECT
        co.*,
        m.first_date, m.last_date, m.days_active,
        m.spend, m.impressions, m.clicks,
        m.ctr, m.cpm, m.cpc,

        -- Attribution counts
        COALESCE(at.signups,       0) AS signups,
        COALESCE(at.d0_conv,       0) AS d0_conv,
        COALESCE(at.d0_trials,     0) AS d0_trials,
        COALESCE(at.d0_revenue,    0) AS d0_revenue,
        COALESCE(at.d6_mandate,    0) AS d6_mandate,
        COALESCE(at.d6_non_mandate,0) AS d6_non_mandate,
        COALESCE(at.d6_trials,     0) AS d6_trials,
        COALESCE(at.d6_revenue,    0) AS d6_revenue,
        COALESCE(at.total_revenue, 0) AS total_revenue,

        -- Derived rates
        CASE WHEN COALESCE(at.signups, 0) > 0
             THEN ROUND((at.d6_mandate + at.d6_non_mandate)::numeric * 100 / at.signups, 1)
        END AS d6_pct,
        CASE WHEN COALESCE(at.signups, 0) > 0
             THEN ROUND(at.total_revenue::numeric / at.signups, 0)
        END AS ltv_inr,
        CASE WHEN m.spend > 0 AND COALESCE(at.signups, 0) > 0
             THEN ROUND(m.spend::numeric / at.signups, 0)
        END AS cac_inr,
        -- D0 ROAS
        CASE WHEN m.spend > 0 AND COALESCE(at.d0_revenue, 0) > 0
             THEN ROUND(at.d0_revenue::numeric / m.spend, 3)
        END AS d0_roas,
        -- D6 ROAS
        CASE WHEN m.spend > 0 AND COALESCE(at.d6_revenue, 0) > 0
             THEN ROUND(at.d6_revenue::numeric / m.spend, 3)
        END AS d6_roas
    FROM cohort co
    LEFT JOIN media m  ON m.ad_id = co.ad_id
    LEFT JOIN attr  at ON at.ad_id = co.ad_id
),

-- Percentile ranks within Android cohort
ranked AS (
    SELECT *,
        PERCENT_RANK() OVER (ORDER BY ctr    ASC  NULLS LAST) AS ctr_pct,
        PERCENT_RANK() OVER (ORDER BY cpm    DESC NULLS LAST) AS cpm_pct,
        PERCENT_RANK() OVER (ORDER BY cpc    DESC NULLS LAST) AS cpc_pct,
        PERCENT_RANK() OVER (ORDER BY d6_pct ASC  NULLS LAST) AS d6_pct_pct,
        PERCENT_RANK() OVER (ORDER BY d6_roas ASC NULLS LAST) AS d6_roas_pct,
        PERCENT_RANK() OVER (ORDER BY cac_inr DESC NULLS LAST) AS cac_pct
    FROM joined
    WHERE spend IS NOT NULL AND spend > 0
),

scored AS (
    SELECT *,
        CASE WHEN ctr IS NOT NULL THEN
            ROUND((0.35*ctr_pct + 0.35*cpm_pct + 0.30*cpc_pct)::numeric * 100, 1)
        END AS media_score,
        ROUND((
            0.50 * COALESCE(d6_pct_pct,  0.5) +
            0.30 * COALESCE(d6_roas_pct, 0.5) +
            0.20 * COALESCE(cac_pct,     0.5)
        )::numeric * 100, 1) AS conv_score
    FROM ranked
)

SELECT
    ad_id, ad_name, adset_name, campaign_id, campaign_name,
    daily_budget, lifetime_budget, effective_status, created_date,
    first_date, last_date, days_active,
    spend, impressions, clicks, ctr, cpm, cpc,
    signups,
    d0_conv, d0_trials, d0_revenue, d0_roas,
    d6_mandate, d6_non_mandate, d6_trials, d6_revenue, d6_roas,
    d6_pct, ltv_inr, cac_inr, total_revenue,
    media_score, conv_score,
    ROUND((
        0.50 * COALESCE(media_score, conv_score, 0) +
        0.50 * COALESCE(conv_score, media_score, 0)
    )::numeric, 1) AS composite_score
FROM scored
ORDER BY campaign_name,
         composite_score DESC NULLS LAST,
         spend DESC NULLS LAST
""")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _inr(v, zero_dash=False):
    if v is None: return "—"
    i = int(float(v))
    if zero_dash and i == 0: return "—"
    return f"₹{i:,}"

def _pct(v, d=1):
    if v is None: return "—"
    return f"{float(v):.{d}f}%"

def _n(v):
    if v is None: return "—"
    i = int(float(v))
    return "—" if i == 0 else f"{i:,}"

def _f(v, fmt=".2f", suf=""):
    if v is None: return "—"
    return f"{float(v):{fmt}}{suf}"

def _budget(daily, lifetime):
    if daily:
        try: return f"₹{int(float(daily)):,}/day"
        except: pass
    if lifetime:
        try: return f"₹{int(float(lifetime)):,} lifetime"
        except: pass
    return "Adset-level"

def _tag(score):
    if score is None: return ("NO DATA", C_GREY, C_WHITE)
    s = float(score)
    if s >= 60: return ("KEEP",   C_GREEN,  C_GREEN_BG)
    if s >= 30: return ("REVIEW", C_YELLOW, C_YELLOW_BG)
    return ("REMOVE", C_RED, C_RED_BG)


# ── PDF styles ────────────────────────────────────────────────────────────────
def build_styles():
    return {
        "title":     ParagraphStyle("title",   fontName="Helvetica-Bold", fontSize=22, textColor=C_WHITE),
        "subtitle":  ParagraphStyle("sub",     fontName="Helvetica",      fontSize=9,  textColor=colors.HexColor("#c8cfe0")),
        "section":   ParagraphStyle("sec",     fontName="Helvetica-Bold", fontSize=13, textColor=C_PRIMARY, spaceBefore=12, spaceAfter=5),
        "camp_name": ParagraphStyle("cn",      fontName="Helvetica-Bold", fontSize=10, textColor=C_WHITE),
        "camp_meta": ParagraphStyle("cm",      fontName="Helvetica",      fontSize=8,  textColor=colors.HexColor("#c8cfe0")),
        "note":      ParagraphStyle("note",    fontName="Helvetica-Oblique", fontSize=7.5, textColor=C_GREY, spaceAfter=4),
        "ins_title": ParagraphStyle("it",      fontName="Helvetica-Bold", fontSize=11, textColor=C_PRIMARY, spaceBefore=8, spaceAfter=4),
        "ins_body":  ParagraphStyle("ib",      fontName="Helvetica",      fontSize=8.5, textColor=C_DARK, leading=13, spaceAfter=2),
        "ins_bullet":ParagraphStyle("ibul",    fontName="Helvetica",      fontSize=8.5, textColor=C_DARK, leading=13, leftIndent=12, firstLineIndent=-12, spaceAfter=2),
        "camp_ins":  ParagraphStyle("ci",      fontName="Helvetica-Bold", fontSize=8.5, textColor=C_PRIMARY, spaceBefore=5, spaceAfter=2),
        "cell":      ParagraphStyle("cell",    fontName="Helvetica",      fontSize=7.5, leading=9),
        "cell_r":    ParagraphStyle("cell_r",  fontName="Helvetica",      fontSize=7.5, leading=9, alignment=TA_RIGHT),
        "cell_b":    ParagraphStyle("cell_b",  fontName="Helvetica-Bold", fontSize=7.5, leading=9),
    }


# ── Tables ────────────────────────────────────────────────────────────────────
def summary_table(camps_data, styles):
    TH = ParagraphStyle("th", fontName="Helvetica-Bold", fontSize=7.5, textColor=C_WHITE, alignment=TA_CENTER)
    TR = ParagraphStyle("tr", fontName="Helvetica-Bold", fontSize=7.5, textColor=C_WHITE, alignment=TA_RIGHT)
    headers = [
        Paragraph("Campaign", TH), Paragraph("Spend", TR), Paragraph("Sgn", TR),
        Paragraph("CAC", TR),
        Paragraph("D0\nConv", TR), Paragraph("D0\nTrial", TR), Paragraph("D0\nROAS", TR),
        Paragraph("D6\nMandt", TR), Paragraph("D6\nN-Mdt", TR),
        Paragraph("D6\nTrial", TR), Paragraph("D6\nROAS", TR),
        Paragraph("LTV", TR), Paragraph("K/R/X", TR),
    ]
    rows = [headers]
    for cname, c in sorted(camps_data.items(), key=lambda x: x[1]["spend"], reverse=True):
        sgn    = int(c["signups"]) if c["signups"] else 0
        cac    = _inr(c["spend"] / sgn if sgn else None)
        d0r    = f"{c['d0_revenue']/c['spend']:.2f}x" if c["spend"] and c["d0_revenue"] else "—"
        d6r    = f"{c['d6_revenue']/c['spend']:.2f}x" if c["spend"] and c["d6_revenue"] else "—"
        ltv    = _inr(c["total_revenue"] / sgn if sgn else None)
        krx    = f"{c['keep']}/{c['review']}/{c['remove']}"
        s = styles
        rows.append([
            Paragraph(cname[:52], s["cell"]),
            Paragraph(_inr(c["spend"]), s["cell_r"]),
            Paragraph(_n(c["signups"]), s["cell_r"]),
            Paragraph(cac, s["cell_r"]),
            Paragraph(_n(c["d0_conv"]), s["cell_r"]),
            Paragraph(_n(c["d0_trials"]), s["cell_r"]),
            Paragraph(d0r, s["cell_r"]),
            Paragraph(_n(c["d6_mandate"]), s["cell_r"]),
            Paragraph(_n(c["d6_non_mandate"]), s["cell_r"]),
            Paragraph(_n(c["d6_trials"]), s["cell_r"]),
            Paragraph(d6r, s["cell_r"]),
            Paragraph(ltv, s["cell_r"]),
            Paragraph(krx, s["cell_r"]),
        ])
    col_w = [7.5*cm, 2.2*cm, 1.5*cm, 1.8*cm,
             1.2*cm, 1.2*cm, 1.5*cm,
             1.3*cm, 1.3*cm, 1.3*cm, 1.5*cm,
             1.8*cm, 1.7*cm]
    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  C_ANDROID_D),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_LIGHT_BG]),
        ("GRID",          (0,0), (-1,-1), 0.4, C_MID_BG),
        ("TOPPADDING",    (0,0), (-1,-1), 4), ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ("LEFTPADDING",   (0,0), (-1,-1), 5), ("RIGHTPADDING", (0,0),(-1,-1), 5),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]))
    return tbl


def ads_table(ad_rows, styles):
    TH = ParagraphStyle("th2", fontName="Helvetica-Bold", fontSize=6.5, textColor=C_WHITE, alignment=TA_CENTER)
    TR = ParagraphStyle("tr2", fontName="Helvetica-Bold", fontSize=6.5, textColor=C_WHITE, alignment=TA_RIGHT)
    TS = ParagraphStyle("ts",  fontName="Helvetica",      fontSize=6.5, textColor=C_GREY,  alignment=TA_CENTER)
    headers = [
        Paragraph("Ad Name", TH),
        Paragraph("Score\n(M/C)", TR),
        Paragraph("Tag", TR),
        Paragraph("Spend", TR),
        Paragraph("CTR", TR),
        Paragraph("CPM", TR),
        Paragraph("Signups", TR),
        Paragraph("CAC", TR),
        # D0 group
        Paragraph("D0\nConv", TR),
        Paragraph("D0\nTrial", TR),
        Paragraph("D0\nROAS", TR),
        # D6 group
        Paragraph("D6\nMandt", TR),
        Paragraph("D6\nN-Mdt", TR),
        Paragraph("D6\nTrial", TR),
        Paragraph("D6\nROAS", TR),
        # Summary
        Paragraph("LTV", TR),
        Paragraph("Days", TR),
    ]
    rows = [headers]
    tag_meta = []

    for i, r in enumerate(ad_rows, 1):
        tag_text, tag_fg, tag_bg = _tag(r["composite_score"])
        cs  = f"{float(r['composite_score']):.0f}" if r["composite_score"] is not None else "—"
        ms  = f"{float(r['media_score']):.0f}"     if r["media_score"]     is not None else "—"
        cvs = f"{float(r['conv_score']):.0f}"      if r["conv_score"]      is not None else "—"
        name = (r["ad_name"] or r["ad_id"])[:48]
        s = styles
        SR = ParagraphStyle("sr", fontName="Helvetica", fontSize=6.5, leading=8, alignment=TA_RIGHT)
        SL = ParagraphStyle("sl", fontName="Helvetica", fontSize=6.5, leading=8)
        rows.append([
            Paragraph(name, SL),
            Paragraph(f"{cs}\n({ms}/{cvs})", SR),
            Paragraph(tag_text, SR),
            Paragraph(_inr(r["spend"]), SR),
            Paragraph(_pct(r["ctr"], 2), SR),
            Paragraph(_inr(r["cpm"]), SR),
            Paragraph(_n(r["signups"]), SR),
            Paragraph(_inr(r["cac_inr"]), SR),
            # D0
            Paragraph(_n(r["d0_conv"]), SR),
            Paragraph(_n(r["d0_trials"]), SR),
            Paragraph(_f(r["d0_roas"], ".2f", "x"), SR),
            # D6
            Paragraph(_n(r["d6_mandate"]), SR),
            Paragraph(_n(r["d6_non_mandate"]), SR),
            Paragraph(_n(r["d6_trials"]), SR),
            Paragraph(_f(r["d6_roas"], ".2f", "x"), SR),
            # Summary
            Paragraph(_inr(r["ltv_inr"]), SR),
            Paragraph(str(int(r["days_active"])) if r["days_active"] else "—", SR),
        ])
        tag_meta.append((i, tag_fg, tag_bg, r["composite_score"]))

    col_w = [5.2*cm, 1.5*cm, 1.4*cm, 1.7*cm, 1.1*cm, 1.4*cm, 1.5*cm, 1.5*cm,
             1.1*cm, 1.1*cm, 1.3*cm,
             1.2*cm, 1.2*cm, 1.2*cm, 1.3*cm,
             1.4*cm, 0.9*cm]
    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    cmds = [
        ("BACKGROUND",    (0,0), (-1,0),  C_DARK),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_LIGHT_BG]),
        ("GRID",          (0,0), (-1,-1), 0.3, C_MID_BG),
        ("TOPPADDING",    (0,0), (-1,-1), 3), ("BOTTOMPADDING",(0,0),(-1,-1), 3),
        ("LEFTPADDING",   (0,0), (-1,-1), 4), ("RIGHTPADDING", (0,0),(-1,-1), 4),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]
    for row_i, fg, bg, score in tag_meta:
        cmds += [
            ("BACKGROUND", (2, row_i), (2, row_i), bg),
            ("TEXTCOLOR",  (2, row_i), (2, row_i), fg),
            ("FONTNAME",   (2, row_i), (2, row_i), "Helvetica-Bold"),
            ("TEXTCOLOR",  (1, row_i), (1, row_i), fg),
            ("FONTNAME",   (1, row_i), (1, row_i), "Helvetica-Bold"),
        ]
    tbl.setStyle(TableStyle(cmds))
    return tbl


def camp_header(cname, c, styles):
    signups   = int(c["signups"]) if c["signups"] else 0
    d6_total  = c["d6_mandate"] + c["d6_non_mandate"]
    d6p       = f"{d6_total*100/signups:.1f}%" if signups else "—"
    ltv       = _inr(c["total_revenue"] / signups) if signups else "—"
    cac       = _inr(c["spend"] / signups) if signups else "—"
    d0_roas_v = f"{c['d0_revenue']/c['spend']:.2f}x" if c["spend"] and c["d0_revenue"] else "—"
    d6_roas_v = f"{c['d6_revenue']/c['spend']:.2f}x" if c["spend"] and c["d6_revenue"] else "—"
    budget    = _budget(c["daily_budget"], c["lifetime_budget"])
    meta = (
        f"Budget: {budget}  |  Spend: {_inr(c['spend'])}  |  Ads: {c['ads']}  |  "
        f"Signups: {_n(c['signups'])}  |  CAC: {cac}  |  LTV: {ltv}  |  "
        f"D0: Conv {_n(c['d0_conv'])} / Trial {_n(c['d0_trials'])} / ROAS {d0_roas_v}  |  "
        f"D6: Mandt {_n(c['d6_mandate'])} / Non-Mdt {_n(c['d6_non_mandate'])} / "
        f"Trial {_n(c['d6_trials'])} / ROAS {d6_roas_v} ({d6p})  |  "
        f"KEEP {c['keep']} / REVIEW {c['review']} / REMOVE {c['remove']}"
    )
    tbl = Table([
        [Paragraph(cname, styles["camp_name"])],
        [Paragraph(meta,  styles["camp_meta"])],
    ], colWidths=[27*cm])
    tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), C_ANDROID_D),
        ("TOPPADDING",   (0,0), (-1,-1), 6), ("BOTTOMPADDING",(0,0),(-1,-1), 5),
        ("LEFTPADDING",  (0,0), (-1,-1), 10), ("RIGHTPADDING", (0,0),(-1,-1), 10),
    ]))
    return tbl


# ── LLM Insights ─────────────────────────────────────────────────────────────
def build_summary(campaigns, camps_data) -> str:
    lines = ["=== UNIVEST ANDROID META ADS — CURRENT ACTIVE CAMPAIGNS ===",
             f"Report date: {datetime.now().strftime('%d %b %Y')}",
             f"Attribution: Singular (install_date >= {ATTR_SINCE})",
             "Conversion: D6% = users converting within 6 days of signup",
             "Note: D6 for signups after Apr 17 may be incomplete (< 6 days elapsed)",
             ""]
    total_spend   = sum(c["spend"]         for c in camps_data.values())
    total_signups = sum(c["signups"]       for c in camps_data.values())
    total_d6      = sum(c["d6_mandate"] + c["d6_non_mandate"] for c in camps_data.values())
    total_rev     = sum(c["total_revenue"] for c in camps_data.values())
    lines.append(
        f"Total spend: ₹{int(total_spend):,}  |  Signups: {int(total_signups):,}  |  "
        f"D6 conv: {int(total_d6):,} ({total_d6*100/total_signups:.1f}%)  |  "
        f"Revenue: ₹{int(total_rev):,}  |  "
        f"Blended CAC: {_inr(total_spend/total_signups) if total_signups else '—'}  |  "
        f"ROAS: {total_rev/total_spend:.2f}x" if total_spend else ""
    )
    lines.append("")
    for cname, c in sorted(camps_data.items(), key=lambda x: x[1]["spend"], reverse=True):
        sgn    = int(c["signups"])
        d6_tot = c["d6_mandate"] + c["d6_non_mandate"]
        d6p    = f"{d6_tot*100/sgn:.1f}%" if sgn else "—"
        cac    = f"₹{int(c['spend']/sgn):,}" if sgn else "—"
        d0r_v  = f"{c['d0_revenue']/c['spend']:.2f}x" if c["spend"] and c["d0_revenue"] else "—"
        d6r_v  = f"{c['d6_revenue']/c['spend']:.2f}x" if c["spend"] and c["d6_revenue"] else "—"
        ltv_v  = f"₹{int(c['total_revenue']/sgn):,}" if sgn and c["total_revenue"] else "—"
        lines.append(f"--- CAMPAIGN: {cname} ---")
        lines.append(f"Budget: {_budget(c['daily_budget'],c['lifetime_budget'])}  |  Spend: ₹{int(c['spend']):,}  |  Ads: {c['ads']}")
        lines.append(
            f"Signups: {sgn:,}  |  CAC: {cac}  |  LTV: {ltv_v}  |  "
            f"D0: Conv {int(c['d0_conv'])} / Trial {int(c['d0_trials'])} / ROAS {d0r_v}  |  "
            f"D6: Mandate {int(c['d6_mandate'])} / Non-Mdt {int(c['d6_non_mandate'])} / "
            f"Trial {int(c['d6_trials'])} / ROAS {d6r_v} ({d6p})"
        )
        lines.append(f"KEEP {c['keep']} / REVIEW {c['review']} / REMOVE {c['remove']}")
        lines.append("Top ads:")
        for r in campaigns[cname][:6]:
            tag, _, _ = _tag(r["composite_score"])
            sc  = f"{float(r['composite_score']):.0f}" if r["composite_score"] is not None else "—"
            ms  = f"{float(r['media_score']):.0f}"     if r["media_score"]     is not None else "—"
            cvs = f"{float(r['conv_score']):.0f}"      if r["conv_score"]      is not None else "—"
            lines.append(
                f"  [{tag} {sc}|M:{ms} C:{cvs}] {(r['ad_name'] or r['ad_id'])[:50]} | "
                f"Spend:{_inr(r['spend'])} CTR:{_pct(r['ctr'],2)} CPM:{_inr(r['cpm'])} | "
                f"Signups:{_n(r['signups'])} D6%:{_pct(r['d6_pct'])} CAC:{_inr(r['cac_inr'])} "
                f"D0ROAS:{_f(r['d0_roas'],'.2f','x')} D6ROAS:{_f(r['d6_roas'],'.2f','x')}"
            )
        if len(campaigns[cname]) > 6:
            lines.append(f"  ... and {len(campaigns[cname])-6} more ads")
        lines.append("")
    return "\n".join(lines)


def generate_insights(summary: str) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not _HAS_ANTHROPIC or not api_key:
        print("\n[WARN] ANTHROPIC_API_KEY not set — add it to .env or pass inline.")
        placeholder = "• AI Insights unavailable — ANTHROPIC_API_KEY not configured."
        return {k: placeholder for k in
                ["executive_summary","campaign_insights","creative_patterns","recommended_actions","risk_flags"]}
    client = _anthropic.Anthropic(api_key=api_key)
    prompt = f"""You are a senior performance marketing analyst for Univest, an Indian fintech app
(stock market / options trading education, subscription ~₹500-2000/month).

Here is the Android Meta Ads performance data (attribution via Singular):

{summary}

Context:
- Android uses Singular attribution — D6% (conversion within 6 days) is the gold metric
- D6% > 5% is good; > 10% is strong; < 2% is a red flag
- CAC < ₹500 is efficient; ₹500-1000 is acceptable; > ₹1000 needs justification
- ROAS > 0.5x at 6 days is directionally good (most LTV comes later)
- KEEP ≥60 | REVIEW 30-59 | REMOVE <30 (percentile scores within Android cohort)

Respond in this EXACT format:

EXECUTIVE_SUMMARY
• [3-4 bullets: overall efficiency, key wins, key concerns]

CAMPAIGN_INSIGHTS
CAMPAIGN: [exact campaign name]
[2-3 sentences: what's working, what to pause, budget recommendation]

CREATIVE_PATTERNS
• [4-5 bullets: creative types/angles that drive D6 conversion vs just signups]

RECOMMENDED_ACTIONS
• [5-6 specific prioritised actions with expected impact]

RISK_FLAGS
• [2-3 anomalies or things needing investigation]
"""
    msg = client.messages.create(model="claude-sonnet-4-6", max_tokens=2000,
                                  messages=[{"role": "user", "content": prompt}])
    raw = msg.content[0].text

    def extract(text, start, ends):
        i = text.find(start)
        if i == -1: return ""
        i += len(start)
        end = len(text)
        for e in ends:
            j = text.find(e, i)
            if j != -1: end = min(end, j)
        return text[i:end].strip()

    secs = ["EXECUTIVE_SUMMARY","CAMPAIGN_INSIGHTS","CREATIVE_PATTERNS","RECOMMENDED_ACTIONS","RISK_FLAGS"]
    return {s.lower(): extract(raw, s, [x for x in secs if x != s]) for s in secs}


def insights_flowables(ins: dict, styles) -> list:
    story = []
    def bullets(text):
        out = []
        for line in text.split("\n"):
            line = line.strip()
            if not line: continue
            if line.startswith("CAMPAIGN:"):
                out.append(Paragraph(line, styles["camp_ins"]))
            elif line.startswith(("•","–","-","*")):
                out.append(Paragraph(line.lstrip("•–-* "), styles["ins_bullet"]))
            else:
                out.append(Paragraph(line, styles["ins_body"]))
        return out

    story.append(Paragraph("Executive Summary", styles["ins_title"]))
    story.append(HRFlowable(width="100%", thickness=1, color=C_MID_BG, spaceAfter=5))
    story += bullets(ins.get("executive_summary",""))
    story.append(Spacer(1, 8))

    def col_block(title, key, bg):
        inner = [Paragraph(title, ParagraphStyle("ch", fontName="Helvetica-Bold", fontSize=9, textColor=C_WHITE))]
        inner += bullets(ins.get(key,""))
        t = Table([[f] for f in inner], colWidths=[12.5*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",   (0,0),(-1,0),  bg),
            ("BACKGROUND",   (0,1),(-1,-1), C_LIGHT_BG),
            ("TOPPADDING",   (0,0),(-1,-1), 5), ("BOTTOMPADDING",(0,0),(-1,-1), 5),
            ("LEFTPADDING",  (0,0),(-1,-1), 8), ("RIGHTPADDING", (0,0),(-1,-1), 8),
            ("BOX",          (0,0),(-1,-1), 0.5, C_MID_BG),
        ]))
        return t

    two = Table([[col_block("Creative Patterns", "creative_patterns", C_ANDROID_D),
                  col_block("Risk Flags",        "risk_flags",        C_ACCENT)]],
                colWidths=[13.5*cm, 13.5*cm])
    two.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),
                              ("LEFTPADDING",(0,0),(-1,-1),0), ("RIGHTPADDING",(0,0),(-1,-1),0),
                              ("TOPPADDING",(0,0),(-1,-1),0), ("BOTTOMPADDING",(0,0),(-1,-1),0)]))
    story.append(two)
    story.append(Spacer(1, 8))

    story.append(Paragraph("Recommended Actions", styles["ins_title"]))
    story.append(HRFlowable(width="100%", thickness=1, color=C_MID_BG, spaceAfter=5))
    story += bullets(ins.get("recommended_actions",""))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Campaign Insights", styles["ins_title"]))
    story.append(HRFlowable(width="100%", thickness=1, color=C_MID_BG, spaceAfter=5))
    story += bullets(ins.get("campaign_insights",""))
    return story


# ── Main ─────────────────────────────────────────────────────────────────────
async def fetch():
    from services.shared.db import AsyncSessionLocal
    async with AsyncSessionLocal() as s:
        r = await s.execute(QUERY, {"attr_since": ATTR_SINCE})
        return r.mappings().all()


async def main():
    print("Fetching Android ad data...")
    rows = await fetch()
    if not rows:
        print("No active Android ads with spend found.")
        return
    print(f"  {len(rows)} ads")

    # Group by campaign
    campaigns = defaultdict(list)
    camp_meta = {}
    for r in rows:
        cname = r["campaign_name"] or r["campaign_id"]
        campaigns[cname].append(r)
        if cname not in camp_meta:
            camp_meta[cname] = {"daily_budget": r["daily_budget"],
                                 "lifetime_budget": r["lifetime_budget"]}

    camps_data = {}
    for cname, ads in campaigns.items():
        camps_data[cname] = dict(
            spend          = sum(float(r["spend"]          or 0) for r in ads),
            signups        = sum(int(r["signups"]           or 0) for r in ads),
            d0_conv        = sum(int(r["d0_conv"]           or 0) for r in ads),
            d0_trials      = sum(int(r["d0_trials"]         or 0) for r in ads),
            d0_revenue     = sum(float(r["d0_revenue"]      or 0) for r in ads),
            d6_mandate     = sum(int(r["d6_mandate"]        or 0) for r in ads),
            d6_non_mandate = sum(int(r["d6_non_mandate"]    or 0) for r in ads),
            d6_trials      = sum(int(r["d6_trials"]         or 0) for r in ads),
            d6_revenue     = sum(float(r["d6_revenue"]      or 0) for r in ads),
            total_revenue  = sum(float(r["total_revenue"]   or 0) for r in ads),
            ads      = len(ads),
            keep   = sum(1 for r in ads if r["composite_score"] and float(r["composite_score"]) >= 60),
            review = sum(1 for r in ads if r["composite_score"] and 30 <= float(r["composite_score"]) < 60),
            remove = sum(1 for r in ads if r["composite_score"] is None or float(r["composite_score"]) < 30),
            daily_budget    = camp_meta[cname]["daily_budget"],
            lifetime_budget = camp_meta[cname]["lifetime_budget"],
        )

    total_spend   = sum(c["spend"]         for c in camps_data.values())
    total_signups = sum(c["signups"]       for c in camps_data.values())
    total_d6      = sum(c["d6_mandate"] + c["d6_non_mandate"] for c in camps_data.values())
    total_rev     = sum(c["total_revenue"] for c in camps_data.values())
    total_keep    = sum(c["keep"]          for c in camps_data.values())
    total_review  = sum(c["review"]        for c in camps_data.values())
    total_remove  = sum(c["remove"]        for c in camps_data.values())

    all_first = min(r["first_date"] for r in rows if r["first_date"])
    all_last  = max(r["last_date"]  for r in rows if r["last_date"])

    print("Generating AI insights...")
    summary  = build_summary(campaigns, camps_data)
    insights = generate_insights(summary)
    print("  Done.")

    print("Building PDF...")
    styles = build_styles()
    doc = SimpleDocTemplate(OUTPUT_PATH, pagesize=landscape(A4),
                            leftMargin=1.2*cm, rightMargin=1.2*cm,
                            topMargin=1.5*cm,  bottomMargin=1.5*cm)
    story = []

    # Title banner
    t = Table([[Paragraph("Univest — Android Meta Ads Report", styles["title"])],
               [Paragraph(f"Active campaigns  ·  Data {all_first} → {all_last}  ·  "
                           f"Attribution: Singular (D6)  ·  "
                           f"Generated {datetime.now().strftime('%d %b %Y, %H:%M')}",
                           styles["subtitle"])]],
              colWidths=[28*cm])
    t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),C_DARK),
                            ("TOPPADDING",(0,0),(-1,-1),14), ("BOTTOMPADDING",(0,0),(-1,-1),14),
                            ("LEFTPADDING",(0,0),(-1,-1),16), ("RIGHTPADDING",(0,0),(-1,-1),16)]))
    story.append(t)
    story.append(Spacer(1, 10))

    # KPI tiles
    KV = ParagraphStyle("kv", fontName="Helvetica-Bold", fontSize=16, textColor=C_ANDROID_D, alignment=TA_CENTER)
    KL = ParagraphStyle("kl", fontName="Helvetica",      fontSize=8,  textColor=C_GREY, alignment=TA_CENTER)
    d6p_g  = f"{total_d6*100/total_signups:.1f}%" if total_signups else "—"
    cac_g  = _inr(total_spend / total_signups) if total_signups else "—"
    roas_g = f"{total_rev/total_spend:.2f}x" if total_spend and total_rev else "—"
    ltv_g  = _inr(total_rev / total_signups) if total_signups else "—"
    kpis = [
        (f"₹{int(total_spend):,}", "Total Spend"),
        (str(len(rows)),            "Active Ads"),
        (str(len(campaigns)),       "Campaigns"),
        (f"{int(total_signups):,}", "Signups"),
        (d6p_g,                     "D6 Conv%"),
        (f"{int(total_d6):,}",      "D6 Conversions"),
        (ltv_g,                     "Avg LTV (D6)"),
        (cac_g,                     "Blended CAC"),
        (roas_g,                    "Blended ROAS"),
        (f"{total_keep}/{total_review}/{total_remove}", "K/R/X"),
    ]
    kpi_row = [[Table([[Paragraph(v,KV)],[Paragraph(l,KL)]], colWidths=[2.8*cm]) for v,l in kpis]]
    kpi_tbl = Table(kpi_row, colWidths=[2.8*cm]*10)
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),C_LIGHT_BG), ("BOX",(0,0),(-1,-1),0.5,C_MID_BG),
        ("INNERGRID",(0,0),(-1,-1),0.5,C_MID_BG),
        ("TOPPADDING",(0,0),(-1,-1),8), ("BOTTOMPADDING",(0,0),(-1,-1),8),
    ]))
    story.append(kpi_tbl)
    story.append(Spacer(1, 8))

    # Legend
    story.append(Paragraph(
        "Scoring (percentile-ranked within Android cohort):  "
        "Media = 0.35×CTR + 0.35×(1−CPM) + 0.30×(1−CPC)  ·  "
        "Conv = 0.50×D6% + 0.30×ROAS + 0.20×(1−CAC)  ·  "
        "Composite = 0.50×Media + 0.50×Conv  ·  "
        "Attribution via Singular (install_date ≥ Apr 10)  ·  "
        "D6 for Apr 17+ signups may be incomplete  ·  "
        "<font color='#1a7a4a'><b>KEEP ≥60</b></font>  "
        "<font color='#856404'><b>REVIEW 30–59</b></font>  "
        "<font color='#721c24'><b>REMOVE &lt;30</b></font>",
        styles["note"],
    ))
    story.append(Spacer(1, 6))

    # AI Insights
    story.append(Paragraph("AI Insights", styles["section"]))
    story.append(HRFlowable(width="100%", thickness=1.5, color=C_ACCENT, spaceAfter=8))
    story += insights_flowables(insights, styles)
    story.append(PageBreak())

    # Campaign summary
    story.append(Paragraph("Campaign Overview", styles["section"]))
    story.append(summary_table(camps_data, styles))
    story.append(Spacer(1, 5))
    story.append(Paragraph(
        "D6% = users converting within 6 days of signup  ·  "
        "LTV = total D6 revenue / signups  ·  CAC = spend / signups  ·  ROAS = revenue / spend",
        styles["note"],
    ))
    story.append(PageBreak())

    # Per-campaign ad tables
    story.append(Paragraph("Ad-Level Detail by Campaign", styles["section"]))
    story.append(Paragraph(
        "Score shown as Composite (Media/Conv)  ·  "
        "All conversion metrics via Singular attribution",
        styles["note"],
    ))
    story.append(Spacer(1, 4))

    for cname in sorted(campaigns, key=lambda c: camps_data[c]["spend"], reverse=True):
        story.append(camp_header(cname, camps_data[cname], styles))
        story.append(Spacer(1, 3))
        story.append(ads_table(campaigns[cname], styles))
        story.append(Spacer(1, 12))

    doc.build(story)
    print(f"\nSaved → {OUTPUT_PATH}")


asyncio.run(main())
