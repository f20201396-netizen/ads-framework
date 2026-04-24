"""
Export the full active-ads scorecard (Apr 10-15 cohort, Apr 10-22 media+attr window)
as a PDF report to ~/Downloads/univest_ads_report_apr10_15.pdf
"""

import asyncio
import logging
import os
from collections import defaultdict
from datetime import date, datetime

import anthropic
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
    TableStyle,
)
from sqlalchemy import text

logging.basicConfig(level=logging.WARNING)

OUTPUT_PATH = "/Users/macbook/Downloads/univest_ads_report_apr10_15.pdf"

COHORT_SINCE = date(2026, 4, 10)
COHORT_UNTIL = date(2026, 4, 15)
MEDIA_SINCE  = date(2026, 4, 10)
MEDIA_UNTIL  = date(2026, 4, 22)
ATTR_SINCE   = date(2026, 4, 10)
ATTR_UNTIL   = date(2026, 4, 22)

# ── Colour palette ────────────────────────────────────────────────────────────
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
C_GREY_TEXT = colors.HexColor("#666677")

QUERY = text("""
WITH
cohort AS (
    SELECT DISTINCT
        a.id                    AS ad_id,
        a.name                  AS ad_name,
        a.adset_id,
        a.campaign_id,
        a.effective_status,
        a.created_time::date    AS created_date,
        c.name                  AS campaign_name,
        c.daily_budget,
        c.lifetime_budget,
        s.name                  AS adset_name
    FROM insights_daily i
    JOIN ads       a ON a.id  = i.ad_id
    JOIN campaigns c ON c.id  = a.campaign_id
    JOIN adsets    s ON s.id  = a.adset_id
    WHERE i.date BETWEEN :cohort_since AND :cohort_until
      AND i.attribution_window = '7d_click'
      AND i.spend > 0
      AND c.name NOT ILIKE '%Retar%'
),
media AS (
    SELECT
        ad_id,
        SUM(spend)       AS spend,
        SUM(impressions) AS impressions,
        SUM(clicks)      AS clicks,
        CASE WHEN SUM(impressions) > 0
             THEN SUM(clicks)::numeric * 100 / SUM(impressions) END AS ctr,
        CASE WHEN SUM(impressions) > 0
             THEN SUM(spend)::numeric * 1000 / SUM(impressions) END AS cpm,
        CASE WHEN SUM(clicks) > 0
             THEN SUM(spend)::numeric / SUM(clicks) END AS cpc,
        COUNT(DISTINCT date) AS days_active
    FROM insights_daily
    WHERE date BETWEEN :media_since AND :media_until
      AND attribution_window = '7d_click'
    GROUP BY ad_id
),
attr AS (
    SELECT
        ae.meta_creative_id AS ad_id,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                            THEN ae.user_id END) AS signups,
        COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                             AND ae.days_since_signup <= 6
                            THEN ae.user_id END) AS d6_conversions,
        COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                            THEN ae.user_id END) AS d0_trials,
        ROUND(COUNT(DISTINCT CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                                   AND ae.days_since_signup <= 6
                                  THEN ae.user_id END)::numeric * 100
              / NULLIF(COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'
                                           THEN ae.user_id END), 0), 2) AS d6_conv_pct,
        ROUND(SUM(CASE WHEN ae.event_name IN ('conversion','repeat_conversion')
                       THEN ae.revenue_inr ELSE 0 END)::numeric
              / NULLIF(SUM(CASE WHEN ae.event_name = 'signup' THEN 1 ELSE 0 END), 0), 2) AS avg_ltv_inr
    FROM attribution_events ae
    WHERE ae.install_date BETWEEN :attr_since AND :attr_until
      AND ae.is_reattributed = FALSE
      AND ae.network = 'Facebook'
    GROUP BY ae.meta_creative_id
),
joined AS (
    SELECT co.*,
        m.spend, m.impressions, m.clicks, m.ctr, m.cpm, m.cpc, m.days_active,
        at.signups, at.d6_conversions, at.d0_trials, at.d6_conv_pct, at.avg_ltv_inr,
        CASE WHEN m.spend > 0 AND at.signups > 0
             THEN ROUND(m.spend::numeric / at.signups, 0) END AS cac_inr,
        CASE WHEN m.spend > 0 AND at.avg_ltv_inr IS NOT NULL
             THEN ROUND((at.avg_ltv_inr * at.signups) / m.spend, 3) END AS roas
    FROM cohort co
    LEFT JOIN media m  ON m.ad_id = co.ad_id
    LEFT JOIN attr  at ON at.ad_id = co.ad_id
),
ranked AS (
    SELECT *,
        PERCENT_RANK() OVER (ORDER BY ctr         ASC  NULLS LAST) AS ctr_pct,
        PERCENT_RANK() OVER (ORDER BY cpm         DESC NULLS LAST) AS cpm_pct,
        PERCENT_RANK() OVER (ORDER BY cpc         DESC NULLS LAST) AS cpc_pct,
        PERCENT_RANK() OVER (ORDER BY d6_conv_pct ASC  NULLS LAST) AS d6_pct,
        PERCENT_RANK() OVER (ORDER BY roas        ASC  NULLS LAST) AS roas_pct,
        PERCENT_RANK() OVER (ORDER BY cac_inr     DESC NULLS LAST) AS cac_pct
    FROM joined
),
scored AS (
    SELECT *,
        CASE WHEN ctr IS NOT NULL THEN
            ROUND((0.35*ctr_pct + 0.35*cpm_pct + 0.30*cpc_pct)::numeric * 100, 1)
        END AS media_score,
        CASE WHEN d6_conv_pct IS NOT NULL OR roas IS NOT NULL THEN
            ROUND((0.50*COALESCE(d6_pct,0.5) + 0.30*COALESCE(roas_pct,0.5)
                   + 0.20*COALESCE(cac_pct,0.5))::numeric * 100, 1)
        END AS conv_score
    FROM ranked
)
SELECT
    ad_id, ad_name, adset_name, campaign_id, campaign_name,
    daily_budget, lifetime_budget, effective_status, created_date, days_active,
    ROUND(spend::numeric, 0) AS spend_inr, impressions,
    ROUND(ctr::numeric, 3) AS ctr_pct, ROUND(cpm::numeric, 1) AS cpm,
    ROUND(cpc::numeric, 1) AS cpc,
    signups, d0_trials, d6_conversions, d6_conv_pct,
    ROUND(avg_ltv_inr::numeric, 0) AS avg_ltv_inr,
    roas, cac_inr, media_score, conv_score,
    ROUND((0.50*COALESCE(media_score,conv_score,0)
           + 0.50*COALESCE(conv_score,media_score,0))::numeric, 1) AS composite_score
FROM scored
ORDER BY campaign_name, composite_score DESC NULLS LAST, spend_inr DESC NULLS LAST
""")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _inr(v, zero_dash=False):
    if v is None:
        return "—"
    i = int(float(v))
    if zero_dash and i == 0:
        return "—"
    return f"₹{i:,}"

def _pct(v, decimals=1):
    if v is None:
        return "—"
    return f"{float(v):.{decimals}f}%"

def _fmt(v, fmt=".2f", suffix=""):
    if v is None:
        return "—"
    return f"{float(v):{fmt}}{suffix}"

def _budget(daily, lifetime):
    if daily:
        try:
            return f"₹{int(float(daily)):,}/day"
        except Exception:
            pass
    if lifetime:
        try:
            return f"₹{int(float(lifetime)):,} lifetime"
        except Exception:
            pass
    return "Adset-level"

def _tag(score):
    if score is None:
        return ("NO DATA", C_GREY_TEXT, C_WHITE)
    s = float(score)
    if s >= 60:
        return ("KEEP",   C_GREEN,  C_GREEN_BG)
    if s >= 30:
        return ("REVIEW", C_YELLOW, C_YELLOW_BG)
    return ("REMOVE", C_RED, C_RED_BG)

def _score_color(score):
    if score is None:
        return C_GREY_TEXT
    s = float(score)
    if s >= 60:
        return C_GREEN
    if s >= 30:
        return C_YELLOW
    return C_RED


def build_styles():
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=22,
                                 textColor=C_WHITE, alignment=TA_LEFT, spaceAfter=4),
        "subtitle": ParagraphStyle("subtitle", fontName="Helvetica", fontSize=10,
                                    textColor=colors.HexColor("#c8cfe0"),
                                    alignment=TA_LEFT, spaceAfter=2),
        "section": ParagraphStyle("section", fontName="Helvetica-Bold", fontSize=13,
                                   textColor=C_PRIMARY, spaceBefore=14, spaceAfter=6),
        "camp_name": ParagraphStyle("camp_name", fontName="Helvetica-Bold", fontSize=10,
                                     textColor=C_WHITE),
        "camp_meta": ParagraphStyle("camp_meta", fontName="Helvetica", fontSize=8,
                                     textColor=colors.HexColor("#c8cfe0")),
        "note": ParagraphStyle("note", fontName="Helvetica-Oblique", fontSize=7.5,
                                textColor=C_GREY_TEXT, spaceAfter=4),
        "cell": ParagraphStyle("cell", fontName="Helvetica", fontSize=7.5,
                                leading=9, alignment=TA_LEFT),
        "cell_r": ParagraphStyle("cell_r", fontName="Helvetica", fontSize=7.5,
                                  leading=9, alignment=TA_RIGHT),
        "cell_bold": ParagraphStyle("cell_bold", fontName="Helvetica-Bold", fontSize=7.5,
                                     leading=9, alignment=TA_LEFT),
    }


def summary_table(campaigns_data, styles):
    """Campaign-level overview table."""
    hdr_style = ParagraphStyle("th", fontName="Helvetica-Bold", fontSize=8,
                                textColor=C_WHITE, alignment=TA_CENTER)
    hdr_r = ParagraphStyle("thr", fontName="Helvetica-Bold", fontSize=8,
                             textColor=C_WHITE, alignment=TA_RIGHT)

    headers = [
        Paragraph("Campaign", hdr_style),
        Paragraph("Budget", hdr_r),
        Paragraph("Ads", hdr_r),
        Paragraph("Spend", hdr_r),
        Paragraph("Signups", hdr_r),
        Paragraph("D6%", hdr_r),
        Paragraph("CAC", hdr_r),
        Paragraph("ROAS", hdr_r),
        Paragraph("K / R / X", hdr_r),
    ]

    rows = [headers]
    for cname, camp in sorted(campaigns_data.items(),
                               key=lambda x: x[1]["spend"], reverse=True):
        d6pct = _pct(camp["d6"] * 100 / camp["signups"] if camp["signups"] else None)
        cac   = _inr(camp["spend"] / camp["signups"] if camp["spend"] and camp["signups"] else None)
        roas  = _fmt(camp["rev"] / camp["spend"] if camp["spend"] and camp["rev"] else None, ".2f", "x")
        krx   = f"{camp['keep']} / {camp['review']} / {camp['remove']}"

        s = styles
        rows.append([
            Paragraph(cname[:55], s["cell"]),
            Paragraph(_budget(camp["daily_budget"], camp["lifetime_budget"]), s["cell_r"]),
            Paragraph(str(camp["ads"]), s["cell_r"]),
            Paragraph(_inr(camp["spend"]), s["cell_r"]),
            Paragraph(f"{camp['signups']:,}", s["cell_r"]),
            Paragraph(d6pct, s["cell_r"]),
            Paragraph(cac, s["cell_r"]),
            Paragraph(roas, s["cell_r"]),
            Paragraph(krx, s["cell_r"]),
        ])

    col_widths = [9.5*cm, 2.8*cm, 1*cm, 2.4*cm, 1.8*cm, 1.4*cm, 2.2*cm, 1.8*cm, 2*cm]
    tbl = Table(rows, colWidths=col_widths, repeatRows=1)

    style_cmds = [
        ("BACKGROUND",  (0,0), (-1,0),  C_PRIMARY),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [C_WHITE, C_LIGHT_BG]),
        ("GRID",        (0,0), (-1,-1), 0.4, C_MID_BG),
        ("TOPPADDING",  (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ("LEFTPADDING", (0,0), (-1,-1), 5),
        ("RIGHTPADDING",(0,0), (-1,-1), 5),
        ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
    ]
    tbl.setStyle(TableStyle(style_cmds))
    return tbl


def ads_table(ad_rows, styles):
    """Per-ad detail table for one campaign."""
    hdr_style = ParagraphStyle("th2", fontName="Helvetica-Bold", fontSize=7,
                                textColor=C_WHITE, alignment=TA_CENTER)
    hdr_r = ParagraphStyle("thr2", fontName="Helvetica-Bold", fontSize=7,
                             textColor=C_WHITE, alignment=TA_RIGHT)

    headers = [
        Paragraph("Ad Name", hdr_style),
        Paragraph("Score", hdr_r),
        Paragraph("Tag", hdr_r),
        Paragraph("Spend", hdr_r),
        Paragraph("CTR", hdr_r),
        Paragraph("CPM", hdr_r),
        Paragraph("CPC", hdr_r),
        Paragraph("Sgn", hdr_r),
        Paragraph("Trl", hdr_r),
        Paragraph("D6", hdr_r),
        Paragraph("D6%", hdr_r),
        Paragraph("LTV", hdr_r),
        Paragraph("CAC", hdr_r),
        Paragraph("ROAS", hdr_r),
        Paragraph("Status", hdr_r),
    ]

    rows = [headers]
    tag_colors = []  # (row_idx, fg, bg)

    for i, r in enumerate(ad_rows, 1):
        tag_text, tag_fg, tag_bg = _tag(r["composite_score"])
        score_str = f"{float(r['composite_score']):.0f}" if r["composite_score"] is not None else "—"
        ms = f"{float(r['media_score']):.0f}" if r["media_score"] is not None else "—"
        cs = f"{float(r['conv_score']):.0f}"  if r["conv_score"]  is not None else "—"

        name = (r["ad_name"] or r["ad_id"])[:48]
        status = (r["effective_status"] or "").lower()[:8]

        s = styles
        rows.append([
            Paragraph(name, s["cell"]),
            Paragraph(f"{score_str}\n({ms}/{cs})", s["cell_r"]),
            Paragraph(tag_text, s["cell_r"]),
            Paragraph(_inr(r["spend_inr"]), s["cell_r"]),
            Paragraph(_pct(r["ctr_pct"], 2), s["cell_r"]),
            Paragraph(_inr(r["cpm"]), s["cell_r"]),
            Paragraph(_inr(r["cpc"]), s["cell_r"]),
            Paragraph(str(int(r["signups"])) if r["signups"] is not None else "—", s["cell_r"]),
            Paragraph(str(int(r["d0_trials"])) if r["d0_trials"] is not None else "—", s["cell_r"]),
            Paragraph(str(int(r["d6_conversions"])) if r["d6_conversions"] is not None else "—", s["cell_r"]),
            Paragraph(_pct(r["d6_conv_pct"], 1), s["cell_r"]),
            Paragraph(_inr(r["avg_ltv_inr"], zero_dash=True), s["cell_r"]),
            Paragraph(_inr(r["cac_inr"]), s["cell_r"]),
            Paragraph(_fmt(r["roas"], ".2f", "x"), s["cell_r"]),
            Paragraph(status, s["cell_r"]),
        ])
        tag_colors.append((i, tag_fg, tag_bg))

    col_widths = [7.2*cm, 1.4*cm, 1.4*cm, 1.8*cm, 1.2*cm, 1.4*cm, 1.2*cm,
                  0.9*cm, 0.9*cm, 0.8*cm, 1.1*cm, 1.5*cm, 1.5*cm, 1.4*cm, 1.5*cm]

    tbl = Table(rows, colWidths=col_widths, repeatRows=1)

    style_cmds = [
        ("BACKGROUND",   (0,0), (-1,0),  C_DARK),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_LIGHT_BG]),
        ("GRID",         (0,0), (-1,-1), 0.3, C_MID_BG),
        ("TOPPADDING",   (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
    ]
    # Color the Tag column per row
    for row_idx, fg, bg in tag_colors:
        style_cmds += [
            ("BACKGROUND",  (2, row_idx), (2, row_idx), bg),
            ("TEXTCOLOR",   (2, row_idx), (2, row_idx), fg),
            ("FONTNAME",    (2, row_idx), (2, row_idx), "Helvetica-Bold"),
        ]
        # Color the score column
        style_cmds += [
            ("TEXTCOLOR", (1, row_idx), (1, row_idx), fg),
            ("FONTNAME",  (1, row_idx), (1, row_idx), "Helvetica-Bold"),
        ]

    tbl.setStyle(TableStyle(style_cmds))
    return tbl


def header_block(camp_name, camp_data, styles):
    """Dark banner for each campaign section."""
    budget = _budget(camp_data["daily_budget"], camp_data["lifetime_budget"])
    d6pct  = f"{camp_data['d6']*100/camp_data['signups']:.1f}%" if camp_data["signups"] else "—"
    rev    = camp_data["rev"]
    roas   = f"{rev/camp_data['spend']:.2f}x" if camp_data["spend"] and rev else "—"

    meta_line = (
        f"Budget: {budget}  |  Spend: {_inr(camp_data['spend'])}  |  "
        f"Signups: {camp_data['signups']:,}  |  D6 conv: {camp_data['d6']} ({d6pct})  |  "
        f"ROAS: {roas}  |  "
        f"KEEP {camp_data['keep']}  REVIEW {camp_data['review']}  REMOVE {camp_data['remove']}"
    )

    name_para = Paragraph(camp_name, styles["camp_name"])
    meta_para = Paragraph(meta_line, styles["camp_meta"])

    tbl = Table([[name_para], [meta_para]], colWidths=[26.5*cm])
    tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), C_PRIMARY),
        ("TOPPADDING",   (0,0), (-1,-1), 6),
        ("BOTTOMPADDING",(0,0), (-1,-1), 5),
        ("LEFTPADDING",  (0,0), (-1,-1), 10),
        ("RIGHTPADDING", (0,0), (-1,-1), 10),
    ]))
    return tbl


def build_data_summary(campaigns, camps_data) -> str:
    """Compact text summary fed to Claude for insights generation."""
    lines = []
    lines.append("=== UNIVEST META ADS PERFORMANCE REPORT ===")
    lines.append(f"Period: Apr 10-15, 2026 (cohort) | Media & attribution: Apr 10-22")
    lines.append(f"Total campaigns: {len(campaigns)} | Total ads: {sum(c['ads'] for c in camps_data.values())}")
    lines.append(f"Total spend: ₹{int(sum(c['spend'] for c in camps_data.values())):,}")
    total_signups = sum(c['signups'] for c in camps_data.values())
    total_d6 = sum(c['d6'] for c in camps_data.values())
    total_rev = sum(c['rev'] for c in camps_data.values())
    total_spend = sum(c['spend'] for c in camps_data.values())
    lines.append(f"Total signups: {total_signups:,} | D6 conversions: {total_d6} | D6 rate: {total_d6*100/total_signups:.1f}%")
    lines.append(f"Blended CAC: ₹{int(total_spend/total_signups):,} | Blended ROAS: {total_rev/total_spend:.2f}x" if total_signups and total_spend else "")
    lines.append("")
    lines.append("=== SCORING ===")
    lines.append("Composite score 0-100 (percentile ranked within cohort).")
    lines.append("Media score = CTR + CPM efficiency + CPC efficiency.")
    lines.append("Conv score = D6 conversion rate + ROAS + CAC efficiency.")
    lines.append("KEEP ≥60 | REVIEW 30-59 | REMOVE <30")
    lines.append("")

    for cname in sorted(campaigns, key=lambda c: camps_data[c]["spend"], reverse=True):
        c = camps_data[cname]
        d6pct = f"{c['d6']*100/c['signups']:.1f}%" if c['signups'] else "—"
        roas  = f"{c['rev']/c['spend']:.2f}x" if c['spend'] and c['rev'] else "—"
        cac   = f"₹{int(c['spend']/c['signups']):,}" if c['spend'] and c['signups'] else "—"
        budget = _budget(c['daily_budget'], c['lifetime_budget'])
        lines.append(f"--- CAMPAIGN: {cname} ---")
        lines.append(f"Budget: {budget} | Spend: ₹{int(c['spend']):,} | Ads: {c['ads']}")
        lines.append(f"Signups: {c['signups']:,} | D6: {c['d6']} ({d6pct}) | CAC: {cac} | ROAS: {roas}")
        lines.append(f"Tag breakdown: KEEP {c['keep']} / REVIEW {c['review']} / REMOVE {c['remove']}")
        lines.append("Top ads:")
        for r in campaigns[cname][:5]:
            tag_text, _, _ = _tag(r["composite_score"])
            score = f"{float(r['composite_score']):.0f}" if r["composite_score"] is not None else "—"
            ms    = f"{float(r['media_score']):.0f}"     if r["media_score"]     is not None else "—"
            cs    = f"{float(r['conv_score']):.0f}"      if r["conv_score"]      is not None else "—"
            sgn   = str(int(r["signups"])) if r["signups"] is not None else "—"
            d6c   = str(int(r["d6_conversions"])) if r["d6_conversions"] is not None else "—"
            d6p   = _pct(r["d6_conv_pct"], 1)
            ltv   = _inr(r["avg_ltv_inr"], zero_dash=True)
            cac_a = _inr(r["cac_inr"])
            roas_a = _fmt(r["roas"], ".2f", "x")
            cpm_a  = _inr(r["cpm"])
            ctr_a  = _pct(r["ctr_pct"], 2)
            status = (r["effective_status"] or "").lower()
            name   = (r["ad_name"] or r["ad_id"])[:60]
            lines.append(
                f"  [{tag_text} {score} | M:{ms} C:{cs}] {name} | "
                f"Spend:{_inr(r['spend_inr'])} CTR:{ctr_a} CPM:{cpm_a} | "
                f"Sgn:{sgn} D6:{d6c}({d6p}) LTV:{ltv} CAC:{cac_a} ROAS:{roas_a} | {status}"
            )
        if len(campaigns[cname]) > 5:
            lines.append(f"  ... and {len(campaigns[cname])-5} more ads")
        lines.append("")

    return "\n".join(lines)


def generate_insights(data_summary: str) -> dict:
    """
    Call Claude to generate structured insights.
    Returns dict with keys: executive_summary, campaign_insights, patterns, actions.
    """
    client = anthropic.Anthropic()

    prompt = f"""You are a senior performance marketing analyst reviewing Meta Ads data for Univest,
an Indian fintech app (stock market / options trading education platform).

Here is the full performance data:

{data_summary}

Generate a structured analysis with these exact sections. Be specific — cite ad names,
numbers, and percentages. Keep each bullet to 1-2 lines. Use Indian marketing context
(INR, CAC benchmarks for fintech apps ~₹200-500 is good, >₹800 is concerning,
D6 conversion rate >8% is strong, <3% is weak for this product).

Respond in this exact format:

EXECUTIVE_SUMMARY
• [3-4 bullets covering overall health, biggest wins, biggest concerns]

CAMPAIGN_INSIGHTS
[For each campaign, one paragraph: what's working, what to cut, budget recommendation]
CAMPAIGN: [exact campaign name]
[2-3 sentences]

CREATIVE_PATTERNS
• [4-5 bullets on cross-campaign creative themes — which content types/angles perform, which don't]

RECOMMENDED_ACTIONS
• [5-6 specific, prioritised actions with expected impact]

RISK_FLAGS
• [2-3 things that look anomalous or need investigation]
"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text

    def extract_section(text, start_marker, end_markers):
        start = text.find(start_marker)
        if start == -1:
            return ""
        start += len(start_marker)
        end = len(text)
        for em in end_markers:
            idx = text.find(em, start)
            if idx != -1:
                end = min(end, idx)
        return text[start:end].strip()

    sections = ["EXECUTIVE_SUMMARY", "CAMPAIGN_INSIGHTS", "CREATIVE_PATTERNS",
                "RECOMMENDED_ACTIONS", "RISK_FLAGS"]

    result = {}
    for i, sec in enumerate(sections):
        next_secs = sections[i+1:]
        result[sec.lower()] = extract_section(raw, sec, next_secs)

    return result


def insights_section(insights: dict, campaigns, camps_data, styles) -> list:
    """Build PDF flowables for the LLM Insights page."""
    story = []

    ins_title = ParagraphStyle("ins_title", fontName="Helvetica-Bold", fontSize=11,
                                textColor=C_PRIMARY, spaceBefore=10, spaceAfter=4)
    ins_body  = ParagraphStyle("ins_body", fontName="Helvetica", fontSize=8.5,
                                textColor=C_DARK, leading=13, spaceAfter=3)
    ins_bullet = ParagraphStyle("ins_bullet", fontName="Helvetica", fontSize=8.5,
                                 textColor=C_DARK, leading=13, leftIndent=12,
                                 firstLineIndent=-12, spaceAfter=2)
    camp_head  = ParagraphStyle("camp_head", fontName="Helvetica-Bold", fontSize=8.5,
                                 textColor=C_PRIMARY, spaceBefore=6, spaceAfter=2)

    def render_bullets(text):
        """Parse bullet text and return list of Paragraph flowables."""
        items = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("CAMPAIGN:"):
                items.append(Paragraph(line, camp_head))
            elif line.startswith(("•", "-", "*")):
                items.append(Paragraph(line.lstrip("•-* "), ins_bullet))
            else:
                items.append(Paragraph(line, ins_body))
        return items

    # Executive Summary
    story.append(Paragraph("Executive Summary", ins_title))
    story.append(HRFlowable(width="100%", thickness=1, color=C_MID_BG, spaceAfter=6))
    for p in render_bullets(insights.get("executive_summary", "")):
        story.append(p)
    story.append(Spacer(1, 8))

    # Two-column layout: Creative Patterns | Risk Flags
    def col_block(title, key, bg):
        inner = [Paragraph(title, ParagraphStyle("ct", fontName="Helvetica-Bold",
                                                   fontSize=9, textColor=C_WHITE))]
        for p in render_bullets(insights.get(key, "")):
            inner.append(p)
        tbl = Table([[f] for f in inner], colWidths=[12.5*cm])
        tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0,0), (-1,0), bg),
            ("BACKGROUND",   (0,1), (-1,-1), C_LIGHT_BG),
            ("TOPPADDING",   (0,0), (-1,-1), 5),
            ("BOTTOMPADDING",(0,0), (-1,-1), 5),
            ("LEFTPADDING",  (0,0), (-1,-1), 8),
            ("RIGHTPADDING", (0,0), (-1,-1), 8),
            ("BOX",          (0,0), (-1,-1), 0.5, C_MID_BG),
        ]))
        return tbl

    left_tbl  = col_block("Creative Patterns", "creative_patterns", C_PRIMARY)
    right_tbl = col_block("Risk Flags",        "risk_flags",        C_ACCENT)
    two_col = Table([[left_tbl, right_tbl]], colWidths=[13.5*cm, 13.5*cm])
    two_col.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING",  (0,0), (-1,-1), 0),
        ("RIGHTPADDING", (0,0), (-1,-1), 0),
        ("TOPPADDING",   (0,0), (-1,-1), 0),
        ("BOTTOMPADDING",(0,0), (-1,-1), 0),
    ]))
    story.append(two_col)
    story.append(Spacer(1, 10))

    # Recommended Actions
    story.append(Paragraph("Recommended Actions", ins_title))
    story.append(HRFlowable(width="100%", thickness=1, color=C_MID_BG, spaceAfter=6))
    for p in render_bullets(insights.get("recommended_actions", "")):
        story.append(p)
    story.append(Spacer(1, 10))

    # Campaign Insights
    story.append(Paragraph("Campaign-Level Insights", ins_title))
    story.append(HRFlowable(width="100%", thickness=1, color=C_MID_BG, spaceAfter=6))
    for p in render_bullets(insights.get("campaign_insights", "")):
        story.append(p)

    return story


async def fetch_data():
    from services.shared.db import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        result = await session.execute(QUERY, {
            "cohort_since": COHORT_SINCE, "cohort_until": COHORT_UNTIL,
            "media_since":  MEDIA_SINCE,  "media_until":  MEDIA_UNTIL,
            "attr_since":   ATTR_SINCE,   "attr_until":   ATTR_UNTIL,
        })
        return result.mappings().all()


async def main():
    print("Fetching data...")
    rows = await fetch_data()
    print(f"  {len(rows)} ads across campaigns")

    # ── Build grouped data ────────────────────────────────────────────────────
    campaigns = defaultdict(list)
    camp_meta = {}
    for r in rows:
        cname = r["campaign_name"] or r["campaign_id"]
        campaigns[cname].append(r)
        if cname not in camp_meta:
            camp_meta[cname] = {
                "daily_budget":    r["daily_budget"],
                "lifetime_budget": r["lifetime_budget"],
            }

    def camp_stats(ads):
        spend   = sum(float(r["spend_inr"] or 0)        for r in ads)
        signups = sum(int(r["signups"] or 0)             for r in ads)
        d6      = sum(int(r["d6_conversions"] or 0)      for r in ads)
        trials  = sum(int(r["d0_trials"] or 0)           for r in ads)
        rev     = sum(float(r["avg_ltv_inr"] or 0) * int(r["signups"] or 0) for r in ads)
        keep    = sum(1 for r in ads if r["composite_score"] and float(r["composite_score"]) >= 60)
        review  = sum(1 for r in ads if r["composite_score"] and 30 <= float(r["composite_score"]) < 60)
        remove  = sum(1 for r in ads if r["composite_score"] is None or float(r["composite_score"]) < 30)
        return dict(spend=spend, signups=signups, d6=d6, trials=trials, rev=rev,
                    keep=keep, review=review, remove=remove, ads=len(ads),
                    daily_budget=camp_meta[cname]["daily_budget"],
                    lifetime_budget=camp_meta[cname]["lifetime_budget"])

    camps_data = {cname: camp_stats(ads) for cname, ads in campaigns.items()}

    total_spend   = sum(c["spend"]   for c in camps_data.values())
    total_signups = sum(c["signups"] for c in camps_data.values())
    total_d6      = sum(c["d6"]      for c in camps_data.values())
    total_rev     = sum(c["rev"]     for c in camps_data.values())

    # ── Build PDF ─────────────────────────────────────────────────────────────
    print("Building PDF...")
    doc = SimpleDocTemplate(
        OUTPUT_PATH,
        pagesize=landscape(A4),
        leftMargin=1.2*cm, rightMargin=1.2*cm,
        topMargin=1.5*cm,  bottomMargin=1.5*cm,
    )

    styles = build_styles()
    story  = []

    # ── Cover / title block ───────────────────────────────────────────────────
    title_data = [[
        Paragraph("Univest Ads Performance Report", styles["title"]),
        Paragraph(
            f"Apr 10–15, 2026 cohort  ·  Media & attribution through Apr 22  ·  "
            f"Generated {datetime.now().strftime('%d %b %Y, %H:%M')}",
            styles["subtitle"],
        ),
    ]]
    title_tbl = Table(title_data, colWidths=[28*cm])
    title_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), C_DARK),
        ("TOPPADDING",   (0,0), (-1,-1), 14),
        ("BOTTOMPADDING",(0,0), (-1,-1), 14),
        ("LEFTPADDING",  (0,0), (-1,-1), 16),
        ("RIGHTPADDING", (0,0), (-1,-1), 16),
    ]))
    story.append(title_tbl)
    story.append(Spacer(1, 10))

    # ── Global KPI row ────────────────────────────────────────────────────────
    d6pct_global = f"{total_d6*100/total_signups:.1f}%" if total_signups else "—"
    roas_global  = f"{total_rev/total_spend:.2f}x"      if total_spend  else "—"
    cac_global   = f"₹{int(total_spend/total_signups):,}" if total_signups else "—"

    kpi_style = ParagraphStyle("kpi_v", fontName="Helvetica-Bold", fontSize=16,
                                textColor=C_PRIMARY, alignment=TA_CENTER)
    kpi_lbl   = ParagraphStyle("kpi_l", fontName="Helvetica", fontSize=8,
                                textColor=C_GREY_TEXT, alignment=TA_CENTER)
    kpis = [
        (f"₹{int(total_spend):,}", "Total Spend (Apr 10–22)"),
        (f"{len(rows)}", "Active Ads"),
        (f"{len(campaigns)}", "Campaigns"),
        (f"{total_signups:,}", "Signups"),
        (f"{total_d6:,}", "D6 Conversions"),
        (d6pct_global, "D6 Rate"),
        (cac_global, "Blended CAC"),
        (roas_global, "Blended ROAS"),
    ]
    kpi_cells = [[Paragraph(v, kpi_style), Paragraph(l, kpi_lbl)] for v, l in kpis]
    kpi_tbl = Table([kpi_cells[0::1]], colWidths=[3.5*cm]*8)
    kpi_tbl = Table([[Table([[Paragraph(v, kpi_style)], [Paragraph(l, kpi_lbl)]],
                             colWidths=[3.4*cm]) for v, l in kpis]],
                     colWidths=[3.5*cm]*8)
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), C_LIGHT_BG),
        ("BOX",          (0,0), (-1,-1), 0.5, C_MID_BG),
        ("INNERGRID",    (0,0), (-1,-1), 0.5, C_MID_BG),
        ("TOPPADDING",   (0,0), (-1,-1), 8),
        ("BOTTOMPADDING",(0,0), (-1,-1), 8),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
    ]))
    story.append(kpi_tbl)
    story.append(Spacer(1, 12))

    # ── Scoring legend ────────────────────────────────────────────────────────
    legend_style = ParagraphStyle("legend", fontName="Helvetica", fontSize=8,
                                   textColor=C_GREY_TEXT)
    story.append(Paragraph(
        "Scoring formula (percentile-ranked within cohort):  "
        "Media score = 0.35×CTR + 0.35×(1−CPM) + 0.30×(1−CPC)  ·  "
        "Conv score = 0.50×D6% + 0.30×ROAS + 0.20×(1−CAC)  ·  "
        "Composite = 0.50×Media + 0.50×Conv  ·  "
        "<font color='#1a7a4a'><b>KEEP ≥60</b></font>   "
        "<font color='#856404'><b>REVIEW 30–59</b></font>   "
        "<font color='#721c24'><b>REMOVE &lt;30</b></font>",
        legend_style,
    ))
    story.append(Spacer(1, 10))

    # ── LLM Insights ─────────────────────────────────────────────────────────
    print("Generating LLM insights (calling Claude)...")
    data_summary = build_data_summary(campaigns, camps_data)
    insights = generate_insights(data_summary)
    print("  Insights generated.")

    story.append(Paragraph("AI Insights", styles["section"]))
    story.append(HRFlowable(width="100%", thickness=1.5, color=C_ACCENT, spaceAfter=8))
    for flowable in insights_section(insights, campaigns, camps_data, styles):
        story.append(flowable)
    story.append(PageBreak())

    # ── Campaign summary table ────────────────────────────────────────────────
    story.append(Paragraph("Campaign Overview", styles["section"]))
    story.append(summary_table(camps_data, styles))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "Spend = Apr 10–22  ·  Signups / D6 / LTV / CAC / ROAS = attribution window Apr 10–22  ·  "
        "LTV reflects early revenue only (≤12 days post-signup)",
        styles["note"],
    ))

    # ── Per-campaign detail ───────────────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("Ad-Level Detail by Campaign", styles["section"]))
    story.append(Paragraph(
        "Score shown as Composite (Media/Conv).  "
        "KEEP ≥60  ·  REVIEW 30–59  ·  REMOVE <30  ·  "
        "LTV = avg revenue per signup (early estimate)",
        styles["note"],
    ))
    story.append(Spacer(1, 4))

    for cname in sorted(campaigns, key=lambda c: camps_data[c]["spend"], reverse=True):
        ad_rows = campaigns[cname]
        story.append(header_block(cname, camps_data[cname], styles))
        story.append(Spacer(1, 3))
        story.append(ads_table(ad_rows, styles))
        story.append(Spacer(1, 12))

    # ── Build ─────────────────────────────────────────────────────────────────
    doc.build(story)
    print(f"\nSaved: {OUTPUT_PATH}")


asyncio.run(main())
