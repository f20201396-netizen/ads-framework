"""
iOS Meta Ads — current active campaigns report.
Saves to ~/Downloads/univest_ios_ads_report.pdf

Attribution source: Meta pixel actions JSONB (insights_daily.actions)
— Singular iOS data is sparse, so we use Meta's own pixel funnel:
  mobile_app_install  → installs
  add_payment_info    → payment intent (subscription funnel entry)
  purchase            → completed purchase

Scoring (percentile-ranked within iOS cohort):
  media_score = 0.35*CTR + 0.35*(1-CPM) + 0.30*(1-CPC)
  conv_score  = 0.40*(1-CPInstall) + 0.35*payment_rate + 0.25*purchase_rate
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

OUTPUT_PATH = "/Users/macbook/Downloads/univest_ios_ads_report.pdf"

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
C_IOS       = colors.HexColor("#147EFB")   # Apple blue

# ── SQL ───────────────────────────────────────────────────────────────────────
# Extract 7d_click value for a given action_type from the actions JSONB array.
# `add_payment_info` and `fb_mobile_add_payment_info` are the same event —
# use whichever is present. Same for purchase variants.
QUERY = text("""
WITH
-- Active iOS non-retargeting ads
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
    WHERE a.effective_status = 'ACTIVE'
      AND c.effective_status = 'ACTIVE'
      AND c.name ILIKE '%ios%'
      AND c.name NOT ILIKE '%Retar%'
),

-- Media + pixel funnel aggregated over all available data
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
        END AS cpc,

        -- App installs (7d_click)
        SUM(COALESCE((
            SELECT (elem->>'7d_click')::numeric
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(i.actions) = 'array' THEN i.actions ELSE '[]'::jsonb END) elem
            WHERE elem->>'action_type' = 'mobile_app_install'
            LIMIT 1
        ), 0)) AS installs,

        -- Add payment info (7d_click) — subscription funnel entry
        SUM(COALESCE((
            SELECT (elem->>'7d_click')::numeric
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(i.actions) = 'array' THEN i.actions ELSE '[]'::jsonb END) elem
            WHERE elem->>'action_type' = 'add_payment_info'
            LIMIT 1
        ), 0)) AS add_payment_info,

        -- Purchases (7d_click)
        SUM(COALESCE((
            SELECT (elem->>'7d_click')::numeric
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(i.actions) = 'array' THEN i.actions ELSE '[]'::jsonb END) elem
            WHERE elem->>'action_type' = 'purchase'
            LIMIT 1
        ), 0)) AS purchases,

        -- App store visits (total, not 7d_click — these fire on view)
        SUM(COALESCE((
            SELECT (elem->>'value')::numeric
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(i.actions) = 'array' THEN i.actions ELSE '[]'::jsonb END) elem
            WHERE elem->>'action_type' = 'app_store_visit'
            LIMIT 1
        ), 0)) AS app_store_visits

    FROM insights_daily i
    WHERE i.attribution_window = '7d_click'
      AND i.spend > 0
    GROUP BY i.ad_id
),

-- Join and derive funnel rates
joined AS (
    SELECT
        co.*,
        m.first_date, m.last_date, m.days_active,
        m.spend, m.impressions, m.clicks,
        m.ctr, m.cpm, m.cpc,
        m.installs, m.add_payment_info, m.purchases, m.app_store_visits,
        -- Rates (as %)
        CASE WHEN m.installs > 0
             THEN ROUND(m.add_payment_info * 100.0 / m.installs, 1)
        END AS payment_rate,
        CASE WHEN m.installs > 0
             THEN ROUND(m.purchases * 100.0 / m.installs, 1)
        END AS purchase_rate,
        -- Cost per install
        CASE WHEN m.installs > 0
             THEN ROUND(m.spend / m.installs, 0)
        END AS cpi,
        -- CTR to store
        CASE WHEN m.clicks > 0
             THEN ROUND(m.app_store_visits * 100.0 / m.clicks, 1)
        END AS click_to_store_rate
    FROM cohort co
    LEFT JOIN media m ON m.ad_id = co.ad_id
),

-- Percentile ranks within active iOS cohort
ranked AS (
    SELECT *,
        PERCENT_RANK() OVER (ORDER BY ctr           ASC  NULLS LAST) AS ctr_pct,
        PERCENT_RANK() OVER (ORDER BY cpm           DESC NULLS LAST) AS cpm_pct,
        PERCENT_RANK() OVER (ORDER BY cpc           DESC NULLS LAST) AS cpc_pct,
        PERCENT_RANK() OVER (ORDER BY cpi           DESC NULLS LAST) AS cpi_pct,
        PERCENT_RANK() OVER (ORDER BY payment_rate  ASC  NULLS LAST) AS pay_pct,
        PERCENT_RANK() OVER (ORDER BY purchase_rate ASC  NULLS LAST) AS pur_pct
    FROM joined
),

scored AS (
    SELECT *,
        CASE WHEN ctr IS NOT NULL THEN
            ROUND((0.35*ctr_pct + 0.35*cpm_pct + 0.30*cpc_pct)::numeric * 100, 1)
        END AS media_score,
        CASE WHEN payment_rate IS NOT NULL OR purchase_rate IS NOT NULL THEN
            ROUND((
                0.40*COALESCE(cpi_pct, 0.5) +
                0.35*COALESCE(pay_pct, 0.5) +
                0.25*COALESCE(pur_pct, 0.5)
            )::numeric * 100, 1)
        END AS conv_score
    FROM ranked
)

SELECT
    ad_id, ad_name, adset_name, campaign_id, campaign_name,
    daily_budget, lifetime_budget, effective_status, created_date,
    first_date, last_date, days_active,
    spend, impressions, clicks, ctr, cpm, cpc,
    installs, add_payment_info, purchases, app_store_visits,
    payment_rate, purchase_rate, cpi, click_to_store_rate,
    media_score, conv_score,
    ROUND((
        0.50 * COALESCE(media_score, conv_score, 0) +
        0.50 * COALESCE(conv_score, media_score, 0)
    )::numeric, 1) AS composite_score
FROM scored
ORDER BY campaign_name,
         CASE WHEN spend IS NULL OR spend = 0 THEN 1 ELSE 0 END,
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
    TH = ParagraphStyle("th", fontName="Helvetica-Bold", fontSize=8, textColor=C_WHITE, alignment=TA_CENTER)
    TR = ParagraphStyle("tr", fontName="Helvetica-Bold", fontSize=8, textColor=C_WHITE, alignment=TA_RIGHT)
    headers = [
        Paragraph("Campaign", TH), Paragraph("Budget", TR), Paragraph("Ads", TR),
        Paragraph("Spend", TR), Paragraph("Installs", TR), Paragraph("CPI", TR),
        Paragraph("Pay%", TR), Paragraph("Pur%", TR), Paragraph("K/R/X", TR),
    ]
    rows = [headers]
    for cname, c in sorted(camps_data.items(), key=lambda x: x[1]["spend"], reverse=True):
        pay_r = _pct(c["add_payment_info"] * 100 / c["installs"] if c["installs"] else None)
        pur_r = _pct(c["purchases"] * 100 / c["installs"] if c["installs"] else None)
        cpi   = _inr(c["spend"] / c["installs"] if c["installs"] else None)
        krx   = f"{c['keep']} / {c['review']} / {c['remove']}"
        s = styles
        rows.append([
            Paragraph(cname[:60], s["cell"]),
            Paragraph(_budget(c["daily_budget"], c["lifetime_budget"]), s["cell_r"]),
            Paragraph(str(c["ads"]), s["cell_r"]),
            Paragraph(_inr(c["spend"]), s["cell_r"]),
            Paragraph(_n(c["installs"]), s["cell_r"]),
            Paragraph(cpi, s["cell_r"]),
            Paragraph(pay_r, s["cell_r"]),
            Paragraph(pur_r, s["cell_r"]),
            Paragraph(krx, s["cell_r"]),
        ])
    col_w = [9.5*cm, 2.8*cm, 1*cm, 2.4*cm, 2*cm, 2*cm, 1.6*cm, 1.6*cm, 2*cm]
    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,0),  C_IOS),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_LIGHT_BG]),
        ("GRID",         (0,0), (-1,-1), 0.4, C_MID_BG),
        ("TOPPADDING",   (0,0), (-1,-1), 4), ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ("LEFTPADDING",  (0,0), (-1,-1), 5), ("RIGHTPADDING", (0,0),(-1,-1), 5),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
    ]))
    return tbl


def ads_table(ad_rows, styles):
    TH = ParagraphStyle("th2", fontName="Helvetica-Bold", fontSize=7, textColor=C_WHITE, alignment=TA_CENTER)
    TR = ParagraphStyle("tr2", fontName="Helvetica-Bold", fontSize=7, textColor=C_WHITE, alignment=TA_RIGHT)
    headers = [
        Paragraph("Ad Name", TH),
        Paragraph("Score\n(M/C)", TR),
        Paragraph("Tag", TR),
        Paragraph("Spend", TR),
        Paragraph("Imp", TR),
        Paragraph("CTR", TR),
        Paragraph("CPM", TR),
        Paragraph("CPC", TR),
        Paragraph("Installs", TR),
        Paragraph("CPI", TR),
        Paragraph("Pay\nInfo", TR),
        Paragraph("Pay%", TR),
        Paragraph("Purch", TR),
        Paragraph("Pur%", TR),
        Paragraph("Store\nVisits", TR),
        Paragraph("Days", TR),
    ]
    rows = [headers]
    tag_meta = []

    for i, r in enumerate(ad_rows, 1):
        tag_text, tag_fg, tag_bg = _tag(r["composite_score"])
        cs  = f"{float(r['composite_score']):.0f}" if r["composite_score"] is not None else "—"
        ms  = f"{float(r['media_score']):.0f}"     if r["media_score"]     is not None else "—"
        cvs = f"{float(r['conv_score']):.0f}"      if r["conv_score"]      is not None else "—"
        name = (r["ad_name"] or r["ad_id"])[:50]
        s = styles
        rows.append([
            Paragraph(name, s["cell"]),
            Paragraph(f"{cs}\n({ms}/{cvs})", s["cell_r"]),
            Paragraph(tag_text, s["cell_r"]),
            Paragraph(_inr(r["spend"]), s["cell_r"]),
            Paragraph(f"{int(float(r['impressions'])):,}" if r["impressions"] else "—", s["cell_r"]),
            Paragraph(_pct(r["ctr"], 2), s["cell_r"]),
            Paragraph(_inr(r["cpm"]), s["cell_r"]),
            Paragraph(_inr(r["cpc"]), s["cell_r"]),
            Paragraph(_n(r["installs"]), s["cell_r"]),
            Paragraph(_inr(r["cpi"]), s["cell_r"]),
            Paragraph(_n(r["add_payment_info"]), s["cell_r"]),
            Paragraph(_pct(r["payment_rate"]), s["cell_r"]),
            Paragraph(_n(r["purchases"]), s["cell_r"]),
            Paragraph(_pct(r["purchase_rate"]), s["cell_r"]),
            Paragraph(_n(r["app_store_visits"]), s["cell_r"]),
            Paragraph(str(int(r["days_active"])) if r["days_active"] else "—", s["cell_r"]),
        ])
        tag_meta.append((i, tag_fg, tag_bg, r["composite_score"]))

    col_w = [6.5*cm, 1.4*cm, 1.4*cm, 1.8*cm, 1.8*cm, 1.1*cm, 1.4*cm, 1.2*cm,
             1.6*cm, 1.5*cm, 1.2*cm, 1.2*cm, 1.2*cm, 1.2*cm, 1.6*cm, 1*cm]
    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    cmds = [
        ("BACKGROUND",   (0,0), (-1,0),  C_DARK),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_LIGHT_BG]),
        ("GRID",         (0,0), (-1,-1), 0.3, C_MID_BG),
        ("TOPPADDING",   (0,0), (-1,-1), 3), ("BOTTOMPADDING",(0,0),(-1,-1), 3),
        ("LEFTPADDING",  (0,0), (-1,-1), 4), ("RIGHTPADDING", (0,0),(-1,-1), 4),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
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
    installs = int(c["installs"]) if c["installs"] else 0
    pay_r    = f"{c['add_payment_info']*100/installs:.1f}%" if installs else "—"
    pur_r    = f"{c['purchases']*100/installs:.1f}%"        if installs else "—"
    cpi      = _inr(c["spend"] / installs) if installs else "—"
    budget   = _budget(c["daily_budget"], c["lifetime_budget"])
    meta = (
        f"Budget: {budget}  |  Spend: {_inr(c['spend'])}  |  Ads: {c['ads']}  |  "
        f"Installs: {_n(c['installs'])}  |  CPI: {cpi}  |  "
        f"Pay info: {_n(c['add_payment_info'])} ({pay_r})  |  "
        f"Purchases: {_n(c['purchases'])} ({pur_r})  |  "
        f"KEEP {c['keep']} / REVIEW {c['review']} / REMOVE {c['remove']}"
    )
    tbl = Table([
        [Paragraph(cname, styles["camp_name"])],
        [Paragraph(meta,  styles["camp_meta"])],
    ], colWidths=[27*cm])
    tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), C_IOS),
        ("TOPPADDING",   (0,0), (-1,-1), 6), ("BOTTOMPADDING",(0,0),(-1,-1), 5),
        ("LEFTPADDING",  (0,0), (-1,-1), 10), ("RIGHTPADDING", (0,0),(-1,-1), 10),
    ]))
    return tbl


# ── LLM Insights ─────────────────────────────────────────────────────────────
def build_summary(campaigns, camps_data) -> str:
    lines = ["=== UNIVEST iOS META ADS — CURRENT ACTIVE CAMPAIGNS ===",
             f"Report date: {datetime.now().strftime('%d %b %Y')}",
             "Attribution: Meta pixel (7d_click) — Singular iOS data excluded (sparse)",
             "Funnel: Impression → Click → App Store Visit → Install → Add Payment Info → Purchase",
             ""]
    total_spend    = sum(c["spend"]            for c in camps_data.values())
    total_installs = sum(c["installs"]         for c in camps_data.values())
    total_pay      = sum(c["add_payment_info"] for c in camps_data.values())
    total_pur      = sum(c["purchases"]        for c in camps_data.values())
    lines.append(f"Total spend: ₹{int(total_spend):,}  |  Installs: {int(total_installs):,}  |  "
                 f"CPI: {_inr(total_spend/total_installs) if total_installs else '—'}  |  "
                 f"Add payment info: {int(total_pay):,}  |  Purchases: {int(total_pur):,}")
    lines.append(f"Overall payment rate: {total_pay*100/total_installs:.1f}% install→payment  |  "
                 f"Purchase rate: {total_pur*100/total_installs:.1f}% install→purchase"
                 if total_installs else "")
    lines.append("")
    for cname, c in sorted(camps_data.items(), key=lambda x: x[1]["spend"], reverse=True):
        ins = int(c["installs"]) if c["installs"] else 0
        pay_r = f"{c['add_payment_info']*100/ins:.1f}%" if ins else "—"
        pur_r = f"{c['purchases']*100/ins:.1f}%" if ins else "—"
        cpi   = f"₹{int(c['spend']/ins):,}" if ins else "—"
        lines.append(f"--- CAMPAIGN: {cname} ---")
        lines.append(f"Budget: {_budget(c['daily_budget'], c['lifetime_budget'])}  |  "
                     f"Spend: ₹{int(c['spend']):,}  |  Ads: {c['ads']}")
        lines.append(f"Installs: {ins:,}  |  CPI: {cpi}  |  "
                     f"Pay info: {int(c['add_payment_info'])} ({pay_r})  |  "
                     f"Purchases: {int(c['purchases'])} ({pur_r})")
        lines.append(f"KEEP {c['keep']} / REVIEW {c['review']} / REMOVE {c['remove']}")
        lines.append("Top ads:")
        for r in campaigns[cname][:6]:
            tag, _, _ = _tag(r["composite_score"])
            sc  = f"{float(r['composite_score']):.0f}" if r["composite_score"] is not None else "—"
            ms  = f"{float(r['media_score']):.0f}"     if r["media_score"]     is not None else "—"
            cvs = f"{float(r['conv_score']):.0f}"      if r["conv_score"]      is not None else "—"
            lines.append(
                f"  [{tag} {sc}|M:{ms} C:{cvs}] {(r['ad_name'] or r['ad_id'])[:55]} | "
                f"Spend:{_inr(r['spend'])} CTR:{_pct(r['ctr'],2)} CPM:{_inr(r['cpm'])} | "
                f"Inst:{_n(r['installs'])} CPI:{_inr(r['cpi'])} "
                f"Pay:{_n(r['add_payment_info'])}({_pct(r['payment_rate'])}) "
                f"Pur:{_n(r['purchases'])}({_pct(r['purchase_rate'])}) | "
                f"{(r['effective_status'] or '').lower()}"
            )
        if len(campaigns[cname]) > 6:
            lines.append(f"  ... and {len(campaigns[cname])-6} more ads")
        lines.append("")
    return "\n".join(lines)


def generate_insights(summary: str) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not _HAS_ANTHROPIC or not api_key:
        msg = ("ANTHROPIC_API_KEY not set — add it to .env or pass inline.\n"
               "Run: ANTHROPIC_API_KEY=sk-ant-... .venv/bin/python3 -u scripts/export_ios_report_pdf.py")
        print(f"\n[WARN] {msg}")
        placeholder = "• AI Insights unavailable — ANTHROPIC_API_KEY not configured."
        return {
            "executive_summary": placeholder,
            "campaign_insights": placeholder,
            "creative_patterns": placeholder,
            "recommended_actions": placeholder,
            "risk_flags": "• Set ANTHROPIC_API_KEY in .env to enable AI analysis.",
        }
    client = _anthropic.Anthropic(api_key=api_key)
    prompt = f"""You are a senior performance marketing analyst for Univest, an Indian fintech app
(stock market / options trading education, subscription ~₹500-2000/month).

Here is the iOS Meta Ads performance data:

{summary}

Context:
- iOS attribution is via Meta pixel only (Singular doesn't reliably track iOS installs)
- Funnel: Ad → Install → Add Payment Info (subscription intent) → Purchase
- Good iOS CPI for Indian fintech: ₹300-600. Above ₹1,000 is expensive.
- Payment info rate > 15% of installs is strong. < 5% is weak.
- Purchase rate > 3% of installs is decent. > 8% is strong.
- KEEP ≥60 | REVIEW 30-59 | REMOVE <30 (percentile scores)

Respond in this EXACT format (no extra headers):

EXECUTIVE_SUMMARY
• [3-4 bullets: overall iOS efficiency, key wins, key concerns]

CAMPAIGN_INSIGHTS
CAMPAIGN: [exact campaign name]
[2-3 sentences: what's working, what to pause, budget recommendation]

CREATIVE_PATTERNS
• [4-5 bullets: which creative types/angles drive installs vs payment conversions, patterns across campaigns]

RECOMMENDED_ACTIONS
• [5-6 specific prioritised actions with expected impact]

RISK_FLAGS
• [2-3 anomalies or things needing investigation]
"""
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
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

    secs = ["EXECUTIVE_SUMMARY","CAMPAIGN_INSIGHTS","CREATIVE_PATTERNS",
            "RECOMMENDED_ACTIONS","RISK_FLAGS"]
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

    # Two-column: Creative Patterns | Risk Flags
    def col_block(title, key, bg):
        inner = [Paragraph(title, ParagraphStyle("ch", fontName="Helvetica-Bold",
                                                   fontSize=9, textColor=C_WHITE))]
        inner += bullets(ins.get(key,""))
        t = Table([[f] for f in inner], colWidths=[12.5*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",   (0,0),(-1,0),  bg),
            ("BACKGROUND",   (0,1),(-1,-1), C_LIGHT_BG),
            ("TOPPADDING",   (0,0),(-1,-1), 5), ("BOTTOMPADDING",(0,0),(-1,-1),5),
            ("LEFTPADDING",  (0,0),(-1,-1), 8), ("RIGHTPADDING", (0,0),(-1,-1),8),
            ("BOX",          (0,0),(-1,-1), 0.5, C_MID_BG),
        ]))
        return t
    two = Table([[col_block("Creative Patterns", "creative_patterns", C_IOS),
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
        r = await s.execute(QUERY)
        return r.mappings().all()


async def main():
    print("Fetching iOS ad data...")
    rows = await fetch()
    if not rows:
        print("No active iOS ads with spend found.")
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

    def c_stats(ads):
        return dict(
            spend            = sum(float(r["spend"] or 0)            for r in ads),
            installs         = sum(float(r["installs"] or 0)         for r in ads),
            add_payment_info = sum(float(r["add_payment_info"] or 0) for r in ads),
            purchases        = sum(float(r["purchases"] or 0)        for r in ads),
            app_store_visits = sum(float(r["app_store_visits"] or 0) for r in ads),
            ads              = len(ads),
            keep   = sum(1 for r in ads if r["composite_score"] and float(r["composite_score"]) >= 60),
            review = sum(1 for r in ads if r["composite_score"] and 30 <= float(r["composite_score"]) < 60),
            remove = sum(1 for r in ads if r["composite_score"] is None or float(r["composite_score"]) < 30),
            daily_budget    = camp_meta[list(camps_data.keys())[0]]["daily_budget"] if False else None,
            lifetime_budget = None,
        )

    # Build camps_data with meta
    camps_data = {}
    for cname, ads in campaigns.items():
        cs = dict(
            spend            = sum(float(r["spend"] or 0)            for r in ads),
            installs         = sum(float(r["installs"] or 0)         for r in ads),
            add_payment_info = sum(float(r["add_payment_info"] or 0) for r in ads),
            purchases        = sum(float(r["purchases"] or 0)        for r in ads),
            app_store_visits = sum(float(r["app_store_visits"] or 0) for r in ads),
            ads              = len(ads),
            keep   = sum(1 for r in ads if r["composite_score"] and float(r["composite_score"]) >= 60),
            review = sum(1 for r in ads if r["composite_score"] and 30 <= float(r["composite_score"]) < 60),
            remove = sum(1 for r in ads if r["composite_score"] is None or float(r["composite_score"]) < 30),
            daily_budget    = camp_meta[cname]["daily_budget"],
            lifetime_budget = camp_meta[cname]["lifetime_budget"],
        )
        camps_data[cname] = cs

    total_spend    = sum(c["spend"]            for c in camps_data.values())
    total_installs = sum(c["installs"]         for c in camps_data.values())
    total_pay      = sum(c["add_payment_info"] for c in camps_data.values())
    total_pur      = sum(c["purchases"]        for c in camps_data.values())

    # Insight dates
    all_first = min(r["first_date"] for r in rows if r["first_date"])
    all_last  = max(r["last_date"]  for r in rows if r["last_date"])

    # LLM insights
    print("Generating AI insights...")
    summary  = build_summary(campaigns, camps_data)
    insights = generate_insights(summary)
    print("  Done.")

    # Build PDF
    print("Building PDF...")
    styles = build_styles()
    doc = SimpleDocTemplate(OUTPUT_PATH, pagesize=landscape(A4),
                            leftMargin=1.2*cm, rightMargin=1.2*cm,
                            topMargin=1.5*cm,  bottomMargin=1.5*cm)
    story = []

    # Title banner
    t = Table([[Paragraph("Univest — iOS Meta Ads Report", styles["title"])],
               [Paragraph(f"Active campaigns  ·  Data {all_first} → {all_last}  ·  "
                           f"Attribution: Meta pixel (7d_click)  ·  "
                           f"Generated {datetime.now().strftime('%d %b %Y, %H:%M')}",
                           styles["subtitle"])]],
              colWidths=[28*cm])
    t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),C_DARK),
                            ("TOPPADDING",(0,0),(-1,-1),14), ("BOTTOMPADDING",(0,0),(-1,-1),14),
                            ("LEFTPADDING",(0,0),(-1,-1),16), ("RIGHTPADDING",(0,0),(-1,-1),16)]))
    story.append(t)
    story.append(Spacer(1, 10))

    # KPI tiles
    KV = ParagraphStyle("kv", fontName="Helvetica-Bold", fontSize=16, textColor=C_IOS, alignment=TA_CENTER)
    KL = ParagraphStyle("kl", fontName="Helvetica",      fontSize=8,  textColor=C_GREY, alignment=TA_CENTER)
    cpi_g = _inr(total_spend / total_installs) if total_installs else "—"
    pay_r = f"{total_pay*100/total_installs:.1f}%" if total_installs else "—"
    pur_r = f"{total_pur*100/total_installs:.1f}%" if total_installs else "—"
    kpis = [
        (f"₹{int(total_spend):,}", "Total Spend"),
        (str(len(rows)),           "Active Ads"),
        (str(len(campaigns)),      "Campaigns"),
        (f"{int(total_installs):,}", "Installs"),
        (cpi_g,                    "Blended CPI"),
        (f"{int(total_pay):,}",    "Add Payment Info"),
        (pay_r,                    "Payment Rate"),
        (f"{int(total_pur):,}",    "Purchases"),
        (pur_r,                    "Purchase Rate"),
    ]
    kpi_row = [[Table([[Paragraph(v,KV)],[Paragraph(l,KL)]], colWidths=[3*cm]) for v,l in kpis]]
    kpi_tbl = Table(kpi_row, colWidths=[3.1*cm]*9)
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),C_LIGHT_BG), ("BOX",(0,0),(-1,-1),0.5,C_MID_BG),
        ("INNERGRID",(0,0),(-1,-1),0.5,C_MID_BG),
        ("TOPPADDING",(0,0),(-1,-1),8), ("BOTTOMPADDING",(0,0),(-1,-1),8),
    ]))
    story.append(kpi_tbl)
    story.append(Spacer(1, 8))

    # Legend
    story.append(Paragraph(
        "Scoring (percentile-ranked within iOS cohort):  "
        "Media = 0.35×CTR + 0.35×(1−CPM) + 0.30×(1−CPC)  ·  "
        "Conv = 0.40×(1−CPI) + 0.35×Payment% + 0.25×Purchase%  ·  "
        "Composite = 0.50×Media + 0.50×Conv  ·  "
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

    # Campaign summary table
    story.append(Paragraph("Campaign Overview", styles["section"]))
    story.append(summary_table(camps_data, styles))
    story.append(Spacer(1, 5))
    story.append(Paragraph(
        "Funnel: Install = mobile_app_install (7d_click)  ·  "
        "Pay info = add_payment_info (7d_click)  ·  Purchase = purchase (7d_click)  ·  "
        "CPI = cost per install",
        styles["note"],
    ))
    story.append(PageBreak())

    # Per-campaign ad tables
    story.append(Paragraph("Ad-Level Detail by Campaign", styles["section"]))
    story.append(Paragraph(
        "Score shown as Composite (Media/Conv)  ·  "
        "Pay% = add_payment_info / installs  ·  Pur% = purchases / installs",
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
