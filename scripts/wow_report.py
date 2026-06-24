#!/usr/bin/env python3
"""
4-week Week-on-Week campaign metrics report.

Metrics per campaign per week:
  Spend ₹, D6 CAC ₹, D6 ROAS, FB CAC ₹ (Meta CPI), FB ROAS (Meta purchase ROAS)

Weeks are rolling 7-day buckets ending yesterday:
  W1 = most recent (today-7 … today-1)
  W4 = oldest      (today-28 … today-22)

Usage:
    .venv/bin/python3 scripts/wow_report.py
    .venv/bin/python3 scripts/wow_report.py --sheet-id <SHEET_ID>
    .venv/bin/python3 scripts/wow_report.py --sheet-id <SHEET_ID> --tab "WoW Campaigns"
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))

DB_DSN               = "postgresql://macbook@localhost/meta_ads"
SERVICE_ACCOUNT_FILE = "/Users/macbook/Downloads/univest-applications-d51f19bb3ffc.json"
GST                  = 1.18


def db_conn():
    return psycopg2.connect(DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()


# ── Week boundaries ───────────────────────────────────────────────────────────

def _make_weeks(today: date, n: int = 4):
    """Return list of (label, start, end) oldest-first.
    Most recent week (W4) is 5 days (ends today-3); older weeks are 7 days each."""
    weeks = []
    for w in range(n):
        if w == 0:  # most recent week: 5 days
            end   = today - timedelta(days=3)
            start = today - timedelta(days=7)
        else:
            end   = today - timedelta(days=1 + w * 7)
            start = today - timedelta(days=7 + w * 7)
        label = f"W{n - w} ({start.strftime('%d %b')}–{end.strftime('%d %b')})"
        weeks.append((label, start, end))
    weeks.reverse()
    return weeks


# ── SQL ───────────────────────────────────────────────────────────────────────

MEDIA_SQL = """
WITH base AS (
    SELECT
        i.campaign_id,
        i.spend,
        COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(i.conversions)='array' THEN i.conversions ELSE '[]'::jsonb END) a
            WHERE a->>'action_type' = 'subscribe_total'
        ), 0) AS results,
        COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(i.action_values)='array' THEN i.action_values ELSE '[]'::jsonb END) a
            WHERE a->>'action_type' = 'omni_purchase'
        ), 0) AS purchase_revenue,
        CASE
            WHEN i.date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
            WHEN i.date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
            WHEN i.date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
            WHEN i.date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
        END AS wk
    FROM insights_daily i
    WHERE i.attribution_window = '7d_click'
      AND i.date BETWEEN %(w1s)s AND %(w4e)s
)
SELECT
    b.campaign_id,
    c.name                                AS campaign_name,
    b.wk,
    ROUND(SUM(b.spend)::numeric, 0)       AS spend,
    SUM(b.results)                        AS results,
    SUM(b.purchase_revenue)               AS purchase_revenue
FROM base b
JOIN campaigns c ON c.id = b.campaign_id
WHERE b.wk IS NOT NULL
GROUP BY b.campaign_id, c.name, b.wk
ORDER BY c.name, b.wk
"""

ATTR_SQL = """
SELECT
    ae.meta_campaign_id                   AS campaign_id,
    CASE
        WHEN ae.install_date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
        WHEN ae.install_date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
        WHEN ae.install_date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
        WHEN ae.install_date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
    END                                   AS wk,
    COUNT(DISTINCT CASE
        WHEN ae.event_name IN ('conversion','repeat_conversion')
         AND ae.days_since_signup <= 6
        THEN ae.user_id END)              AS d6_conv,
    SUM(CASE
        WHEN ae.event_name IN ('conversion','repeat_conversion')
         AND ae.days_since_signup <= 6
        THEN ae.revenue_inr ELSE 0 END)   AS d6_revenue
FROM attribution_events ae
WHERE ae.install_date BETWEEN %(w1s)s AND %(w4e)s
  AND ae.meta_campaign_id IS NOT NULL
GROUP BY ae.meta_campaign_id, wk
HAVING CASE
    WHEN ae.install_date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
    WHEN ae.install_date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
    WHEN ae.install_date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
    WHEN ae.install_date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
END IS NOT NULL
"""

CREATIVE_MEDIA_SQL = """
WITH base AS (
    SELECT
        i.ad_id,
        ad.name AS ad_name,
        i.spend,
        COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(i.conversions)='array' THEN i.conversions ELSE '[]'::jsonb END) a
            WHERE a->>'action_type' = 'subscribe_total'
        ), 0) AS results,
        COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(i.action_values)='array' THEN i.action_values ELSE '[]'::jsonb END) a
            WHERE a->>'action_type' = 'omni_purchase'
        ), 0) AS purchase_revenue,
        CASE
            WHEN i.date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
            WHEN i.date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
            WHEN i.date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
            WHEN i.date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
        END AS wk
    FROM insights_daily i
    JOIN campaigns c  ON c.id = i.campaign_id AND c.name ILIKE %(campaign_pattern)s
    JOIN ads ad       ON ad.id = i.ad_id
    WHERE i.attribution_window = '7d_click'
      AND i.date BETWEEN %(w1s)s AND %(w4e)s
)
SELECT
    b.ad_id,
    b.ad_name,
    b.wk,
    ROUND(SUM(b.spend)::numeric, 0)   AS spend,
    SUM(b.results)                    AS results,
    SUM(b.purchase_revenue)           AS purchase_revenue
FROM base b
WHERE b.wk IS NOT NULL
GROUP BY b.ad_id, b.ad_name, b.wk
ORDER BY b.ad_name, b.wk
"""

CREATIVE_ATTR_SQL = """
SELECT
    ae.meta_creative_id                   AS ad_id,
    CASE
        WHEN ae.install_date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
        WHEN ae.install_date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
        WHEN ae.install_date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
        WHEN ae.install_date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
    END                                   AS wk,
    COUNT(DISTINCT CASE
        WHEN ae.event_name IN ('conversion','repeat_conversion')
         AND ae.days_since_signup <= 6
        THEN ae.user_id END)              AS d6_conv,
    SUM(CASE
        WHEN ae.event_name IN ('conversion','repeat_conversion')
         AND ae.days_since_signup <= 6
        THEN ae.revenue_inr ELSE 0 END)   AS d6_revenue
FROM attribution_events ae
JOIN ads a        ON a.id::text = ae.meta_creative_id
JOIN campaigns c  ON c.id = a.campaign_id AND c.name ILIKE %(campaign_pattern)s
WHERE ae.install_date BETWEEN %(w1s)s AND %(w4e)s
GROUP BY ae.meta_creative_id, wk
HAVING CASE
    WHEN ae.install_date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
    WHEN ae.install_date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
    WHEN ae.install_date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
    WHEN ae.install_date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
END IS NOT NULL
"""

_NONIOS_NONRETARGET_EXCLUDE = """
  AND NOT (
    LOWER(COALESCE(c.name, '')) LIKE '%%ios%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%retarget%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%remarketing%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%remarket%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%retgt%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%rtgt%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%rtrgt%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%bot%%'
    OR LOWER(COALESCE(c.name, '')) LIKE '%%bof%%'
  )
"""

ALL_PROSP_MEDIA_SQL = """
WITH base AS (
    SELECT
        i.ad_id,
        i.spend,
        COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(i.conversions)='array' THEN i.conversions ELSE '[]'::jsonb END) a
            WHERE a->>'action_type' = 'subscribe_total'
        ), 0) AS results,
        COALESCE((
            SELECT SUM((a->>'value')::numeric)
            FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(i.action_values)='array' THEN i.action_values ELSE '[]'::jsonb END) a
            WHERE a->>'action_type' = 'omni_purchase'
        ), 0) AS purchase_revenue,
        CASE
            WHEN i.date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
            WHEN i.date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
            WHEN i.date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
            WHEN i.date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
        END AS wk
    FROM insights_daily i
    JOIN campaigns c ON c.id = i.campaign_id
    WHERE i.attribution_window = '7d_click'
      AND i.date BETWEEN %(w1s)s AND %(w4e)s
""" + _NONIOS_NONRETARGET_EXCLUDE + """
)
SELECT
    b.wk,
    ROUND(SUM(b.spend)::numeric, 0)   AS spend,
    SUM(b.results)                    AS results,
    SUM(b.purchase_revenue)           AS purchase_revenue
FROM base b
WHERE b.wk IS NOT NULL
GROUP BY b.wk
"""

ALL_PROSP_ATTR_SQL = """
SELECT
    CASE
        WHEN ae.install_date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
        WHEN ae.install_date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
        WHEN ae.install_date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
        WHEN ae.install_date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
    END                                   AS wk,
    COUNT(DISTINCT CASE
        WHEN ae.event_name IN ('conversion','repeat_conversion')
         AND ae.days_since_signup <= 6
        THEN ae.user_id END)              AS d6_conv,
    SUM(CASE
        WHEN ae.event_name IN ('conversion','repeat_conversion')
         AND ae.days_since_signup <= 6
        THEN ae.revenue_inr ELSE 0 END)   AS d6_revenue
FROM attribution_events ae
JOIN ads a       ON a.id::text = ae.meta_creative_id
JOIN campaigns c ON c.id = a.campaign_id
WHERE ae.install_date BETWEEN %(w1s)s AND %(w4e)s
""" + _NONIOS_NONRETARGET_EXCLUDE + """
GROUP BY wk
HAVING CASE
    WHEN ae.install_date BETWEEN %(w1s)s AND %(w1e)s THEN 'w1'
    WHEN ae.install_date BETWEEN %(w2s)s AND %(w2e)s THEN 'w2'
    WHEN ae.install_date BETWEEN %(w3s)s AND %(w3e)s THEN 'w3'
    WHEN ae.install_date BETWEEN %(w4s)s AND %(w4e)s THEN 'w4'
END IS NOT NULL
"""


# ── Build pivot table ─────────────────────────────────────────────────────────

def build_wow_data(conn, weeks):
    params = {}
    for i, (_, start, end) in enumerate(weeks, 1):
        params[f"w{i}s"] = start.isoformat()
        params[f"w{i}e"] = end.isoformat()

    media_rows = q(conn, MEDIA_SQL, params)
    attr_rows  = q(conn, ATTR_SQL,  params)

    # Index attr by (campaign_id, week)
    attr_idx: dict[tuple, dict] = {}
    for r in attr_rows:
        attr_idx[(str(r["campaign_id"]), r["wk"])] = r

    # Index media by (campaign_id, week); collect campaign names
    camp_names: dict[str, str] = {}
    media_idx: dict[tuple, dict] = {}
    for r in media_rows:
        cid = str(r["campaign_id"])
        camp_names[cid] = r["campaign_name"]
        media_idx[(cid, r["wk"])] = r

    all_cids = sorted(camp_names.keys(),
                      key=lambda c: -(media_idx.get((c, f"w{len(weeks)}"), {}).get("spend") or 0))

    wk_keys = [f"w{i}" for i in range(1, len(weeks) + 1)]

    out = []
    for cid in all_cids:
        row = {"campaign_name": camp_names[cid]}
        for wk in wk_keys:
            m = media_idx.get((cid, wk), {})
            a = attr_idx.get((cid, wk), {})

            spend   = float(m.get("spend") or 0)
            results = float(m.get("results") or 0)
            fb_rev  = float(m.get("purchase_revenue") or 0)
            d6_conv = float(a.get("d6_conv") or 0)
            d6_rev  = float(a.get("d6_revenue") or 0)

            row[f"{wk}_spend"]   = spend
            row[f"{wk}_fb_cac"]  = (spend / results) if results else None  # Cost per Result
            row[f"{wk}_fb_roas"] = (fb_rev / spend)  if spend   else None  # omni_purchase / spend
            row[f"{wk}_d6_cac"]  = (spend / d6_conv) if d6_conv else None
            row[f"{wk}_d6_roas"] = (d6_rev / spend)  if spend   else None
        out.append(row)
    return out


def build_wow_creative_data(conn, weeks, campaign_pattern="%Test4%", top_n=10, rank_wk=None):
    """rank_wk: if set (e.g. 'w4'), rank by that week's spend; otherwise rank by 30d total."""
    params = {"campaign_pattern": campaign_pattern}
    for i, (_, start, end) in enumerate(weeks, 1):
        params[f"w{i}s"] = start.isoformat()
        params[f"w{i}e"] = end.isoformat()

    media_rows = q(conn, CREATIVE_MEDIA_SQL, params)
    attr_rows  = q(conn, CREATIVE_ATTR_SQL,  params)

    attr_idx: dict[tuple, dict] = {}
    for r in attr_rows:
        attr_idx[(str(r["ad_id"]), r["wk"])] = r

    ad_names: dict[str, str] = {}
    media_idx: dict[tuple, dict] = {}
    for r in media_rows:
        aid = str(r["ad_id"])
        ad_names[aid] = r["ad_name"]
        media_idx[(aid, r["wk"])] = r

    wk_keys = [f"w{i}" for i in range(1, len(weeks) + 1)]

    def _rank_spend(aid):
        if rank_wk:
            return float(media_idx.get((aid, rank_wk), {}).get("spend") or 0)
        return sum(float(media_idx.get((aid, wk), {}).get("spend") or 0) for wk in wk_keys)

    sorted_aids = sorted(ad_names.keys(), key=lambda a: -_rank_spend(a))
    top_aids  = set(sorted_aids[:top_n])
    rest_aids = set(sorted_aids[top_n:])

    def _blank_agg():
        return {wk: {"spend": 0.0, "results": 0.0, "fb_rev": 0.0, "d6_conv": 0.0, "d6_rev": 0.0} for wk in wk_keys}

    agg      = _blank_agg()  # top 10
    agg_rest = _blank_agg()  # remaining

    out = []
    for aid in sorted_aids[:top_n]:
        row = {"ad_name": ad_names[aid]}
        for wk in wk_keys:
            m = media_idx.get((aid, wk), {})
            a = attr_idx.get((aid, wk), {})

            spend   = float(m.get("spend") or 0)
            results = float(m.get("results") or 0)
            fb_rev  = float(m.get("purchase_revenue") or 0)
            d6_conv = float(a.get("d6_conv") or 0)
            d6_rev  = float(a.get("d6_revenue") or 0)

            agg[wk]["spend"]   += spend
            agg[wk]["results"] += results
            agg[wk]["fb_rev"]  += fb_rev
            agg[wk]["d6_conv"] += d6_conv
            agg[wk]["d6_rev"]  += d6_rev

            row[f"{wk}_spend"]   = spend
            row[f"{wk}_fb_cac"]  = (spend / results) if results else None
            row[f"{wk}_fb_roas"] = (fb_rev / spend)  if spend   else None
            row[f"{wk}_d6_cac"]  = (spend / d6_conv) if d6_conv else None
            row[f"{wk}_d6_roas"] = (d6_rev / spend)  if spend   else None
        out.append(row)

    # Accumulate rest (non-top-10) across all their weeks
    for aid in rest_aids:
        for wk in wk_keys:
            m = media_idx.get((aid, wk), {})
            a = attr_idx.get((aid, wk), {})
            agg_rest[wk]["spend"]   += float(m.get("spend") or 0)
            agg_rest[wk]["results"] += float(m.get("results") or 0)
            agg_rest[wk]["fb_rev"]  += float(m.get("purchase_revenue") or 0)
            agg_rest[wk]["d6_conv"] += float(a.get("d6_conv") or 0)
            agg_rest[wk]["d6_rev"]  += float(a.get("d6_revenue") or 0)

    def _agg_to_row(bucket, label):
        row = {"ad_name": label}
        for wk in wk_keys:
            sp  = bucket[wk]["spend"]
            res = bucket[wk]["results"]
            row[f"{wk}_spend"]   = sp
            row[f"{wk}_fb_cac"]  = (sp / res)                    if res                    else None
            row[f"{wk}_fb_roas"] = (bucket[wk]["fb_rev"]  / sp)  if sp                    else None
            row[f"{wk}_d6_cac"]  = (sp / bucket[wk]["d6_conv"])  if bucket[wk]["d6_conv"] else None
            row[f"{wk}_d6_roas"] = (bucket[wk]["d6_rev"]  / sp)  if sp                    else None
        return row

    return out, [
        _agg_to_row(agg,      f"▶ Aggregate (Top {top_n})"),
        _agg_to_row(agg_rest, f"▶ Aggregate (Non-Top {top_n})"),
    ]


def build_campaign_agg_row(conn, weeks, campaign_pattern: str, label: str) -> dict:
    """Aggregate ALL creatives in a campaign into a single metrics row."""
    params = {"campaign_pattern": f"%{campaign_pattern}%"}
    for i, (_, start, end) in enumerate(weeks, 1):
        params[f"w{i}s"] = start.isoformat()
        params[f"w{i}e"] = end.isoformat()

    media_rows = q(conn, CREATIVE_MEDIA_SQL, params)
    attr_rows  = q(conn, CREATIVE_ATTR_SQL,  params)

    wk_keys = [f"w{i}" for i in range(1, len(weeks) + 1)]
    bucket  = {wk: {"spend": 0.0, "results": 0.0, "fb_rev": 0.0, "d6_conv": 0.0, "d6_rev": 0.0} for wk in wk_keys}

    attr_idx: dict[tuple, dict] = {}
    for r in attr_rows:
        attr_idx[(str(r["ad_id"]), r["wk"])] = r

    for r in media_rows:
        wk = r["wk"]
        if wk not in bucket:
            continue
        aid = str(r["ad_id"])
        a   = attr_idx.get((aid, wk), {})
        bucket[wk]["spend"]   += float(r.get("spend") or 0)
        bucket[wk]["results"] += float(r.get("results") or 0)
        bucket[wk]["fb_rev"]  += float(r.get("purchase_revenue") or 0)
        bucket[wk]["d6_conv"] += float(a.get("d6_conv") or 0)
        bucket[wk]["d6_rev"]  += float(a.get("d6_revenue") or 0)

    row = {"ad_name": f"▶ {label}"}
    for wk in wk_keys:
        sp  = bucket[wk]["spend"]
        res = bucket[wk]["results"]
        row[f"{wk}_spend"]   = sp
        row[f"{wk}_fb_cac"]  = (sp / res)                   if res                   else None
        row[f"{wk}_fb_roas"] = (bucket[wk]["fb_rev"]  / sp) if sp                    else None
        row[f"{wk}_d6_cac"]  = (sp / bucket[wk]["d6_conv"]) if bucket[wk]["d6_conv"] else None
        row[f"{wk}_d6_roas"] = (bucket[wk]["d6_rev"]  / sp) if sp                    else None
    return row


def build_all_prosp_agg_row(conn, weeks, label="▶ All Non-iOS/Retargeting Campaigns") -> dict:
    """Aggregate ALL non-iOS/retargeting campaign metrics into a single row."""
    params = {}
    for i, (_, start, end) in enumerate(weeks, 1):
        params[f"w{i}s"] = start.isoformat()
        params[f"w{i}e"] = end.isoformat()

    media_rows = q(conn, ALL_PROSP_MEDIA_SQL, params)
    attr_rows  = q(conn, ALL_PROSP_ATTR_SQL,  params)

    wk_keys = [f"w{i}" for i in range(1, len(weeks) + 1)]
    bucket  = {wk: {"spend": 0.0, "results": 0.0, "fb_rev": 0.0, "d6_conv": 0.0, "d6_rev": 0.0} for wk in wk_keys}

    for r in media_rows:
        wk = r["wk"]
        if wk in bucket:
            bucket[wk]["spend"]   = float(r.get("spend") or 0)
            bucket[wk]["results"] = float(r.get("results") or 0)
            bucket[wk]["fb_rev"]  = float(r.get("purchase_revenue") or 0)

    for r in attr_rows:
        wk = r["wk"]
        if wk in bucket:
            bucket[wk]["d6_conv"] = float(r.get("d6_conv") or 0)
            bucket[wk]["d6_rev"]  = float(r.get("d6_revenue") or 0)

    row = {"ad_name": f"▶ {label}"}
    for wk in wk_keys:
        sp  = bucket[wk]["spend"]
        res = bucket[wk]["results"]
        row[f"{wk}_spend"]   = sp
        row[f"{wk}_fb_cac"]  = (sp / res)                   if res                   else None
        row[f"{wk}_fb_roas"] = (bucket[wk]["fb_rev"]  / sp) if sp                    else None
        row[f"{wk}_d6_cac"]  = (sp / bucket[wk]["d6_conv"]) if bucket[wk]["d6_conv"] else None
        row[f"{wk}_d6_roas"] = (bucket[wk]["d6_rev"]  / sp) if sp                    else None
    return row


# ── Console output ────────────────────────────────────────────────────────────

def _inr(v, gst=True):
    if v is None:
        return "—"
    v = v * GST if gst else v
    if v >= 1_00_000:
        return f"₹{v/1_00_000:.1f}L"
    if v >= 1_000:
        return f"₹{v/1_000:.0f}k"
    return f"₹{v:.0f}"

def _roas(v):
    return "—" if v is None else f"{v * 100:.1f}%"

def print_table(weeks, rows):
    wk_keys  = [f"w{i}" for i in range(1, len(weeks) + 1)]
    wk_labels = [w[0] for w in weeks]

    print(f"\n{'Campaign':<45}", end="")
    for lbl in wk_labels:
        print(f"  {lbl:<40}", end="")
    print()
    print("─" * (45 + 42 * len(weeks)))

    for r in rows:
        print(f"\n{r['campaign_name']:<45}")
        metrics = [
            ("Spend",   lambda r, w: _inr(r.get(f"{w}_spend"), gst=True)),
            ("FB Cost/Result", lambda r, w: _inr(r.get(f"{w}_fb_cac"), gst=True)),
            ("FB ROAS", lambda r, w: _roas(r.get(f"{w}_fb_roas"))),
            ("D6 CAC",  lambda r, w: _inr(r.get(f"{w}_d6_cac"), gst=True)),
            ("D6 ROAS", lambda r, w: _roas(r.get(f"{w}_d6_roas"))),
        ]
        for label, fn in metrics:
            print(f"  {label:<43}", end="")
            for wk in wk_keys:
                print(f"  {fn(r, wk):<40}", end="")
            print()


# ── Google Sheets writer ──────────────────────────────────────────────────────

def _inr_str(v, decimals=0):
    if v is None:
        return ""
    return f"₹{v:,.{decimals}f}"

def stamp_refreshed(sh):
    from datetime import datetime
    ts = datetime.now().strftime("%Y-%m-%d %H:%M IST")
    try:
        ws = sh.worksheet("Last Refreshed")
        ws.clear()
    except Exception:
        ws = sh.add_worksheet("Last Refreshed", rows=3, cols=3)
    ws.update("A1", [["Last Refreshed", ts]])
    ws.format("A1", {"textFormat": {"bold": True}})


def write_to_sheet(sh, tab_name, weeks, rows, id_col="campaign_name", id_label="Campaign", agg_rows=None):
    try:
        sh.del_worksheet(sh.worksheet(tab_name))
    except Exception:
        pass

    now_str   = datetime.now().strftime("%d %b %Y, %H:%M IST")
    wk_keys   = [f"w{i}" for i in range(1, len(weeks) + 1)]
    wk_labels = [w[0] for w in weeks]

    metric_cols = ["Spend ₹", "FB Cost/Result ₹", "FB ROAS", "D6 CAC ₹", "D6 ROAS"]
    N_METRICS   = len(metric_cols)
    N_WEEKS     = len(weeks)
    TOTAL_COLS  = 1 + N_WEEKS * N_METRICS

    # Row 0: group headers (week labels)
    group_row = [id_label]
    for lbl in wk_labels:
        group_row += [lbl] + [""] * (N_METRICS - 1)

    # Row 1: metric headers
    header_row = [id_label] + metric_cols * N_WEEKS

    GST = 1.18

    def _sp(v):
        return "" if v is None else _inr_str(round(v * GST, 0), 0)

    def _ro(v):
        return "" if v is None else round(float(v), 4)  # stored as decimal; formatted as % via API

    data_rows = [group_row, header_row]
    for r in rows:
        row_data = [r[id_col]]
        for wk in wk_keys:
            row_data += [
                _sp(r.get(f"{wk}_spend")),
                _sp(r.get(f"{wk}_fb_cac")),
                _ro(r.get(f"{wk}_fb_roas")),
                _sp(r.get(f"{wk}_d6_cac")),
                _ro(r.get(f"{wk}_d6_roas")),
            ]
        data_rows.append(row_data)

    # Aggregate rows (blended ROAS) — blank separator then bold agg rows
    agg_start_row = None
    if agg_rows:
        data_rows.append([])
        agg_start_row = len(data_rows)  # 0-indexed sheet row where agg rows begin
        for ar in agg_rows:
            agg_data = [ar.get(id_col, "")]
            for wk in wk_keys:
                agg_data += [
                    _sp(ar.get(f"{wk}_spend")),
                    _sp(ar.get(f"{wk}_fb_cac")),
                    _ro(ar.get(f"{wk}_fb_roas")),
                    _sp(ar.get(f"{wk}_d6_cac")),
                    _ro(ar.get(f"{wk}_d6_roas")),
                ]
            data_rows.append(agg_data)

    data_rows.append([])
    data_rows.append([f"Last updated: {now_str}", f"{len(rows)} campaigns"])

    ws = sh.add_worksheet(tab_name, rows=max(len(data_rows) + 20, 200), cols=TOTAL_COLS + 2)
    ws.update(values=data_rows, range_name="A1")

    # Colors per week group (cycling through 4 palette entries)
    WEEK_COLORS = [
        {"red": 0.102, "green": 0.204, "blue": 0.376},  # dark blue
        {"red": 0.067, "green": 0.392, "blue": 0.176},  # dark green
        {"red": 0.345, "green": 0.376, "blue": 0.471},  # slate
        {"red": 0.502, "green": 0.314, "blue": 0.063},  # amber
    ]
    WEEK_COLORS_DARK = [
        {"red": 0.073, "green": 0.145, "blue": 0.267},
        {"red": 0.047, "green": 0.275, "blue": 0.122},
        {"red": 0.267, "green": 0.298, "blue": 0.388},
        {"red": 0.380, "green": 0.235, "blue": 0.047},
    ]
    WHITE = {"red": 1, "green": 1, "blue": 1}

    requests = []

    # Identity header cells (rows 0+1, col 0)
    ID_COLOR = {"red": 0.051, "green": 0.278, "blue": 0.133}
    for row_idx in range(2):
        requests.append({"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1,
                      "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"backgroundColor": ID_COLOR,
                      "textFormat": {"bold": True, "foregroundColor": WHITE, "fontSize": 10},
                      "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat",
        }})

    for wi, (_, col_color, col_dark) in enumerate(zip(wk_keys, WEEK_COLORS, WEEK_COLORS_DARK)):
        cs = 1 + wi * N_METRICS
        ce = cs + N_METRICS

        # Group header merge + color
        requests.append({"mergeCells": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": cs, "endColumnIndex": ce},
            "mergeType": "MERGE_ALL",
        }})
        requests.append({"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": cs, "endColumnIndex": ce},
            "cell": {"userEnteredFormat": {"backgroundColor": col_color,
                      "textFormat": {"bold": True, "foregroundColor": WHITE, "fontSize": 10},
                      "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat",
        }})
        # Metric header row color (slightly darker)
        requests.append({"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": cs, "endColumnIndex": ce},
            "cell": {"userEnteredFormat": {"backgroundColor": col_dark,
                      "textFormat": {"bold": True, "foregroundColor": WHITE, "fontSize": 9},
                      "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP"}},
            "fields": "userEnteredFormat",
        }})

    # Freeze 2 header rows + Campaign column
    requests.append({"updateSheetProperties": {
        "properties": {"sheetId": ws.id,
                       "gridProperties": {"frozenRowCount": 2, "frozenColumnCount": 1}},
        "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
    }})

    # Column widths
    requests.append({"updateDimensionProperties": {
        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
        "properties": {"pixelSize": 280}, "fields": "pixelSize",
    }})
    for i in range(1, TOTAL_COLS):
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": 100}, "fields": "pixelSize",
        }})

    # Red→yellow→green gradient on ROAS columns (FB ROAS=offset 2, D6 ROAS=offset 4)
    # Values are now percentages (e.g. "50%" stored as text). Use a single combined
    # range across all week blocks so one gradientRule covers the whole tab.
    roas_col_offsets = [2, 4]
    for offset in roas_col_offsets:
        roas_ranges = [
            {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": len(data_rows) - 2,
             "startColumnIndex": 1 + wi * N_METRICS + offset,
             "endColumnIndex":   1 + wi * N_METRICS + offset + 1}
            for wi in range(N_WEEKS)
        ]
        requests.append({"addConditionalFormatRule": {"rule": {
            "ranges": roas_ranges,
            "gradientRule": {
                "minpoint": {"color": {"red": 0.820, "green": 0.188, "blue": 0.149},
                             "type": "MIN"},
                "midpoint": {"color": {"red": 1.0,   "green": 0.851, "blue": 0.400},
                             "type": "PERCENTILE", "value": "50"},
                "maxpoint": {"color": {"red": 0.420, "green": 0.655, "blue": 0.310},
                             "type": "MAX"},
            },
        }, "index": 0}})

    # Bold styling for aggregate rows
    if agg_rows and agg_start_row is not None:
        for i, _ in enumerate(agg_rows):
            r_idx = agg_start_row + i
            requests.append({"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": r_idx, "endRowIndex": r_idx + 1,
                          "startColumnIndex": 0, "endColumnIndex": TOTAL_COLS},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.953, "green": 0.953, "blue": 0.953},
                    "textFormat": {"bold": True},
                }},
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.bold",
            }})
        # Apply % format to ROAS cols in agg rows too
        for offset in roas_col_offsets:
            for wi in range(N_WEEKS):
                col_idx = 1 + wi * N_METRICS + offset
                for i, _ in enumerate(agg_rows):
                    r_idx = agg_start_row + i
                    requests.append({"repeatCell": {
                        "range": {"sheetId": ws.id, "startRowIndex": r_idx, "endRowIndex": r_idx + 1,
                                  "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }})

    # Format ROAS columns as percentage
    for offset in roas_col_offsets:
        for wi in range(N_WEEKS):
            col_idx = 1 + wi * N_METRICS + offset
            requests.append({"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 2,
                          "endRowIndex": len(data_rows) - 2,
                          "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                "fields": "userEnteredFormat.numberFormat",
            }})

    # Filter on header row
    requests.append({"setBasicFilter": {"filter": {
        "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(data_rows),
                  "startColumnIndex": 0, "endColumnIndex": TOTAL_COLS},
    }}})

    sh.batch_update({"requests": requests})
    print(f"  WoW tab '{tab_name}': {len(rows)} campaigns × {N_WEEKS} weeks written.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="4-week WoW campaign metrics report")
    parser.add_argument("--sheet-id", default="",
                        help="Google Sheet ID to write the WoW tab to")
    parser.add_argument("--tab", default="WoW — Campaigns",
                        help="Tab name (default: 'WoW — Campaigns')")
    args = parser.parse_args()

    today = date.today()
    weeks = _make_weeks(today, n=4)

    print("Week definitions:")
    for label, start, end in weeks:
        print(f"  {label}")

    print("\nConnecting to DB...")
    conn = db_conn()

    print("Building WoW data...")
    rows = build_wow_data(conn, weeks)
    print(f"  {len(rows)} campaigns with data.")

    print_table(weeks, rows)

    if args.sheet_id:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"],
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(args.sheet_id)
        print(f"\nWriting to sheet: {sh.url}")
        write_to_sheet(sh, args.tab, weeks, rows)

        print("\nBuilding Test4 creative WoW data (top 10 by 30d spend)...")
        creative_rows, creative_agg = build_wow_creative_data(conn, weeks)
        print(f"  {len(creative_rows)} creatives.")

        bid_cap_row = build_campaign_agg_row(
            conn, weeks,
            "FB_MOF_AAA_Android_Start-Trial_Pan-India_200426_Bid-Cap",
            "FB_MOF_AAA_Android_Start-Trial_Pan-India_200426_Bid-Cap",
        )
        creative_agg.append(bid_cap_row)

        print("  Building all non-iOS/retargeting aggregate row...")
        all_prosp_row = build_all_prosp_agg_row(conn, weeks)
        creative_agg.append(all_prosp_row)

        write_to_sheet(sh, "WoW — Test4 Creatives", weeks, creative_rows,
                       id_col="ad_name", id_label="Creative", agg_rows=creative_agg)

        print("\nBuilding Test4 creative WoW data (top 10 by last 7d spend)...")
        creative_7d_rows, creative_7d_agg = build_wow_creative_data(
            conn, weeks, rank_wk=f"w{len(weeks)}"
        )
        print(f"  {len(creative_7d_rows)} creatives.")
        bid_cap_row_7d = build_campaign_agg_row(
            conn, weeks,
            "FB_MOF_AAA_Android_Start-Trial_Pan-India_200426_Bid-Cap",
            "FB_MOF_AAA_Android_Start-Trial_Pan-India_200426_Bid-Cap",
        )
        creative_7d_agg.append(bid_cap_row_7d)
        creative_7d_agg.append(all_prosp_row)
        write_to_sheet(sh, "WoW — Test4 Creatives (7d)", weeks, creative_7d_rows,
                       id_col="ad_name", id_label="Creative", agg_rows=creative_7d_agg)
        stamp_refreshed(sh)
        print(f"\nDone: {sh.url}")

    conn.close()


if __name__ == "__main__":
    main()
