# Scoring Model — Phase 7 Design Document

This document maps each of the 8 scoring dimensions to concrete SQL against the warehouse,
identifies the columns that need to exist, and flags the amendments required before Phase 7
code can be written. Read this before touching migration files.

---

## Attribution Join Pattern

`attribution_events` does **not** have a `meta_ad_id` column. Attribution is recorded at the
creative level: `meta_creative_id` = `tracker_creative_id` from Singular.

The canonical join from attribution to ads is:

```sql
ads a
  LEFT JOIN attribution_events ae ON ae.meta_creative_id = a.creative_id
```

Implication: if two ads share the same creative, they receive identical attributed signals.
This is correct — the attribution signal belongs to the creative, not the delivery wrapper.
Scoring at ad_id level is still valid; the per-dimension raw metrics will be equal for siblings.

---

## Attribution Window

`insights_daily` has attribution_window as part of its primary key. All scoring queries that
join `insights_daily` must filter to a single window. Use `7d_click` as the canonical window.

```sql
AND i.attribution_window = '7d_click'
```

---

## Fleet Definition (In-scope Ads)

```sql
-- Ads with >= ₹15,000 spend in the last 30 days, active or recently paused.
SELECT
    a.id                AS ad_id,
    a.creative_id,
    a.account_id,
    a.adset_id,
    a.campaign_id,
    MIN(i.date)         AS first_spend_date,
    CURRENT_DATE - MIN(i.date) AS ad_age_days,      -- maturity proxy
    SUM(i.spend)        AS spend_30d
FROM ads a
JOIN insights_daily i ON i.ad_id = a.id
    AND i.date >= CURRENT_DATE - 30
    AND i.attribution_window = '7d_click'
WHERE a.effective_status IN ('ACTIVE', 'PAUSED')
GROUP BY a.id, a.creative_id, a.account_id, a.adset_id, a.campaign_id
HAVING SUM(i.spend) >= 15000
```

`ad_age_days` = days since first observed spend in the 30-day window. For fully matured ads
that have been running longer, `MIN(i.date)` may undercount age (it only sees the 30-day window).
A better `ad_age_days` uses the ad's `created_time` column from the `ads` table:

```sql
CURRENT_DATE - DATE(a.created_time) AS ad_age_days
```

Use `ads.created_time` as the maturity clock. Maturity gate: `ad_age_days >= 6`.

---

## Dimension SQL

### Dimension 1 — D6 Efficiency Matured (weight 18)

**What it measures:** D6 CAC — how much spend it takes to produce one paying user within 6 days
of signup. Lower is better (direction = `lower_is_better`).

**Target:** ₹15,000 per paying user. Fleet p75 = strong, fleet p25 = weak.

**Data sources:** `insights_daily` (spend) + `attribution_events` (paying users).

```sql
-- Per-ad D6 CAC
SELECT
    a.id                                                        AS ad_id,
    SUM(i.spend)                                                AS total_spend,
    COUNT(DISTINCT CASE
        WHEN ae.event_name IN ('trial', 'conversion')
         AND ae.days_since_signup <= 6
         AND ae.is_reattributed = FALSE
        THEN ae.user_id END)                                    AS paying_users_d6,
    SUM(i.spend)
        / NULLIF(COUNT(DISTINCT CASE
            WHEN ae.event_name IN ('trial', 'conversion')
             AND ae.days_since_signup <= 6
             AND ae.is_reattributed = FALSE
            THEN ae.user_id END), 0)                            AS d6_cac_inr
FROM ads a
JOIN insights_daily i
    ON i.ad_id = a.id
    AND i.date >= CURRENT_DATE - 30
    AND i.attribution_window = '7d_click'
LEFT JOIN attribution_events ae
    ON ae.meta_creative_id = a.creative_id
    AND ae.is_reattributed = FALSE
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `paying_users_d6 = 0` (denominator is zero → CAC is undefined).

---

### Dimension 2 — D0 Funnel Completion (weight 14)

**What it measures:** How well an ad pushes users through the top of the funnel on the same
day as signup: signup → trial_start → D0 paid conversion. A composite of two rates:
`d0_trial_rate = d0_trials / signups` and `d0_conv_rate = d0_conversions / signups`.

The raw metric used for percentile ranking is `d0_conv_rate`; `d0_trial_rate` is stored in
the dimensions JSONB for context and reasoning bullets.

**Direction:** `higher_is_better`.

```sql
SELECT
    a.id                                                        AS ad_id,
    COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'    THEN ae.user_id END) AS signups,
    COUNT(DISTINCT CASE WHEN ae.event_name = 'trial'
                         AND ae.days_since_signup = 0    THEN ae.user_id END) AS d0_trials,
    COUNT(DISTINCT CASE WHEN ae.event_name = 'conversion'
                         AND ae.days_since_signup = 0    THEN ae.user_id END) AS d0_conversions
FROM ads a
LEFT JOIN attribution_events ae
    ON ae.meta_creative_id = a.creative_id
    AND ae.is_reattributed = FALSE
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `signups = 0`.

⚠ **Phase 6 gap — Activation events not ingested.** The spec references `event_catalog.funnel_step`
for classifying events into funnel stages beyond the current four event types. If "activation"
events (3rd-card unlock, mandate) need to appear in this funnel, they must first be ingested —
see [Phase 6 Amendments](#phase-6-amendments) below. For Phase 7a, this dimension runs on the
four existing event types only.

---

### Dimension 3 — D6 Funnel Conversion (weight 12)

**What it measures:** Percentage of signups who become paying users within 6 days.
`paying_users_d6 / signups`. Direction: `higher_is_better`.

```sql
SELECT
    a.id                                                        AS ad_id,
    COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'    THEN ae.user_id END) AS signups,
    COUNT(DISTINCT CASE
        WHEN ae.event_name IN ('trial', 'conversion')
         AND ae.days_since_signup <= 6
         AND ae.is_reattributed = FALSE
        THEN ae.user_id END)                                    AS paying_users_d6,
    COUNT(DISTINCT CASE
        WHEN ae.event_name IN ('trial', 'conversion')
         AND ae.days_since_signup <= 6
         AND ae.is_reattributed = FALSE
        THEN ae.user_id END)::numeric
        / NULLIF(COUNT(DISTINCT CASE WHEN ae.event_name = 'signup' THEN ae.user_id END), 0)
                                                                AS d6_conversion_rate
FROM ads a
LEFT JOIN attribution_events ae
    ON ae.meta_creative_id = a.creative_id
    AND ae.is_reattributed = FALSE
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `signups = 0`.

---

### Dimension 4 — Revenue Depth (weight 14)

**What it measures:** Whether revenue ramps from D6 → D15 → D30. An ad that collects revenue
early and keeps collecting is deeper than one that fires once. Raw metric: `revenue_d30 / revenue_d6`
(the "deepening ratio"). Direction: `higher_is_better`.

**D30 caveat:** Only include cohorts where `install_date <= CURRENT_DATE - 30` (i.e. the user
has had a full 30 days). If the ad is younger than 30 days, D30 data is partial; this dimension
should be `data_available = false` for ads where `ad_age_days < 30`.

```sql
SELECT
    a.id                                                        AS ad_id,
    SUM(CASE WHEN ae.days_since_signup <= 6  AND NOT ae.is_reattributed
             THEN ae.revenue_inr ELSE 0 END)                    AS revenue_d6,
    SUM(CASE WHEN ae.days_since_signup <= 15 AND NOT ae.is_reattributed
             THEN ae.revenue_inr ELSE 0 END)                    AS revenue_d15,
    SUM(CASE WHEN ae.days_since_signup <= 30
             AND NOT ae.is_reattributed
             AND ae.install_date <= CURRENT_DATE - 30           -- matured cohorts only
             THEN ae.revenue_inr ELSE 0 END)                    AS revenue_d30_matured
FROM ads a
LEFT JOIN attribution_events ae
    ON ae.meta_creative_id = a.creative_id
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `revenue_d6 = 0` (no revenue signal at all).

---

### Dimension 5 — LTV on New Users (weight 15) — North Star

**What it measures:** D30 new-user revenue per ₹1 of spend. This is the north star for Level 2
weight calibration. Direction: `higher_is_better`.

```sql
SELECT
    a.id                                                        AS ad_id,
    SUM(i.spend)                                                AS spend_30d,
    SUM(CASE
        WHEN NOT ae.is_reattributed
         AND ae.days_since_signup <= 30
         AND ae.install_date <= CURRENT_DATE - 30               -- matured cohorts only
        THEN ae.revenue_inr ELSE 0 END)                         AS d30_new_user_revenue,
    SUM(CASE
        WHEN NOT ae.is_reattributed
         AND ae.days_since_signup <= 30
         AND ae.install_date <= CURRENT_DATE - 30
        THEN ae.revenue_inr ELSE 0 END)
        / NULLIF(SUM(i.spend), 0)                               AS d30_roas_new_users
FROM ads a
JOIN insights_daily i
    ON i.ad_id = a.id
    AND i.date >= CURRENT_DATE - 30
    AND i.attribution_window = '7d_click'
LEFT JOIN attribution_events ae
    ON ae.meta_creative_id = a.creative_id
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `ad_age_days < 30` (no matured cohorts yet).

---

### Dimension 6 — Signup Cost Efficiency (weight 8)

**What it measures:** Two sub-metrics:
- `signup_cac = spend / signups`. Target ₹800.
- `p0p1_cac = spend / COUNT(signups WHERE priority IN ('PAYMENT-P0', 'PAYMENT-P1'))`. Target ₹2,400.

Raw metric for percentile: `signup_cac`. `p0p1_cac` stored as context. Both directions: `lower_is_better`.

`priority` is on `attribution_events` directly (inherited from `users.priority` at ingestion time).

```sql
SELECT
    a.id                                                        AS ad_id,
    SUM(i.spend)                                                AS spend_30d,
    COUNT(DISTINCT CASE WHEN ae.event_name = 'signup'    THEN ae.user_id END) AS signups,
    COUNT(DISTINCT CASE
        WHEN ae.event_name = 'signup'
         AND ae.priority IN ('PAYMENT-P0', 'PAYMENT-P1')
        THEN ae.user_id END)                                    AS p0p1_signups,
    SUM(i.spend)
        / NULLIF(COUNT(DISTINCT CASE WHEN ae.event_name = 'signup' THEN ae.user_id END), 0)
                                                                AS signup_cac_inr,
    SUM(i.spend)
        / NULLIF(COUNT(DISTINCT CASE
            WHEN ae.event_name = 'signup'
             AND ae.priority IN ('PAYMENT-P0', 'PAYMENT-P1')
            THEN ae.user_id END), 0)                            AS p0p1_cac_inr
FROM ads a
JOIN insights_daily i
    ON i.ad_id = a.id
    AND i.date >= CURRENT_DATE - 30
    AND i.attribution_window = '7d_click'
LEFT JOIN attribution_events ae
    ON ae.meta_creative_id = a.creative_id
    AND ae.is_reattributed = FALSE
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `signups = 0`.

---

### Dimension 7 — Video Engagement (weight 10)

**What it measures:** How well the creative holds viewers. Three sub-metrics:
- `hook_rate = video_p25 / video_plays` — did they watch the first 25%?
- `hold_rate = video_p100 / video_plays` — did they watch to the end?
- `thruplay_rate = video_thruplay / video_plays` — Meta's "effective view" signal.

Raw metric for percentile: `hook_rate`. All directions: `higher_is_better`.

**Data source:** `insights_daily` video_* columns. These are JSONB arrays:
```json
[{"action_type": "video_view", "value": "12345"}]
```

The relevant action_type values are:
- `video_play_actions` → `action_type = "video_view"`
- `video_p25_watched_actions` → `action_type = "video_view"`  (Meta uses same type)
- `video_p100_watched_actions` → same
- `video_thruplay_watched_actions` → `action_type = "video_thruplay_watched"`

Extraction helper (reusable):
```sql
-- Extract a numeric value from a JSONB action array by action_type
-- Usage: _jsonb_action_sum(video_play_actions, 'video_view')
CREATE OR REPLACE FUNCTION _jsonb_action_sum(arr JSONB, atype TEXT)
RETURNS NUMERIC AS $$
    SELECT COALESCE(SUM((elem->>'value')::numeric), 0)
    FROM jsonb_array_elements(COALESCE(arr, '[]'::jsonb)) elem
    WHERE elem->>'action_type' = atype
$$ LANGUAGE SQL IMMUTABLE;
```

Per-ad video metrics:
```sql
SELECT
    a.id                                                        AS ad_id,
    SUM(_jsonb_action_sum(i.video_play_actions,        'video_view'))          AS total_plays,
    SUM(_jsonb_action_sum(i.video_p25_watched_actions, 'video_view'))          AS p25_views,
    SUM(_jsonb_action_sum(i.video_p100_watched_actions,'video_view'))          AS p100_views,
    SUM(_jsonb_action_sum(i.video_thruplay_watched_actions, 'video_thruplay_watched'))
                                                                               AS thruplays,
    SUM(_jsonb_action_sum(i.video_p25_watched_actions, 'video_view'))
        / NULLIF(SUM(_jsonb_action_sum(i.video_play_actions, 'video_view')), 0) AS hook_rate,
    SUM(_jsonb_action_sum(i.video_p100_watched_actions,'video_view'))
        / NULLIF(SUM(_jsonb_action_sum(i.video_play_actions, 'video_view')), 0) AS hold_rate,
    SUM(_jsonb_action_sum(i.video_thruplay_watched_actions,'video_thruplay_watched'))
        / NULLIF(SUM(_jsonb_action_sum(i.video_play_actions, 'video_view')), 0) AS thruplay_rate
FROM ads a
JOIN insights_daily i
    ON i.ad_id = a.id
    AND i.date >= CURRENT_DATE - 30
    AND i.attribution_window = '7d_click'
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `total_plays = 0` (non-video ad, or video with no plays).

The DB function `_jsonb_action_sum` will be created in the Phase 7 migration.

---

### Dimension 8 — Activation (weight 9)

**What it measures:** Mandate conversion rate — users who set up an auto-debit mandate within
the attribution window, as a share of total signups. Direction: `higher_is_better`.

3rd-card unlock is out of scope.

Raw metric: `mandate_users / signups`. Depends on `is_mandate` flag (Amendment A1).

```sql
SELECT
    a.id                                                        AS ad_id,
    COUNT(DISTINCT CASE WHEN ae.event_name = 'signup' THEN ae.user_id END) AS signups,
    COUNT(DISTINCT CASE
        WHEN ae.is_mandate = TRUE
         AND NOT ae.is_reattributed
        THEN ae.user_id END)                                    AS mandate_users,
    COUNT(DISTINCT CASE
        WHEN ae.is_mandate = TRUE
         AND NOT ae.is_reattributed
        THEN ae.user_id END)::numeric
        / NULLIF(COUNT(DISTINCT CASE WHEN ae.event_name = 'signup' THEN ae.user_id END), 0)
                                                                AS mandate_rate
FROM ads a
LEFT JOIN attribution_events ae
    ON ae.meta_creative_id = a.creative_id
    AND ae.is_reattributed = FALSE
WHERE a.id IN (/* fleet filter */)
GROUP BY a.id
```

`data_available = false` when `signups = 0` or before Amendment A1 migration runs.

---

## Phase 6 Amendments

The following are gaps between the Phase 6 spec's prerequisites and what actually exists in
the schema. These must be resolved (or explicitly deferred) before Phase 7 code can land.

### Amendment A — Activation signals for Dimension 8

#### A1 — `is_mandate` flag on `attribution_events` (schema change needed)

**Status:** Resolved in design. Requires a small migration + SQL update.

Mandate conversions = `user_transaction_history WHERE order_id ILIKE '%md%' AND status = 'CHARGED'`.
These transactions are already ingested by `conversions.sql` as `event_name = 'conversion'`
or `repeat_conversion` (when `amount > 50`). We don't need a new event type — just a flag.

**Changes required:**

Migration: add column to `attribution_events`:
```sql
ALTER TABLE attribution_events ADD COLUMN is_mandate BOOLEAN NOT NULL DEFAULT FALSE;
```
(Because it's a partitioned table, this propagates to all child partitions automatically in Postgres 14+.)

`conversions.sql`: add one line to the SELECT:
```sql
(r.order_id ILIKE '%md%')  AS is_mandate,
```
`signups.sql`: add `FALSE AS is_mandate` (signups are never mandates).

After the next backfill run over the full history, `is_mandate` will be populated for all
existing conversion rows. Historical rows ingested before this column exists will default FALSE —
which is correct for signups, and a known limitation for conversions (fixable by re-running the
conversions backfill over the full date range after the migration).

**Dropping `event_catalog`:** The original spec proposed an `event_catalog` lookup table.
With Dimension 8 scoped to mandate only (a single boolean flag), a catalog table adds no
value. Dropped. If additional activation signals are added later, extend `attribution_events`
with another boolean flag on the same pattern.

### Amendment B — `insights_with_conversions` MV (mentioned as prerequisite, not present)

**Status:** Materialized view does not exist. Not needed for Phase 7.

Phase 7 scores against `insights_daily` and `attribution_events` directly. The `insights_with_conversions`
MV would have been a convenience join — Phase 7 doesn't require it.

**Resolution:** No action needed for Phase 7. If it's useful for the API later, it can be
added as a separate migration.

### Amendment C — `meta_ad_id` column on `attribution_events` (not present, not needed)

**Status:** Column does not exist. Not needed — resolved by the join pattern documented above.

Attribution events join to ads via `attribution_events.meta_creative_id = ads.creative_id`.
This is correct behavior and requires no schema change.

---

## Columns Confirmed Present

| Column / Field | Table | Used By |
|---|---|---|
| `meta_creative_id` | `attribution_events` | All attribution dimensions (join key) |
| `event_name` | `attribution_events` | D1, D2, D3, D5, D6 |
| `days_since_signup` | `attribution_events` | D1, D2, D3, D4, D5 |
| `revenue_inr` | `attribution_events` | D4, D5 |
| `is_reattributed` | `attribution_events` | D1, D3, D4, D5, D6 |
| `is_mandate` | `attribution_events` | D8 — **to be added (Amendment A1)** |
| `install_date` | `attribution_events` | D4, D5 (matured cohort filter) |
| `priority` | `attribution_events` | D6 (P0/P1 quality split) |
| `spend` | `insights_daily` | D1, D5, D6, fleet filter |
| `impressions` | `insights_daily` | fleet filter |
| `video_play_actions` | `insights_daily` | D7 |
| `video_p25_watched_actions` | `insights_daily` | D7 |
| `video_p100_watched_actions` | `insights_daily` | D7 |
| `video_thruplay_watched_actions` | `insights_daily` | D7 |
| `attribution_window` | `insights_daily` | All (filter to `7d_click`) |
| `created_time` | `ads` | Maturity gate |
| `creative_id` | `ads` | Attribution join |
| `effective_status` | `ads` | Fleet filter |
| `object_type` | `ad_creatives` | LLM prompt (is_video flag) |
| `call_to_action_type` | `ad_creatives` | LLM prompt |

---

## Dimension Summary Table

| # | Dimension | Weight | Raw Metric | Direction | `data_available = false` when |
|---|-----------|--------|------------|-----------|-------------------------------|
| 1 | D6 Efficiency Matured | 18 | `spend / paying_users_d6` | lower_is_better | paying_users_d6 = 0 |
| 2 | D0 Funnel Completion | 14 | `d0_conversions / signups` | higher_is_better | signups = 0 |
| 3 | D6 Funnel Conversion | 12 | `paying_users_d6 / signups` | higher_is_better | signups = 0 |
| 4 | Revenue Depth | 14 | `revenue_d30 / revenue_d6` | higher_is_better | revenue_d6 = 0 |
| 5 | LTV on New Users | 15 | `d30_new_user_revenue / spend` | higher_is_better | ad_age_days < 30 |
| 6 | Signup Cost Efficiency | 8 | `spend / signups` | lower_is_better | signups = 0 |
| 7 | Video Engagement | 10 | `hook_rate` (p25/plays) | higher_is_better | total_plays = 0 |
| 8 | Activation | 9 | activation events / signups | higher_is_better | **BLOCKED** (Phase 6 amendment A) |

Weights sum to 100. When Dimension 8 is unavailable (`data_available: false`), its weight
is excluded from the denominator and the remaining 91 points are normalized to 100.

---

## Scoring Window Alignment

There is an intentional mismatch between the spend window (last 30 days from `insights_daily`)
and the attribution window (all-time for events attributed to this creative). This is correct:

- **Spend** is measured over the last 30 days — that's how much we've paid recently.
- **Attribution** is lifetime — every signup and conversion attributed to this ad, regardless
  of when they happened. A creative that ran 3 months ago may have trailing D30 revenue still
  arriving.

For D4 (Revenue Depth) and D5 (LTV), only cohorts with `install_date <= CURRENT_DATE - 30`
contribute to D30 figures. Newer cohorts are immature and excluded from the D30 denominator.

---

## Open Questions — Need Sign-off Before Migration

1. **D30 maturity for LTV (Dimension 5)**: Until the fleet has been running for 30+ days,
   most ads will have `data_available: false` for Dimension 5. The effective weight pool
   during early weeks will therefore be 85 (out of 100). Is that acceptable for the
   launch period, or should static D30 benchmarks be used until live data matures?

3. **`7d_click` as canonical window**: Confirmed? Singular attribution (used for conversion
   counting) is install-time and not tied to Meta click windows, so this only affects the
   `spend` aggregation from `insights_daily`.

4. **Fleet filter `effective_status`**: Should paused ads be scored? Current proposal: yes —
   useful for "should we resume this?" decisions.

~~**Mandate source**~~ — resolved: `order_id ILIKE '%md%'` in `user_transaction_history`.
~~**3rd-card unlock**~~ — out of scope. Dimension 8 = mandate rate only.
~~**`event_catalog` table**~~ — dropped.
