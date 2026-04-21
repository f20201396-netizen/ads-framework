# Attribution Schema — Phase 6 Kickoff Document

> Introspected: 2026-04-21  
> GCP Project: `univest-applications`  
> Connection: `projects/univest-applications/locations/asia-south2/connections/univest_db`  
> Status: **Awaiting sign-off before alembic/ingestion work begins**

---

## Q1 — Which BQ dataset and tables are the source of truth?

There is **no BigQuery staging dataset** for attribution. The connection `univest_db` is a **live federated query connection** (Cloud SQL / AlloyDB in `asia-south2`) pointing directly to the Univest production PostgreSQL database. All EXTERNAL_QUERY calls hit the live prod DB in real time.

**Source tables (prod Postgres, queried via EXTERNAL_QUERY):**

| Table | Role |
|---|---|
| `user_additional_details` | **Primary attribution table** — one row per user, Singular install attribution written at signup |
| `users` | User master — `id`, `created_at`, `referred_by`, `is_kyc_completed`, etc. |
| `user_transaction_history` | Revenue table — one row per payment attempt, use `status='CHARGED' AND amount>50` |
| `user_trade_cards_unlocks` | Engagement milestone table — rows per trade card unlock |
| `user_devices` | Device table — `user_id`, `os` for platform filtering |
| `Demat_Campaigns` | Exclusion list — demat-vertical campaigns to separate from pro-subscription analysis |
| `singular_campaign_metrics` | Aggregate campaign-level Singular metrics (impressions, clicks, installs, cost by date/source/creative). Separate from user-level attribution. |

---

## Q2 — Freshness

**Real-time via federated connection.** There is no ETL lag — EXTERNAL_QUERY hits the prod DB directly. However:
- This means production DB query load is shared with our analytics queries
- No BigQuery caching on federated results — every EXTERNAL_QUERY is a live Postgres query
- Cost model: BQ charges for the bytes transferred from Postgres to BQ, not Postgres query cost

**Implication for Phase 6**: Our `BQClient.stream_query()` must be written conservatively — no unbounded scans. Every query must filter on `users.created_at` or `payment_date` to bound the Postgres scan.

---

## Q3 — Event Model

**Model (b): Attribution is on the user row, not on events.**

`user_additional_details` has one row per user, populated at install time by Singular. It contains:
- The Singular-resolved network, campaign, adset, creative IDs
- Two `event_name` values only: `sign_up_success` (3.3M users) and `create_name_email` (1.5M users)
- Unix epoch timestamps for `event_utc_timestamp` and `install_utc_timestamp`

Post-install conversion events (subscription purchases) live in `user_transaction_history`, linked to the user via `user_id`. There is **no attribution column on `user_transaction_history`** — attribution is inherited by joining back to `user_additional_details`.

```
user_additional_details (1 row/user, install attribution)
  └─ user_id → users.id
       └─ user_id → user_transaction_history (N payments per user)
```

---

## Q4 — Column Mapping

### `user_additional_details` → `attribution_events` (our Postgres table)

| Source column | Our column | Notes |
|---|---|---|
| `user_id` | `user_id` | FK to `users.id` |
| `network` | `network` | Exact Singular value. Meta = `'Facebook'` |
| `partner_site` | `publisher_site` | Sub-network: `Facebook`, `Instagram`, `Audience Network` |
| `tracker_campaign_id` | `meta_campaign_id` | Meta campaign ID (when network=Facebook) |
| `tracker_sub_campaign_id` | `meta_adset_id` | Meta adset ID |
| `tracker_creative_id` | `meta_creative_id` | Meta creative/ad ID |
| `tracker_campaign_name` | `campaign_name` | Human-readable, from Singular |
| `tracker_sub_campaign_name` | `adset_name` | Human-readable |
| `creative` | `creative_name` | Creative name (note: column alias in query) |
| `event_name` | `event_name` | `sign_up_success` or `create_name_email` |
| `event_utc_timestamp` | `event_time` | Unix epoch seconds → convert to timestamptz |
| `install_utc_timestamp` | `install_time` | Unix epoch seconds → convert to timestamptz |
| `is_reengagement` | `is_reattributed` | `'0'`=new user, `'1'`=re-engagement (stored as text) |
| `is_organic` | `is_organic` | `'0'`/`'1'` as text |
| `is_viewthrough` | `is_viewthrough` | `'0'`/`'1'` as text |
| `platform` | `platform` | `Android`, `iOS` |
| `os_version` | `os_version` | |
| `device_brand` | `device_brand` | |
| `device_model` | `device_model` | |
| `city_field` (from `singular_campaign_metrics`) | `city` | On aggregate table, not user table |
| — | `event_date` | Derived: `DATE(TO_TIMESTAMP(event_utc_timestamp))` |
| — | `source_table` | Hardcoded `'user_additional_details'` |

### `user_transaction_history` → revenue columns

| Source column | Notes |
|---|---|
| `user_id` | Join to `user_additional_details.user_id` for attribution |
| `amount` | INR (implicit, single market). Filter `> 50` |
| `status` | Use `= 'CHARGED'` |
| `payment_date` | Revenue event time |
| `plan_id` | Determines product line (see Q5) |
| `order_id` | Mandate payments contain `'md'` in order_id |

---

## Q5 — Conversion Events

`user_additional_details` only records **install-level** events. All post-install events are inferred from `user_transaction_history` and joins to other tables.

**Funnel events and their sources:**

| Event | Table | Derivation |
|---|---|---|
| `install` | `user_additional_details` | Row exists with any network |
| `signup` | `users` | `users.created_at` (all users have this) |
| `kyc_complete` | `users` | `users.is_kyc_completed = true` |
| `first_trade` | `users` | `users.first_order_executed = true` |
| `trial_start` | `user_transaction_history` | `plan_id IN ('plan_000','plan_000_plus','plan_000_super')` OR `plan_id ILIKE '%trial%'` AND `status='CHARGED'` |
| `3rd_tcard_unlock` | `user_trade_cards_unlocks` | `COUNT(user_id) >= 3 GROUP BY user_id` |
| `first_paid_subscription` | `user_transaction_history` | First row with `status='CHARGED' AND amount>50`, non-trial plan |
| `mandate_payment` | `user_transaction_history` | `order_id LIKE '%md%'` |
| `repeat_subscription` | `user_transaction_history` | ROW_NUMBER > 1 within user |

**Plan ID → Product line mapping** (from `user_transaction_history`):

| Pattern | Product line |
|---|---|
| `plan_000`, `plan_000_plus`, `plan_000_super`, `*trial*` | Trial / free |
| `plan_ios_*`, `plan_android_*` (non-super, non-alpha, non-edge) | Pro Lite |
| `plan_ios_super_*`, `plan_android_super_*` | Pro Super |
| `plan_ios_alpha_*`, `plan_android_alpha_*` | Pro Gold (Alpha) |
| `plan_ios_edge_*`, `plan_android_edge_*` | Pro Edge |
| `pro_ios_*`, `pro_android_*` | Pro subscription |
| `pro_super_ios_*` | Pro Super |
| `pro_alpha_ios_*` | Pro Alpha |
| `pro_edge_ios_*` | Pro Edge |
| `plan_ios_loyalty_*`, `plan_ios_super_loyalty_*` | Loyalty/renewal |
| Plans from `basket_user_subscription` | Pro Basket |
| Plans from `commodity_user_subscription` | Pro Commodity |
| Plans from `mf_user_subscription` | MF Advisory |

> **Action needed**: Confirm the exact plan_id prefix → product_line mapping before seeding `event_catalog.yaml`. Run:
> ```sql
> SELECT DISTINCT plan_id FROM user_transaction_history 
> WHERE status='CHARGED' AND amount>50 ORDER BY plan_id;
> ```

---

## Q6 — Network Values (Meta)

From `user_additional_details.network`:

| Value | Count | Use |
|---|---|---|
| `Facebook` | 490,896 | **Only Meta value in network column** |

From `user_additional_details.partner_site` (sub-network within Facebook installs):

| Value | Meaning |
|---|---|
| `Facebook` | Facebook feed / stories |
| `Instagram` | Instagram feed / stories / reels |
| `Audience Network` | Meta Audience Network |

**MV filter**: `network = 'Facebook'`  
**Platform split**: Use `partner_site` for Facebook vs. Instagram breakdown.

No `Instagram` or `Audience Network` as top-level `network` values — all Meta traffic routes through `network='Facebook'` with `partner_site` distinguishing the sub-platform.

---

## Q7 — Join-Key Match Rate Against Meta

From the sample rows, `tracker_campaign_id` values look like valid Meta campaign IDs (16–18 digit integers):
- `120240321550440636` ✓ (format matches Meta campaign IDs)
- `23857236238190436` ✓
- `120207788211960437` ✓

Attribution coverage (all users):
- Total users: 7,160,258
- With `network`: 4,779,810 (66.8%)
- With `tracker_campaign_id`: 4,355,499 (60.8%)
- With `tracker_sub_campaign_id`: 4,332,308 (60.5%)
- With `tracker_creative_id`: 4,348,633 (60.8%)

**Join key for MV**: `attribution_events.meta_campaign_id = insights_campaign_daily.campaign_id`  
**Ad-level join**: `attribution_events.meta_creative_id` matches `insights_daily.ad_id` (Meta uses the ad ID, not the creative ID, as the row key in insights — needs verification by comparing a known ad_id from our Phase 1–5 sync against `tracker_creative_id` values).

> **Action needed before MV**: Run the match-rate query from the spec against 30 days of Facebook installs vs. our Postgres `ads.id` list to confirm `tracker_creative_id` → `ad_id` match rate ≥ 90%.

---

## Q8 — Revenue Model

- All revenue is in **INR** (implicit — single India market, no currency column on `user_transaction_history`)
- No FX conversion needed — `fx_rates_daily` table from spec can be omitted or added later
- Attribution chain: `user_transaction_history.user_id` → `user_additional_details.user_id` → `tracker_campaign_id / tracker_sub_campaign_id / tracker_creative_id`
- Revenue is **not** directly on the conversion row — it must be joined through the user
- Mandate payments identified by `order_id LIKE '%md%'`
- Repeat revenue: ROW_NUMBER > 1 within user on `user_transaction_history`

---

## Q9 — Partitioning and Clustering

**No BQ partitioning** — data is queried live from Postgres via federated connection. BQ partitioning does not apply to EXTERNAL_QUERY tables.

**Postgres indexes** (inferred from query patterns and table sizes):
- `user_additional_details`: likely indexed on `user_id` (PK FK)
- `users`: `id` (PK), `created_at` (used in WHERE frequently)
- `user_transaction_history`: `user_id`, `payment_date`, `status`

**Cost control strategy** (since there is no BQ partition filter):
- Every EXTERNAL_QUERY must include a `WHERE` clause that bounds the Postgres scan
- Mandatory filter: `users.created_at >= $since` or `payment_date >= $since`
- Client `dry_run()` method will estimate transferred bytes before executing

---

## Q10 — Reattribution / Re-engagement

**Explicit flag**: `user_additional_details.is_reengagement`
- `'0'` = new user acquisition
- `'1'` = re-engagement
- Stored as **text** (not boolean) — cast with `is_reengagement = '1'`

The existing query already handles this: `WHERE ch."Adset ID" IS NULL AND referred_by IS NULL` for new user isolation. Our MV will use `is_reattributed = false` for CAC calculations and show re-engagement in a separate bucket.

---

## Q11 — Postbacks to Meta CAPI

**Unknown — needs ops confirmation.** Singular can be configured to forward conversion events to Meta's Conversions API. If this is active:
- `insights_daily.actions` already includes Singular-forwarded events
- The "pixel-vs-MMP delta" collapses and becomes diagnostic only

**Action needed**: Confirm with whoever set up Singular whether CAPI forwarding is enabled. Check Meta Events Manager → Data Sources → Connected Integrations for Singular.

---

## Q12 — Data Volume

| Table | Row count | Date range |
|---|---|---|
| `users` / `user_additional_details` | ~7.16M users | 2022-08-08 → live |
| `user_transaction_history` | 11.4M rows | 2023-07-09 → live |
| Of which `status=CHARGED AND amount>50` | 615,741 | — |
| Facebook-attributed users | 490,896 | — |

Rows/day estimate (last 6 months):
- New signups: ~7.16M total / ~3.75 years ≈ ~5,200 signups/day average; likely higher recently
- New paid conversions: 615K / ~2.75 years ≈ ~613 conversions/day average

**Backfill strategy**: Chunk by `users.created_at` month (not `event_utc_timestamp` since it's a Unix string). Process sequentially, oldest first.

---

## Proposed `bq_column → postgres_column` Mapping Table

```
user_additional_details (source)     attribution_events (our Postgres table)
─────────────────────────────────────────────────────────────────────────────
user_id                          →   user_id            bigint
network                          →   network            text
partner_site                     →   publisher_site     text  (Facebook/Instagram/Audience Network)
tracker_campaign_id              →   meta_campaign_id   text  (nullable, Meta only)
tracker_sub_campaign_id          →   meta_adset_id      text  (nullable, Meta only)
tracker_creative_id              →   meta_creative_id   text  (nullable, Meta only)
tracker_campaign_name            →   campaign_name      text
tracker_sub_campaign_name        →   adset_name         text
creative (column)                →   creative_name      text  (NOT tracker_name — see bug note)
event_name                       →   event_name         text  (sign_up_success | create_name_email)
TO_TIMESTAMP(event_utc_timestamp)→   event_time         timestamptz
TO_TIMESTAMP(install_utc_timestamp)→ install_time       timestamptz
DATE(users.created_at)           →   event_date         date  (partition column)
is_reengagement = '1'            →   is_reattributed    bool
is_organic = '1'                 →   is_organic         bool
is_viewthrough = '1'             →   is_viewthrough     bool
platform                         →   platform           text
os_version                       →   os_version         text
device_brand                     →   device_brand       text
device_model                     →   device_model       text
'user_additional_details'        →   source_table       text
full row as JSON                 →   raw                jsonb

user_transaction_history (source)    attribution_events / MV
─────────────────────────────────────────────────────────────
user_id                          →   (join key to attribution_events.user_id)
amount WHERE status=CHARGED,>50  →   revenue_inr        numeric(12,2)
payment_date                     →   revenue_event_time timestamptz
plan_id                          →   product_line       text  (via catalog mapping)
order_id LIKE '%md%'             →   is_mandate         bool
ROW_NUMBER() = 1                 →   is_first_payment   bool
```

---

## Query Review — Provided Metabase Query

The query shared maps exactly to the data model above. Below are confirmed bugs and issues:

### Bug 1 — `network2` is a dead column
`network2` is computed in `user_data` using `partner_site` (Facebook/Instagram/Audience Network split) but is **never referenced** in `signup_metrics` or the final SELECT. Either promote it to a GROUP BY dimension or remove it.

### Bug 2 — `tcardunlock` HAVING `= 3` should be `>= 3`
```sql
HAVING COUNT(user_id) = 3   -- ❌ excludes users who unlocked >3 cards
HAVING COUNT(user_id) >= 3  -- ✓ "reached the 3rd unlock milestone"
```
Users who unlocked 4+ cards are silently excluded from `third_tcard_unlock`. This understates the metric.

### Bug 3 — `COUNT(d6_repeat_con)` should be `SUM`
In `signup_metrics`:
```sql
COUNT(d6_repeat_con) AS d0_d6_overall,        -- ❌ counts non-NULL rows (1 per user)
SUM(fp.d6_repeat_con) AS d0_d6_overall,       -- ✓ sums the repeat conversion counts
```
`d6_repeat_con` is already a COUNT from `first_payments`. Wrapping it in `COUNT()` counts how many users have a non-NULL value (essentially 1 per user who joined). Compare to `SUM(d6_mandate_repeat_con)` three lines later — that one correctly uses SUM.

### Bug 4 — Correlated subquery for `user_devices` is slow and fragile
```sql
-- ❌ Current (correlated, slow)
uad.user_id IN (
    SELECT u.id FROM user_devices ud 
    WHERE ud.user_id = u.id AND ud.os IN ('android','Android Web') 
)
-- ✓ Better (EXISTS or JOIN)
EXISTS (
    SELECT 1 FROM user_devices ud2
    WHERE ud2.user_id = uad.user_id
      AND ud2.os IN ('android', 'Android Web')
)
```
The inner `u.id` references the outer LEFT JOIN alias — works in Postgres but runs a correlated subquery per row in `user_additional_details`. On millions of rows this is a full table scan per user.

### Bug 5 — `tracker_name` is `uad.creative`, not tracker name
In `user_data`: `uad.creative tracker_name` — the column aliased as `tracker_name` is actually the **creative name** from Singular (`creative` column), not `tracker_name`. This is consistent with the final `regexp_replace(ud.tracker_name, ':.*$', '', 'g')` call stripping suffixes from creative names. The column naming is confusing — worth renaming to `creative_name` for clarity.

### Bug 6 — `trial` CTE gets LAST trial, not first
```sql
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY payment_date DESC)  -- last trial
```
With `WHERE rk = 1` this returns the most recent trial date. If the intent is to measure trial-to-paid conversion, you'd want `ORDER BY payment_date ASC` (first trial). If the intent is "is this user currently in a trial?", DESC is correct. **Clarify intent.**

### Bug 7 — Metabase template syntax strips to nothing in raw SQL
`[[AND tracker_campaign_name = {{tracker_campaign_name}}]]` is Metabase-only. Our ingestion SQL files will use `@param` style. No action needed for the dashboard query, but document the difference.

### Performance note — `first_payments` has no date pushdown
The `user_transaction_history` join in `first_payments` has no date filter. With 11.4M rows it relies entirely on the `user_id IN (SELECT ...)` anti-join from `user_data` to bound the scan. Adding `AND payment_date >= (CURRENT_DATE - INTERVAL '6 months' - INTERVAL '90 days')` would allow Postgres to use the `payment_date` index and significantly speed up the query.

---

## Open Questions (need answers before code)

1. **CAPI postbacks** (Q11): Is Singular forwarding conversions to Meta CAPI? Check Meta Events Manager.
2. **`tracker_creative_id` = Meta `ad_id` or `creative_id`?** Sample rows show values like `120240321884180636` — need to verify against our Phase 1–5 synced `ads.id` column. Run match-rate query.
3. **Product line → plan_id mapping**: Need exhaustive `DISTINCT plan_id` list to build `event_catalog.yaml` accurately.
4. **`users.priority` column**: Appears in the query (`priority = 'PAYMENT-P0'`, `'PAYMENT-P1'`) but is not in the `users` schema I fetched. It may be in `user_additional_details` or a joined table — confirm which table `priority` comes from.
5. **Demat exclusion logic**: The `Demat_Campaigns` anti-join excludes certain adset IDs. Should Phase 6 ingest ALL users (including demat) or only non-demat? The existing Metabase query excludes demat — document whether the MV should too.
