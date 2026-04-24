# ads-framework — Project Context for Claude

## What this project does

Two-part system for Univest's marketing analytics:

1. **Meta Ads warehouse** — pulls campaign/adset/ad structure, daily insights, audiences, pixels, and catalogs from the Meta Marketing API into a local Postgres database. A FastAPI service exposes it read-only.
2. **Attribution pipeline (Phase 6)** — joins Univest's first-party conversion data (from prod Postgres via BigQuery EXTERNAL_QUERY) with the Meta data to compute D0/D6 signup-to-conversion metrics per campaign and adset.

## Tech stack

- Python 3.11, FastAPI, pydantic v2, SQLAlchemy 2.x async (asyncpg)
- PostgreSQL 15+ with monthly declarative partitioning on `attribution_events`
- APScheduler (AsyncIOScheduler) for the worker sync jobs
- `google-cloud-bigquery` for federated queries against prod Postgres
- Alembic for migrations
- `.venv/` — always use `.venv/bin/python3`, not system `python3`

## Environment setup

`.env` file (never commit):
```
DATABASE_URL=postgresql+asyncpg://macbook@localhost/meta_ads
META_ACCESS_TOKEN=...
META_BUSINESS_ID=...
META_AD_ACCOUNT_IDS=act_...
ADMIN_API_KEY=...
GOOGLE_APPLICATION_CREDENTIALS=/Users/macbook/Downloads/google-json-key.json
BQ_COST_CAP_BYTES=5000000000
```

Local Postgres: user `macbook`, no password, Unix socket, database `meta_ads`.
No `postgres` superuser — `alembic.ini` uses `macbook@localhost/meta_ads`.

GCP service account key: `/Users/macbook/Downloads/google-json-key.json`
GCP project: `univest-applications`

## Running things

```bash
# Migrations
.venv/bin/alembic upgrade head

# API server
.venv/bin/uvicorn services.api.main:app --reload

# Worker (scheduler)
.venv/bin/python3 -m services.worker.main

# One-off backfill (no server needed)
.venv/bin/python3 scripts/run_backfill.py signups     2022-08-01 2026-04-22
.venv/bin/python3 scripts/run_backfill.py conversions 2023-07-01 2026-04-22

# Refresh materialized views manually
psql meta_ads -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_campaign_conversions;"
psql meta_ads -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_adset_conversions;"
```

## Repository layout

```
services/
  api/
    main.py              FastAPI app factory, CORS, error contract
    deps.py              Depends() helpers (DB session, auth)
    schemas.py           Pydantic response schemas
    routers/
      structure.py       /structure  — campaigns, adsets, ads, accounts
      insights.py        /insights   — daily metrics
      audiences.py       /audiences  — custom audiences, pixels, pixel stats, custom conversions
      catalogs.py        /catalogs   — product catalogs and product sets
      conversions.py     /conversions — attribution metrics from materialized views
      admin.py           /admin      — manual job triggers, backfill, cursor inspection
  shared/
    config.py            Settings (pydantic-settings, reads .env)
    db.py                AsyncSessionLocal, engine
    models.py            SQLAlchemy ORM models (all tables)
    bq_client.py         BQClient — dry_run + stream_rows via EXTERNAL_QUERY
    meta_client.py       Async Meta API client with rate limiting
    rate_limiter.py      Token-bucket rate limiter
    constants.py         Meta API field lists, attribution windows
  worker/
    main.py              APScheduler entrypoint
    upsert.py            Generic upsert helpers + track_run context manager
    parsers.py           Meta API response → dict normalisation
    jobs/
      sync_structure.py         campaigns/adsets/ads/accounts
      sync_insights.py          daily ad-level insights
      sync_higher_levels.py     campaign/adset-level rollups
      sync_breakdowns.py        age/gender/placement breakdowns
      sync_aux.py               audiences, pixels, catalogs
      sync_attribution.py       BQ → attribution_events upsert + MV refresh
      backfill.py               Meta historical backfill (not BQ)
    sql/attribution/
      signups.sql               Postgres SQL for signup events
      conversions.sql           Postgres SQL for trial/conversion events

db/
  alembic/versions/
    0001_initial_schema.py      Meta Ads tables
    0002_attribution_schema.py  attribution_events, cursors, cost log, MVs

scripts/
  run_backfill.py        CLI wrapper for backfill_attribution()
  export_openapi.py      Writes openapi.json

docs/
  attribution-schema.md  Full BQ introspection notes (read before touching attribution)
```

## Attribution pipeline — key facts

### Data source
Univest prod Postgres is accessed **live** via BigQuery EXTERNAL_QUERY (no staging dataset).
BQ connection: `projects/univest-applications/locations/asia-south2/connections/univest_db`
Every attribution query hits prod Postgres in real time — keep them bounded.

### Attribution model
One row per user in `user_additional_details` (install-time Singular data).
Join keys to Meta:
- `tracker_campaign_id` → `meta_campaign_id`
- `tracker_sub_campaign_id` → `meta_adset_id`
- `tracker_creative_id` → `meta_creative_id` (16-18 digit IDs matching Meta format)

Only Meta network value in prod: `'Facebook'`. Sub-platform (Instagram vs Facebook) is in `partner_site`.

### Revenue source
`user_transaction_history WHERE status = 'CHARGED'`
- Trial: `amount = 1 AND plan_id ILIKE '%trial%'`
- Conversion: `amount > 50 AND ROW_NUMBER per user = 1`
- Repeat conversion: `amount > 50 AND ROW_NUMBER per user > 1`

### `priority` column
Lives on `users` table, not `user_additional_details`. Both SQL files join `u.priority`.

### Metrics computed
D0 = `days_since_signup = 0`, D6 = `days_since_signup <= 6`
- signups, d0/d6 conversions, d0/d6 trials
- conversion %, trial % (over signups)
- avg_ltv_inr = total_revenue_inr / signups
- cac_inr = spend / signups, attributed_roas = revenue / spend

### `bq_client._wrap()` — critical detail
BQ EXTERNAL_QUERY takes a single-line double-quoted string. The `_wrap()` method:
1. Strips `-- line comments` with regex
2. Collapses all whitespace/newlines to single spaces
3. Escapes `\` and `"` for BQ string literal embedding

Do NOT put literal newlines or unescaped double quotes in the SQL that gets passed to `_wrap`.

### Partitions
`attribution_events` is range-partitioned monthly on `install_date`.
Partitions exist from `2022-08` through `2027-12` (created in migration 0002).
New partitions must be added manually before data arrives outside that range.

### Watermarks
`attribution_sync_cursor` table tracks per-job watermarks:
- `signups` cursor starts at `2022-08-01`
- `conversions` cursor starts at `2023-07-01`

The scheduler jobs advance the cursor to `NOW()` after each successful run.

### Backfill behaviour
`backfill_attribution()` in `sync_attribution.py` walks month boundaries.
It is idempotent — re-running any window is safe (upsert on `(id, install_date)`).

**Known gotcha:** if you launch the backfill with `| head -N` in the shell, Python gets
SIGPIPE after N lines and may silently skip months without erroring. Always run backfill
without piping, or pipe only after the process completes. If the DB max `install_date`
is earlier than expected after a backfill, re-run the missing window — it's safe.

## Scheduler jobs (worker)

| Job ID | Trigger | Function |
|---|---|---|
| `sync_structure` | every 30 min | `sync_account_structure` |
| `sync_insights_daily` | every 1 h | `sync_insights_daily` |
| `sync_higher_levels` | every 1 h | `sync_insights_higher_levels` |
| `sync_breakdowns` | every 6 h | `sync_insights_breakdowns` |
| `sync_aux` | daily 21:30 UTC | `sync_audiences_pixels_catalogs` |
| `sync_pixel_stats` | daily 21:30 UTC | `sync_pixel_stats` |
| `sync_attribution_signups` | every 15 min | `sync_attribution_signups` |
| `sync_attribution_conversions` | every 15 min | `sync_attribution_conversions` |
| `refresh_conversion_mv` | every 1 h | `refresh_conversion_mv` |

## API error contract

All errors return:
```json
{"error": {"code": "SNAKE_CASE", "message": "...", "details": {...}}}
```

All list endpoints are paginated with cursor-based pagination (base64-encoded offset).

## Admin endpoints

`/admin/*` requires `X-Admin-Key` header matching `settings.admin_api_key`.

- `POST /admin/jobs/{job_name}/run` — trigger any job manually
- `POST /admin/attribution/backfill` — trigger historical backfill
- `GET  /admin/attribution/cursors` — inspect watermark state

## Database — key tables

| Table | Description |
|---|---|
| `campaigns` | Meta campaign structure |
| `adsets` | Meta adset structure |
| `ads` | Meta ad structure |
| `ad_creatives` | Creative metadata |
| `insights_ad_daily` | Daily ad-level metrics |
| `insights_campaign_daily` | Campaign rollup |
| `insights_adset_daily` | Adset rollup |
| `attribution_events` | First-party events (signup/trial/conversion) — partitioned |
| `attribution_sync_cursor` | BQ ingestion watermarks |
| `bq_query_costs` | Per-query cost audit log |
| `mv_campaign_conversions` | Materialized: D0/D6 + LTV at campaign level |
| `mv_adset_conversions` | Materialized: D0/D6 + LTV at adset level |

## Cost safety

- Every BQ query dry-runs first; aborts if estimated bytes > `BQ_COST_CAP_BYTES` (default 5 GB)
- All query costs logged to `bq_query_costs`
- The SQL files use time-bounded `WHERE` clauses — never query without a date range

## Docs

`docs/attribution-schema.md` — full BQ schema introspection, answers to 12 architectural
questions, and a bug analysis of the original Metabase query. Read before making changes
to attribution logic.
