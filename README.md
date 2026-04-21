# ads-framework

Meta Ads Data Warehouse — full extraction, Postgres warehouse, and FastAPI read-only service.

## Architecture

```
Meta Graph API v21.0
        │
services/worker          ← APScheduler-driven sync jobs
        │ asyncpg
PostgreSQL 15 (partitioned fact tables)
        │ SQLAlchemy async
services/api             ← FastAPI, read-only, cursor-paginated
        │
Next.js dashboard
```

## Setup

### 1. Copy environment template

```bash
cp .env.example .env
```

Fill in:

| Variable | Description |
|---|---|
| `META_ACCESS_TOKEN` | System-user or user access token |
| `META_BUSINESS_ID` | Top-level Business Manager ID |
| `META_AD_ACCOUNT_IDS` | Comma-separated `act_XXXXXXX` IDs |
| `DATABASE_URL` | `postgresql+asyncpg://user:pass@host/db` |
| `ADMIN_API_KEY` | Secret for `X-Admin-Key` header |
| `FRONTEND_ORIGIN` | Allowed CORS origin (e.g. `https://dash.example.com`) |

### 2. Start services

```bash
make up       # starts postgres, api, worker
make migrate  # applies alembic schema
```

### 3. Seed historical data

```bash
make seed     # backfills from 2024-01-01 → today (all attribution windows)
```

This fires `historical_backfill()` which iterates every attribution window (`1d_click`, `7d_click`, `28d_click`, `1d_view`, `7d_view`) and all 26 breakdowns. Expect this to take several hours for a large account.

### 4. Generate OpenAPI spec

```bash
make openapi  # writes openapi.json to repo root
```

---

## API Overview

Base URL: `http://localhost:8000`

Interactive docs: `/docs` (Swagger UI), `/redoc`

### Structure

| Method | Path | Description |
|---|---|---|
| GET | `/structure/accounts` | List all ad accounts |
| GET | `/structure/accounts/{id}` | Single account |
| GET | `/structure/campaigns` | List campaigns (`?account_id=`, `?status=`, `?q=`) |
| GET | `/structure/campaigns/{id}` | Single campaign |
| GET | `/structure/adsets` | List ad sets (`?campaign_id=`, `?status=`) |
| GET | `/structure/adsets/{id}` | Single ad set |
| GET | `/structure/ads` | List ads (`?adset_id=`, `?status=`) |
| GET | `/structure/ads/{id}` | Single ad |
| GET | `/structure/creatives` | List creatives (`?account_id=`) |
| GET | `/structure/creatives/{id}` | Single creative |
| GET | `/structure/creatives/{id}/preview` | Meta iframe preview (`?format=`) |

### Insights

| Method | Path | Description |
|---|---|---|
| GET | `/insights/timeseries` | Daily metric series for any object |
| GET | `/insights/breakdown` | Breakdown-sliced metrics (age, gender, country …) |
| GET | `/insights/compare` | Side-by-side two date ranges with delta |
| GET | `/insights/top` | Ranked entities by a metric over a period |

All insights endpoints accept `?level=ad|adset|campaign|account`, `?metrics=` (comma-separated), and `?attribution_window=`.

### Audiences

| Method | Path | Description |
|---|---|---|
| GET | `/audiences/custom-audiences` | List custom audiences |
| GET | `/audiences/custom-audiences/{id}` | Single audience |
| GET | `/audiences/pixels` | List pixels |
| GET | `/audiences/pixels/{id}` | Single pixel |
| GET | `/audiences/pixels/{id}/stats` | Pixel event counts by day |
| GET | `/audiences/custom-conversions` | List custom conversions |
| GET | `/audiences/custom-conversions/{id}` | Single conversion |

### Catalogs

| Method | Path | Description |
|---|---|---|
| GET | `/catalogs` | List product catalogs (`?business_id=`) |
| GET | `/catalogs/{id}` | Single catalog |
| GET | `/catalogs/{id}/product-sets` | List product sets in catalog |
| GET | `/catalogs/{id}/product-sets/{ps_id}` | Single product set |

### Admin (requires `X-Admin-Key`)

| Method | Path | Description |
|---|---|---|
| POST | `/admin/sync/{job_name}` | Trigger a sync job immediately |
| POST | `/admin/backfill` | Trigger historical backfill (`?since=&until=`) |
| GET | `/admin/sync-runs` | List sync run history |
| GET | `/admin/sync-runs/{id}` | Single sync run detail |
| GET | `/admin/rate-limits` | Rate-limit header history |

Valid `job_name` values: `sync_structure`, `sync_insights_daily`, `sync_higher_levels`, `sync_breakdowns`, `sync_aux`, `sync_pixel_stats`.

---

## Pagination

All list endpoints are cursor-paginated:

```
GET /structure/campaigns?limit=50&cursor=<opaque>
→ { "data": [...], "cursor": "<next>", "total": 1234 }
```

`cursor` is a base64-encoded offset. When `cursor` is `null` you are on the last page.

## Error contract

All errors use:

```json
{
  "error": {
    "code": "not_found",
    "message": "Campaign not found",
    "details": {}
  }
}
```

---

## Runbook

### Rotating the Meta access token

1. Generate a new token in Meta Business Manager (System Users → Generate Token).
2. Update `META_ACCESS_TOKEN` in `.env`.
3. Restart the worker: `docker compose restart worker`.

No DB migration is needed — the token is config-only.

### Adding a new breakdown

1. Add the breakdown string to `INSIGHT_BREAKDOWNS` in `services/shared/constants.py`.
2. Add its key fields to `_BREAKDOWN_KEY_FIELDS` in `services/worker/parsers.py`.
3. On the next `sync_breakdowns` run (or via `POST /admin/sync/sync_breakdowns`) it will be picked up automatically.

### Adding a new scalar metric

1. Add the column to `InsightsDaily` (and sibling tables) in `services/shared/models.py`.
2. Write and apply an alembic migration: `alembic revision --autogenerate -m "add metric"` then `alembic upgrade head`.
3. Add the metric name to `_SCALAR_METRICS` in `services/api/routers/insights.py`.
4. Add it to the appropriate `INSIGHTS_*_FIELDS` constant in `services/shared/constants.py`.
5. Add the parser coercion in `services/worker/parsers.py`.
6. Re-export the OpenAPI spec: `make openapi`.

### Re-generating openapi.json

```bash
make openapi
git add openapi.json && git commit -m "chore: refresh openapi.json"
```

---

## Scheduler jobs

| Job | Trigger | What it does |
|---|---|---|
| `sync_structure` | every 30 min | Businesses → accounts → campaigns → creatives → ad sets → ads |
| `sync_insights_daily` | every 1 h | Ad-level daily insights, last 3 days, `7d_click` |
| `sync_higher_levels` | every 1 h | Adset/campaign/account daily insights, last 3 days |
| `sync_breakdowns` | every 6 h | All 26 breakdowns, last 7 days |
| `sync_aux` | 21:30 UTC | Audiences, pixels, conversions, catalogs |
| `sync_pixel_stats` | 21:30 UTC | Pixel event counts per day |
