# Apps Script PoC — Campaign Level mirror

Proof of concept: replicate the Python pipeline's "Campaign Level" tab using only Meta API (spend) and BigQuery EXTERNAL_QUERY (attribution). No new BQ datasets, no BQ writes. Persistent state lives in hidden tabs on the dashboard sheet.

## Setup

1. **Bind to dashboard sheet**
   - Open https://docs.google.com/spreadsheets/d/1EBu7vZWGdLUVdL4I6a0J22soLIoXKWWIRRWTGk3BZ7s
   - Extensions → Apps Script → opens the bound script editor.

2. **Push files (option A: clasp)**
   ```bash
   npm install -g @google/clasp
   clasp login
   cd /Users/macbook/ads-framework/apps-script-poc
   clasp create --type sheets --title "Univest Meta Ads PoC" --rootDir .
   # if the project already exists, run `clasp clone <scriptId>` instead
   clasp push
   ```

3. **Push files (option B: paste)**
   - In the Apps Script editor, create one file per `.gs` in this directory (File → New → Script). Paste contents verbatim.
   - Replace the auto-generated `appsscript.json` with the one in this folder (View → Show project manifest → paste).

4. **Configure secrets** (one-time)
   - In the Apps Script editor, run `Config.setupSecrets()` once with the values prompted (or paste them inline at the top of `Config.gs` if you prefer). Required keys:
     - `META_ACCESS_TOKEN`, `META_APP_SECRET`
     - `AD_ACCOUNT_IDS` (csv, format `act_xxx,act_yyy`)
     - `BQ_PROJECT` (default `univest-applications`)
     - `BQ_CONNECTION` (default `projects/univest-applications/locations/asia-south2/connections/univest_db`)
     - `SHEET_ID` (default `1EBu7vZWGdLUVdL4I6a0J22soLIoXKWWIRRWTGk3BZ7s`)

5. **Smoke tests**
   - Run `Smoke.testBq()` — expects `[{f0_: 1}]`.
   - Run `Smoke.testMeta()` — expects a JSON list of ad accounts.

6. **First sync**
   - Run `Triggers.runPoc()` manually. Cold-start pulls 90 days of campaign spend, then runs the attribution query and writes to the test tab. Expect ~3-5 min first time, <2 min steady state.

7. **Schedule**
   - Run `Triggers.installTrigger()` once. Creates a 30-min recurring time trigger. Manage via `Triggers.uninstallTrigger()`.

## Validation

The Python pipeline keeps writing `Campaign Level — Meta`. This Apps Script writes `Campaign Level — Apps Script Test`. Diff them after each run; pass criteria = all 53 campaigns match within rounding for 5 consecutive cycles.

## File map

| File | Purpose |
|---|---|
| `Config.gs` | Secrets via PropertiesService, constants |
| `MetaClient.gs` | UrlFetchApp wrapper with backoff |
| `BqClient.gs` | BigQuery query + paginated results, read-only |
| `HiddenSpend.gs` | `_spend_campaign` hidden tab upsert + read |
| `IngestSpendCampaign.gs` | Pull last 3 (or 90 first run) days of campaign insights |
| `FetchAttribution.gs` | Single BQ query that returns per-campaign per-bucket counts/revenue |
| `Aggregate.gs` | Port of `CAMPAIGN_LEVEL_SQL` + grade + pred D6 ROAS picker |
| `WriteSheet.gs` | Write the test tab |
| `Triggers.gs` | `runPoc()` orchestrator + trigger management |
| `Smoke.gs` | Smoke tests for setup verification |

## Reference (Python source of truth)

- `scripts/update_meta_dashboard.py:CAMPAIGN_LEVEL_SQL` (~lines 1099-1404) — every column we reproduce
- `scripts/update_meta_dashboard.py:build_campaign_data` — pred D6 ROAS picker (mature→overall→global, ≥10 trials threshold)
- `scripts/update_meta_dashboard.py:compute_ad_scores(use_pred_for_recent=True)` — recent grade uses predicted D6 ROAS
- `services/worker/sql/attribution/signups.sql`, `conversions.sql` — basis for `FetchAttribution.gs` SQL
