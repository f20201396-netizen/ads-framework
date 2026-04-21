"""Attribution schema — Phase 6.

Tables:
  attribution_events          — one row per event (signup/trial/conversion/repeat)
                                partitioned monthly on install_date (user signup date)
  attribution_sync_cursor     — per-job watermark
  bq_query_costs              — query cost audit log

Materialized views (refreshed CONCURRENTLY hourly):
  mv_campaign_conversions     — D0/D6 + LTV rolled up to campaign × install_date
  mv_adset_conversions        — same at adset level

Revision ID: 0002
Revises: 0001
"""

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


# ── helpers ────────────────────────────────────────────────────────────────

def _idx(table: str, *cols, unique=False):
    suffix = "_".join(cols)
    op.execute(
        f"CREATE {'UNIQUE ' if unique else ''}INDEX IF NOT EXISTS "
        f"ix_{table}_{suffix} ON {table} ({', '.join(cols)})"
    )


def _attr_partitions():
    """Monthly partitions from 2022-08 through 2027-12."""
    stmts = []
    for year in range(2022, 2028):
        start_month = 8 if year == 2022 else 1
        for month in range(start_month, 13):
            lo = f"{year}-{month:02d}-01"
            hi_year, hi_month = (year, month + 1) if month < 12 else (year + 1, 1)
            hi = f"{hi_year}-{hi_month:02d}-01"
            name = f"attribution_events_{year}_{month:02d}"
            stmts.append(
                f"CREATE TABLE IF NOT EXISTS {name} "
                f"PARTITION OF attribution_events "
                f"FOR VALUES FROM ('{lo}') TO ('{hi}')"
            )
    return stmts


# ── upgrade ────────────────────────────────────────────────────────────────

def upgrade():
    # ------------------------------------------------------------------
    # attribution_events  (partitioned by install_date)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE attribution_events (
            id                  TEXT        NOT NULL,
            user_id             BIGINT      NOT NULL,
            event_name          TEXT        NOT NULL,
            event_time          TIMESTAMPTZ NOT NULL,
            install_date        DATE        NOT NULL,
            days_since_signup   INTEGER,
            network             TEXT,
            publisher_site      TEXT,
            meta_campaign_id    TEXT,
            meta_adset_id       TEXT,
            meta_creative_id    TEXT,
            campaign_name       TEXT,
            adset_name          TEXT,
            creative_name       TEXT,
            revenue_inr         NUMERIC(12,2),
            plan_id             TEXT,
            is_trial            BOOLEAN     NOT NULL DEFAULT FALSE,
            is_first_payment    BOOLEAN     NOT NULL DEFAULT FALSE,
            is_reattributed     BOOLEAN     NOT NULL DEFAULT FALSE,
            is_organic          BOOLEAN     NOT NULL DEFAULT FALSE,
            is_viewthrough      BOOLEAN     NOT NULL DEFAULT FALSE,
            platform            TEXT,
            os_version          TEXT,
            device_brand        TEXT,
            device_model        TEXT,
            priority            TEXT,
            source_table        TEXT        NOT NULL,
            raw                 JSONB       NOT NULL DEFAULT '{}',
            synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id, install_date)
        ) PARTITION BY RANGE (install_date)
    """)

    for stmt in _attr_partitions():
        op.execute(stmt)

    # indexes on parent table
    _idx("attribution_events", "meta_campaign_id", "install_date")
    _idx("attribution_events", "meta_adset_id",    "install_date")
    _idx("attribution_events", "meta_creative_id", "install_date")
    _idx("attribution_events", "event_name",       "install_date")
    _idx("attribution_events", "network",          "install_date")
    _idx("attribution_events", "user_id")

    # ------------------------------------------------------------------
    # attribution_sync_cursor
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE attribution_sync_cursor (
            job_name                TEXT        PRIMARY KEY,
            last_processed_time     TIMESTAMPTZ,
            last_run_at             TIMESTAMPTZ,
            rows_ingested_last_run  INTEGER     NOT NULL DEFAULT 0,
            bytes_processed_last_run BIGINT     NOT NULL DEFAULT 0,
            error                   JSONB
        )
    """)

    # seed cursors
    op.execute("""
        INSERT INTO attribution_sync_cursor (job_name, last_processed_time)
        VALUES
            ('signups',     '2022-08-01 00:00:00+00'),
            ('conversions', '2023-07-01 00:00:00+00')
        ON CONFLICT DO NOTHING
    """)

    # ------------------------------------------------------------------
    # bq_query_costs
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE bq_query_costs (
            id              BIGSERIAL   PRIMARY KEY,
            query_label     TEXT,
            bytes_processed BIGINT,
            rows_returned   INTEGER,
            run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            duration_ms     INTEGER
        )
    """)
    _idx("bq_query_costs", "query_label", "run_at")

    # ------------------------------------------------------------------
    # MV: mv_campaign_conversions
    # ------------------------------------------------------------------
    op.execute("""
        CREATE MATERIALIZED VIEW mv_campaign_conversions AS
        WITH conv AS (
            SELECT
                meta_campaign_id                                                AS campaign_id,
                install_date,
                COUNT(DISTINCT CASE WHEN event_name = 'signup'                 THEN user_id END) AS signups,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup = 0                 THEN user_id END) AS d0_conversions,
                COUNT(DISTINCT CASE WHEN event_name = 'trial'
                                     AND days_since_signup = 0                 THEN user_id END) AS d0_trials,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup <= 6                THEN user_id END) AS d6_conversions,
                COUNT(DISTINCT CASE WHEN event_name = 'trial'
                                     AND days_since_signup <= 6                THEN user_id END) AS d6_trials,
                SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                         THEN revenue_inr ELSE 0 END)                          AS total_revenue_inr
            FROM attribution_events
            WHERE meta_campaign_id IS NOT NULL
            GROUP BY 1, 2
        ),
        spend AS (
            SELECT
                campaign_id,
                date          AS spend_date,
                SUM(spend)    AS spend,
                SUM(impressions) AS impressions,
                SUM(clicks)   AS clicks
            FROM insights_campaign_daily
            WHERE attribution_window = '7d_click'
            GROUP BY 1, 2
        )
        SELECT
            c.campaign_id,
            c.install_date,
            c.signups,
            c.d0_conversions,
            c.d0_trials,
            c.d6_conversions,
            c.d6_trials,
            c.total_revenue_inr,
            ROUND(c.d0_conversions * 100.0 / NULLIF(c.signups, 0), 2)  AS d0_conversion_pct,
            ROUND(c.d0_trials      * 100.0 / NULLIF(c.signups, 0), 2)  AS d0_trial_pct,
            ROUND(c.d6_conversions * 100.0 / NULLIF(c.signups, 0), 2)  AS d6_conversion_pct,
            ROUND(c.d6_trials      * 100.0 / NULLIF(c.signups, 0), 2)  AS d6_trial_pct,
            ROUND(c.total_revenue_inr / NULLIF(c.signups, 0), 2)        AS avg_ltv_inr,
            s.spend,
            s.impressions,
            s.clicks,
            ROUND(s.spend / NULLIF(c.signups, 0), 2)                   AS cac_inr,
            ROUND(c.total_revenue_inr / NULLIF(s.spend, 0), 4)         AS attributed_roas
        FROM conv c
        LEFT JOIN spend s
               ON s.campaign_id = c.campaign_id
              AND s.spend_date   = c.install_date
        WITH NO DATA
    """)
    op.execute("CREATE UNIQUE INDEX uix_mv_campaign_conv ON mv_campaign_conversions (campaign_id, install_date)")
    op.execute("CREATE INDEX ix_mv_campaign_conv_date ON mv_campaign_conversions (install_date)")

    # ------------------------------------------------------------------
    # MV: mv_adset_conversions
    # ------------------------------------------------------------------
    op.execute("""
        CREATE MATERIALIZED VIEW mv_adset_conversions AS
        WITH conv AS (
            SELECT
                meta_campaign_id                                                AS campaign_id,
                meta_adset_id                                                   AS adset_id,
                install_date,
                COUNT(DISTINCT CASE WHEN event_name = 'signup'                 THEN user_id END) AS signups,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup = 0                 THEN user_id END) AS d0_conversions,
                COUNT(DISTINCT CASE WHEN event_name = 'trial'
                                     AND days_since_signup = 0                 THEN user_id END) AS d0_trials,
                COUNT(DISTINCT CASE WHEN event_name IN ('conversion','repeat_conversion')
                                     AND days_since_signup <= 6                THEN user_id END) AS d6_conversions,
                COUNT(DISTINCT CASE WHEN event_name = 'trial'
                                     AND days_since_signup <= 6                THEN user_id END) AS d6_trials,
                SUM(CASE WHEN event_name IN ('conversion','repeat_conversion')
                         THEN revenue_inr ELSE 0 END)                          AS total_revenue_inr
            FROM attribution_events
            WHERE meta_adset_id IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        spend AS (
            SELECT
                campaign_id,
                adset_id,
                date          AS spend_date,
                SUM(spend)    AS spend,
                SUM(impressions) AS impressions,
                SUM(clicks)   AS clicks
            FROM insights_adset_daily
            WHERE attribution_window = '7d_click'
            GROUP BY 1, 2, 3
        )
        SELECT
            c.campaign_id,
            c.adset_id,
            c.install_date,
            c.signups,
            c.d0_conversions,
            c.d0_trials,
            c.d6_conversions,
            c.d6_trials,
            c.total_revenue_inr,
            ROUND(c.d0_conversions * 100.0 / NULLIF(c.signups, 0), 2)  AS d0_conversion_pct,
            ROUND(c.d0_trials      * 100.0 / NULLIF(c.signups, 0), 2)  AS d0_trial_pct,
            ROUND(c.d6_conversions * 100.0 / NULLIF(c.signups, 0), 2)  AS d6_conversion_pct,
            ROUND(c.d6_trials      * 100.0 / NULLIF(c.signups, 0), 2)  AS d6_trial_pct,
            ROUND(c.total_revenue_inr / NULLIF(c.signups, 0), 2)        AS avg_ltv_inr,
            s.spend,
            s.impressions,
            s.clicks,
            ROUND(s.spend / NULLIF(c.signups, 0), 2)                   AS cac_inr,
            ROUND(c.total_revenue_inr / NULLIF(s.spend, 0), 4)         AS attributed_roas
        FROM conv c
        LEFT JOIN spend s
               ON s.campaign_id  = c.campaign_id
              AND s.adset_id     = c.adset_id
              AND s.spend_date   = c.install_date
        WITH NO DATA
    """)
    op.execute("CREATE UNIQUE INDEX uix_mv_adset_conv ON mv_adset_conversions (adset_id, install_date)")
    op.execute("CREATE INDEX ix_mv_adset_conv_campaign ON mv_adset_conversions (campaign_id, install_date)")


def downgrade():
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_adset_conversions")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_campaign_conversions")
    op.execute("DROP TABLE IF EXISTS bq_query_costs")
    op.execute("DROP TABLE IF EXISTS attribution_sync_cursor")
    op.execute("DROP TABLE IF EXISTS attribution_events CASCADE")
