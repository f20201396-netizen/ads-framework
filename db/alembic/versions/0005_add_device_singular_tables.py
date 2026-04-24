"""Add user_devices and singular_campaign_metrics tables.

user_devices: local mirror of prod user_devices for iOS/Android platform detection.
singular_campaign_metrics: Singular MMP cost data for accurate spend figures.

Revision ID: 0005
Revises: 0004
"""

from alembic import op
import sqlalchemy as sa

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── user_devices ──────────────────────────────────────────────────────────
    # One row per user_id (upserted on conflict) — stores primary device OS.
    op.create_table(
        "user_devices",
        sa.Column("user_id",    sa.BigInteger(),              nullable=False),
        sa.Column("os",         sa.Text()),
        sa.Column("synced_at",  sa.DateTime(timezone=True),  server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("user_id"),
    )

    # ── singular_campaign_metrics ─────────────────────────────────────────────
    # Daily aggregate campaign cost from Singular MMP.
    # Primary key is synthetic; unique constraint prevents duplicate upserts.
    op.create_table(
        "singular_campaign_metrics",
        sa.Column("id",            sa.BigInteger(),               primary_key=True, autoincrement=True),
        sa.Column("date",          sa.Date(),                     nullable=False),
        sa.Column("source",        sa.Text(),                     nullable=False, server_default=""),
        sa.Column("campaign_name", sa.Text(),                     nullable=False, server_default=""),
        sa.Column("cost",          sa.Numeric(18, 2)),
        sa.Column("installs",      sa.BigInteger()),
        sa.Column("clicks",        sa.BigInteger()),
        sa.Column("impressions",   sa.BigInteger()),
        sa.Column("synced_at",     sa.DateTime(timezone=True),   server_default=sa.func.now()),
        sa.UniqueConstraint("date", "source", "campaign_name", name="uq_singular_campaign_metrics"),
    )
    op.create_index("ix_singular_campaign_metrics_date", "singular_campaign_metrics", ["date"])

    # Cursor rows for the two new jobs
    op.execute(
        "INSERT INTO attribution_sync_cursor (job_name) "
        "VALUES ('user_devices'), ('singular_campaign_metrics') "
        "ON CONFLICT (job_name) DO NOTHING"
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM attribution_sync_cursor "
        "WHERE job_name IN ('user_devices', 'singular_campaign_metrics')"
    )
    op.drop_index("ix_singular_campaign_metrics_date", "singular_campaign_metrics")
    op.drop_table("singular_campaign_metrics")
    op.drop_table("user_devices")
