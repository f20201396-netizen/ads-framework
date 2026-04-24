"""Add Google Ads tables: campaigns, ad_groups, ads, insights_daily.

Revision ID: 0004
Revises: 0003
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── google_campaigns ───────────────────────────────────────────────────
    op.create_table(
        "google_campaigns",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("customer_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text()),
        sa.Column("status", sa.Text()),
        sa.Column("advertising_channel_type", sa.Text()),
        sa.Column("bidding_strategy_type", sa.Text()),
        sa.Column("daily_budget", sa.Numeric(18, 2)),
        sa.Column("start_date", sa.Date()),
        sa.Column("end_date", sa.Date()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
    )
    op.create_index("ix_google_campaigns_customer_id", "google_campaigns", ["customer_id"])

    # ── google_ad_groups ───────────────────────────────────────────────────
    op.create_table(
        "google_ad_groups",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("campaign_id", sa.BigInteger(), sa.ForeignKey("google_campaigns.id"), nullable=False),
        sa.Column("customer_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text()),
        sa.Column("status", sa.Text()),
        sa.Column("type", sa.Text()),
        sa.Column("cpc_bid", sa.Numeric(18, 2)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
    )
    op.create_index("ix_google_ad_groups_campaign_id", "google_ad_groups", ["campaign_id"])

    # ── google_ads ─────────────────────────────────────────────────────────
    op.create_table(
        "google_ads",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("ad_group_id", sa.BigInteger(), sa.ForeignKey("google_ad_groups.id"), nullable=False),
        sa.Column("campaign_id", sa.BigInteger(), nullable=False),
        sa.Column("customer_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text()),
        sa.Column("status", sa.Text()),
        sa.Column("type", sa.Text()),
        sa.Column("final_urls", JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
    )
    op.create_index("ix_google_ads_ad_group_id", "google_ads", ["ad_group_id"])
    op.create_index("ix_google_ads_campaign_id", "google_ads", ["campaign_id"])

    # ── google_insights_daily ──────────────────────────────────────────────
    op.create_table(
        "google_insights_daily",
        sa.Column("ad_id", sa.BigInteger(), nullable=False),
        sa.Column("ad_group_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_id", sa.BigInteger(), nullable=False),
        sa.Column("customer_id", sa.Text(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("impressions", sa.BigInteger()),
        sa.Column("clicks", sa.BigInteger()),
        sa.Column("spend", sa.Numeric(18, 2)),
        sa.Column("ctr", sa.Numeric(10, 4)),
        sa.Column("avg_cpm", sa.Numeric(18, 2)),
        sa.Column("avg_cpc", sa.Numeric(18, 2)),
        sa.Column("conversions", sa.Numeric(10, 2)),
        sa.Column("conversions_value", sa.Numeric(18, 2)),
        sa.Column("view_through_conversions", sa.BigInteger()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint("ad_id", "date"),
    )
    op.create_index("ix_google_insights_campaign_date", "google_insights_daily", ["campaign_id", "date"])
    op.create_index("ix_google_insights_date", "google_insights_daily", ["date"])


def downgrade() -> None:
    op.drop_table("google_insights_daily")
    op.drop_table("google_ads")
    op.drop_table("google_ad_groups")
    op.drop_table("google_campaigns")
