"""Fix singular_campaign_metrics: correct column names and add os for platform split.

Prod table uses start_date (not date), adn_campaign_name (not campaign_name),
adn_cost (not cost), etc.  Also adds os column so we can store Android vs iOS
spend separately, enabling direct (non-proportional) spend allocation.

Revision ID: 0006
Revises: 0005
"""

from alembic import op
import sqlalchemy as sa

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the original table (it's empty — just created in 0005) and recreate
    # with the correct schema that mirrors prod column semantics.
    op.drop_index("ix_singular_campaign_metrics_date", "singular_campaign_metrics")
    op.drop_constraint("uq_singular_campaign_metrics", "singular_campaign_metrics")
    op.drop_table("singular_campaign_metrics")

    op.create_table(
        "singular_campaign_metrics",
        sa.Column("id",            sa.BigInteger(),              primary_key=True, autoincrement=True),
        # Mirrors prod start_date column
        sa.Column("date",          sa.Date(),                    nullable=False),
        sa.Column("source",        sa.Text(),                    nullable=False, server_default=""),
        sa.Column("campaign_name", sa.Text(),                    nullable=False, server_default=""),
        # OS level — 'Android', 'iOS', 'Web', 'Mixed'
        sa.Column("os",            sa.Text(),                    nullable=False, server_default=""),
        sa.Column("cost",          sa.Numeric(18, 2)),
        sa.Column("installs",      sa.BigInteger()),
        sa.Column("clicks",        sa.BigInteger()),
        sa.Column("impressions",   sa.BigInteger()),
        sa.Column("synced_at",     sa.DateTime(timezone=True),  server_default=sa.func.now()),
        sa.UniqueConstraint("date", "source", "campaign_name", "os",
                            name="uq_singular_campaign_metrics"),
    )
    op.create_index("ix_singular_campaign_metrics_date", "singular_campaign_metrics", ["date"])


def downgrade() -> None:
    op.drop_index("ix_singular_campaign_metrics_date", "singular_campaign_metrics")
    op.drop_table("singular_campaign_metrics")

    op.create_table(
        "singular_campaign_metrics",
        sa.Column("id",            sa.BigInteger(),              primary_key=True, autoincrement=True),
        sa.Column("date",          sa.Date(),                    nullable=False),
        sa.Column("source",        sa.Text(),                    nullable=False, server_default=""),
        sa.Column("campaign_name", sa.Text(),                    nullable=False, server_default=""),
        sa.Column("cost",          sa.Numeric(18, 2)),
        sa.Column("installs",      sa.BigInteger()),
        sa.Column("clicks",        sa.BigInteger()),
        sa.Column("impressions",   sa.BigInteger()),
        sa.Column("synced_at",     sa.DateTime(timezone=True),  server_default=sa.func.now()),
        sa.UniqueConstraint("date", "source", "campaign_name",
                            name="uq_singular_campaign_metrics"),
    )
    op.create_index("ix_singular_campaign_metrics_date", "singular_campaign_metrics", ["date"])
