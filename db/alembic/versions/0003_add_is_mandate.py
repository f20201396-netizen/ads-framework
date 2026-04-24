"""Add is_mandate column to attribution_events.

Mandate payments = order_id LIKE '%md%' (NACH/e-mandate auto-debit).
Column is populated by the conversions attribution job going forward;
a backfill is needed for historical data.

Revision ID: 0003
Revises: 0002
"""

from alembic import op
import sqlalchemy as sa

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add to parent table — Postgres propagates to all partitions automatically
    op.add_column(
        "attribution_events",
        sa.Column("is_mandate", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("attribution_events", "is_mandate")
