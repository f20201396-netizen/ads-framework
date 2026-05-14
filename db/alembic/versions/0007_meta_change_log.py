"""Add meta_change_log table — Meta Ads Activities (audit log) ingestion.

Backed by Meta Marketing API GET /{ad_account_id}/activities. Each row is one
audit-log event (e.g. status change, budget edit, creative update). We watermark
by event_time (UTC) so subsequent syncs pull only new events.

Revision ID: 0007
Revises: 0006
"""

from alembic import op
import sqlalchemy as sa


revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "meta_change_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text()),
        sa.Column("actor_name", sa.Text()),
        sa.Column("object_id", sa.Text()),
        sa.Column("object_type", sa.Text()),
        sa.Column("object_name", sa.Text()),
        sa.Column("translated_event_type", sa.Text()),
        sa.Column("application_id", sa.Text()),
        sa.Column("application_name", sa.Text()),
        sa.Column("extra_data", sa.dialects.postgresql.JSONB(), nullable=False, server_default="{}"),
        # Idempotency: same Meta event reported twice should not double-insert.
        # Composite unique covers the natural identity of an audit row.
        sa.UniqueConstraint(
            "account_id", "event_time", "event_type", "object_id", "actor_id",
            name="uq_meta_change_log_dedup",
        ),
    )
    op.create_index(
        "ix_meta_change_log_account_time",
        "meta_change_log",
        ["account_id", sa.text("event_time DESC")],
    )
    op.create_index(
        "ix_meta_change_log_object",
        "meta_change_log",
        ["object_id", sa.text("event_time DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_meta_change_log_object", table_name="meta_change_log")
    op.drop_index("ix_meta_change_log_account_time", table_name="meta_change_log")
    op.drop_table("meta_change_log")
