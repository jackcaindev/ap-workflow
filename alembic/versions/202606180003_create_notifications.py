"""create notifications

Revision ID: 202606180003
Revises: 202606180002
Create Date: 2026-06-18 12:30:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202606180003"
down_revision: str | None = "202606180002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "notifications",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "sent_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("total_count", sa.Integer(), nullable=False),
        sa.Column("complete_count", sa.Integer(), nullable=False),
        sa.Column("awaiting_review_count", sa.Integer(), nullable=False),
        sa.Column("failed_count", sa.Integer(), nullable=False),
    )
    op.create_index("ix_notifications_sent_at", "notifications", ["sent_at"])


def downgrade() -> None:
    op.drop_index("ix_notifications_sent_at", table_name="notifications")
    op.drop_table("notifications")
