"""create workflow audit logs

Revision ID: 202607010001
Revises: 202606180005
Create Date: 2026-07-01 10:00:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202607010001"
down_revision: str | None = "202606180005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_audit_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("actor", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.run_id"], ondelete="CASCADE"),
    )
    op.create_index("ix_workflow_audit_logs_run_id", "workflow_audit_logs", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_workflow_audit_logs_run_id", table_name="workflow_audit_logs")
    op.drop_table("workflow_audit_logs")
