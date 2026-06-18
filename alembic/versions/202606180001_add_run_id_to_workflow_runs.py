"""add run id to workflow runs

Revision ID: 202606180001
Revises: 202606160001
Create Date: 2026-06-18 11:15:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202606180001"
down_revision: str | None = "202606160001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("workflow_runs", sa.Column("run_id", sa.String(length=36), nullable=True))
    # Existing development rows get UUIDs during migration so the column can be
    # made non-null without assuming the database is empty.
    op.execute("UPDATE workflow_runs SET run_id = gen_random_uuid()::text WHERE run_id IS NULL")
    op.alter_column("workflow_runs", "run_id", nullable=False)
    op.create_index("ix_workflow_runs_run_id", "workflow_runs", ["run_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_workflow_runs_run_id", table_name="workflow_runs")
    op.drop_column("workflow_runs", "run_id")
