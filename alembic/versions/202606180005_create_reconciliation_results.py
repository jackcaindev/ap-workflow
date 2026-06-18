"""create reconciliation results

Revision ID: 202606180005
Revises: 202606180004
Create Date: 2026-06-18 13:05:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202606180005"
down_revision: str | None = "202606180004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "reconciliation_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("shipment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=True),
        sa.Column("checks", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("missing_docs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("exception_reasons", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["shipment_id"], ["shipments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.run_id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_reconciliation_results_shipment_id",
        "reconciliation_results",
        ["shipment_id"],
    )
    op.create_index("ix_reconciliation_results_run_id", "reconciliation_results", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_reconciliation_results_run_id", table_name="reconciliation_results")
    op.drop_index("ix_reconciliation_results_shipment_id", table_name="reconciliation_results")
    op.drop_table("reconciliation_results")
