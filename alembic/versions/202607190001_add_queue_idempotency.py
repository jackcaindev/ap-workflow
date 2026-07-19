"""add reliable queue idempotency constraints

Revision ID: 202607190001
Revises: 202607010001
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202607190001"
down_revision: str | None = "202607010001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("documents", sa.Column("source_type", sa.String(length=32)))
    op.add_column("documents", sa.Column("source_idempotency_key", sa.String(length=64)))
    op.add_column("documents", sa.Column("source_message_id", sa.String(length=255)))
    op.add_column("documents", sa.Column("source_part_id", sa.String(length=255)))
    op.add_column("documents", sa.Column("source_enqueued_at", sa.DateTime(timezone=True)))
    op.add_column("documents", sa.Column("content_sha256", sa.String(length=64)))
    op.create_index(
        "ix_documents_source_idempotency_key",
        "documents",
        ["source_idempotency_key"],
        unique=True,
    )
    op.execute(
        "DELETE FROM reconciliation_results a USING reconciliation_results b "
        "WHERE a.run_id IS NOT NULL AND a.run_id = b.run_id AND a.ctid < b.ctid"
    )
    op.create_index(
        "uq_reconciliation_results_run_id",
        "reconciliation_results",
        ["run_id"],
        unique=True,
        postgresql_where=sa.text("run_id IS NOT NULL"),
    )
    op.execute(
        "DELETE FROM workflow_audit_logs a USING workflow_audit_logs b "
        "WHERE a.run_id = b.run_id AND a.event_type = b.event_type AND a.id > b.id"
    )
    op.create_index(
        "uq_workflow_audit_logs_run_event",
        "workflow_audit_logs",
        ["run_id", "event_type"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_workflow_audit_logs_run_event", table_name="workflow_audit_logs")
    op.drop_index("uq_reconciliation_results_run_id", table_name="reconciliation_results")
    op.drop_index("ix_documents_source_idempotency_key", table_name="documents")
    op.drop_column("documents", "content_sha256")
    op.drop_column("documents", "source_enqueued_at")
    op.drop_column("documents", "source_part_id")
    op.drop_column("documents", "source_message_id")
    op.drop_column("documents", "source_idempotency_key")
    op.drop_column("documents", "source_type")
