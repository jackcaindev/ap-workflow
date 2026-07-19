"""add missing-document SLA exceptions

Revision ID: 202607190004
Revises: 202607190003
Create Date: 2026-07-19 16:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202607190004"
down_revision: str | None = "202607190003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "reconciliation_results",
        sa.Column("evaluation_source", sa.String(length=32), server_default="legacy", nullable=False),
    )
    op.add_column(
        "reconciliation_results",
        sa.Column("evaluation_key", sa.String(length=255), nullable=True),
    )
    op.execute(
        "UPDATE reconciliation_results SET evaluation_source = "
        "CASE WHEN run_id IS NULL THEN 'legacy' ELSE 'document_workflow' END"
    )
    op.create_index(
        "ix_reconciliation_results_evaluation_key",
        "reconciliation_results",
        ["evaluation_key"],
    )
    op.create_index(
        "uq_reconciliation_results_evaluation_key",
        "reconciliation_results",
        ["evaluation_key"],
        unique=True,
        postgresql_where=sa.text("evaluation_key IS NOT NULL"),
    )

    op.create_table(
        "shipment_exceptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("shipment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("missing_docs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reason_codes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["shipment_id"], ["shipments.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("shipment_id", "kind", name="uq_shipment_exceptions_shipment_kind"),
        sa.CheckConstraint("kind = 'missing_required_documents'", name="ck_shipment_exceptions_kind"),
        sa.CheckConstraint("status IN ('active', 'resolved')", name="ck_shipment_exceptions_status"),
    )
    op.create_index("ix_shipment_exceptions_shipment_id", "shipment_exceptions", ["shipment_id"])
    op.create_index("ix_shipment_exceptions_status", "shipment_exceptions", ["status"])

    op.create_table(
        "shipment_exception_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("exception_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reconciliation_result_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("transition", sa.String(length=16), nullable=False),
        sa.Column("before_state", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("after_state", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("notification_status", sa.String(length=16), server_default="pending", nullable=False),
        sa.Column("notification_attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("notification_last_error", sa.Text(), nullable=True),
        sa.Column("notification_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["exception_id"], ["shipment_exceptions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["reconciliation_result_id"], ["reconciliation_results.id"], ondelete="SET NULL"
        ),
        sa.UniqueConstraint("exception_id", "version", name="uq_shipment_exception_events_version"),
        sa.CheckConstraint(
            "transition IN ('opened', 'changed', 'resolved')",
            name="ck_shipment_exception_events_transition",
        ),
        sa.CheckConstraint(
            "notification_status IN ('pending', 'sending', 'sent', 'failed')",
            name="ck_shipment_exception_events_notification_status",
        ),
    )
    op.create_index(
        "ix_shipment_exception_events_exception_id", "shipment_exception_events", ["exception_id"]
    )
    op.create_index(
        "ix_shipment_exception_events_reconciliation_result_id",
        "shipment_exception_events",
        ["reconciliation_result_id"],
    )
    op.create_index(
        "ix_shipment_exception_events_notification_status",
        "shipment_exception_events",
        ["notification_status"],
    )


def downgrade() -> None:
    op.drop_table("shipment_exception_events")
    op.drop_table("shipment_exceptions")
    op.drop_index(
        "uq_reconciliation_results_evaluation_key", table_name="reconciliation_results"
    )
    op.drop_index(
        "ix_reconciliation_results_evaluation_key", table_name="reconciliation_results"
    )
    op.drop_column("reconciliation_results", "evaluation_key")
    op.drop_column("reconciliation_results", "evaluation_source")
