"""correct business state semantics

Revision ID: 202607190002
Revises: 202607190001
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202607190002"
down_revision: str | None = "202607190001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "workflow_runs", sa.Column("processing_status", sa.String(length=32), nullable=True)
    )
    op.add_column(
        "workflow_runs", sa.Column("posting_status", sa.String(length=32), nullable=True)
    )
    op.create_table(
        "review_decisions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("disposition", sa.String(length=16), nullable=False),
        sa.Column(
            "decided_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("reviewer_id", sa.String(length=255), nullable=True),
        sa.CheckConstraint(
            "disposition IN ('approved', 'rejected')",
            name="ck_review_decisions_disposition",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["workflow_runs.run_id"], ondelete="CASCADE"
        ),
        sa.UniqueConstraint("run_id", name="uq_review_decisions_run_id"),
    )
    op.create_index("ix_review_decisions_run_id", "review_decisions", ["run_id"])

    for column_name in ("approved_count", "rejected_count", "ready_for_posting_count"):
        op.add_column(
            "notifications",
            sa.Column(column_name, sa.Integer(), server_default="0", nullable=False),
        )

    bind = op.get_bind()
    conflicts = bind.execute(
        sa.text(
            """
            SELECT wr.run_id
            FROM workflow_runs wr
            JOIN workflow_audit_logs wal ON wal.run_id = wr.run_id
            WHERE wal.event_type IN ('approved', 'rejected')
              AND wr.status NOT IN ('approved', 'rejected')
            GROUP BY wr.run_id
            HAVING COUNT(*) FILTER (WHERE wal.event_type = 'approved') > 0
               AND COUNT(*) FILTER (WHERE wal.event_type = 'rejected') > 0
            """
        )
    ).scalars().all()
    if conflicts:
        raise RuntimeError(
            "Conflicting historical review decisions require resolution: "
            + ", ".join(conflicts)
        )

    bind.execute(
        sa.text(
            """
            WITH audit_decisions AS (
                SELECT
                    wr.run_id,
                    CASE
                        WHEN wr.status IN ('approved', 'rejected') THEN wr.status
                        WHEN COUNT(*) FILTER (WHERE wal.event_type = 'approved') > 0
                            THEN 'approved'
                        WHEN COUNT(*) FILTER (WHERE wal.event_type = 'rejected') > 0
                            THEN 'rejected'
                    END AS disposition,
                    COALESCE(
                        MIN(wal.created_at) FILTER (
                            WHERE wal.event_type = CASE
                                WHEN wr.status IN ('approved', 'rejected') THEN wr.status
                                WHEN EXISTS (
                                    SELECT 1 FROM workflow_audit_logs wa
                                    WHERE wa.run_id = wr.run_id AND wa.event_type = 'approved'
                                ) THEN 'approved'
                                ELSE 'rejected'
                            END
                        ),
                        wr.updated_at
                    ) AS decided_at
                FROM workflow_runs wr
                LEFT JOIN workflow_audit_logs wal
                    ON wal.run_id = wr.run_id
                   AND wal.event_type IN ('approved', 'rejected')
                GROUP BY wr.run_id, wr.status, wr.updated_at
            )
            INSERT INTO review_decisions (id, run_id, disposition, decided_at, reviewer_id)
            SELECT gen_random_uuid(), run_id, disposition, decided_at, NULL
            FROM audit_decisions
            WHERE disposition IS NOT NULL
            ON CONFLICT (run_id) DO NOTHING
            """
        )
    )

    bind.execute(
        sa.text(
            """
            UPDATE workflow_runs wr
            SET processing_status = CASE
                    WHEN EXISTS (
                        SELECT 1 FROM review_decisions rd WHERE rd.run_id = wr.run_id
                    ) THEN 'complete'
                    WHEN wr.status IN ('complete', 'approved', 'rejected', 'partial', 'reconciled')
                        THEN 'complete'
                    WHEN wr.status = 'awaiting_review' THEN 'awaiting_review'
                    WHEN wr.status = 'exception' AND wr.interrupt_payload IS NOT NULL
                        THEN 'awaiting_review'
                    WHEN wr.status IN ('failed', 'exception') THEN 'failed'
                    WHEN wr.status IN ('pending', 'running', 'retrying') THEN wr.status
                    ELSE 'failed'
                END,
                posting_status = CASE
                    WHEN EXISTS (
                        SELECT 1 FROM review_decisions rd
                        WHERE rd.run_id = wr.run_id AND rd.disposition = 'approved'
                    ) THEN 'ready_for_posting'
                    WHEN EXISTS (
                        SELECT 1 FROM review_decisions rd
                        WHERE rd.run_id = wr.run_id AND rd.disposition = 'rejected'
                    ) THEN 'blocked'
                    WHEN wr.status = 'reconciled' OR EXISTS (
                        SELECT 1
                        FROM documents d
                        JOIN shipments s ON s.id = d.shipment_id
                        WHERE d.id = wr.document_id
                          AND s.reconciliation_status = 'reconciled'
                          AND wr.status IN ('complete', 'reconciled')
                    ) THEN 'ready_for_posting'
                    ELSE 'not_ready'
                END
            """
        )
    )
    bind.execute(
        sa.text(
            """
            UPDATE workflow_runs wr
            SET status = COALESCE(
                (SELECT rd.disposition FROM review_decisions rd WHERE rd.run_id = wr.run_id),
                wr.processing_status
            )
            """
        )
    )

    bind.execute(
        sa.text(
            """
            UPDATE reconciliation_results rr
            SET checks = COALESCE(
                (
                    SELECT jsonb_agg(
                        (item - 'passed') || jsonb_build_object(
                            'outcome',
                            CASE
                                WHEN item->>'passed' = 'false' THEN 'failed'
                                WHEN item->>'passed' IS NULL THEN 'not_evaluated'
                                WHEN item->>'check_name' = 'missing_docs'
                                     AND jsonb_array_length(COALESCE(rr.missing_docs, '[]'::jsonb)) > 0
                                    THEN 'not_evaluated'
                                WHEN COALESCE(item->>'details', '') LIKE 'skipped:%'
                                    THEN 'not_evaluated'
                                WHEN COALESCE(item->>'details', '') LIKE '%within 3-day grace period%'
                                    THEN 'not_evaluated'
                                ELSE 'passed'
                            END
                        )
                        ORDER BY ordinal
                    )
                    FROM jsonb_array_elements(COALESCE(rr.checks, '[]'::jsonb))
                        WITH ORDINALITY AS entry(item, ordinal)
                ),
                '[]'::jsonb
            )
            """
        )
    )

    bind.execute(
        sa.text(
            """
            WITH latest_results AS (
                SELECT DISTINCT ON (rr.shipment_id)
                    rr.shipment_id,
                    rr.id AS result_id,
                    jsonb_array_length(COALESCE(rr.exception_reasons, '[]'::jsonb)) AS exception_count,
                    jsonb_array_length(COALESCE(rr.missing_docs, '[]'::jsonb)) AS missing_count,
                    COALESCE((
                        SELECT COUNT(*)
                        FROM jsonb_array_elements(COALESCE(rr.checks, '[]'::jsonb)) item
                        WHERE item->>'outcome' = 'not_evaluated'
                    ), 0) AS not_evaluated_count
                FROM reconciliation_results rr
                ORDER BY rr.shipment_id, rr.created_at DESC
            )
            UPDATE shipments s
            SET reconciliation_status = CASE
                WHEN latest.exception_count > 0 THEN 'exception'
                WHEN latest.missing_count > 0 OR latest.not_evaluated_count > 0 THEN 'partial'
                WHEN latest.result_id IS NOT NULL THEN 'reconciled'
                WHEN s.reconciliation_status IN ('awaiting_review', 'approved', 'rejected', 'failed')
                    THEN 'exception'
                ELSE 'pending'
            END
            FROM latest_results latest
            WHERE s.reconciliation_status NOT IN ('pending', 'partial', 'exception', 'reconciled')
              AND latest.shipment_id = s.id
            """
        )
    )
    bind.execute(
        sa.text(
            """
            UPDATE shipments
            SET reconciliation_status = CASE
                WHEN reconciliation_status IN ('awaiting_review', 'approved', 'rejected', 'failed')
                    THEN 'exception'
                ELSE 'pending'
            END
            WHERE reconciliation_status NOT IN ('pending', 'partial', 'exception', 'reconciled')
            """
        )
    )

    op.alter_column("workflow_runs", "processing_status", nullable=False, server_default="pending")
    op.alter_column("workflow_runs", "posting_status", nullable=False, server_default="not_ready")
    op.create_check_constraint(
        "ck_workflow_runs_processing_status",
        "workflow_runs",
        "processing_status IN ('pending', 'running', 'retrying', 'awaiting_review', 'complete', 'failed')",
    )
    op.create_check_constraint(
        "ck_workflow_runs_posting_status",
        "workflow_runs",
        "posting_status IN ('not_ready', 'ready_for_posting', 'posting', 'posted', "
        "'payment_scheduled', 'paid', 'blocked', 'failed')",
    )
    op.create_check_constraint(
        "ck_shipments_reconciliation_status",
        "shipments",
        "reconciliation_status IN ('pending', 'partial', 'reconciled', 'exception')",
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            UPDATE reconciliation_results rr
            SET checks = COALESCE(
                (
                    SELECT jsonb_agg(
                        (item - 'outcome') || jsonb_build_object(
                            'passed', CASE WHEN item->>'outcome' = 'failed' THEN false ELSE true END
                        )
                        ORDER BY ordinal
                    )
                    FROM jsonb_array_elements(COALESCE(rr.checks, '[]'::jsonb))
                        WITH ORDINALITY AS entry(item, ordinal)
                ),
                '[]'::jsonb
            )
            """
        )
    )
    bind.execute(
        sa.text(
            """
            UPDATE workflow_runs wr
            SET status = COALESCE(
                (SELECT rd.disposition FROM review_decisions rd WHERE rd.run_id = wr.run_id),
                wr.processing_status
            )
            """
        )
    )
    op.drop_constraint("ck_shipments_reconciliation_status", "shipments", type_="check")
    op.drop_constraint("ck_workflow_runs_posting_status", "workflow_runs", type_="check")
    op.drop_constraint("ck_workflow_runs_processing_status", "workflow_runs", type_="check")
    for column_name in ("ready_for_posting_count", "rejected_count", "approved_count"):
        op.drop_column("notifications", column_name)
    op.drop_index("ix_review_decisions_run_id", table_name="review_decisions")
    op.drop_table("review_decisions")
    op.drop_column("workflow_runs", "posting_status")
    op.drop_column("workflow_runs", "processing_status")
