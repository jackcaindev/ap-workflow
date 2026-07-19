from __future__ import annotations

import os
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from psycopg import sql
from psycopg.types.json import Jsonb
from sqlalchemy.engine import make_url

from tests.conftest import TEST_DATABASE_URL


def _migration_url(database_name: str) -> str:
    return make_url(TEST_DATABASE_URL).set(database=database_name).render_as_string(
        hide_password=False
    )


def _psycopg_url(database_name: str) -> str:
    return _migration_url(database_name).replace("postgresql+asyncpg://", "postgresql://", 1)


def test_business_state_migration_backfills_legacy_records() -> None:
    """Exercise the real migration against a disposable PostgreSQL database."""
    database_name = f"business_state_{uuid4().hex}"
    admin_url = _psycopg_url("postgres")
    previous_database_url = os.environ.get("DATABASE_URL")

    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))

    try:
        os.environ["DATABASE_URL"] = _migration_url(database_name)
        alembic_config = Config("alembic.ini")
        command.upgrade(alembic_config, "202607190001")

        with psycopg.connect(_psycopg_url(database_name)) as connection:
            statuses = [
                "complete",
                "awaiting_review",
                "approved",
                "rejected",
                "failed",
                "partial",
                "exception",
                "reconciled",
            ]
            for index, status in enumerate(statuses, start=1):
                document_id = connection.execute(
                    """
                    INSERT INTO documents (filename, doc_type, status)
                    VALUES (%s, 'invoice', 'extracted')
                    RETURNING id
                    """,
                    (f"{status}.pdf",),
                ).fetchone()[0]
                interrupt_payload = Jsonb({"reason": "review"}) if status in {
                    "awaiting_review",
                    "exception",
                } else None
                connection.execute(
                    """
                    INSERT INTO workflow_runs
                        (document_id, status, interrupt_payload, run_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (document_id, status, interrupt_payload, f"legacy-{index}"),
                )

            shipment_id = uuid4()
            connection.execute(
                """
                INSERT INTO shipments (id, load_number, reconciliation_status)
                VALUES (%s, 'LEGACY-LOAD', 'complete')
                """,
                (shipment_id,),
            )
            connection.execute(
                """
                INSERT INTO reconciliation_results
                    (id, shipment_id, run_id, checks, missing_docs, exception_reasons)
                VALUES (%s, %s, 'legacy-1', %s, %s, %s)
                """,
                (
                    uuid4(),
                    shipment_id,
                    Jsonb(
                        [
                            {"check_name": "amount", "passed": False, "details": "mismatch"},
                            {"check_name": "pod_date", "passed": True, "details": "skipped: no POD"},
                            {"check_name": "carrier", "passed": True, "details": "matched"},
                        ]
                    ),
                    Jsonb([]),
                    Jsonb(["amount mismatch"]),
                ),
            )
            connection.commit()

        command.upgrade(alembic_config, "head")

        with psycopg.connect(_psycopg_url(database_name)) as connection:
            rows = dict(
                connection.execute(
                    """
                    SELECT run_id, processing_status || ':' || posting_status || ':' || status
                    FROM workflow_runs
                    """
                ).fetchall()
            )
            assert rows["legacy-1"] == "complete:not_ready:complete"
            assert rows["legacy-2"] == "awaiting_review:not_ready:awaiting_review"
            assert rows["legacy-3"] == "complete:ready_for_posting:approved"
            assert rows["legacy-4"] == "complete:blocked:rejected"
            assert rows["legacy-5"] == "failed:not_ready:failed"
            assert rows["legacy-6"] == "complete:not_ready:complete"
            assert rows["legacy-7"] == "awaiting_review:not_ready:awaiting_review"
            assert rows["legacy-8"] == "complete:ready_for_posting:complete"

            decisions = connection.execute(
                "SELECT run_id, disposition FROM review_decisions ORDER BY run_id"
            ).fetchall()
            assert decisions == [("legacy-3", "approved"), ("legacy-4", "rejected")]

            checks = connection.execute(
                "SELECT checks FROM reconciliation_results WHERE run_id = 'legacy-1'"
            ).fetchone()[0]
            assert [check["outcome"] for check in checks] == [
                "failed",
                "not_evaluated",
                "passed",
            ]
            assert all("passed" not in check for check in checks)
            shipment_status = connection.execute(
                "SELECT reconciliation_status FROM shipments WHERE id = %s",
                (shipment_id,),
            ).fetchone()[0]
            assert shipment_status == "exception"
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(database_name)
                )
            )
