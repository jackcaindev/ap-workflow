from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.models.document import Document
from app.models.reconciliation_result import ReconciliationResult
from app.models.review_decision import ReviewDecision
from app.models.shipment import Shipment
from app.models.shipment_exception import ShipmentException, ShipmentExceptionEvent
from app.models.workflow_run import WorkflowRun
from app.services.missing_document_sla import (
    ScannerHealth,
    evaluate_missing_document_policy,
    run_scanner_iteration,
    scan_missing_document_slas,
)
from app.services.notifier import dispatch_pending_sla_notifications
from app.services.reconciliation import reconcile_shipment
from app.services.shipment import upsert_shipment
from app.workflow.graph import route_after_match
from app.workflow.nodes import match_node
from tests.conftest import create_document, create_shipment_with_documents


NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)
SLA = timedelta(hours=72)


async def test_partial_shipment_remains_within_grace(db_session):
    shipment = await create_shipment_with_documents(
        db_session,
        load_number="GRACE-LOAD",
        include_bol=False,
        include_pod=False,
        created_at=NOW - timedelta(hours=71),
    )

    result = await reconcile_shipment(shipment, db_session, now=NOW, sla_duration=SLA)
    policy = evaluate_missing_document_policy(shipment, now=NOW, sla_duration=SLA)

    assert shipment.reconciliation_status == "partial"
    assert policy.state == "within_grace"
    assert result.exception_reasons == []
    missing_checks = [check for check in result.checks if check["check_name"].startswith("required_") and check["outcome"] == "not_evaluated"]
    assert {check["check_name"] for check in missing_checks} == {
        "required_bol_present",
        "required_pod_present",
    }
    assert await db_session.scalar(select(func.count(ShipmentException.id))) == 0


@pytest.mark.parametrize(
    ("missing_type", "include_kwargs", "reason_code"),
    [
        ("invoice", {"include_invoice": False}, "missing_required_invoice_sla_exceeded"),
        ("rate_con", {"include_rate_con": False}, "missing_required_rate_con_sla_exceeded"),
        ("bol", {"include_bol": False}, "missing_required_bol_sla_exceeded"),
        ("pod", {"include_pod": False}, "missing_required_pod_sla_exceeded"),
    ],
)
async def test_each_required_document_uses_shared_overdue_policy(
    db_session, missing_type, include_kwargs, reason_code
):
    shipment = await create_shipment_with_documents(
        db_session,
        load_number=f"OVERDUE-{missing_type.upper()}",
        created_at=NOW - SLA,
        **include_kwargs,
    )

    result = await reconcile_shipment(shipment, db_session, now=NOW, sla_duration=SLA)
    check = next(
        item for item in result.checks if item["check_name"] == f"required_{missing_type}_present"
    )

    assert shipment.reconciliation_status == "exception"
    assert check["outcome"] == "failed"
    assert check["reason_code"] == reason_code
    assert reason_code in result.exception_reasons


async def test_repeated_scans_are_idempotent_and_create_no_review(db_session, client):
    shipment = await create_shipment_with_documents(
        db_session,
        load_number="SCAN-ONCE",
        include_pod=False,
        created_at=NOW - timedelta(days=4),
    )

    assert await scan_missing_document_slas(now=NOW, sla_duration=SLA) == 1
    assert await scan_missing_document_slas(now=NOW, sla_duration=SLA) == 0

    assert await db_session.scalar(select(func.count(ShipmentException.id))) == 1
    assert await db_session.scalar(select(func.count(ShipmentExceptionEvent.id))) == 1
    assert await db_session.scalar(
        select(func.count(ReconciliationResult.id)).where(
            ReconciliationResult.evaluation_source == "scheduled_sla"
        )
    ) == 1
    assert await db_session.scalar(select(func.count(WorkflowRun.id))) == 0
    assert await db_session.scalar(select(func.count(ReviewDecision.id))) == 0
    detail = (await client.get(f"/shipments/{shipment.id}")).json()
    assert detail["missing_document_state"] == "overdue"
    assert detail["missing_document_exception"]["status"] == "active"
    assert [event["transition"] for event in detail["missing_document_exception"]["events"]] == ["opened"]


async def test_late_document_resolves_overdue_exception(db_session):
    shipment = await create_shipment_with_documents(
        db_session,
        load_number="LATE-POD",
        include_pod=False,
        created_at=NOW - timedelta(days=4),
    )
    await reconcile_shipment(
        shipment,
        db_session,
        now=NOW,
        sla_duration=SLA,
        evaluation_source="scheduled_sla",
        evaluation_key=f"test-open:{shipment.id}",
    )
    pod_extraction = {
        "load_number": shipment.load_number,
        "carrier_name": "ACME FREIGHT",
        "delivery_date": "2026-07-18",
        "condition": "good",
        "doc_type": "proof_of_delivery",
    }
    pod = await create_document(
        db_session,
        doc_type="proof_of_delivery",
        extracted_data=pod_extraction,
    )
    locked_shipment = await upsert_shipment(
        shipment.load_number, pod, pod_extraction, db_session
    )
    assert locked_shipment is not None

    await reconcile_shipment(
        locked_shipment,
        db_session,
        now=NOW + timedelta(hours=1),
        sla_duration=SLA,
        evaluation_source="document_workflow",
        evaluation_key=f"late-pod:{pod.id}",
    )

    exception = await db_session.scalar(select(ShipmentException))
    events = list(
        (await db_session.scalars(select(ShipmentExceptionEvent).order_by(ShipmentExceptionEvent.version))).all()
    )
    assert exception is not None
    assert exception.status == "resolved"
    assert locked_shipment.reconciliation_status == "reconciled"
    assert [event.transition for event in events] == ["opened", "resolved"]


async def test_notification_dispatches_once_per_transition(db_session, client, monkeypatch):
    await create_shipment_with_documents(
        db_session,
        load_number="NOTIFY-ONCE",
        include_pod=False,
        created_at=NOW - timedelta(days=4),
    )
    await scan_missing_document_slas(now=NOW, sla_duration=SLA)
    calls = []

    def fake_send(payload):
        calls.append(payload)
        return True

    monkeypatch.setattr("app.services.notifier._send_shipment_exception_email", fake_send)
    assert await dispatch_pending_sla_notifications() == 1
    assert await dispatch_pending_sla_notifications() == 0
    assert len(calls) == 1
    records = (await client.get("/notifications")).json()
    assert records[0]["kind"] == "shipment_exception"
    assert records[0]["transition"] == "opened"
    assert records[0]["notification_status"] == "sent"


async def test_scanner_failure_is_visible_and_next_iteration_recovers(monkeypatch):
    logged = []
    monkeypatch.setattr(
        "app.services.missing_document_sla.logger.exception",
        lambda message: logged.append(message),
    )
    health = ScannerHealth()
    calls = 0

    async def flaky_scan(**_):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("database unavailable")
        return 0

    assert not await run_scanner_iteration(
        health, now=NOW, sla_duration=SLA, scan=flaky_scan
    )
    assert health.last_error == "scan_failed"
    assert health.consecutive_failures == 1
    assert await run_scanner_iteration(
        health, now=NOW + timedelta(minutes=5), sla_duration=SLA, scan=flaky_scan
    )
    assert health.last_error is None
    assert health.consecutive_failures == 0
    assert health.last_result_count == 0
    assert logged == ["Scheduled missing-document SLA scan failed"]


async def test_sla_only_document_reconciliation_bypasses_langgraph_review(db_session):
    shipment = await create_shipment_with_documents(
        db_session,
        load_number="DOCUMENT-SLA-ONLY",
        include_bol=False,
        include_pod=False,
        created_at=datetime.now(UTC) - timedelta(days=4),
    )
    bol_extraction = {
        "bol_number": "BOL-LATE",
        "load_number": shipment.load_number,
        "carrier_name": "ACME FREIGHT",
        "pickup_date": "2026-06-01",
        "doc_type": "bill_of_lading",
    }
    bol = await create_document(
        db_session,
        doc_type="bill_of_lading",
        extracted_data=bol_extraction,
    )
    run = WorkflowRun(
        run_id="11111111-1111-1111-1111-111111111111",
        document_id=bol.id,
        status="running",
        processing_status="running",
        posting_status="not_ready",
    )
    db_session.add(run)
    await db_session.commit()

    update = await match_node(
        {
            "run_id": run.run_id,
            "document_id": str(bol.id),
            "extraction": bol_extraction,
            "iteration_count": 0,
        }
    )

    assert update["exception_reason"] is None
    assert update["match_result"]["matched"] is False
    assert update["match_result"]["requires_review"] is False
    assert update["match_result"]["reviewable_exception_reasons"] == []
    assert "missing_required_pod_sla_exceeded" in update["match_result"]["exception_reasons"]
    assert route_after_match(update) == "complete_node"
