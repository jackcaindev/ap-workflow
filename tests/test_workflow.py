import asyncio

from app.models.document import Document
from app.models.review_decision import ReviewDecision
from app.models.workflow_run import WorkflowRun
from app.workflow.nodes import complete_node
from sqlalchemy import func, select

from tests.conftest import SAMPLE_INVOICE_EXTRACTION, create_shipment_with_documents


async def _start_review(client, sample_invoice_pdf, mock_claude) -> str:
    mock_claude(
        doc_type="invoice",
        extraction={**SAMPLE_INVOICE_EXTRACTION, "total_amount": 3000.0},
    )
    with sample_invoice_pdf.open("rb") as pdf_file:
        response = await client.post(
            "/workflow/run",
            files={"file": ("sample_invoice.pdf", pdf_file, "application/pdf")},
        )
    assert response.status_code == 200
    assert response.json()["processing_status"] == "awaiting_review"
    return response.json()["run_id"]


async def test_workflow_run_returns_run_id_and_status(
    client,
    sample_invoice_pdf,
    mock_claude,
    seeded_rate_confirmations,
):
    mock_claude(
        doc_type="invoice",
        extraction={
            **SAMPLE_INVOICE_EXTRACTION,
            "total_amount": 3000.0,
        },
    )

    with sample_invoice_pdf.open("rb") as pdf_file:
        response = await client.post(
            "/workflow/run",
            files={"file": ("sample_invoice.pdf", pdf_file, "application/pdf")},
        )

    assert response.status_code == 200
    payload = response.json()
    assert "run_id" in payload
    assert payload["status"] == "awaiting_review"
    assert payload["processing_status"] == "awaiting_review"
    assert payload["review_disposition"] == "pending"
    assert payload["posting_status"] == "not_ready"

    status_response = await client.get(f"/workflow/{payload['run_id']}/status")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "awaiting_review"


async def test_workflow_resume_approved_updates_status(
    client,
    sample_invoice_pdf,
    mock_claude,
    seeded_rate_confirmations,
):
    mock_claude(
        doc_type="invoice",
        extraction={
            **SAMPLE_INVOICE_EXTRACTION,
            "total_amount": 3000.0,
        },
    )

    with sample_invoice_pdf.open("rb") as pdf_file:
        run_response = await client.post(
            "/workflow/run",
            files={"file": ("sample_invoice.pdf", pdf_file, "application/pdf")},
        )

    run_id = run_response.json()["run_id"]
    assert run_response.json()["status"] == "awaiting_review"

    resume_response = await client.post(
        f"/workflow/{run_id}/resume",
        json={"decision": "approved"},
    )

    assert resume_response.status_code == 200
    assert resume_response.json()["status"] == "approved"
    assert resume_response.json()["processing_status"] == "complete"
    assert resume_response.json()["review_disposition"] == "approved"
    assert resume_response.json()["posting_status"] == "ready_for_posting"
    assert resume_response.json()["idempotent"] is False

    status_response = await client.get(f"/workflow/{run_id}/status")
    assert status_response.json()["status"] == "approved"


async def test_workflow_happy_path_without_review_is_ready_for_posting(
    client,
    db_session,
    sample_invoice_pdf,
    mock_claude,
    seeded_rate_confirmations,
):
    await create_shipment_with_documents(db_session)
    mock_claude(doc_type="invoice", extraction=SAMPLE_INVOICE_EXTRACTION)

    with sample_invoice_pdf.open("rb") as pdf_file:
        response = await client.post(
            "/workflow/run",
            files={"file": ("sample_invoice.pdf", pdf_file, "application/pdf")},
        )

    payload = response.json()
    assert payload["status"] == "complete"
    assert payload["processing_status"] == "complete"
    assert payload["reconciliation_status"] == "reconciled"
    assert payload["review_disposition"] == "not_required"
    assert payload["posting_status"] == "ready_for_posting"

    detail = await client.get(f"/workflow/{payload['run_id']}")
    assert detail.status_code == 200
    # Retained only as a public compatibility projection; business behavior is
    # asserted through the independent dimensions above.
    assert detail.json()["match_result"]["matched"] is True


async def test_rejected_review_is_durable(
    client, sample_invoice_pdf, mock_claude, seeded_rate_confirmations
):
    run_id = await _start_review(client, sample_invoice_pdf, mock_claude)

    response = await client.post(
        f"/workflow/{run_id}/resume", json={"decision": "rejected"}
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "rejected"
    assert payload["processing_status"] == "complete"
    assert payload["reconciliation_status"] == "exception"
    assert payload["review_disposition"] == "rejected"
    assert payload["posting_status"] == "blocked"
    assert payload["reviewed_at"] is not None


async def test_repeated_identical_decision_is_idempotent(
    client, db_session, sample_invoice_pdf, mock_claude, seeded_rate_confirmations
):
    run_id = await _start_review(client, sample_invoice_pdf, mock_claude)
    first = await client.post(
        f"/workflow/{run_id}/resume", json={"decision": "approved"}
    )
    second = await client.post(
        f"/workflow/{run_id}/resume", json={"decision": "approved"}
    )

    assert first.json()["idempotent"] is False
    assert second.status_code == 200
    assert second.json()["idempotent"] is True
    assert second.json()["reviewed_at"] == first.json()["reviewed_at"]
    count = await db_session.scalar(
        select(func.count(ReviewDecision.id)).where(ReviewDecision.run_id == run_id)
    )
    assert count == 1


async def test_conflicting_second_decision_is_rejected(
    client, sample_invoice_pdf, mock_claude, seeded_rate_confirmations
):
    run_id = await _start_review(client, sample_invoice_pdf, mock_claude)
    await client.post(f"/workflow/{run_id}/resume", json={"decision": "approved"})

    response = await client.post(
        f"/workflow/{run_id}/resume", json={"decision": "rejected"}
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "review_decision_conflict"


async def test_concurrent_conflicting_decisions_only_one_succeeds(
    client, db_session, sample_invoice_pdf, mock_claude, seeded_rate_confirmations
):
    run_id = await _start_review(client, sample_invoice_pdf, mock_claude)

    approved, rejected = await asyncio.gather(
        client.post(f"/workflow/{run_id}/resume", json={"decision": "approved"}),
        client.post(f"/workflow/{run_id}/resume", json={"decision": "rejected"}),
    )

    assert sorted([approved.status_code, rejected.status_code]) == [200, 409]
    count = await db_session.scalar(
        select(func.count(ReviewDecision.id)).where(ReviewDecision.run_id == run_id)
    )
    assert count == 1


async def test_decision_before_interruption_is_rejected(client, db_session):
    document = Document(filename="not-paused.pdf", doc_type="invoice", status="received")
    db_session.add(document)
    await db_session.flush()
    run = WorkflowRun(
        run_id="00000000-0000-0000-0000-000000000001",
        document_id=document.id,
        status="running",
        processing_status="running",
        posting_status="not_ready",
    )
    db_session.add(run)
    await db_session.commit()

    response = await client.post(
        f"/workflow/{run.run_id}/resume", json={"decision": "approved"}
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "workflow_not_awaiting_review"


async def test_complete_node_preserves_reviewed_legacy_status_from_old_state_shape():
    result = await complete_node(
        {
            "run_id": "paused-old-checkpoint",
            "human_decision": "approved",
            "exception_reason": "amount_variance",
            "match_result": {"reconciliation_status": "exception"},
            "iteration_count": 4,
        }
    )

    assert result["status"] == "approved"
    assert result["processing_status"] == "complete"
    assert result["posting_status"] == "ready_for_posting"
