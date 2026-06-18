from tests.conftest import SAMPLE_INVOICE_EXTRACTION


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
    assert resume_response.json() == {"run_id": run_id, "status": "complete"}

    status_response = await client.get(f"/workflow/{run_id}/status")
    assert status_response.json()["status"] == "complete"
