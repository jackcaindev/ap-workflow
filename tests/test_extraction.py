from app.schemas.extraction import InvoiceExtraction

from tests.conftest import SAMPLE_INVOICE_EXTRACTION


async def test_extract_invoice_pdf_returns_invoice_extraction_schema(
    client,
    sample_invoice_pdf,
    mock_claude,
):
    mock_claude(doc_type="invoice", extraction=SAMPLE_INVOICE_EXTRACTION)

    with sample_invoice_pdf.open("rb") as pdf_file:
        response = await client.post(
            "/extract",
            files={"file": ("sample_invoice.pdf", pdf_file, "application/pdf")},
        )

    assert response.status_code == 200

    extraction = InvoiceExtraction.model_validate(response.json())
    assert extraction.doc_type == "invoice"
    assert extraction.invoice_number == SAMPLE_INVOICE_EXTRACTION["invoice_number"]
    assert extraction.carrier_name == SAMPLE_INVOICE_EXTRACTION["carrier_name"].strip().upper()
    assert extraction.load_number == SAMPLE_INVOICE_EXTRACTION["load_number"]
    assert extraction.total_amount == SAMPLE_INVOICE_EXTRACTION["total_amount"]
    assert len(extraction.line_items) == len(SAMPLE_INVOICE_EXTRACTION["line_items"])
    assert 0 <= extraction.confidence <= 1
