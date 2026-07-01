import base64
import json
from pathlib import Path
from typing import Any, Literal

from anthropic import AsyncAnthropic
from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.document import Document
from app.schemas.extraction import (
    BOLExtraction,
    DocumentType,
    ExtractionResult,
    InvoiceExtraction,
    PODExtraction,
)
from app.services.json_utils import strip_code_fences


CLAUDE_MODEL = "claude-sonnet-4-6"

CLASSIFICATION_PROMPT = (
    "What type of freight document is this? Reply with exactly one of: "
    "invoice, rate_confirmation, bill_of_lading, proof_of_delivery"
)

SYSTEM_PROMPT = """You extract structured data from freight accounts payable documents.
Return JSON only. Do not include markdown, code fences, explanations, or surrounding text.
doc_type must be one of: invoice, rate_confirmation, bill_of_lading, proof_of_delivery.
Use null when an optional value is absent. Use numeric values for amounts and confidence.
"""

EXTRACTION_PROMPTS: dict[str, str] = {
    "invoice": (
        "Extract this invoice as JSON with exactly these fields: "
        "invoice_number, carrier_name, load_number, invoice_date, total_amount, "
        "line_items, doc_type, confidence. line_items must contain description, "
        "quantity, unit_price, and total. doc_type must be invoice."
    ),
    "rate_confirmation": (
        "Extract this rate confirmation as JSON using the existing invoice-shaped "
        "schema for compatibility: invoice_number, carrier_name, load_number, "
        "invoice_date, total_amount, line_items, doc_type, confidence. Use the "
        "rate/load confirmation number as invoice_number, the shipment or issue "
        "date as invoice_date, and the agreed rate as total_amount. doc_type must "
        "be rate_confirmation. Use an empty line_items array if no charges are itemized."
    ),
    "bill_of_lading": (
        "Extract this bill of lading as JSON with exactly these fields: "
        "bol_number, load_number, carrier_name, shipper_name, consignee_name, "
        "pickup_date, commodity_description, pieces, weight_lbs, doc_type. "
        "doc_type must be bill_of_lading."
    ),
    "proof_of_delivery": (
        "Extract this proof of delivery as JSON with exactly these fields: "
        "bol_number, load_number, carrier_name, delivery_date, delivery_time, "
        "pieces_received, condition, receiver_name, doc_type. doc_type must be "
        "proof_of_delivery."
    ),
}

EXTRACTION_SCHEMAS: dict[str, type[BaseModel]] = {
    "invoice": InvoiceExtraction,
    "rate_confirmation": InvoiceExtraction,
    "bill_of_lading": BOLExtraction,
    "proof_of_delivery": PODExtraction,
}

SupportedKind = Literal["pdf", "image"]


class ExtractionError(Exception):
    """Raised when Claude returns output that cannot be parsed into our schema."""


class UnsupportedFileTypeError(Exception):
    """Raised when the uploaded file extension is not supported for extraction."""


def detect_file_type(filename: str) -> tuple[SupportedKind, str]:
    suffix = Path(filename).suffix.lower()
    media_types = {
        ".pdf": ("pdf", "application/pdf"),
        ".png": ("image", "image/png"),
        ".jpg": ("image", "image/jpeg"),
        ".jpeg": ("image", "image/jpeg"),
    }

    try:
        return media_types[suffix]
    except KeyError as exc:
        raise UnsupportedFileTypeError(f"Unsupported file type: {suffix or 'none'}") from exc


def build_document_block(file_bytes: bytes, filename: str) -> dict[str, Any]:
    kind, media_type = detect_file_type(filename)
    encoded = base64.b64encode(file_bytes).decode("ascii")

    # Anthropic's Messages API uses different block types for PDFs and images:
    # PDFs are "document" blocks, while PNG/JPEG files are "image" blocks. Both
    # carry base64 data under a source object with an explicit media_type.
    block_type = "document" if kind == "pdf" else "image"
    return {
        "type": block_type,
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": encoded,
        },
    }


def extract_response_text(content: Any) -> str:
    text_blocks = [
        block.text
        for block in content
        if getattr(block, "type", None) == "text" and getattr(block, "text", None)
    ]
    return "".join(text_blocks).strip()


def validate_document_type(response_text: str) -> DocumentType:
    doc_type = response_text.strip().strip("\"'`").lower()
    if doc_type not in EXTRACTION_SCHEMAS:
        raise ExtractionError(f"Claude returned unsupported document type: {response_text}")
    return doc_type  # type: ignore[return-value]


def validate_claude_json(response_text: str, doc_type: DocumentType) -> ExtractionResult:
    cleaned = strip_code_fences(response_text)
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ExtractionError("Claude returned non-JSON output") from exc

    schema = EXTRACTION_SCHEMAS[doc_type]
    try:
        # model_validate() is preferred over Schema(**payload) because
        # it is the Pydantic v2 validation entrypoint for untrusted parsed data.
        # It accepts mappings and model-like objects consistently, then runs all
        # field and model validators before producing the typed schema object.
        return schema.model_validate(payload)
    except ValidationError as exc:
        raise ExtractionError(
            f"Claude JSON did not match the {doc_type} extraction schema"
        ) from exc


async def extract_document(
    file_bytes: bytes,
    filename: str,
    db: AsyncSession,
    document_id: int | None = None,
) -> ExtractionResult:
    document_block = build_document_block(file_bytes, filename)

    settings = get_settings()
    if not settings.ANTHROPIC_API_KEY:
        raise ExtractionError("ANTHROPIC_API_KEY is not configured")

    client = AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)

    classification_message = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=16,
        temperature=0,
        system="You classify freight documents. Reply with one exact label only.",
        messages=[
            {
                "role": "user",
                "content": [
                    document_block,
                    {
                        "type": "text",
                        "text": CLASSIFICATION_PROMPT,
                    },
                ],
            }
        ],
    )
    doc_type = validate_document_type(extract_response_text(classification_message.content))

    message = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=2048,
        temperature=0,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": [
                    document_block,
                    {
                        "type": "text",
                        "text": EXTRACTION_PROMPTS[doc_type],
                    },
                ],
            }
        ],
    )

    extraction = validate_claude_json(extract_response_text(message.content), doc_type)

    document = await db.get(Document, document_id) if document_id is not None else None
    if document is None:
        document = Document(filename=filename, doc_type=extraction.doc_type)
        db.add(document)

    document.filename = filename
    document.doc_type = extraction.doc_type
    document.status = "extracted"
    # The source file is binary, so raw_text is intentionally left empty for this
    # vision endpoint. The structured extraction is persisted in JSONB.
    document.raw_text = None
    document.extracted_data = extraction.model_dump(mode="json")

    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise

    return extraction
