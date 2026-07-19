from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.load_numbers import normalize_load_number
from app.models.document import Document
from app.models.rate_confirmation import RateConfirmation
from app.models.shipment import Shipment


DOC_FIELDS = {
    "invoice": ("invoice_id", "has_invoice"),
    "rate_confirmation": ("rate_con_id", "has_rate_con"),
    "bill_of_lading": ("bol_id", "has_bol"),
    "proof_of_delivery": ("pod_id", "has_pod"),
}


def _present_count(shipment: Shipment) -> int:
    return sum(
        [
            shipment.has_invoice,
            shipment.has_rate_con,
            shipment.has_bol,
            shipment.has_pod,
        ]
    )


def rate_confirmation_extraction(rate_confirmation: RateConfirmation) -> dict[str, Any]:
    return {
        "rate_confirmation_id": str(rate_confirmation.id),
        "load_number": rate_confirmation.load_number,
        "carrier_name": rate_confirmation.carrier_name,
        "origin": rate_confirmation.origin,
        "destination": rate_confirmation.destination,
        "agreed_rate": rate_confirmation.agreed_rate,
        "currency": rate_confirmation.currency,
        "shipment_date": rate_confirmation.shipment_date.isoformat(),
        "doc_type": "rate_confirmation",
    }


async def _rate_confirmation_document(
    rate_confirmation: RateConfirmation,
    shipment: Shipment,
    db: AsyncSession,
) -> Document:
    result = await db.execute(
        select(Document).where(
            Document.doc_type == "rate_confirmation",
            Document.extracted_data["rate_confirmation_id"].astext == str(rate_confirmation.id),
        )
    )
    document = result.scalar_one_or_none()
    extraction = rate_confirmation_extraction(rate_confirmation)
    if document is None:
        document = Document(
            filename=f"manual-rate-confirmation-{rate_confirmation.load_number}.json",
            doc_type="rate_confirmation",
            status="extracted",
            raw_text=None,
            extracted_data=extraction,
        )
        db.add(document)
        await db.flush()

    document.shipment_id = shipment.id
    document.extracted_data = extraction
    return document


async def _attach_existing_rate_confirmation(
    shipment: Shipment,
    db: AsyncSession,
) -> None:
    if shipment.rate_con_id is not None:
        shipment.has_rate_con = True
        return

    result = await db.execute(
        select(RateConfirmation).where(RateConfirmation.load_number == shipment.load_number)
    )
    rate_confirmation = result.scalar_one_or_none()
    if rate_confirmation is None:
        return

    # shipments.rate_con_id points at documents.id, so a rate confirmation from
    # the management table is represented by a lightweight companion document.
    document = await _rate_confirmation_document(rate_confirmation, shipment, db)
    shipment.rate_con_id = document.id
    shipment.has_rate_con = True
    if not shipment.carrier_name:
        shipment.carrier_name = rate_confirmation.carrier_name


async def upsert_shipment(
    load_number: str | None,
    document: Document,
    extraction: dict[str, Any],
    db: AsyncSession,
) -> Shipment | None:
    normalized_load_number = normalize_load_number(load_number)
    if normalized_load_number is None:
        return None

    await db.execute(
        insert(Shipment)
        .values(
            id=uuid4(),
            load_number=normalized_load_number,
            reconciliation_status="pending",
        )
        .on_conflict_do_nothing(index_elements=[Shipment.load_number])
    )
    result = await db.execute(
        select(Shipment)
        .where(Shipment.load_number == normalized_load_number)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    shipment = result.scalar_one()

    document.shipment_id = shipment.id
    doc_fields = DOC_FIELDS.get(document.doc_type)
    if doc_fields is not None:
        doc_id_field, present_field = doc_fields
        if getattr(shipment, doc_id_field) is None:
            setattr(shipment, doc_id_field, document.id)
        setattr(shipment, present_field, True)

    if not shipment.carrier_name and extraction.get("carrier_name"):
        shipment.carrier_name = extraction["carrier_name"]

    await _attach_existing_rate_confirmation(shipment, db)

    present_count = _present_count(shipment)
    if present_count == 0:
        shipment.reconciliation_status = "pending"
    elif present_count < len(DOC_FIELDS):
        shipment.reconciliation_status = "partial"

    # The caller owns the transaction. Workflow assembly keeps this row lock
    # through reconciliation; manual rate-confirmation creation commits after
    # this function returns.
    await db.flush()
    return shipment
