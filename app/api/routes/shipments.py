from collections import Counter, defaultdict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models.document import Document
from app.models.rate_confirmation import RateConfirmation
from app.models.reconciliation_result import ReconciliationResult
from app.models.shipment import Shipment


router = APIRouter(tags=["shipments"])


DOC_ID_FIELDS = {
    "invoice": "invoice_id",
    "rate_con": "rate_con_id",
    "bol": "bol_id",
    "pod": "pod_id",
}


def _shipment_summary(shipment: Shipment) -> dict:
    return {
        "id": str(shipment.id),
        "load_number": shipment.load_number,
        "carrier_name": shipment.carrier_name,
        "reconciliation_status": shipment.reconciliation_status,
        "has_invoice": shipment.has_invoice,
        "has_rate_con": shipment.has_rate_con,
        "has_bol": shipment.has_bol,
        "has_pod": shipment.has_pod,
        "created_at": shipment.created_at,
        "updated_at": shipment.updated_at,
    }


async def _latest_reconciliation_result(
    shipment_id: UUID,
    db: AsyncSession,
) -> ReconciliationResult | None:
    result = await db.execute(
        select(ReconciliationResult)
        .where(ReconciliationResult.shipment_id == shipment_id)
        .order_by(ReconciliationResult.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


def _document_payload(document: Document) -> dict:
    return {
        "id": document.id,
        "filename": document.filename,
        "doc_type": document.doc_type,
        "status": document.status,
        "extracted_data": document.extracted_data,
        "created_at": document.created_at,
    }


async def _document_payloads(shipment: Shipment, db: AsyncSession) -> dict:
    documents = {}
    for label, field_name in DOC_ID_FIELDS.items():
        document_id = getattr(shipment, field_name)
        document = await db.get(Document, document_id) if document_id else None
        documents[label] = None if document is None else _document_payload(document)

    if documents["rate_con"] is None and shipment.has_rate_con:
        result = await db.execute(
            select(RateConfirmation).where(RateConfirmation.load_number == shipment.load_number)
        )
        rate_confirmation = result.scalar_one_or_none()
        if rate_confirmation is not None:
            documents["rate_con"] = {
                "id": None,
                "filename": "Manual Entry",
                "doc_type": "rate_confirmation",
                "status": "extracted",
                "extracted_data": {
                    "carrier_name": rate_confirmation.carrier_name,
                    "load_number": rate_confirmation.load_number,
                    "agreed_rate": rate_confirmation.agreed_rate,
                    "origin": rate_confirmation.origin,
                    "destination": rate_confirmation.destination,
                    "shipment_date": rate_confirmation.shipment_date.isoformat(),
                },
                "created_at": rate_confirmation.created_at.isoformat(),
            }

    return documents


@router.get("/shipments")
async def list_shipments(
    status_filter: str | None = Query(default=None, alias="status"),
    db: AsyncSession = Depends(get_session),
) -> list[dict]:
    query = select(Shipment).order_by(Shipment.updated_at.desc())
    if status_filter:
        query = query.where(Shipment.reconciliation_status == status_filter)

    result = await db.execute(query)
    return [_shipment_summary(shipment) for shipment in result.scalars()]


@router.get("/shipments/{shipment_id}")
async def shipment_detail(
    shipment_id: UUID,
    db: AsyncSession = Depends(get_session),
) -> dict:
    shipment = await db.get(Shipment, shipment_id)
    if shipment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Shipment not found",
        )

    reconciliation = await _latest_reconciliation_result(shipment.id, db)
    return {
        **_shipment_summary(shipment),
        "documents": await _document_payloads(shipment, db),
        "reconciliation_result": None if reconciliation is None else {
            "id": str(reconciliation.id),
            "run_id": reconciliation.run_id,
            "checks": reconciliation.checks,
            "missing_docs": reconciliation.missing_docs,
            "exception_reasons": reconciliation.exception_reasons,
            "created_at": reconciliation.created_at,
        },
    }


@router.get("/analytics/carriers")
async def carrier_analytics(db: AsyncSession = Depends(get_session)) -> list[dict]:
    shipments_result = await db.execute(select(Shipment))
    shipments = list(shipments_result.scalars().all())

    reconciliation_result = await db.execute(
        select(ReconciliationResult).order_by(ReconciliationResult.created_at.desc())
    )
    latest_by_shipment: dict[UUID, ReconciliationResult] = {}
    for result in reconciliation_result.scalars():
        latest_by_shipment.setdefault(result.shipment_id, result)

    grouped: dict[str, list[Shipment]] = defaultdict(list)
    for shipment in shipments:
        grouped[shipment.carrier_name or "Unknown carrier"].append(shipment)

    rows = []
    for carrier_name, carrier_shipments in grouped.items():
        total_shipments = len(carrier_shipments)
        exception_shipments = [
            shipment
            for shipment in carrier_shipments
            if shipment.reconciliation_status == "exception"
        ]
        reasons: Counter[str] = Counter()
        for shipment in exception_shipments:
            latest = latest_by_shipment.get(shipment.id)
            if latest is not None:
                reasons.update(latest.exception_reasons or [])

        exception_count = len(exception_shipments)
        rows.append(
            {
                "carrier_name": carrier_name,
                "total_shipments": total_shipments,
                "exception_count": exception_count,
                "exception_rate": exception_count / total_shipments if total_shipments else 0,
                "most_common_exception_type": reasons.most_common(1)[0][0]
                if reasons
                else None,
            }
        )

    return sorted(rows, key=lambda row: row["exception_rate"], reverse=True)
