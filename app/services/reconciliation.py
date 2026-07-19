from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.document import Document
from app.models.rate_confirmation import RateConfirmation
from app.models.reconciliation_result import ReconciliationResult
from app.models.shipment import Shipment
from app.schemas.reconciliation import CheckOutcome, ReconciliationCheck
from app.services.shipment import rate_confirmation_extraction


AMOUNT_TOLERANCE = 0.05
DOC_TYPE_LABELS = {
    "invoice": "invoice",
    "rate_confirmation": "rate_con",
    "bill_of_lading": "bol",
    "proof_of_delivery": "pod",
}


def _normalize_carrier(value: Any) -> str:
    # Carrier names arrive from OCR/vision extraction with inconsistent case and
    # whitespace. Normalizing before exact comparison prevents false exceptions
    # caused by formatting differences rather than a real carrier mismatch.
    return " ".join(str(value or "").strip().upper().split())


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None


def _amount_from(extraction: dict[str, Any] | None, *keys: str) -> float | None:
    extraction = extraction or {}
    for key in keys:
        value = extraction.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _doc_extraction(document: Document | None) -> dict[str, Any] | None:
    if document is None:
        return None
    return document.extracted_data or {}


async def _shipment_documents(
    shipment: Shipment,
    db: AsyncSession,
) -> dict[str, Document | None]:
    return {
        "invoice": await db.get(Document, shipment.invoice_id) if shipment.invoice_id else None,
        "rate_con": await db.get(Document, shipment.rate_con_id) if shipment.rate_con_id else None,
        "bol": await db.get(Document, shipment.bol_id) if shipment.bol_id else None,
        "pod": await db.get(Document, shipment.pod_id) if shipment.pod_id else None,
    }


def _missing_docs(shipment: Shipment, rate_con: dict[str, Any] | None = None) -> list[str]:
    missing = []
    if not shipment.has_invoice:
        missing.append("invoice")
    if not shipment.has_rate_con and rate_con is None:
        missing.append("rate_con")
    if not shipment.has_bol:
        missing.append("bol")
    if not shipment.has_pod:
        missing.append("pod")
    return missing


def _check(
    checks: list[dict[str, Any]],
    exception_reasons: list[str],
    check_name: str,
    outcome: CheckOutcome,
    details: str,
    reason: str | None = None,
) -> None:
    checks.append(
        ReconciliationCheck(
            check_name=check_name,
            outcome=outcome,
            details=details,
        ).model_dump()
    )
    if outcome == "failed":
        exception_reasons.append(reason or check_name)


async def reconcile_shipment(
    shipment: Shipment,
    db: AsyncSession,
    run_id: str | None = None,
) -> ReconciliationResult:
    documents = await _shipment_documents(shipment, db)
    invoice = _doc_extraction(documents["invoice"])
    rate_con = _doc_extraction(documents["rate_con"])
    bol = _doc_extraction(documents["bol"])
    pod = _doc_extraction(documents["pod"])

    rate_confirmation_result = await db.execute(
        select(RateConfirmation).where(RateConfirmation.load_number == shipment.load_number)
    )
    rate_confirmation = rate_confirmation_result.scalar_one_or_none()
    if rate_confirmation is not None:
        rate_con = rate_confirmation_extraction(rate_confirmation)
        shipment.has_rate_con = True

    checks: list[dict[str, Any]] = []
    exception_reasons: list[str] = []

    invoice_total = _amount_from(invoice, "total_amount", "invoice_total")
    agreed_rate = _amount_from(rate_con, "agreed_rate", "total_amount", "rate")
    if invoice_total is None or agreed_rate is None:
        _check(
            checks,
            exception_reasons,
            "amount_variance",
            "not_evaluated",
            "skipped: invoice or rate confirmation amount is missing",
        )
    else:
        variance = invoice_total - agreed_rate
        passed = abs(variance) <= abs(agreed_rate) * AMOUNT_TOLERANCE
        _check(
            checks,
            exception_reasons,
            "amount_variance",
            "passed" if passed else "failed",
            f"invoice={invoice_total}, agreed_rate={agreed_rate}, variance={variance}",
            "amount_variance",
        )

    invoice_carrier = _normalize_carrier((invoice or {}).get("carrier_name"))
    rate_con_carrier = _normalize_carrier((rate_con or {}).get("carrier_name"))
    if not invoice_carrier or not rate_con_carrier:
        _check(
            checks,
            exception_reasons,
            "carrier_match",
            "not_evaluated",
            "skipped: invoice or rate confirmation carrier is missing",
        )
    else:
        _check(
            checks,
            exception_reasons,
            "carrier_match",
            "passed" if invoice_carrier == rate_con_carrier else "failed",
            f"invoice={invoice_carrier}, rate_con={rate_con_carrier}",
            "carrier_mismatch",
        )

    bol_pickup_date = _parse_date((bol or {}).get("pickup_date") or (bol or {}).get("shipment_date"))
    rate_con_ship_date = _parse_date(
        (rate_con or {}).get("shipment_date") or (rate_con or {}).get("invoice_date")
    )
    if bol_pickup_date is None or rate_con_ship_date is None:
        _check(
            checks,
            exception_reasons,
            "bol_pickup_date",
            "not_evaluated",
            "skipped: BOL pickup date or rate confirmation shipment date is missing",
        )
    else:
        day_delta = abs((bol_pickup_date - rate_con_ship_date).days)
        _check(
            checks,
            exception_reasons,
            "bol_pickup_date",
            "passed" if day_delta <= 1 else "failed",
            f"bol_pickup_date={bol_pickup_date}, rate_con_shipment_date={rate_con_ship_date}",
            "bol_pickup_date_mismatch",
        )

    shipment_age = datetime.now(UTC) - shipment.created_at
    # PODs often arrive after invoice/rate paperwork, so a missing POD is only
    # exceptional after a short grace period. Three days is enough time for the
    # proof document to surface without blocking early partial reconciliation.
    if shipment.has_pod:
        delivery_date = (pod or {}).get("delivery_date")
        condition = (pod or {}).get("condition")
        _check(
            checks,
            exception_reasons,
            "pod_delivery_confirmation",
            "passed" if delivery_date and condition else "failed",
            f"delivery_date={delivery_date or 'missing'}, condition={condition or 'missing'}",
            "pod_incomplete",
        )
    elif shipment_age > timedelta(days=3):
        _check(
            checks,
            exception_reasons,
            "pod_delivery_confirmation",
            "failed",
            "POD missing more than 3 days after shipment creation",
            "missing_pod",
        )
    else:
        _check(
            checks,
            exception_reasons,
            "pod_delivery_confirmation",
            "not_evaluated",
            "POD not received yet, still within 3-day grace period",
        )

    missing_docs = _missing_docs(shipment, rate_con)
    _check(
        checks,
        exception_reasons,
        "missing_docs",
        "not_evaluated" if missing_docs else "passed",
        ", ".join(missing_docs) if missing_docs else "none",
    )

    if exception_reasons:
        shipment.reconciliation_status = "exception"
    elif missing_docs or any(check["outcome"] == "not_evaluated" for check in checks):
        # Reconciliation runs on partial document sets because freight paperwork
        # arrives over time. Recording partial results lets operations see what
        # already passed and what is still missing instead of waiting for all
        # four documents before surfacing useful status.
        shipment.reconciliation_status = "partial"
    else:
        shipment.reconciliation_status = "reconciled"

    result = None
    if run_id is not None:
        existing = await db.execute(
            select(ReconciliationResult).where(ReconciliationResult.run_id == run_id)
        )
        result = existing.scalar_one_or_none()
    if result is None:
        result = ReconciliationResult(
            shipment_id=shipment.id,
            run_id=run_id,
            checks=checks,
            missing_docs=missing_docs,
            exception_reasons=exception_reasons,
        )
        db.add(result)
    else:
        result.shipment_id = shipment.id
        result.checks = checks
        result.missing_docs = missing_docs
        result.exception_reasons = exception_reasons
    await db.commit()
    await db.refresh(result)
    return result
