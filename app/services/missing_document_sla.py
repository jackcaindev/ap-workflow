import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.reconciliation_result import ReconciliationResult
from app.models.shipment import Shipment
from app.models.shipment_exception import ShipmentException, ShipmentExceptionEvent


logger = logging.getLogger(__name__)

MISSING_DOCUMENT_EXCEPTION_KIND = "missing_required_documents"
REQUIRED_DOCUMENTS = {
    "invoice": ("has_invoice", "missing_required_invoice_sla_exceeded"),
    "rate_con": ("has_rate_con", "missing_required_rate_con_sla_exceeded"),
    "bol": ("has_bol", "missing_required_bol_sla_exceeded"),
    "pod": ("has_pod", "missing_required_pod_sla_exceeded"),
}
SLA_REASON_CODES = frozenset(reason for _, reason in REQUIRED_DOCUMENTS.values())


@dataclass(frozen=True)
class MissingDocumentPolicyResult:
    state: str
    deadline_at: datetime
    missing_docs: tuple[str, ...]
    reason_codes: tuple[str, ...]
    checks: tuple[dict, ...]


@dataclass
class ScannerHealth:
    last_started_at: datetime | None = None
    last_succeeded_at: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    last_result_count: int | None = None

    def payload(self) -> dict:
        status = "degraded" if self.last_error else "ok"
        return {
            "status": status,
            "last_started_at": self.last_started_at,
            "last_succeeded_at": self.last_succeeded_at,
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
        }


def evaluate_missing_document_policy(
    shipment: Shipment,
    *,
    now: datetime,
    sla_duration: timedelta,
) -> MissingDocumentPolicyResult:
    deadline_at = shipment.created_at + sla_duration
    missing_docs = tuple(
        doc_type
        for doc_type, (presence_field, _) in REQUIRED_DOCUMENTS.items()
        if not getattr(shipment, presence_field)
    )
    overdue = bool(missing_docs) and now >= deadline_at
    state = "overdue" if overdue else "within_grace" if missing_docs else "complete"
    checks = []
    reason_codes = []
    for doc_type, (presence_field, overdue_reason) in REQUIRED_DOCUMENTS.items():
        present = bool(getattr(shipment, presence_field))
        if present:
            outcome = "passed"
            details = f"Required {doc_type} is present"
            reason_code = None
        elif overdue:
            outcome = "failed"
            details = f"Required {doc_type} was not received by {deadline_at.isoformat()}"
            reason_code = overdue_reason
            reason_codes.append(overdue_reason)
        else:
            outcome = "not_evaluated"
            details = f"Required {doc_type} is pending until {deadline_at.isoformat()}"
            reason_code = None
        checks.append(
            {
                "check_name": f"required_{doc_type}_present",
                "outcome": outcome,
                "details": details,
                "reason_code": reason_code,
            }
        )
    return MissingDocumentPolicyResult(
        state=state,
        deadline_at=deadline_at,
        missing_docs=missing_docs,
        reason_codes=tuple(reason_codes),
        checks=tuple(checks),
    )


def _exception_state(
    *, status: str, missing_docs: list[str], reason_codes: list[str], deadline_at: datetime
) -> dict:
    return {
        "status": status,
        "missing_docs": missing_docs,
        "reason_codes": reason_codes,
        "deadline_at": deadline_at.isoformat(),
    }


async def sync_missing_document_exception(
    db: AsyncSession,
    shipment: Shipment,
    policy: MissingDocumentPolicyResult,
    result: ReconciliationResult,
    *,
    now: datetime,
) -> ShipmentExceptionEvent | None:
    existing = await db.scalar(
        select(ShipmentException)
        .where(
            ShipmentException.shipment_id == shipment.id,
            ShipmentException.kind == MISSING_DOCUMENT_EXCEPTION_KIND,
        )
        .with_for_update()
    )
    desired_active = policy.state == "overdue"

    if existing is None and not desired_active:
        return None

    if existing is None:
        exception = ShipmentException(
            shipment_id=shipment.id,
            kind=MISSING_DOCUMENT_EXCEPTION_KIND,
            status="active",
            missing_docs=list(policy.missing_docs),
            reason_codes=list(policy.reason_codes),
            deadline_at=policy.deadline_at,
            version=1,
            opened_at=now,
            resolved_at=None,
        )
        db.add(exception)
        await db.flush()
        event = ShipmentExceptionEvent(
            exception_id=exception.id,
            reconciliation_result_id=result.id,
            version=1,
            transition="opened",
            before_state=None,
            after_state=_exception_state(
                status="active",
                missing_docs=exception.missing_docs,
                reason_codes=exception.reason_codes,
                deadline_at=exception.deadline_at,
            ),
            occurred_at=now,
        )
        db.add(event)
        return event

    before_state = _exception_state(
        status=existing.status,
        missing_docs=existing.missing_docs,
        reason_codes=existing.reason_codes,
        deadline_at=existing.deadline_at,
    )
    desired_missing = list(policy.missing_docs) if desired_active else []
    desired_reasons = list(policy.reason_codes) if desired_active else []
    unchanged = (
        existing.status == ("active" if desired_active else "resolved")
        and existing.missing_docs == desired_missing
        and existing.reason_codes == desired_reasons
        and existing.deadline_at == policy.deadline_at
    )
    if unchanged:
        return None

    transition = "resolved" if not desired_active else "opened" if existing.status == "resolved" else "changed"
    existing.version += 1
    existing.status = "active" if desired_active else "resolved"
    existing.missing_docs = desired_missing
    existing.reason_codes = desired_reasons
    existing.deadline_at = policy.deadline_at
    existing.resolved_at = None if desired_active else now
    if transition == "opened":
        existing.opened_at = now

    event = ShipmentExceptionEvent(
        exception_id=existing.id,
        reconciliation_result_id=result.id,
        version=existing.version,
        transition=transition,
        before_state=before_state,
        after_state=_exception_state(
            status=existing.status,
            missing_docs=existing.missing_docs,
            reason_codes=existing.reason_codes,
            deadline_at=existing.deadline_at,
        ),
        occurred_at=now,
    )
    db.add(event)
    return event


async def claim_and_evaluate_one(*, now: datetime, sla_duration: timedelta) -> bool:
    async with AsyncSessionLocal() as db:
        active_exception = exists(
            select(ShipmentException.id).where(
                ShipmentException.shipment_id == Shipment.id,
                ShipmentException.kind == MISSING_DOCUMENT_EXCEPTION_KIND,
                ShipmentException.status == "active",
            )
        )
        shipment = await db.scalar(
            select(Shipment)
            .where(
                Shipment.created_at <= now - sla_duration,
                or_(
                    Shipment.has_invoice.is_(False),
                    Shipment.has_rate_con.is_(False),
                    Shipment.has_bol.is_(False),
                    Shipment.has_pod.is_(False),
                ),
                ~active_exception,
            )
            .order_by(Shipment.created_at.asc(), Shipment.id.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if shipment is None:
            await db.rollback()
            return False

        policy = evaluate_missing_document_policy(shipment, now=now, sla_duration=sla_duration)
        fingerprint = ",".join(policy.missing_docs)
        evaluation_key = (
            f"missing-docs-sla:{shipment.id}:{policy.deadline_at.isoformat()}:{fingerprint}"
        )
        from app.services.reconciliation import reconcile_shipment

        await reconcile_shipment(
            shipment,
            db,
            now=now,
            sla_duration=sla_duration,
            evaluation_source="scheduled_sla",
            evaluation_key=evaluation_key,
        )
        return True


async def scan_missing_document_slas(*, now: datetime, sla_duration: timedelta) -> int:
    count = 0
    while await claim_and_evaluate_one(now=now, sla_duration=sla_duration):
        count += 1
    return count


async def run_scanner_iteration(
    health: ScannerHealth,
    *,
    now: datetime,
    sla_duration: timedelta,
    scan: Callable[..., Awaitable[int]] = scan_missing_document_slas,
) -> bool:
    health.last_started_at = now
    try:
        count = await scan(now=now, sla_duration=sla_duration)
    except Exception:
        health.last_error = "scan_failed"
        health.consecutive_failures += 1
        logger.exception("Scheduled missing-document SLA scan failed")
        return False
    health.last_succeeded_at = now
    health.last_error = None
    health.consecutive_failures = 0
    health.last_result_count = count
    return True
