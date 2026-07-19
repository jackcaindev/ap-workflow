from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models.notification import Notification
from app.models.shipment import Shipment
from app.models.shipment_exception import ShipmentException, ShipmentExceptionEvent


router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.get("")
async def notifications(db: AsyncSession = Depends(get_session)) -> list[dict]:
    result = await db.execute(
        select(Notification).order_by(Notification.sent_at.desc()).limit(10)
    )

    records = [
        {
            "kind": "batch_summary",
            "id": notification.id,
            "sent_at": notification.sent_at,
            "total_count": notification.total_count,
            "complete_count": notification.complete_count,
            "awaiting_review_count": notification.awaiting_review_count,
            "failed_count": notification.failed_count,
            "approved_count": notification.approved_count,
            "rejected_count": notification.rejected_count,
            "ready_for_posting_count": notification.ready_for_posting_count,
        }
        for notification in result.scalars()
    ]
    sla_rows = await db.execute(
        select(ShipmentExceptionEvent, ShipmentException, Shipment)
        .join(ShipmentException, ShipmentException.id == ShipmentExceptionEvent.exception_id)
        .join(Shipment, Shipment.id == ShipmentException.shipment_id)
        .order_by(ShipmentExceptionEvent.occurred_at.desc())
        .limit(10)
    )
    records.extend(
        {
            "kind": "shipment_exception",
            "id": str(event.id),
            "sent_at": event.notification_sent_at,
            "occurred_at": event.occurred_at,
            "notification_status": event.notification_status,
            "transition": event.transition,
            "shipment_id": str(shipment.id),
            "load_number": shipment.load_number,
            "missing_docs": event.after_state.get("missing_docs", []),
            "reason_codes": event.after_state.get("reason_codes", []),
        }
        for event, _, shipment in sla_rows.all()
    )
    return sorted(
        records,
        key=lambda record: record.get("occurred_at") or record.get("sent_at"),
        reverse=True,
    )[:10]
