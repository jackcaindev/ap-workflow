import base64
import logging
from datetime import UTC, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.notification import Notification
from app.models.shipment import Shipment
from app.models.shipment_exception import ShipmentException, ShipmentExceptionEvent
from app.services.gmail_auth import get_gmail_service


logger = logging.getLogger(__name__)


def _format_amount(amount: Any) -> str:
    if amount is None:
        return "unknown amount"
    try:
        return f"${float(amount):,.2f}"
    except (TypeError, ValueError):
        return str(amount)


def _build_summary_body(results: list[dict]) -> str:
    ready = [result for result in results if result.get("posting_status") == "ready_for_posting"]
    needs_review = [
        result for result in results if result.get("processing_status") == "awaiting_review"
    ]
    rejected = [result for result in results if result.get("review_disposition") == "rejected"]
    not_ready = [
        result
        for result in results
        if result.get("processing_status") == "complete"
        and result.get("posting_status") == "not_ready"
    ]
    failed = [result for result in results if result.get("processing_status") == "failed"]

    lines: list[str] = ["AP Workflow batch summary", ""]

    lines.append("✅ Ready for Posting")
    if ready:
        for result in ready:
            carrier = result.get("carrier_name") or "Unknown carrier"
            amount = _format_amount(result.get("total_amount"))
            lines.append(f"- {result.get('filename')}: {carrier}, {amount}")
    else:
        lines.append("- None")

    lines.extend(["", "⛔ Rejected / Blocked"])
    if rejected:
        for result in rejected:
            lines.append(f"- {result.get('filename')} ({result.get('run_id')})")
    else:
        lines.append("- None")

    lines.extend(["", "⏳ Processed, Not Ready"])
    if not_ready:
        for result in not_ready:
            lines.append(f"- {result.get('filename')} ({result.get('run_id')})")
    else:
        lines.append("- None")

    lines.extend(["", "⚠️ Needs Review"])
    if needs_review:
        for result in needs_review:
            reason = result.get("exception_reason") or "No reason provided"
            lines.append(f"- {result.get('filename')} ({result.get('run_id')}): {reason}")
    else:
        lines.append("- None")

    lines.extend(["", "❌ Failed"])
    if failed:
        for result in failed:
            error = result.get("exception_reason") or "Unknown error"
            lines.append(f"- {result.get('filename')}: {error}")
    else:
        lines.append("- None")

    return "\n".join(lines)


def _notification_counts(results: list[dict]) -> dict[str, int]:
    return {
        "total_count": len(results),
        "complete_count": sum(
            1 for result in results if result.get("processing_status") == "complete"
        ),
        "awaiting_review_count": sum(
            1 for result in results if result.get("processing_status") == "awaiting_review"
        ),
        "failed_count": sum(
            1 for result in results if result.get("processing_status") == "failed"
        ),
        "approved_count": sum(
            1 for result in results if result.get("review_disposition") == "approved"
        ),
        "rejected_count": sum(
            1 for result in results if result.get("review_disposition") == "rejected"
        ),
        "ready_for_posting_count": sum(
            1 for result in results if result.get("posting_status") == "ready_for_posting"
        ),
    }


async def _insert_notification_record(results: list[dict]) -> None:
    async with AsyncSessionLocal() as db:
        db.add(Notification(**_notification_counts(results)))
        await db.commit()


def _send_summary_email(results: list[dict]) -> bool:
    try:
        service = get_gmail_service(allow_interactive=False)
    except RuntimeError as exc:
        if str(exc) == "No Gmail token found — run /gmail/auth first":
            return False
        raise

    profile = service.users().getProfile(userId="me").execute()
    email_address = profile["emailAddress"]
    review_count = sum(
        1 for result in results if result.get("processing_status") == "awaiting_review"
    )

    message = MIMEMultipart()
    message["From"] = email_address
    message["To"] = email_address
    message["Subject"] = (
        f"AP Workflow — {len(results)} invoices processed, {review_count} need review"
    )
    message.attach(MIMEText(_build_summary_body(results), "plain", "utf-8"))

    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")
    service.users().messages().send(
        userId="me",
        body={"raw": encoded_message},
    ).execute()
    return True


async def send_batch_summary(results: list[dict]) -> None:
    if not results:
        return

    sent = _send_summary_email(results)
    if sent:
        await _insert_notification_record(results)


def _send_shipment_exception_email(payload: dict[str, Any]) -> bool:
    try:
        service = get_gmail_service(allow_interactive=False)
    except RuntimeError as exc:
        if str(exc) == "No Gmail token found — run /gmail/auth first":
            return False
        raise

    profile = service.users().getProfile(userId="me").execute()
    email_address = profile["emailAddress"]
    transition = payload["transition"]
    load_number = payload["load_number"]
    missing_docs = payload["missing_docs"]
    message = MIMEMultipart()
    message["From"] = email_address
    message["To"] = email_address
    message["Subject"] = f"AP Workflow — shipment {load_number} SLA exception {transition}"
    message.attach(
        MIMEText(
            "\n".join(
                [
                    f"Shipment {load_number}",
                    f"Missing-document SLA exception: {transition}",
                    f"Missing required documents: {', '.join(missing_docs) if missing_docs else 'none'}",
                    f"Reason codes: {', '.join(payload['reason_codes']) if payload['reason_codes'] else 'none'}",
                ]
            ),
            "plain",
            "utf-8",
        )
    )
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")
    service.users().messages().send(userId="me", body={"raw": encoded_message}).execute()
    return True


async def dispatch_pending_sla_notifications() -> int:
    sent_count = 0
    while True:
        async with AsyncSessionLocal() as db:
            row = (
                await db.execute(
                    select(ShipmentExceptionEvent, ShipmentException, Shipment)
                    .join(
                        ShipmentException,
                        ShipmentException.id == ShipmentExceptionEvent.exception_id,
                    )
                    .join(Shipment, Shipment.id == ShipmentException.shipment_id)
                    .where(
                        ShipmentExceptionEvent.notification_status.in_(["pending", "failed"])
                    )
                    .order_by(ShipmentExceptionEvent.occurred_at.asc())
                    .with_for_update(skip_locked=True, of=ShipmentExceptionEvent)
                    .limit(1)
                )
            ).one_or_none()
            if row is None:
                return sent_count
            event, exception, shipment = row
            payload = {
                "transition": event.transition,
                "load_number": shipment.load_number,
                "missing_docs": event.after_state.get("missing_docs", []),
                "reason_codes": event.after_state.get("reason_codes", []),
            }
            event.notification_status = "sending"
            event.notification_attempt_count += 1
            event.notification_last_error = None
            event_id = event.id
            await db.commit()

        try:
            sent = _send_shipment_exception_email(payload)
            if not sent:
                raise RuntimeError("Gmail is not authenticated")
        except Exception as exc:
            async with AsyncSessionLocal() as db:
                failed_event = await db.get(ShipmentExceptionEvent, event_id)
                if failed_event is not None:
                    failed_event.notification_status = "failed"
                    failed_event.notification_last_error = str(exc)
                    await db.commit()
            logger.warning("Shipment SLA notification delivery failed: %s", exc)
            return sent_count

        async with AsyncSessionLocal() as db:
            sent_event = await db.get(ShipmentExceptionEvent, event_id)
            if sent_event is not None:
                sent_event.notification_status = "sent"
                sent_event.notification_sent_at = datetime.now(UTC)
                await db.commit()
        sent_count += 1
