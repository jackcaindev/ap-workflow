import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from app.database import AsyncSessionLocal
from app.models.notification import Notification
from app.services.gmail_auth import get_gmail_service


def _format_amount(amount: Any) -> str:
    if amount is None:
        return "unknown amount"
    try:
        return f"${float(amount):,.2f}"
    except (TypeError, ValueError):
        return str(amount)


def _build_summary_body(results: list[dict]) -> str:
    auto_matched = [result for result in results if result.get("status") == "complete"]
    needs_review = [result for result in results if result.get("status") == "awaiting_review"]
    failed = [result for result in results if result.get("status") == "failed"]

    lines: list[str] = ["AP Workflow batch summary", ""]

    lines.append("✅ Auto-matched")
    if auto_matched:
        for result in auto_matched:
            carrier = result.get("carrier_name") or "Unknown carrier"
            amount = _format_amount(result.get("total_amount"))
            lines.append(f"- {result.get('filename')}: {carrier}, {amount}")
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
        "complete_count": sum(1 for result in results if result.get("status") == "complete"),
        "awaiting_review_count": sum(
            1 for result in results if result.get("status") == "awaiting_review"
        ),
        "failed_count": sum(1 for result in results if result.get("status") == "failed"),
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
    review_count = sum(1 for result in results if result.get("status") == "awaiting_review")

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
