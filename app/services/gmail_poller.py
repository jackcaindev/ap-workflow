import asyncio
import base64
import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from redis.asyncio import Redis

from app.core.config import get_settings
from app.schemas.invoice_job import InvoiceJobEnvelope
from app.services.gmail_auth import get_gmail_service
from app.services.health import RuntimeHealth, reason_code_for_exception, utc_now
from app.services.invoice_queue import enqueue_job


logger = logging.getLogger(__name__)

SUPPORTED_ATTACHMENT_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg"}


def _supported_attachment(filename: str) -> bool:
    return Path(filename).suffix.lower() in SUPPORTED_ATTACHMENT_EXTENSIONS


def _decode_gmail_attachment(data: str) -> bytes:
    # Gmail attachment data is base64url encoded. Padding is restored because
    # some API responses omit it, while Python's decoder expects padded input.
    padded = data + ("=" * (-len(data) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _iter_attachment_parts(parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    for part in parts:
        nested_parts = part.get("parts") or []
        if nested_parts:
            attachments.extend(_iter_attachment_parts(nested_parts))

        filename = part.get("filename") or ""
        body = part.get("body") or {}
        if filename and _supported_attachment(filename) and (
            body.get("attachmentId") or body.get("data")
        ):
            attachments.append(part)
    return attachments


def _list_unread_messages(service: Any) -> list[dict[str, str]]:
    response = (
        service.users()
        .messages()
        .list(
            userId="me",
            labelIds=["INBOX", "UNREAD"],
            # The query narrows Gmail's result set, and code still filters by
            # extension because attachment filenames are the source of truth for
            # whether the workflow can process a file.
            q="in:inbox is:unread has:attachment",
            maxResults=25,
        )
        .execute()
    )
    return response.get("messages", [])


def _download_supported_attachments(service: Any, message_id: str) -> tuple[str, list[dict]]:
    message = (
        service.users()
        .messages()
        .get(userId="me", id=message_id, format="full")
        .execute()
    )
    thread_id = message.get("threadId", "")
    payload = message.get("payload") or {}
    parts = _iter_attachment_parts(payload.get("parts") or [])
    attachments: list[dict] = []

    for part in parts:
        filename = part["filename"]
        body = part.get("body") or {}
        if body.get("data"):
            file_bytes = _decode_gmail_attachment(body["data"])
        else:
            attachment = (
                service.users()
                .messages()
                .attachments()
                .get(userId="me", messageId=message_id, id=body["attachmentId"])
                .execute()
            )
            file_bytes = _decode_gmail_attachment(attachment["data"])

        attachments.append(
            {
                "message_id": message_id,
                "mime_part_id": part.get("partId") or body.get("attachmentId"),
                "filename": filename,
                # Redis values are bytes/strings, not nested binary blobs. Base64
                # keeps the JSON payload transport-safe and reversible for the
                # worker before it calls the document workflow.
                "file_bytes": base64.b64encode(file_bytes).decode("ascii"),
                "gmail_thread_id": thread_id,
            }
        )

    return thread_id, attachments


def _attachment_idempotency_key(
    gmail_account: str, message_id: str, mime_part_id: str
) -> str:
    canonical = f"gmail:v1\0{gmail_account.strip().lower()}\0{message_id}\0{mime_part_id}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _gmail_account(service: Any) -> str:
    profile = service.users().getProfile(userId="me").execute()
    return profile["emailAddress"].strip().lower()


def _mark_message_read(service: Any, message_id: str) -> None:
    service.users().messages().modify(
        userId="me",
        id=message_id,
        body={"removeLabelIds": ["UNREAD"]},
    ).execute()


async def poll_inbox(runtime_health: RuntimeHealth | None = None) -> int:
    settings = get_settings()
    if runtime_health is not None:
        runtime_health.gmail.started(utc_now())
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    processed_messages = 0

    try:
        # Scheduled polling is intentionally non-interactive. The OAuth browser
        # flow belongs behind /gmail/auth; a background task should not hang
        # indefinitely waiting for a user to approve credentials.
        service = await asyncio.to_thread(get_gmail_service, allow_interactive=False)
        gmail_account = await asyncio.to_thread(_gmail_account, service)
        messages = await asyncio.to_thread(_list_unread_messages, service)

        for message in messages:
            message_id = message["id"]
            _, attachments = await asyncio.to_thread(
                _download_supported_attachments,
                service,
                message_id,
            )
            if not attachments:
                continue

            for payload in attachments:
                mime_part_id = payload["mime_part_id"]
                file_bytes = base64.b64decode(payload["file_bytes"], validate=True)
                job = InvoiceJobEnvelope(
                    idempotency_key=_attachment_idempotency_key(
                        gmail_account, message_id, mime_part_id
                    ),
                    gmail_account=gmail_account,
                    message_id=message_id,
                    gmail_thread_id=payload["gmail_thread_id"],
                    mime_part_id=mime_part_id,
                    filename=payload["filename"],
                    file_bytes=payload["file_bytes"],
                    content_sha256=hashlib.sha256(file_bytes).hexdigest(),
                    enqueued_at=datetime.now(UTC),
                )
                await enqueue_job(
                    redis,
                    stream=settings.INVOICE_STREAM,
                    dedupe_prefix=settings.INVOICE_DEDUPE_PREFIX,
                    dedupe_ttl_seconds=settings.INVOICE_DEDUPE_TTL_SECONDS,
                    job=job,
                )

            # Notifications are intentionally not sent from the poller. At this
            # point attachments are only queued; extraction, matching, and final
            # workflow status are not known until the worker processes each job.
            await asyncio.to_thread(_mark_message_read, service, message_id)
            processed_messages += 1

        logger.info("Gmail poll processed %s message(s)", processed_messages)
        if runtime_health is not None:
            runtime_health.gmail.succeeded(utc_now(), processed_messages)
        return processed_messages
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if runtime_health is not None:
            runtime_health.gmail.failed(utc_now(), reason_code_for_exception(exc))
        raise
    finally:
        await redis.aclose()
