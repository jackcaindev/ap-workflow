import asyncio
import base64
import json
import logging
from pathlib import Path
from typing import Any

from redis.asyncio import Redis

from app.core.config import get_settings
from app.services.gmail_auth import get_gmail_service


logger = logging.getLogger(__name__)

SUPPORTED_ATTACHMENT_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg"}
INVOICE_QUEUE = "invoice_queue"
PROCESSED_TTL_SECONDS = 7 * 24 * 60 * 60


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
                "filename": filename,
                # Redis values are bytes/strings, not nested binary blobs. Base64
                # keeps the JSON payload transport-safe and reversible for the
                # worker before it calls the document workflow.
                "file_bytes": base64.b64encode(file_bytes).decode("ascii"),
                "gmail_thread_id": thread_id,
            }
        )

    return thread_id, attachments


def _mark_message_read(service: Any, message_id: str) -> None:
    service.users().messages().modify(
        userId="me",
        id=message_id,
        body={"removeLabelIds": ["UNREAD"]},
    ).execute()


async def poll_inbox() -> int:
    settings = get_settings()
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    processed_messages = 0

    try:
        # Scheduled polling is intentionally non-interactive. The OAuth browser
        # flow belongs behind /gmail/auth; a background task should not hang
        # indefinitely waiting for a user to approve credentials.
        service = await asyncio.to_thread(get_gmail_service, allow_interactive=False)
        messages = await asyncio.to_thread(_list_unread_messages, service)

        for message in messages:
            message_id = message["id"]
            processed_key = f"processed:{message_id}"
            # This idempotency key prevents duplicate queue jobs if a poll cycle
            # crashes after enqueueing attachments but before Gmail removes the
            # UNREAD label, or if two manual polls overlap.
            if await redis.exists(processed_key):
                continue

            _, attachments = await asyncio.to_thread(
                _download_supported_attachments,
                service,
                message_id,
            )
            if not attachments:
                continue

            for payload in attachments:
                await redis.lpush(INVOICE_QUEUE, json.dumps(payload))

            # Notifications are intentionally not sent from the poller. At this
            # point attachments are only queued; extraction, matching, and final
            # workflow status are not known until the worker processes each job.
            await redis.set(processed_key, "1", ex=PROCESSED_TTL_SECONDS)
            await asyncio.to_thread(_mark_message_read, service, message_id)
            processed_messages += 1

        logger.info("Gmail poll processed %s message(s)", processed_messages)
        return processed_messages
    finally:
        await redis.aclose()
