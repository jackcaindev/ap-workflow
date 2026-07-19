import asyncio
import logging
import os
import socket
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from redis.asyncio import Redis
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.core.config import Settings, get_settings
from app.database import AsyncSessionLocal
from app.models.document import Document
from app.models.workflow_run import WorkflowRun
from app.schemas.invoice_job import InvoiceJobEnvelope, PermanentJobError
from app.services.invoice_queue import (
    StreamDelivery,
    acknowledge,
    claim_stale_batch,
    dead_letter,
    ensure_consumer_group,
    parse_delivery,
    read_new_batch,
    record_failure,
)
from app.services.notifier import send_batch_summary
from app.workflow.graph import workflow_graph


logger = logging.getLogger(__name__)
REDELIVERY_SHORT_CIRCUIT_PROCESSING_STATUSES = {"complete", "awaiting_review"}


def graph_config(run_id: str) -> dict[str, dict[str, str]]:
    return {"configurable": {"thread_id": run_id}}


def interrupt_payload(result: dict) -> dict | None:
    interrupts = result.get("__interrupt__") or []
    if not interrupts:
        return None
    return interrupts[0].value


async def persist_run_state(
    run: WorkflowRun,
    *,
    status_value: str,
    processing_status: str | None = None,
    posting_status: str | None = None,
    payload: dict | None = None,
    preserve_payload: bool = False,
) -> None:
    run.status = status_value
    run.processing_status = processing_status or status_value
    if posting_status is not None:
        run.posting_status = posting_status
    if not preserve_payload:
        run.interrupt_payload = payload


def _summary_result(
    *,
    filename: str,
    run_id: str,
    status: str,
    extraction: dict[str, Any] | None = None,
    exception_reason: str | None = None,
    processing_status: str | None = None,
    reconciliation_status: str | None = None,
    review_disposition: str | None = None,
    posting_status: str | None = None,
) -> dict[str, Any]:
    extraction = extraction or {}
    if processing_status is None:
        processing_status = "complete" if status in {"approved", "rejected"} else status
    if review_disposition is None:
        if status in {"approved", "rejected"}:
            review_disposition = status
        elif processing_status == "awaiting_review":
            review_disposition = "pending"
        else:
            review_disposition = "not_required"
    if posting_status is None:
        if review_disposition == "approved" or reconciliation_status == "reconciled":
            posting_status = "ready_for_posting"
        elif review_disposition == "rejected":
            posting_status = "blocked"
        else:
            posting_status = "not_ready"
    return {
        "filename": filename,
        "run_id": run_id,
        "status": status,
        "processing_status": processing_status,
        "reconciliation_status": reconciliation_status,
        "review_disposition": review_disposition,
        "posting_status": posting_status,
        "carrier_name": extraction.get("carrier_name"),
        "total_amount": extraction.get("total_amount"),
        "exception_reason": exception_reason,
    }


def _deterministic_run_id(idempotency_key: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"freight-ap:gmail:{idempotency_key}"))


async def _find_run_for_document(db: Any, document_id: int) -> WorkflowRun | None:
    result = await db.execute(
        select(WorkflowRun).where(WorkflowRun.document_id == document_id).limit(1)
    )
    return result.scalar_one_or_none()


async def _get_or_create_job_records(
    job: InvoiceJobEnvelope,
) -> tuple[Document, WorkflowRun]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Document).where(
                Document.source_idempotency_key == job.idempotency_key
            )
        )
        document = result.scalar_one_or_none()
        if document is not None:
            run = await _find_run_for_document(db, document.id)
            if run is None:
                raise RuntimeError("idempotent document is missing its workflow run")
            return document, run

        document = Document(
            filename=job.filename,
            doc_type="unknown",
            status="received",
            source_type="gmail",
            source_idempotency_key=job.idempotency_key,
            source_message_id=job.message_id,
            source_part_id=job.mime_part_id,
            source_enqueued_at=job.enqueued_at,
            content_sha256=job.content_sha256,
        )
        db.add(document)
        try:
            await db.flush()
            run = WorkflowRun(
                run_id=_deterministic_run_id(job.idempotency_key),
                document_id=document.id,
                status="running",
                processing_status="running",
                posting_status="not_ready",
                interrupt_payload={
                    "gmail_message_id": job.message_id,
                    "gmail_thread_id": job.gmail_thread_id,
                },
            )
            db.add(run)
            await db.commit()
        except IntegrityError:
            await db.rollback()
            result = await db.execute(
                select(Document).where(
                    Document.source_idempotency_key == job.idempotency_key
                )
            )
            document = result.scalar_one()
            run = await _find_run_for_document(db, document.id)
            if run is None:
                raise RuntimeError("concurrent document creation did not create a workflow run")
        return document, run


async def _run_workflow_for_job(job: InvoiceJobEnvelope) -> dict[str, Any]:
    document, existing_run = await _get_or_create_job_records(job)
    run_id = existing_run.run_id
    if existing_run.processing_status == "failed":
        payload = existing_run.interrupt_payload or {}
        raise RuntimeError(payload.get("error") or "workflow previously failed")
    if existing_run.processing_status in REDELIVERY_SHORT_CIRCUIT_PROCESSING_STATUSES:
        payload = existing_run.interrupt_payload or {}
        return _summary_result(
            filename=document.filename,
            run_id=run_id,
            status=existing_run.status,
            processing_status=existing_run.processing_status,
            posting_status=existing_run.posting_status,
            extraction=document.extracted_data,
            exception_reason=payload.get("reason") or payload.get("error"),
        )

    initial_state = {
        "run_id": run_id,
        "document_id": str(document.id),
        "file_bytes": job.decoded_file_bytes(),
        "filename": job.filename,
        "extraction": None,
        "match_result": None,
        "exception_reason": None,
        "triage_route": None,
        "triage_reasoning": None,
        "triage_confidence": None,
        "human_decision": None,
        "status": "running",
        "processing_status": "running",
        "posting_status": "not_ready",
        "messages": [f"Queued from Gmail message {job.message_id}."],
        "iteration_count": 0,
    }

    async with AsyncSessionLocal() as db:
        run_result = await db.execute(
            select(WorkflowRun).where(WorkflowRun.run_id == run_id)
        )
        run = run_result.scalar_one()
        try:
            config = graph_config(run_id)
            snapshot = await workflow_graph.aget_state(config)
            snapshot_values = dict(getattr(snapshot, "values", {}) or {})
            snapshot_next = tuple(getattr(snapshot, "next", ()) or ())
            if snapshot_values and not snapshot_next:
                # The graph committed its final checkpoint but the worker died
                # before updating WorkflowRun or acknowledging Redis.
                result = snapshot_values
            else:
                graph_input = None if snapshot_next else initial_state
                result = await workflow_graph.ainvoke(graph_input, config=config)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await persist_run_state(
                run,
                status_value="retrying",
                processing_status="retrying",
                posting_status="not_ready",
                payload={
                    "error": str(exc),
                    "gmail_message_id": job.message_id,
                    "gmail_thread_id": job.gmail_thread_id,
                },
            )
            await db.commit()
            raise

        payload = interrupt_payload(result)
        if payload is not None:
            payload.update(run.interrupt_payload or {})
            await persist_run_state(
                run,
                status_value="awaiting_review",
                processing_status="awaiting_review",
                posting_status="not_ready",
                payload=payload,
            )
        else:
            await persist_run_state(
                run,
                status_value=result.get("status", "complete"),
                processing_status=result.get("processing_status", "complete"),
                posting_status=result.get("posting_status", "not_ready"),
                preserve_payload=True,
            )
        await db.commit()

    return _summary_result(
        filename=job.filename,
        run_id=run_id,
        status="awaiting_review" if payload is not None else result.get("status", "complete"),
        processing_status=(
            "awaiting_review" if payload is not None else result.get("processing_status", "complete")
        ),
        reconciliation_status=(result.get("match_result") or {}).get("reconciliation_status"),
        posting_status=(
            "not_ready" if payload is not None else result.get("posting_status", "not_ready")
        ),
        extraction=result.get("extraction"),
        exception_reason=(
            result.get("exception_reason")
            or (payload or {}).get("reason")
        ),
    )


async def _mark_job_failed(job: InvoiceJobEnvelope, reason: str) -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Document).where(
                Document.source_idempotency_key == job.idempotency_key
            )
        )
        document = result.scalar_one_or_none()
        if document is None:
            return
        run = await _find_run_for_document(db, document.id)
        if run is not None:
            run.status = "failed"
            run.processing_status = "failed"
            run.posting_status = "not_ready"
            run.interrupt_payload = {
                **(run.interrupt_payload or {}),
                "error": reason,
            }
            await db.commit()


async def _heartbeat(
    redis: Redis,
    delivery: StreamDelivery,
    settings: Settings,
    consumer: str,
) -> None:
    interval = max(1.0, settings.INVOICE_VISIBILITY_TIMEOUT_MS / 3000)
    while True:
        await asyncio.sleep(interval)
        await redis.xclaim(
            settings.INVOICE_STREAM,
            settings.INVOICE_CONSUMER_GROUP,
            consumer,
            min_idle_time=0,
            message_ids=[delivery.stream_id],
            retrycount=delivery.attempt,
            justid=True,
        )


async def _process_delivery(
    redis: Redis,
    delivery: StreamDelivery,
    settings: Settings,
    consumer: str,
) -> dict[str, Any] | None:
    try:
        job = parse_delivery(delivery)
    except PermanentJobError as exc:
        await dead_letter(
            redis,
            stream=settings.INVOICE_STREAM,
            group=settings.INVOICE_CONSUMER_GROUP,
            dead_letter_stream=settings.INVOICE_DEAD_LETTER_STREAM,
            delivery=delivery,
            reason=str(exc),
        )
        return _summary_result(
            filename="unknown", run_id="", status="failed", exception_reason=str(exc)
        )

    queue_age_seconds = max(
        0.0, (datetime.now(UTC) - job.enqueued_at).total_seconds()
    )
    logger.info(
        "Processing invoice stream_id=%s attempt=%s queue_age_seconds=%.3f key=%s",
        delivery.stream_id,
        delivery.attempt,
        queue_age_seconds,
        job.idempotency_key,
    )
    heartbeat = asyncio.create_task(_heartbeat(redis, delivery, settings, consumer))
    try:
        result = await _run_workflow_for_job(job)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        reason = str(exc) or type(exc).__name__
        if delivery.attempt >= settings.INVOICE_MAX_ATTEMPTS:
            await _mark_job_failed(job, reason)
            await dead_letter(
                redis,
                stream=settings.INVOICE_STREAM,
                group=settings.INVOICE_CONSUMER_GROUP,
                dead_letter_stream=settings.INVOICE_DEAD_LETTER_STREAM,
                delivery=delivery,
                reason=reason,
            )
            return _summary_result(
                filename=job.filename,
                run_id=_deterministic_run_id(job.idempotency_key),
                status="failed",
                exception_reason=reason,
            )
        await record_failure(
            redis,
            metadata_prefix=settings.INVOICE_METADATA_PREFIX,
            delivery=delivery,
            reason=reason,
        )
        return None
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat

    await acknowledge(
        redis,
        stream=settings.INVOICE_STREAM,
        group=settings.INVOICE_CONSUMER_GROUP,
        stream_id=delivery.stream_id,
    )
    logger.info(
        "Acknowledged invoice stream_id=%s attempt=%s queue_age_seconds=%.3f key=%s",
        delivery.stream_id,
        delivery.attempt,
        queue_age_seconds,
        job.idempotency_key,
    )
    return result


async def _process_batch(
    redis: Redis,
    deliveries: list[StreamDelivery],
    settings: Settings,
    consumer: str,
) -> list[dict[str, Any]]:
    results = await asyncio.gather(
        *(
            _process_delivery(redis, delivery, settings, consumer)
            for delivery in deliveries
        ),
        return_exceptions=True,
    )
    summaries: list[dict[str, Any]] = []
    for delivery, result in zip(deliveries, results, strict=True):
        if isinstance(result, BaseException):
            if isinstance(result, asyncio.CancelledError):
                raise result
            logger.error(
                "Invoice delivery boundary failed for %s: %s",
                delivery.stream_id,
                result,
            )
            continue
        if result is not None:
            summaries.append(result)
    return summaries


async def _flush_batch_summary(results: list[dict]) -> list[dict]:
    if not results:
        return results
    try:
        await send_batch_summary(results)
    except Exception:
        logger.exception("Failed to send invoice batch summary")
    return []


async def process_invoice_queue() -> None:
    settings = get_settings()
    consumer = f"{socket.gethostname()}:{os.getpid()}:{uuid4()}"
    redis = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_timeout=max(10, settings.INVOICE_READ_BLOCK_MS / 1000 + 5),
        socket_connect_timeout=5,
    )
    batch_results: list[dict] = []
    try:
        await ensure_consumer_group(
            redis,
            stream=settings.INVOICE_STREAM,
            group=settings.INVOICE_CONSUMER_GROUP,
        )
        logger.info("Invoice stream worker started as %s", consumer)
        while True:
            try:
                deliveries = await claim_stale_batch(
                    redis,
                    stream=settings.INVOICE_STREAM,
                    group=settings.INVOICE_CONSUMER_GROUP,
                    consumer=consumer,
                    min_idle_ms=settings.INVOICE_VISIBILITY_TIMEOUT_MS,
                    count=settings.MAX_BATCH_SIZE,
                )
                if not deliveries:
                    deliveries = await read_new_batch(
                        redis,
                        stream=settings.INVOICE_STREAM,
                        group=settings.INVOICE_CONSUMER_GROUP,
                        consumer=consumer,
                        count=settings.MAX_BATCH_SIZE,
                        block_ms=settings.INVOICE_READ_BLOCK_MS,
                    )
                if not deliveries:
                    batch_results = await _flush_batch_summary(batch_results)
                    continue
                batch_results.extend(
                    await _process_batch(redis, deliveries, settings, consumer)
                )
            except RedisTimeoutError:
                batch_results = await _flush_batch_summary(batch_results)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Invoice stream batch failed")
    finally:
        await redis.aclose()
