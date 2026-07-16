import base64
import asyncio
import json
import logging
from typing import Any
from uuid import uuid4

from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.asyncio import Redis

from app.core.config import get_settings
from app.database import AsyncSessionLocal
from app.models.document import Document
from app.models.workflow_run import WorkflowRun
from app.services.extraction import ExtractionError
from app.services.gmail_poller import INVOICE_QUEUE
from app.services.notifier import send_batch_summary
from app.workflow.batch_graph import batch_graph
from app.workflow.graph import workflow_graph


logger = logging.getLogger(__name__)


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
    payload: dict | None = None,
) -> None:
    run.status = status_value
    run.interrupt_payload = payload


def _summary_result(
    *,
    filename: str,
    run_id: str,
    status: str,
    extraction: dict[str, Any] | None = None,
    exception_reason: str | None = None,
) -> dict[str, Any]:
    extraction = extraction or {}
    return {
        "filename": filename,
        "run_id": run_id,
        "status": status,
        "carrier_name": extraction.get("carrier_name"),
        "total_amount": extraction.get("total_amount"),
        "exception_reason": exception_reason,
    }


async def _run_workflow_for_job(job: dict) -> dict[str, Any]:
    file_bytes = base64.b64decode(job["file_bytes"])
    filename = job["filename"]
    run_id = str(uuid4())

    async with AsyncSessionLocal() as db:
        document = Document(
            filename=filename,
            doc_type="unknown",
            status="received",
        )
        db.add(document)
        await db.flush()

        run = WorkflowRun(
            run_id=run_id,
            document_id=document.id,
            status="running",
            interrupt_payload={
                "gmail_message_id": job.get("message_id"),
                "gmail_thread_id": job.get("gmail_thread_id"),
            },
        )
        db.add(run)
        await db.commit()

        initial_state = {
            "run_id": run_id,
            "document_id": str(document.id),
            "file_bytes": file_bytes,
            "filename": filename,
            "extraction": None,
            "match_result": None,
            "exception_reason": None,
            "triage_route": None,
            "triage_reasoning": None,
            "triage_confidence": None,
            "human_decision": None,
            "status": "running",
            "messages": [f"Queued from Gmail message {job.get('message_id')}."],
            "iteration_count": 0,
        }

        try:
            # The worker is async, so it uses the graph's async invocation method
            # even though the conceptual operation is the same workflow invoke
            # used by the HTTP route.
            result = await workflow_graph.ainvoke(initial_state, config=graph_config(run_id))
        except ExtractionError as exc:
            logger.warning("Extraction failed for Gmail job %s: %s", run_id, exc)
            await persist_run_state(
                run,
                status_value="failed",
                payload={"error": str(exc), **(run.interrupt_payload or {})},
            )
            await db.commit()
            return _summary_result(
                filename=filename,
                run_id=run_id,
                status="failed",
                exception_reason=str(exc),
            )
        except Exception as exc:
            logger.exception("Workflow failed for Gmail job %s", run_id)
            await persist_run_state(
                run,
                status_value="failed",
                payload={"error": str(exc), **(run.interrupt_payload or {})},
            )
            await db.commit()
            return _summary_result(
                filename=filename,
                run_id=run_id,
                status="failed",
                exception_reason=str(exc),
            )

        payload = interrupt_payload(result)
        if payload is not None:
            payload.update(run.interrupt_payload or {})
            await persist_run_state(run, status_value="awaiting_review", payload=payload)
            await db.commit()
            return _summary_result(
                filename=filename,
                run_id=run_id,
                status="awaiting_review",
                extraction=result.get("extraction"),
                exception_reason=result.get("exception_reason") or payload.get("reason"),
            )

        final_status = result.get("status", "complete")
        await persist_run_state(run, status_value=final_status)
        await db.commit()
        return _summary_result(
            filename=filename,
            run_id=run_id,
            status=final_status,
            extraction=result.get("extraction"),
            exception_reason=result.get("exception_reason"),
        )


async def _flush_batch_summary(results: list[dict]) -> list[dict]:
    if not results:
        return results

    try:
        await send_batch_summary(results)
    except Exception:
        logger.exception("Failed to send invoice batch summary")
    return []


async def _drain_batch(
    redis: Redis,
    max_batch_size: int,
    wait_timeout: int,
) -> list[str]:
    # Block only for the first job so an idle worker is efficient, then drain
    # immediately available work without waiting between documents.
    item = await redis.brpop(INVOICE_QUEUE, timeout=wait_timeout)
    logger.info("BRPOP returned: %s", item is not None)
    if item is None:
        return []

    _, raw_payload = item
    payloads = [raw_payload]
    # Bounding the non-blocking drain bounds the Send fan-out, protecting the
    # Claude API and database connection capacity during inbox catch-up.
    for _ in range(max(0, max_batch_size - 1)):
        raw_payload = await redis.rpop(INVOICE_QUEUE)
        if raw_payload is None:
            break
        payloads.append(raw_payload)
    return payloads


async def process_invoice_queue() -> None:
    settings = get_settings()
    redis = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        # socket_timeout must exceed BRPOP's timeout, otherwise the client can
        # give up before Redis returns the normal empty-queue result.
        socket_timeout=10,
        socket_connect_timeout=5,
    )

    try:
        logger.info("Invoice worker started")
        batch_results: list[dict] = []
        while True:
            try:
                raw_payloads = await _drain_batch(
                    redis,
                    settings.MAX_BATCH_SIZE,
                    wait_timeout=5,
                )
                if not raw_payloads:
                    # The worker sends summaries when the queue drains because
                    # only completed jobs have extraction/matching results. A
                    # batch email also prevents one notification per invoice
                    # during a large inbox catch-up.
                    batch_results = await _flush_batch_summary(batch_results)
                    continue

                jobs: list[dict] = []
                for raw_payload in raw_payloads:
                    try:
                        job = json.loads(raw_payload)
                        if not isinstance(job, dict):
                            raise TypeError(
                                "Invoice queue payload must be a JSON object"
                            )
                        jobs.append(job)
                    except Exception as exc:
                        logger.exception("Invalid invoice queue payload")
                        batch_results.append(
                            _summary_result(
                                filename="unknown",
                                run_id="",
                                status="failed",
                                exception_reason=str(exc),
                            )
                        )

                if jobs:
                    result = await batch_graph.ainvoke(
                        {"jobs": jobs, "batch_results": []}
                    )
                    batch_results.extend(result["batch_results"])
            except RedisTimeoutError:
                batch_results = await _flush_batch_summary(batch_results)
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                # Per-job failures are converted to branch results. This final
                # boundary keeps batch-graph plumbing bugs from killing the
                # long-running background worker.
                logger.exception("Invoice queue batch failed")
    finally:
        await redis.aclose()
