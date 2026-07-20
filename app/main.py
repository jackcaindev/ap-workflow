import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes.extraction import router as extraction_router
from app.api.routes.gmail import router as gmail_router
from app.api.routes.notifications import router as notifications_router
from app.api.routes.queue_operations import router as queue_operations_router
from app.api.routes.rate_confirmations import router as rate_confirmations_router
from app.api.routes.shipments import router as shipments_router
from app.api.routes.workflow import router as workflow_router
from app.database import dispose_engine
from app.services.gmail_poller import poll_inbox
from app.services.invoice_worker import process_invoice_queue
from app.core.config import get_settings
from app.services.missing_document_sla import ScannerHealth, run_scanner_iteration
from app.services.notifier import dispatch_pending_sla_notifications
from app.schemas.health import LivenessResponse, ProcessPhase, ReadinessResponse
from app.services.health import (
    RuntimeHealth,
    liveness,
    readiness,
    reason_code_for_exception,
    utc_now,
)


logger = logging.getLogger(__name__)


async def gmail_poll_scheduler(runtime_health: RuntimeHealth) -> None:
    while True:
        try:
            await poll_inbox(runtime_health)
        except asyncio.CancelledError:
            raise
        except RuntimeError as exc:
            if str(exc) == "No Gmail token found — run /gmail/auth first":
                logger.info("Gmail not authenticated yet, skipping poll")
            else:
                logger.exception("Scheduled Gmail poll failed")
        except Exception:
            # Gmail credentials may not exist in every local/dev environment.
            # Logging and continuing keeps the API alive while making the
            # ingestion problem visible.
            logger.exception("Scheduled Gmail poll failed")

        await asyncio.sleep(get_settings().GMAIL_POLL_INTERVAL_SECONDS)


async def missing_document_sla_scheduler(
    app: FastAPI, runtime_health: RuntimeHealth
) -> None:
    settings = get_settings()
    sla_duration = timedelta(hours=settings.MISSING_DOCUMENT_SLA_HOURS)
    interval = settings.MISSING_DOCUMENT_SCAN_INTERVAL_SECONDS
    health: ScannerHealth = app.state.missing_document_sla_scanner
    while True:
        now = datetime.now(UTC)
        runtime_health.sla_scanner.started(now)
        succeeded = await run_scanner_iteration(
            health,
            now=now,
            sla_duration=sla_duration,
        )
        if succeeded:
            runtime_health.sla_scanner.succeeded(
                utc_now(), health.last_result_count
            )
            runtime_health.notifications.started(utc_now())
            try:
                sent_count = await dispatch_pending_sla_notifications(runtime_health)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                runtime_health.notifications.failed(
                    utc_now(), reason_code_for_exception(exc)
                )
                logger.exception("Scheduled shipment SLA notification dispatch failed")
            else:
                if runtime_health.notifications.in_progress:
                    runtime_health.notifications.succeeded(utc_now(), sent_count)
        else:
            runtime_health.sla_scanner.failed(
                utc_now(), health.last_error or "scan_failed"
            )
        await asyncio.sleep(interval)


def _record_unexpected_task_exit(
    task: asyncio.Task,
    *,
    runtime_health: RuntimeHealth,
    component: str,
) -> None:
    if runtime_health.phase is not ProcessPhase.RUNNING or task.cancelled():
        return
    task.exception()
    if component == "invoice_worker":
        runtime_health.worker_failed()
    else:
        outcome = getattr(runtime_health, component)
        outcome.failed(utc_now(), "background_task_exited")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # The poller and worker are decoupled by Redis: polling is bursty and bound
    # by Gmail API latency, while invoice processing is slower and Claude/DB
    # bound. Separate tasks keep each concern from blocking the other.
    runtime_health = RuntimeHealth()
    app.state.runtime_health = runtime_health
    gmail_poll_task = asyncio.create_task(gmail_poll_scheduler(runtime_health))
    invoice_worker_task = asyncio.create_task(process_invoice_queue(runtime_health))
    app.state.missing_document_sla_scanner = ScannerHealth()
    missing_document_sla_task = asyncio.create_task(
        missing_document_sla_scheduler(app, runtime_health)
    )
    gmail_poll_task.add_done_callback(
        lambda task: _record_unexpected_task_exit(
            task, runtime_health=runtime_health, component="gmail"
        )
    )
    invoice_worker_task.add_done_callback(
        lambda task: _record_unexpected_task_exit(
            task, runtime_health=runtime_health, component="invoice_worker"
        )
    )
    missing_document_sla_task.add_done_callback(
        lambda task: _record_unexpected_task_exit(
            task, runtime_health=runtime_health, component="sla_scanner"
        )
    )
    runtime_health.phase = ProcessPhase.RUNNING
    try:
        yield
    finally:
        runtime_health.phase = ProcessPhase.STOPPING
        for task in (gmail_poll_task, invoice_worker_task, missing_document_sla_task):
            task.cancel()
        await asyncio.gather(
            gmail_poll_task,
            invoice_worker_task,
            missing_document_sla_task,
            return_exceptions=True,
        )
        await dispose_engine()


app = FastAPI(
    title="Freight AP Workflow API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(extraction_router)
app.include_router(gmail_router)
app.include_router(notifications_router)
app.include_router(queue_operations_router)
app.include_router(rate_confirmations_router)
app.include_router(shipments_router)
app.include_router(workflow_router)


@app.get("/health", tags=["system"])
async def health_check() -> dict:
    health = getattr(app.state, "missing_document_sla_scanner", ScannerHealth())
    return {
        "status": "ok",
        "missing_document_sla_scanner": health.payload(),
    }


@app.get("/health/live", response_model=LivenessResponse, tags=["system"])
async def health_live() -> LivenessResponse:
    runtime = getattr(app.state, "runtime_health", RuntimeHealth())
    return liveness(runtime)


@app.get("/health/ready", response_model=ReadinessResponse, tags=["system"])
async def health_ready() -> ReadinessResponse | JSONResponse:
    runtime = getattr(app.state, "runtime_health", RuntimeHealth())
    payload = await readiness(runtime, get_settings())
    if not payload.ready:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=payload.model_dump(mode="json"),
        )
    return payload
