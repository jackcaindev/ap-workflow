import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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


logger = logging.getLogger(__name__)


async def gmail_poll_scheduler() -> None:
    while True:
        try:
            await poll_inbox()
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

        await asyncio.sleep(300)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # The poller and worker are decoupled by Redis: polling is bursty and bound
    # by Gmail API latency, while invoice processing is slower and Claude/DB
    # bound. Separate tasks keep each concern from blocking the other.
    gmail_poll_task = asyncio.create_task(gmail_poll_scheduler())
    invoice_worker_task = asyncio.create_task(process_invoice_queue())
    try:
        yield
    finally:
        for task in (gmail_poll_task, invoice_worker_task):
            task.cancel()
        for task in (gmail_poll_task, invoice_worker_task):
            with suppress(asyncio.CancelledError):
                await task
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
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
