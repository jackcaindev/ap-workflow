"""Shared pytest fixtures for the freight AP workflow API."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from httpx import ASGITransport, AsyncClient
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

load_dotenv()

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@db:5432/freight_ap_test",
)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ["ANTHROPIC_API_KEY"] = os.environ.get("ANTHROPIC_API_KEY") or "test-key"

from alembic import command
from alembic.config import Config

from app.core.config import get_settings

get_settings.cache_clear()

import app.database as database

_settings = get_settings()
database.engine = create_async_engine(
    _settings.DATABASE_URL,
    pool_pre_ping=True,
    poolclass=NullPool,
)
database.AsyncSessionLocal = async_sessionmaker(
    bind=database.engine,
    expire_on_commit=False,
)

from fastapi import FastAPI

from app.api.routes.extraction import router as extraction_router
from app.api.routes.rate_confirmations import router as rate_confirmations_router
from app.api.routes.queue_operations import router as queue_operations_router
from app.api.routes.shipments import router as shipments_router
from app.api.routes.workflow import router as workflow_router
from app.models.document import Document
from app.models.rate_confirmation import RateConfirmation
from app.models.shipment import Shipment

test_app = FastAPI(title="Freight AP Workflow API", version="0.1.0")
test_app.include_router(extraction_router)
test_app.include_router(rate_confirmations_router)
test_app.include_router(queue_operations_router)
test_app.include_router(shipments_router)
test_app.include_router(workflow_router)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_INVOICE_PDF = FIXTURES_DIR / "sample_invoice.pdf"

TRUNCATE_TABLES_SQL = """
TRUNCATE TABLE
    workflow_audit_logs,
    review_decisions,
    reconciliation_results,
    workflow_runs,
    documents,
    shipments,
    rate_confirmations,
    notifications,
    checkpoint_writes,
    checkpoint_blobs,
    checkpoints
RESTART IDENTITY CASCADE
"""

SEED_RATE_CONFIRMATIONS = [
    {
        "load_number": "LD-1001",
        "carrier_name": "ACME FREIGHT",
        "origin": "Chicago, IL",
        "destination": "Dallas, TX",
        "agreed_rate": 1500.0,
        "shipment_date": date(2026, 6, 1),
    },
    {
        "load_number": "LD-2002",
        "carrier_name": "MIDWEST LOGISTICS",
        "origin": "Detroit, MI",
        "destination": "Atlanta, GA",
        "agreed_rate": 2200.0,
        "shipment_date": date(2026, 6, 10),
    },
]

SAMPLE_INVOICE_EXTRACTION = {
    "invoice_number": "INV-9001",
    "carrier_name": "ACME FREIGHT",
    "load_number": "LD-1001",
    "invoice_date": "2026-06-15",
    "total_amount": 1500.0,
    "line_items": [
        {
            "description": "Linehaul",
            "quantity": 1,
            "unit_price": 1500.0,
            "total": 1500.0,
        }
    ],
    "doc_type": "invoice",
    "confidence": 0.95,
}


def _sync_database_url(database_url: str) -> str:
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _ensure_test_database_exists() -> None:
    url = make_url(TEST_DATABASE_URL)
    database_name = url.database
    if not database_name:
        return

    admin_url = url.set(database="postgres")
    admin_engine = create_async_engine(
        admin_url.render_as_string(hide_password=False),
        isolation_level="AUTOCOMMIT",
    )
    try:
        async with admin_engine.connect() as connection:
            exists = await connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": database_name},
            )
            if exists is None:
                await connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    finally:
        await admin_engine.dispose()


def _run_alembic_migrations() -> None:
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")


async def _setup_langgraph_checkpoints() -> None:
    async with AsyncPostgresSaver.from_conn_string(_sync_database_url(TEST_DATABASE_URL)) as saver:
        await saver.setup()


async def _truncate_tables() -> None:
    async with database.engine.begin() as connection:
        await connection.execute(text(TRUNCATE_TABLES_SQL))


@pytest.fixture(scope="session")
def prepare_database():
    import asyncio

    asyncio.run(_ensure_test_database_exists())
    _run_alembic_migrations()
    asyncio.run(_setup_langgraph_checkpoints())
    # Session setup uses a separate event loop; dispose pooled connections so
    # pytest-asyncio tests open fresh ones on their own loop.
    asyncio.run(database.engine.dispose())
    yield


@pytest_asyncio.fixture(autouse=True)
async def clean_database(prepare_database):
    await _truncate_tables()
    yield


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    async with database.AsyncSessionLocal() as session:
        yield session


@pytest_asyncio.fixture
async def seeded_rate_confirmations(db_session: AsyncSession) -> list[RateConfirmation]:
    records = [
        RateConfirmation(
            load_number=item["load_number"],
            carrier_name=item["carrier_name"],
            origin=item["origin"],
            destination=item["destination"],
            agreed_rate=item["agreed_rate"],
            shipment_date=item["shipment_date"],
        )
        for item in SEED_RATE_CONFIRMATIONS
    ]
    db_session.add_all(records)
    await db_session.commit()
    for record in records:
        await db_session.refresh(record)
    return records


@pytest.fixture
def sample_invoice_pdf() -> Path:
    assert SAMPLE_INVOICE_PDF.exists(), f"Missing fixture PDF: {SAMPLE_INVOICE_PDF}"
    return SAMPLE_INVOICE_PDF


@pytest.fixture
def mock_claude(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    def _configure(
        *,
        doc_type: str,
        extraction: dict[str, Any],
        triage: dict[str, Any] | None = None,
    ) -> None:
        triage_response = triage or {
            "route": "escalate_priority",
            "reasoning": "Invoice amount exceeds agreed rate by more than 5%.",
            "confidence": 0.92,
        }

        async def mock_create(**kwargs: Any) -> SimpleNamespace:
            system_prompt = kwargs.get("system", "")
            if "triage" in system_prompt.lower():
                text_response = json.dumps(triage_response)
            elif "classify" in system_prompt.lower():
                text_response = doc_type
            else:
                text_response = json.dumps(extraction)

            return SimpleNamespace(
                content=[
                    SimpleNamespace(type="text", text=text_response),
                ]
            )

        mock_client = SimpleNamespace(messages=SimpleNamespace(create=mock_create))

        def _mock_anthropic(**_: Any) -> SimpleNamespace:
            return mock_client

        for module in ("app.services.extraction", "app.services.triage"):
            monkeypatch.setattr(f"{module}.AsyncAnthropic", _mock_anthropic)

    return _configure


@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client


async def create_document(
    db_session: AsyncSession,
    *,
    doc_type: str,
    extracted_data: dict[str, Any],
    filename: str | None = None,
) -> Document:
    document = Document(
        filename=filename or f"{doc_type}.json",
        doc_type=doc_type,
        status="extracted",
        extracted_data=extracted_data,
    )
    db_session.add(document)
    await db_session.flush()
    return document


async def create_shipment_with_documents(
    db_session: AsyncSession,
    *,
    load_number: str = "LD-1001",
    carrier_name: str = "ACME FREIGHT",
    include_invoice: bool = True,
    include_rate_con: bool = True,
    include_bol: bool = True,
    include_pod: bool = True,
    invoice_total: float = 1500.0,
    agreed_rate: float = 1500.0,
    bol_pickup_date: str = "2026-06-01",
    pod_delivery_date: str | None = "2026-06-03",
    pod_condition: str | None = "good",
    created_at: datetime | None = None,
    manual_rate_con_only: bool = False,
) -> Shipment:
    shipment = Shipment(
        load_number=load_number,
        carrier_name=carrier_name,
        reconciliation_status="pending",
    )
    if created_at is not None:
        shipment.created_at = created_at
        shipment.updated_at = created_at

    db_session.add(shipment)
    await db_session.flush()

    if include_invoice:
        invoice = await create_document(
            db_session,
            doc_type="invoice",
            filename="invoice.pdf",
            extracted_data={
                "invoice_number": "INV-1",
                "carrier_name": carrier_name,
                "load_number": load_number,
                "invoice_date": "2026-06-15",
                "total_amount": invoice_total,
                "line_items": [],
                "doc_type": "invoice",
                "confidence": 0.9,
            },
        )
        shipment.invoice_id = invoice.id
        shipment.has_invoice = True
        invoice.shipment_id = shipment.id

    if include_rate_con and not manual_rate_con_only:
        rate_con = await create_document(
            db_session,
            doc_type="rate_confirmation",
            filename="rate_con.pdf",
            extracted_data={
                "carrier_name": carrier_name,
                "load_number": load_number,
                "agreed_rate": agreed_rate,
                "shipment_date": bol_pickup_date,
                "doc_type": "rate_confirmation",
            },
        )
        shipment.rate_con_id = rate_con.id
        shipment.has_rate_con = True
        rate_con.shipment_id = shipment.id
    elif include_rate_con and manual_rate_con_only:
        shipment.has_rate_con = True

    if include_bol:
        bol = await create_document(
            db_session,
            doc_type="bill_of_lading",
            filename="bol.pdf",
            extracted_data={
                "bol_number": "BOL-1",
                "load_number": load_number,
                "carrier_name": carrier_name,
                "shipper_name": "Shipper",
                "consignee_name": "Consignee",
                "pickup_date": bol_pickup_date,
                "commodity_description": "General freight",
                "pieces": 10,
                "weight_lbs": 12000.0,
                "doc_type": "bill_of_lading",
            },
        )
        shipment.bol_id = bol.id
        shipment.has_bol = True
        bol.shipment_id = shipment.id

    if include_pod:
        pod = await create_document(
            db_session,
            doc_type="proof_of_delivery",
            filename="pod.pdf",
            extracted_data={
                "bol_number": "BOL-1",
                "load_number": load_number,
                "carrier_name": carrier_name,
                "delivery_date": pod_delivery_date,
                "delivery_time": "14:30",
                "pieces_received": 10,
                "condition": pod_condition,
                "receiver_name": "Receiver",
                "doc_type": "proof_of_delivery",
            },
        )
        shipment.pod_id = pod.id
        shipment.has_pod = True
        pod.shipment_id = shipment.id

    await db_session.commit()
    await db_session.refresh(shipment)
    return shipment


def old_shipment_timestamp() -> datetime:
    return datetime.now(UTC) - timedelta(days=5)
