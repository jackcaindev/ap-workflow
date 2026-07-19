import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.models.reconciliation_result import ReconciliationResult
from app.models.shipment_exception import ShipmentException, ShipmentExceptionEvent
from app.services.missing_document_sla import claim_and_evaluate_one
from tests.conftest import create_shipment_with_documents


pytestmark = pytest.mark.integration

NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)
SLA = timedelta(hours=72)


async def test_concurrent_scanners_claim_one_shipment_once(db_session):
    await create_shipment_with_documents(
        db_session,
        load_number="CONCURRENT-SLA",
        include_pod=False,
        created_at=NOW - timedelta(days=4),
    )

    claimed = await asyncio.gather(
        claim_and_evaluate_one(now=NOW, sla_duration=SLA),
        claim_and_evaluate_one(now=NOW, sla_duration=SLA),
    )

    assert sum(claimed) == 1
    assert await db_session.scalar(select(func.count(ShipmentException.id))) == 1
    assert await db_session.scalar(select(func.count(ShipmentExceptionEvent.id))) == 1
    assert await db_session.scalar(
        select(func.count(ReconciliationResult.id)).where(
            ReconciliationResult.evaluation_source == "scheduled_sla"
        )
    ) == 1


async def test_different_shipments_can_be_claimed_concurrently(db_session):
    for load_number in ("SLA-A", "SLA-B"):
        await create_shipment_with_documents(
            db_session,
            load_number=load_number,
            include_pod=False,
            created_at=NOW - timedelta(days=4),
        )

    claimed = await asyncio.gather(
        claim_and_evaluate_one(now=NOW, sla_duration=SLA),
        claim_and_evaluate_one(now=NOW, sla_duration=SLA),
    )

    assert claimed == [True, True]
    assert await db_session.scalar(select(func.count(ShipmentException.id))) == 2
