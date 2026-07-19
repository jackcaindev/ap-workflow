from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from app.database import AsyncSessionLocal
from app.models.document import Document
from app.models.reconciliation_result import ReconciliationResult
from app.models.shipment import Shipment
from app.models.workflow_run import WorkflowRun
from app.services.reconciliation import reconcile_shipment
from app.services.shipment import upsert_shipment


pytestmark = pytest.mark.integration


async def _create_document_and_run(
    db_session,
    *,
    doc_type: str,
    load_number: str,
    suffix: str,
) -> tuple[int, str, dict[str, Any]]:
    if doc_type == "invoice":
        extraction = {
            "invoice_number": f"INV-{suffix}",
            "carrier_name": "ACME FREIGHT",
            "load_number": load_number,
            "invoice_date": "2026-07-19",
            "total_amount": 1500.0,
            "line_items": [],
            "doc_type": "invoice",
            "confidence": 1.0,
        }
    elif doc_type == "bill_of_lading":
        extraction = {
            "bol_number": f"BOL-{suffix}",
            "load_number": load_number,
            "carrier_name": "ACME FREIGHT",
            "shipper_name": "Shipper",
            "consignee_name": "Consignee",
            "pickup_date": "2026-07-19",
            "commodity_description": "General freight",
            "pieces": 10,
            "weight_lbs": 12000.0,
            "doc_type": "bill_of_lading",
        }
    else:
        raise AssertionError(f"Unsupported test document type: {doc_type}")

    document = Document(
        filename=f"{suffix}.json",
        doc_type=doc_type,
        status="extracted",
        extracted_data=extraction,
    )
    db_session.add(document)
    await db_session.flush()
    run_id = str(uuid4())
    db_session.add(
        WorkflowRun(
            run_id=run_id,
            document_id=document.id,
            status="running",
            processing_status="running",
            posting_status="not_ready",
        )
    )
    await db_session.commit()
    return document.id, run_id, extraction


async def _attach_and_reconcile(
    *,
    document_id: int,
    run_id: str,
    extraction: dict[str, Any],
    pid_ready: asyncio.Future[int] | None = None,
) -> None:
    async with AsyncSessionLocal() as db:
        pid = await db.scalar(text("SELECT pg_backend_pid()"))
        if pid_ready is not None and not pid_ready.done():
            pid_ready.set_result(pid)
        document = await db.get(Document, document_id)
        assert document is not None
        shipment = await upsert_shipment(
            extraction["load_number"], document, extraction, db
        )
        assert shipment is not None
        await reconcile_shipment(shipment, db, run_id=run_id)


async def _backend_is_waiting_on_lock(pid: int) -> bool:
    async with AsyncSessionLocal() as observer:
        return bool(
            await observer.scalar(
                text(
                    "SELECT wait_event_type = 'Lock' "
                    "FROM pg_stat_activity WHERE pid = :pid"
                ),
                {"pid": pid},
            )
        )


async def test_concurrent_different_types_share_one_new_shipment(
    db_session,
    wait_for_condition,
):
    invoice_id, invoice_run_id, invoice_extraction = await _create_document_and_run(
        db_session,
        doc_type="invoice",
        load_number="  concurrent-new  ",
        suffix="invoice-first",
    )
    bol_id, bol_run_id, bol_extraction = await _create_document_and_run(
        db_session,
        doc_type="bill_of_lading",
        load_number="CONCURRENT-NEW",
        suffix="bol-second",
    )

    invoice = await db_session.get(Document, invoice_id)
    assert invoice is not None
    shipment = await upsert_shipment(
        invoice_extraction["load_number"], invoice, invoice_extraction, db_session
    )
    assert shipment is not None

    loop = asyncio.get_running_loop()
    pid_ready: asyncio.Future[int] = loop.create_future()
    bol_task = asyncio.create_task(
        _attach_and_reconcile(
            document_id=bol_id,
            run_id=bol_run_id,
            extraction=bol_extraction,
            pid_ready=pid_ready,
        )
    )
    bol_pid = await pid_ready
    try:
        await wait_for_condition(
            lambda: _backend_is_waiting_on_lock(bol_pid),
            description="BOL assembly waiting for the new shipment transaction",
        )
        await reconcile_shipment(shipment, db_session, run_id=invoice_run_id)
        await bol_task
    finally:
        if not bol_task.done():
            await db_session.rollback()
            bol_task.cancel()
            await asyncio.gather(bol_task, return_exceptions=True)

    async with AsyncSessionLocal() as verification_db:
        shipments = list(
            (
                await verification_db.scalars(
                    select(Shipment).where(Shipment.load_number == "CONCURRENT-NEW")
                )
            ).all()
        )
        assert len(shipments) == 1
        final_shipment = shipments[0]
        assert final_shipment.invoice_id == invoice_id
        assert final_shipment.bol_id == bol_id
        assert final_shipment.has_invoice is True
        assert final_shipment.has_bol is True

        attached_ids = set(
            (
                await verification_db.scalars(
                    select(Document.id).where(
                        Document.shipment_id == final_shipment.id
                    )
                )
            ).all()
        )
        assert {invoice_id, bol_id} <= attached_ids

        final_result = await verification_db.scalar(
            select(ReconciliationResult).where(
                ReconciliationResult.run_id == bol_run_id
            )
        )
        assert final_result is not None
        assert "invoice" not in final_result.missing_docs
        assert "bol" not in final_result.missing_docs


async def test_concurrent_same_type_preserves_first_canonical_document(
    db_session,
    wait_for_condition,
):
    load_number = "SAME-TYPE"
    first_id, first_run_id, first_extraction = await _create_document_and_run(
        db_session,
        doc_type="invoice",
        load_number=load_number,
        suffix="canonical",
    )
    second_id, second_run_id, second_extraction = await _create_document_and_run(
        db_session,
        doc_type="invoice",
        load_number=load_number,
        suffix="noncanonical",
    )
    db_session.add(Shipment(load_number=load_number, reconciliation_status="pending"))
    await db_session.commit()

    first = await db_session.get(Document, first_id)
    assert first is not None
    shipment = await upsert_shipment(load_number, first, first_extraction, db_session)
    assert shipment is not None

    loop = asyncio.get_running_loop()
    pid_ready: asyncio.Future[int] = loop.create_future()
    second_task = asyncio.create_task(
        _attach_and_reconcile(
            document_id=second_id,
            run_id=second_run_id,
            extraction=second_extraction,
            pid_ready=pid_ready,
        )
    )
    second_pid = await pid_ready
    try:
        await wait_for_condition(
            lambda: _backend_is_waiting_on_lock(second_pid),
            description="second invoice waiting for the shipment row",
        )
        await reconcile_shipment(shipment, db_session, run_id=first_run_id)
        await second_task
    finally:
        if not second_task.done():
            await db_session.rollback()
            second_task.cancel()
            await asyncio.gather(second_task, return_exceptions=True)

    async with AsyncSessionLocal() as verification_db:
        final_shipment = await verification_db.scalar(
            select(Shipment).where(Shipment.load_number == load_number)
        )
        assert final_shipment is not None
        assert final_shipment.invoice_id == first_id
        assert set(
            (
                await verification_db.scalars(
                    select(Document.id).where(
                        Document.shipment_id == final_shipment.id
                    )
                )
            ).all()
        ) == {first_id, second_id}


async def test_uncommitted_shipment_does_not_block_a_different_load(db_session):
    first_id, first_run_id, first_extraction = await _create_document_and_run(
        db_session,
        doc_type="invoice",
        load_number="LOAD-A",
        suffix="load-a",
    )
    second_id, second_run_id, second_extraction = await _create_document_and_run(
        db_session,
        doc_type="invoice",
        load_number="LOAD-B",
        suffix="load-b",
    )

    first = await db_session.get(Document, first_id)
    assert first is not None
    first_shipment = await upsert_shipment(
        "LOAD-A", first, first_extraction, db_session
    )
    assert first_shipment is not None

    await asyncio.wait_for(
        _attach_and_reconcile(
            document_id=second_id,
            run_id=second_run_id,
            extraction=second_extraction,
        ),
        timeout=2,
    )
    await reconcile_shipment(first_shipment, db_session, run_id=first_run_id)

    async with AsyncSessionLocal() as verification_db:
        assert await verification_db.scalar(select(func.count(Shipment.id))) == 2


async def test_retried_document_is_idempotent(db_session):
    document_id, run_id, extraction = await _create_document_and_run(
        db_session,
        doc_type="invoice",
        load_number="RETRY-LOAD",
        suffix="retry",
    )

    await _attach_and_reconcile(
        document_id=document_id,
        run_id=run_id,
        extraction=extraction,
    )
    await _attach_and_reconcile(
        document_id=document_id,
        run_id=run_id,
        extraction=extraction,
    )

    async with AsyncSessionLocal() as verification_db:
        shipment = await verification_db.scalar(
            select(Shipment).where(Shipment.load_number == "RETRY-LOAD")
        )
        assert shipment is not None
        assert shipment.invoice_id == document_id
        assert await verification_db.scalar(select(func.count(Shipment.id))) == 1
        assert await verification_db.scalar(
            select(func.count(ReconciliationResult.id)).where(
                ReconciliationResult.run_id == run_id
            )
        ) == 1
