from __future__ import annotations

import argparse
import asyncio
import json
import os
from types import SimpleNamespace
from typing import NoReturn

from redis.asyncio import Redis


RECEIVED_EXIT = 85
PRE_ACK_EXIT = 86
INTERRUPTED_EXIT = 87


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=("receive-exit", "process-exit-before-ack", "interrupt-exit", "resume"),
    )
    parser.add_argument("--stream")
    parser.add_argument("--group")
    parser.add_argument("--consumer")
    parser.add_argument("--dead-letter-stream")
    parser.add_argument("--metadata-prefix")
    parser.add_argument("--job-json")
    parser.add_argument("--load-number")
    parser.add_argument("--run-id")
    parser.add_argument("--decision", choices=("approved", "rejected"))
    return parser


def _crash(exit_code: int) -> NoReturn:
    os._exit(exit_code)


def _install_deterministic_nodes(load_number: str) -> None:
    from app.models.document import Document
    from app.schemas.extraction import InvoiceExtraction
    from app.schemas.triage import TriageDecision
    from app.workflow import nodes

    async def deterministic_extract(
        _file_bytes: bytes,
        filename: str,
        db,
        document_id: int | None = None,
    ) -> InvoiceExtraction:
        extraction = InvoiceExtraction(
            invoice_number=f"INV-{load_number}",
            carrier_name="ACME FREIGHT",
            load_number=load_number,
            invoice_date="2026-07-19",
            total_amount=3000.0,
            line_items=[],
            doc_type="invoice",
            confidence=1.0,
        )
        document = await db.get(Document, document_id)
        if document is None:
            raise AssertionError(f"Missing integration document {document_id}")
        document.filename = filename
        document.doc_type = "invoice"
        document.status = "extracted"
        document.extracted_data = extraction.model_dump(mode="json")
        await db.commit()
        return extraction

    async def deterministic_triage(**_kwargs) -> TriageDecision:
        return TriageDecision(
            route="escalate_priority",
            reasoning="Deterministic integration-test variance.",
            confidence=1.0,
        )

    nodes.extract_document = deterministic_extract
    nodes.triage_exception = deterministic_triage


def _settings(args: argparse.Namespace) -> SimpleNamespace:
    return SimpleNamespace(
        INVOICE_STREAM=args.stream,
        INVOICE_CONSUMER_GROUP=args.group,
        INVOICE_DEAD_LETTER_STREAM=args.dead_letter_stream,
        INVOICE_METADATA_PREFIX=args.metadata_prefix,
        INVOICE_MAX_ATTEMPTS=3,
        INVOICE_VISIBILITY_TIMEOUT_MS=100,
    )


async def _receive_exit(args: argparse.Namespace) -> NoReturn:
    from app.services.invoice_queue import read_new_batch

    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    deliveries = await read_new_batch(
        redis,
        stream=args.stream,
        group=args.group,
        consumer=args.consumer,
        count=1,
        block_ms=1000,
    )
    if len(deliveries) != 1:
        raise AssertionError(f"Expected one delivery, received {deliveries!r}")
    _crash(RECEIVED_EXIT)


async def _process_exit_before_ack(args: argparse.Namespace) -> NoReturn:
    from app.services import invoice_worker
    from app.services.invoice_queue import read_new_batch

    if not args.load_number:
        raise AssertionError("--load-number is required")
    _install_deterministic_nodes(args.load_number)
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    deliveries = await read_new_batch(
        redis,
        stream=args.stream,
        group=args.group,
        consumer=args.consumer,
        count=1,
        block_ms=1000,
    )
    if len(deliveries) != 1:
        raise AssertionError(f"Expected one delivery, received {deliveries!r}")

    async def crash_before_ack(*_args, **_kwargs) -> NoReturn:
        _crash(PRE_ACK_EXIT)

    invoice_worker.acknowledge = crash_before_ack
    await invoice_worker._process_delivery(
        redis,
        deliveries[0],
        _settings(args),
        args.consumer,
    )
    raise AssertionError("Worker returned instead of crashing before ACK")


async def _interrupt_exit(args: argparse.Namespace) -> NoReturn:
    from app.schemas.invoice_job import InvoiceJobEnvelope
    from app.services.invoice_worker import _run_workflow_for_job

    if not args.load_number or not args.job_json:
        raise AssertionError("--load-number and --job-json are required")
    _install_deterministic_nodes(args.load_number)
    job = InvoiceJobEnvelope.model_validate(json.loads(args.job_json))
    result = await _run_workflow_for_job(job)
    if result["processing_status"] != "awaiting_review":
        raise AssertionError(f"Workflow did not interrupt: {result!r}")
    _crash(INTERRUPTED_EXIT)


async def _resume(args: argparse.Namespace) -> None:
    from app.database import AsyncSessionLocal
    from app.services.business_state import decide_review

    if not args.run_id or not args.decision:
        raise AssertionError("--run-id and --decision are required")
    async with AsyncSessionLocal() as db:
        run, decision, idempotent = await decide_review(db, args.run_id, args.decision)
        if run is None or decision is None or idempotent:
            raise AssertionError("Fresh process did not commit the first review decision")


async def _main() -> None:
    args = _parser().parse_args()
    if args.mode == "receive-exit":
        await _receive_exit(args)
    elif args.mode == "process-exit-before-ack":
        await _process_exit_before_ack(args)
    elif args.mode == "interrupt-exit":
        await _interrupt_exit(args)
    else:
        await _resume(args)


if __name__ == "__main__":
    asyncio.run(_main())
