import asyncio
import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from app.database import AsyncSessionLocal
from app.models.document import Document
from app.models.review_decision import ReviewDecision
from app.models.workflow_run import WorkflowRun
from app.schemas.invoice_job import InvoiceJobEnvelope
from app.services import invoice_worker
from app.services.invoice_queue import StreamDelivery, claim_stale_batch


class FakePipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.commands: list[tuple] = []

    async def __aenter__(self) -> "FakePipeline":
        return self

    async def __aexit__(self, *_args) -> None:
        return None

    def xadd(self, stream: str, fields: dict) -> None:
        self.commands.append(("xadd", stream, fields))

    def xack(self, stream: str, group: str, stream_id: str) -> None:
        self.commands.append(("xack", stream, group, stream_id))

    def xdel(self, stream: str, stream_id: str) -> None:
        self.commands.append(("xdel", stream, stream_id))

    def hset(self, key: str, mapping: dict) -> None:
        self.commands.append(("hset", key, mapping))

    async def execute(self) -> list[int]:
        self.redis.operations.extend(self.commands)
        return [1] * len(self.commands)


class FakeRedis:
    def __init__(self) -> None:
        self.operations: list[tuple] = []
        self.claimed_entries: list[tuple[str, dict[str, str]]] = []
        self.pending_attempt = 2

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        assert transaction is True
        return FakePipeline(self)

    async def hset(self, key: str, mapping: dict) -> int:
        self.operations.append(("hset", key, mapping))
        return 1

    async def xclaim(self, *_args, **_kwargs) -> list[str]:
        self.operations.append(("heartbeat",))
        return []

    async def xautoclaim(self, *_args, **_kwargs):
        return ["0-0", self.claimed_entries, []]

    async def xpending_range(self, *_args, **_kwargs):
        return [{"times_delivered": self.pending_attempt}]


def settings(**overrides):
    values = {
        "INVOICE_STREAM": "jobs",
        "INVOICE_CONSUMER_GROUP": "workers",
        "INVOICE_DEAD_LETTER_STREAM": "jobs-dlq",
        "INVOICE_METADATA_PREFIX": "job-meta",
        "INVOICE_DLQ_REPLAY_PREFIX": "job-replays",
        "INVOICE_MAX_ATTEMPTS": 3,
        "INVOICE_VISIBILITY_TIMEOUT_MS": 300_000,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def make_job(filename: str = "invoice.pdf", suffix: str = "a") -> InvoiceJobEnvelope:
    content = f"content-{suffix}".encode()
    import base64

    return InvoiceJobEnvelope(
        idempotency_key=hashlib.sha256(f"key-{suffix}".encode()).hexdigest(),
        gmail_account="ap@example.com",
        message_id=f"message-{suffix}",
        gmail_thread_id=f"thread-{suffix}",
        mime_part_id=f"part-{suffix}",
        filename=filename,
        file_bytes=base64.b64encode(content).decode(),
        content_sha256=hashlib.sha256(content).hexdigest(),
        enqueued_at=datetime.now(UTC),
    )


def delivery(job: InvoiceJobEnvelope, stream_id: str, attempt: int = 1) -> StreamDelivery:
    return StreamDelivery(
        stream_id=stream_id,
        fields={"payload": job.model_dump_json()},
        attempt=attempt,
    )


async def test_success_is_acknowledged_only_after_processing(monkeypatch):
    redis = FakeRedis()
    events: list[str] = []

    async def fake_run(job: InvoiceJobEnvelope) -> dict:
        events.append("processed")
        return invoice_worker._summary_result(
            filename=job.filename, run_id="run-1", status="complete"
        )

    async def fake_acknowledge(_redis, **_kwargs) -> None:
        events.append("acknowledged")

    monkeypatch.setattr(invoice_worker, "_run_workflow_for_job", fake_run)
    monkeypatch.setattr(invoice_worker, "acknowledge", fake_acknowledge)

    result = await invoice_worker._process_delivery(
        redis, delivery(make_job(), "1-0"), settings(), "consumer-a"
    )

    assert result["status"] == "complete"
    assert events == ["processed", "acknowledged"]


async def test_worker_crash_leaves_delivery_recoverable(monkeypatch):
    redis = FakeRedis()
    started = asyncio.Event()

    async def crash_point(_job: InvoiceJobEnvelope) -> dict:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(invoice_worker, "_run_workflow_for_job", crash_point)
    original = delivery(make_job(), "2-0")
    task = asyncio.create_task(
        invoice_worker._process_delivery(redis, original, settings(), "consumer-a")
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not any(op[0] in {"xack", "xdel", "xadd"} for op in redis.operations)
    redis.claimed_entries = [(original.stream_id, original.fields)]
    reclaimed = await claim_stale_batch(
        redis,
        stream="jobs",
        group="workers",
        consumer="consumer-b",
        min_idle_ms=1,
        count=10,
    )
    assert reclaimed == [StreamDelivery("2-0", original.fields, attempt=2)]


async def test_redelivery_does_not_duplicate_document_or_workflow_run():
    job = make_job(suffix="redelivery")

    first_document, first_run = await invoice_worker._get_or_create_job_records(job)
    second_document, second_run = await invoice_worker._get_or_create_job_records(job)

    assert second_document.id == first_document.id
    assert second_run.run_id == first_run.run_id
    async with AsyncSessionLocal() as db:
        document_count = await db.scalar(
            select(func.count(Document.id)).where(
                Document.source_idempotency_key == job.idempotency_key
            )
        )
        run_count = await db.scalar(
            select(func.count(WorkflowRun.id)).where(
                WorkflowRun.run_id == first_run.run_id
            )
        )
    assert document_count == 1
    assert run_count == 1


async def test_redelivery_after_review_short_circuits_graph_and_acknowledges(monkeypatch):
    job = make_job(suffix="reviewed-redelivery")
    _, created_run = await invoice_worker._get_or_create_job_records(job)
    async with AsyncSessionLocal() as db:
        run = await db.scalar(
            select(WorkflowRun).where(WorkflowRun.run_id == created_run.run_id)
        )
        run.status = "approved"
        run.processing_status = "complete"
        run.posting_status = "ready_for_posting"
        db.add(ReviewDecision(run_id=run.run_id, disposition="approved"))
        await db.commit()

    async def unexpected_graph_call(*_args, **_kwargs):
        raise AssertionError("reviewed redelivery must not invoke LangGraph")

    monkeypatch.setattr(invoice_worker.workflow_graph, "aget_state", unexpected_graph_call)
    monkeypatch.setattr(invoice_worker.workflow_graph, "ainvoke", unexpected_graph_call)
    redis = FakeRedis()

    result = await invoice_worker._process_delivery(
        redis,
        delivery(job, "reviewed-1", attempt=2),
        settings(),
        "consumer-b",
    )

    assert result["status"] == "approved"
    assert result["processing_status"] == "complete"
    assert result["review_disposition"] == "approved"
    assert result["posting_status"] == "ready_for_posting"
    assert [operation[0] for operation in redis.operations if operation[0] != "heartbeat"] == [
        "xack",
        "xdel",
    ]


async def test_operator_replay_restarts_failed_run_without_changing_source_identity(monkeypatch):
    job = make_job(suffix="operator-restart")
    document, created_run = await invoice_worker._get_or_create_job_records(job)
    async with AsyncSessionLocal() as db:
        run = await db.scalar(
            select(WorkflowRun).where(WorkflowRun.run_id == created_run.run_id)
        )
        run.status = "failed"
        run.processing_status = "failed"
        run.interrupt_payload = {"error": "provider unavailable"}
        await db.commit()

    with pytest.raises(RuntimeError, match="provider unavailable"):
        await invoice_worker._run_workflow_for_job(job)

    async def empty_state(_config):
        return SimpleNamespace(values={}, next=())

    async def completed_graph(graph_input, *, config):
        assert graph_input["document_id"] == str(document.id)
        assert graph_input["run_id"] == created_run.run_id
        return {
            **graph_input,
            "status": "complete",
            "processing_status": "complete",
            "posting_status": "ready_for_posting",
        }

    monkeypatch.setattr(invoice_worker.workflow_graph, "aget_state", empty_state)
    monkeypatch.setattr(invoice_worker.workflow_graph, "ainvoke", completed_graph)

    result = await invoice_worker._run_workflow_for_job(job, operator_replay=True)

    assert result["processing_status"] == "complete"
    async with AsyncSessionLocal() as db:
        replayed_run = await db.scalar(
            select(WorkflowRun).where(WorkflowRun.run_id == created_run.run_id)
        )
        replayed_document = await db.scalar(
            select(Document).where(Document.id == document.id)
        )
        assert replayed_run.processing_status == "complete"
        assert replayed_document.source_idempotency_key == job.idempotency_key


async def test_replay_delivery_records_processing_and_acknowledged_outcome(monkeypatch):
    redis = FakeRedis()
    job = make_job(suffix="replay-lifecycle")
    replay_delivery = StreamDelivery(
        "7-0",
        {
            "payload": job.model_dump_json(),
            "replay_dlq_id": "6-0",
            "replay_request_id": "request-1",
        },
    )

    async def complete(_job: InvoiceJobEnvelope, *, operator_replay: bool = False):
        assert operator_replay is True
        return invoice_worker._summary_result(
            filename=job.filename, run_id="run-1", status="complete"
        )

    monkeypatch.setattr(invoice_worker, "_run_workflow_for_job", complete)
    result = await invoice_worker._process_delivery(
        redis, replay_delivery, settings(), "consumer-a"
    )

    assert result["processing_status"] == "complete"
    replay_updates = [
        operation[2]
        for operation in redis.operations
        if operation[0] == "hset" and operation[1] == "job-replays:6-0"
    ]
    assert any(fields["action:request-1:state"] == "processing" for fields in replay_updates)
    assert any(fields["action:request-1:state"] == "acknowledged" for fields in replay_updates)
    assert any(
        fields.get("action:request-1:workflow_processing_status") == "complete"
        for fields in replay_updates
    )


async def test_transient_failures_retry_up_to_limit(monkeypatch):
    redis = FakeRedis()
    job = make_job(suffix="retry")

    async def fail(_job: InvoiceJobEnvelope) -> dict:
        raise RuntimeError("temporary outage")

    async def no_op_mark_failed(*_args) -> None:
        return None

    monkeypatch.setattr(invoice_worker, "_run_workflow_for_job", fail)
    monkeypatch.setattr(invoice_worker, "_mark_job_failed", no_op_mark_failed)

    for attempt in (1, 2, 3):
        await invoice_worker._process_delivery(
            redis,
            delivery(job, f"3-{attempt}", attempt=attempt),
            settings(),
            "consumer-a",
        )

    retry_updates = [op for op in redis.operations if op[0] == "hset"]
    dlq_writes = [op for op in redis.operations if op[0] == "xadd"]
    assert [op[2]["attempt_count"] for op in retry_updates] == [1, 2]
    assert len(dlq_writes) == 1
    assert dlq_writes[0][2]["attempt_count"] == "3"


async def test_permanent_failure_enters_dead_letter_stream():
    redis = FakeRedis()
    invalid = StreamDelivery("4-0", {"payload": json.dumps({"schema_version": 99})})

    result = await invoice_worker._process_delivery(
        redis, invalid, settings(), "consumer-a"
    )

    assert result["status"] == "failed"
    assert [op[0] for op in redis.operations] == ["xadd", "xack", "xdel"]
    assert redis.operations[0][1] == "jobs-dlq"


async def test_failed_batch_job_does_not_block_siblings(monkeypatch):
    redis = FakeRedis()
    good_a = make_job("a.pdf", "batch-a")
    broken = make_job("broken.pdf", "batch-broken")
    good_c = make_job("c.pdf", "batch-c")

    async def run(job: InvoiceJobEnvelope) -> dict:
        if job.filename == "broken.pdf":
            raise RuntimeError("branch failed")
        return invoice_worker._summary_result(
            filename=job.filename, run_id=job.idempotency_key[:8], status="complete"
        )

    async def no_op_mark_failed(*_args) -> None:
        return None

    monkeypatch.setattr(invoice_worker, "_run_workflow_for_job", run)
    monkeypatch.setattr(invoice_worker, "_mark_job_failed", no_op_mark_failed)
    deliveries = [
        delivery(good_a, "5-1", attempt=3),
        delivery(broken, "5-2", attempt=3),
        delivery(good_c, "5-3", attempt=3),
    ]

    results = await invoice_worker._process_batch(
        redis, deliveries, settings(), "consumer-a"
    )

    assert {result["filename"] for result in results} == {
        "a.pdf",
        "broken.pdf",
        "c.pdf",
    }
    acked_ids = {op[3] for op in redis.operations if op[0] == "xack"}
    assert acked_ids == {"5-1", "5-2", "5-3"}


async def test_malformed_payload_is_retained_for_investigation():
    redis = FakeRedis()
    malformed = StreamDelivery("6-0", {"payload": "{not-json"})

    await invoice_worker._process_delivery(
        redis, malformed, settings(), "consumer-a"
    )

    dlq_fields = next(op[2] for op in redis.operations if op[0] == "xadd")
    assert dlq_fields["original_stream_id"] == "6-0"
    assert "not-json" in dlq_fields["original_fields"]
    assert "not valid JSON" in dlq_fields["failure_reason"]
