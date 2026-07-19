from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import func, select, text

from app.models.document import Document
from app.models.notification import Notification
from app.models.rate_confirmation import RateConfirmation
from app.models.reconciliation_result import ReconciliationResult
from app.models.review_decision import ReviewDecision
from app.models.workflow_audit_log import WorkflowAuditLog
from app.models.workflow_run import WorkflowRun
from app.services import invoice_worker, notifier
from app.services.business_state import decide_review
from app.services.invoice_queue import (
    StreamDelivery,
    claim_stale_batch,
    ensure_consumer_group,
)
from app.services.notifier import send_batch_summary
from app.workflow.graph import workflow_graph


pytestmark = pytest.mark.integration


async def _wait_until_reclaimable(
    redis_client,
    queue_names,
    wait_for_condition,
    *,
    minimum_idle_ms: int,
):
    async def probe():
        rows = await redis_client.xpending_range(
            queue_names.stream, queue_names.group, "-", "+", 10
        )
        if rows and rows[0]["time_since_delivered"] >= minimum_idle_ms:
            return rows
        return None

    return await wait_for_condition(
        probe,
        description=f"pending delivery idle time >= {minimum_idle_ms}ms",
    )


async def test_worker_death_before_ack_is_reclaimed_by_another_consumer(
    monkeypatch,
    redis_client,
    queue_names,
    worker_settings,
    make_integration_job,
    run_recovery_process,
    wait_for_condition,
):
    job = make_integration_job("receive-crash")
    await ensure_consumer_group(
        redis_client, stream=queue_names.stream, group=queue_names.group
    )
    stream_id = await redis_client.xadd(
        queue_names.stream, {"payload": job.model_dump_json()}
    )

    await run_recovery_process(
        "receive-exit",
        "--stream",
        queue_names.stream,
        "--group",
        queue_names.group,
        "--consumer",
        "consumer-a",
        expected_exit=85,
    )

    pending = await redis_client.xpending_range(
        queue_names.stream, queue_names.group, stream_id, stream_id, 1
    )
    assert pending[0]["consumer"] == "consumer-a"
    assert pending[0]["times_delivered"] == 1
    await _wait_until_reclaimable(
        redis_client,
        queue_names,
        wait_for_condition,
        minimum_idle_ms=worker_settings.INVOICE_VISIBILITY_TIMEOUT_MS,
    )

    reclaimed = await claim_stale_batch(
        redis_client,
        stream=queue_names.stream,
        group=queue_names.group,
        consumer="consumer-b",
        min_idle_ms=worker_settings.INVOICE_VISIBILITY_TIMEOUT_MS,
        count=1,
    )
    assert reclaimed == [
        StreamDelivery(
            stream_id=stream_id,
            fields={"payload": job.model_dump_json()},
            attempt=2,
        )
    ]

    async def deterministic_success(received_job):
        assert received_job == job
        return invoice_worker._summary_result(
            filename=received_job.filename,
            run_id="delivery-only-run",
            status="complete",
        )

    monkeypatch.setattr(
        invoice_worker, "_run_workflow_for_job", deterministic_success
    )
    result = await invoice_worker._process_delivery(
        redis_client,
        reclaimed[0],
        worker_settings,
        "consumer-b",
    )

    assert result["processing_status"] == "complete"
    assert await redis_client.xpending_range(
        queue_names.stream, queue_names.group, "-", "+", 10
    ) == []
    assert await redis_client.xrange(queue_names.stream, stream_id, stream_id) == []


async def test_postgres_commit_before_ack_redelivers_without_duplicate_effects(
    monkeypatch,
    db_session,
    redis_client,
    queue_names,
    worker_settings,
    make_integration_job,
    run_recovery_process,
    wait_for_condition,
):
    load_number = "IT-PRE-ACK"
    db_session.add(
        RateConfirmation(
            load_number=load_number,
            carrier_name="ACME FREIGHT",
            origin="Chicago, IL",
            destination="Dallas, TX",
            agreed_rate=1500.0,
            shipment_date=date(2026, 7, 19),
        )
    )
    await db_session.commit()

    job = make_integration_job("pre-ack")
    await ensure_consumer_group(
        redis_client, stream=queue_names.stream, group=queue_names.group
    )
    stream_id = await redis_client.xadd(
        queue_names.stream, {"payload": job.model_dump_json()}
    )

    await run_recovery_process(
        "process-exit-before-ack",
        "--stream",
        queue_names.stream,
        "--group",
        queue_names.group,
        "--consumer",
        "consumer-a",
        "--dead-letter-stream",
        queue_names.dead_letter_stream,
        "--metadata-prefix",
        queue_names.metadata_prefix,
        "--load-number",
        load_number,
        expected_exit=86,
    )

    expected_run_id = invoice_worker._deterministic_run_id(job.idempotency_key)
    run = await db_session.scalar(
        select(WorkflowRun).where(WorkflowRun.run_id == expected_run_id)
    )
    assert run is not None
    assert run.processing_status == "awaiting_review"
    snapshot = await workflow_graph.aget_state(invoice_worker.graph_config(expected_run_id))
    assert tuple(snapshot.next)
    checkpoint_threads = set(
        (await db_session.execute(text("SELECT DISTINCT thread_id FROM checkpoints"))).scalars()
    )
    assert checkpoint_threads == {expected_run_id}

    reviewed_run, decision, idempotent = await decide_review(
        db_session, expected_run_id, "approved"
    )
    assert reviewed_run is not None and decision is not None
    assert idempotent is False

    await _wait_until_reclaimable(
        redis_client,
        queue_names,
        wait_for_condition,
        minimum_idle_ms=worker_settings.INVOICE_VISIBILITY_TIMEOUT_MS,
    )
    reclaimed = await claim_stale_batch(
        redis_client,
        stream=queue_names.stream,
        group=queue_names.group,
        consumer="consumer-b",
        min_idle_ms=worker_settings.INVOICE_VISIBILITY_TIMEOUT_MS,
        count=1,
    )
    assert len(reclaimed) == 1
    assert reclaimed[0].stream_id == stream_id
    assert reclaimed[0].attempt == 2

    async def unexpected_graph_call(*_args, **_kwargs):
        raise AssertionError("Completed redelivery must not invoke LangGraph")

    monkeypatch.setattr(invoice_worker.workflow_graph, "aget_state", unexpected_graph_call)
    monkeypatch.setattr(invoice_worker.workflow_graph, "ainvoke", unexpected_graph_call)
    summary = await invoice_worker._process_delivery(
        redis_client,
        reclaimed[0],
        worker_settings,
        "consumer-b",
    )
    assert summary["review_disposition"] == "approved"

    monkeypatch.setattr(notifier, "_send_summary_email", lambda _results: True)
    await send_batch_summary([summary])

    assert await db_session.scalar(
        select(func.count(Document.id)).where(
            Document.source_idempotency_key == job.idempotency_key
        )
    ) == 1
    assert await db_session.scalar(
        select(func.count(WorkflowRun.id)).where(WorkflowRun.run_id == expected_run_id)
    ) == 1
    assert await db_session.scalar(
        select(func.count(ReconciliationResult.id)).where(
            ReconciliationResult.run_id == expected_run_id
        )
    ) == 1
    assert await db_session.scalar(
        select(func.count(ReviewDecision.id)).where(
            ReviewDecision.run_id == expected_run_id
        )
    ) == 1
    assert await db_session.scalar(select(func.count(Notification.id))) == 1

    audit_counts = dict(
        (
            await db_session.execute(
                select(WorkflowAuditLog.event_type, func.count(WorkflowAuditLog.id))
                .where(WorkflowAuditLog.run_id == expected_run_id)
                .group_by(WorkflowAuditLog.event_type)
            )
        ).all()
    )
    assert audit_counts["approved"] == 1
    assert all(count == 1 for count in audit_counts.values())
    assert await redis_client.xpending_range(
        queue_names.stream, queue_names.group, "-", "+", 10
    ) == []
    assert await redis_client.xrange(queue_names.stream, stream_id, stream_id) == []
