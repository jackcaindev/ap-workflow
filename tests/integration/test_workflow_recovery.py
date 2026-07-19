from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import func, select

from app.database import AsyncSessionLocal
from app.models.rate_confirmation import RateConfirmation
from app.models.review_decision import ReviewDecision
from app.models.workflow_audit_log import WorkflowAuditLog
from app.models.workflow_run import WorkflowRun
from app.services import business_state, invoice_worker
from app.services.business_state import BusinessStateConflict, decide_review
from app.workflow.graph import workflow_graph


pytestmark = pytest.mark.integration


async def _seed_rate_confirmation(db_session, load_number: str) -> None:
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


async def _start_interrupted_process(
    db_session,
    make_integration_job,
    run_recovery_process,
    *,
    suffix: str,
    load_number: str,
):
    await _seed_rate_confirmation(db_session, load_number)
    job = make_integration_job(suffix)
    await run_recovery_process(
        "interrupt-exit",
        "--job-json",
        job.model_dump_json(),
        "--load-number",
        load_number,
        expected_exit=87,
    )
    return job, invoice_worker._deterministic_run_id(job.idempotency_key)


async def test_human_interrupt_resumes_in_a_fresh_application_process(
    db_session,
    make_integration_job,
    run_recovery_process,
):
    _job, run_id = await _start_interrupted_process(
        db_session,
        make_integration_job,
        run_recovery_process,
        suffix="process-restart",
        load_number="IT-PROCESS-RESTART",
    )

    paused = await workflow_graph.aget_state(invoice_worker.graph_config(run_id))
    assert tuple(paused.next)
    assert paused.values.get("human_decision") is None
    run = await db_session.scalar(select(WorkflowRun).where(WorkflowRun.run_id == run_id))
    assert run is not None
    assert run.processing_status == "awaiting_review"
    assert run.interrupt_payload is not None

    await run_recovery_process(
        "resume",
        "--run-id",
        run_id,
        "--decision",
        "approved",
    )

    completed = await workflow_graph.aget_state(invoice_worker.graph_config(run_id))
    assert tuple(completed.next) == ()
    assert completed.values["human_decision"] == "approved"
    await db_session.refresh(run)
    assert run.processing_status == "complete"
    assert run.posting_status == "ready_for_posting"
    assert await db_session.scalar(
        select(func.count(ReviewDecision.id)).where(ReviewDecision.run_id == run_id)
    ) == 1
    assert await db_session.scalar(
        select(func.count(WorkflowAuditLog.id)).where(
            WorkflowAuditLog.run_id == run_id,
            WorkflowAuditLog.event_type == "approved",
        )
    ) == 1


async def test_checkpoint_decision_repairs_rolled_back_application_transaction(
    monkeypatch,
    db_session,
    make_integration_job,
    run_recovery_process,
):
    _job, run_id = await _start_interrupted_process(
        db_session,
        make_integration_job,
        run_recovery_process,
        suffix="checkpoint-repair",
        load_number="IT-CHECKPOINT-REPAIR",
    )

    original_log_audit_event = business_state.log_audit_event

    async def fail_before_application_commit(*_args, **_kwargs):
        raise RuntimeError("injected failure before application commit")

    monkeypatch.setattr(
        business_state, "log_audit_event", fail_before_application_commit
    )
    with pytest.raises(RuntimeError, match="injected failure"):
        await decide_review(db_session, run_id, "approved")
    await db_session.rollback()
    monkeypatch.setattr(
        business_state, "log_audit_event", original_log_audit_event
    )

    checkpoint = await workflow_graph.aget_state(invoice_worker.graph_config(run_id))
    assert tuple(checkpoint.next) == ()
    assert checkpoint.values["human_decision"] == "approved"

    async with AsyncSessionLocal() as verification_db:
        run = await verification_db.scalar(
            select(WorkflowRun).where(WorkflowRun.run_id == run_id)
        )
        assert run is not None
        assert run.processing_status == "awaiting_review"
        assert await verification_db.scalar(
            select(func.count(ReviewDecision.id)).where(ReviewDecision.run_id == run_id)
        ) == 0

    async with AsyncSessionLocal() as conflicting_db:
        with pytest.raises(BusinessStateConflict) as conflict:
            await decide_review(conflicting_db, run_id, "rejected")
        assert conflict.value.code == "review_decision_conflict"
        await conflicting_db.rollback()

    async def unexpected_resume(*_args, **_kwargs):
        raise AssertionError("Repair must reuse the completed checkpoint")

    monkeypatch.setattr(business_state.workflow_graph, "ainvoke", unexpected_resume)
    async with AsyncSessionLocal() as repair_db:
        repaired_run, repaired_decision, idempotent = await decide_review(
            repair_db, run_id, "approved"
        )
        assert repaired_run is not None and repaired_decision is not None
        assert idempotent is False
        assert repaired_run.processing_status == "complete"
        assert repaired_run.posting_status == "ready_for_posting"

    async with AsyncSessionLocal() as verification_db:
        assert await verification_db.scalar(
            select(func.count(ReviewDecision.id)).where(ReviewDecision.run_id == run_id)
        ) == 1
        assert await verification_db.scalar(
            select(func.count(WorkflowAuditLog.id)).where(
                WorkflowAuditLog.run_id == run_id,
                WorkflowAuditLog.event_type == "approved",
            )
        ) == 1
