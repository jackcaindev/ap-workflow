from dataclasses import dataclass
from typing import Any

from langgraph.types import Command
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.reconciliation_result import ReconciliationResult
from app.models.review_decision import ReviewDecision
from app.models.shipment import Shipment
from app.models.workflow_run import WorkflowRun
from app.schemas.business_state import ReviewDisposition
from app.services.audit import log_audit_event
from app.workflow.graph import workflow_graph


@dataclass(frozen=True)
class BusinessStateConflict(Exception):
    code: str
    message: str
    details: dict[str, Any] | None = None


def graph_config(run_id: str) -> dict[str, dict[str, str]]:
    return {"configurable": {"thread_id": run_id}}


def interrupt_payload(result: dict[str, Any]) -> dict[str, Any] | None:
    interrupts = result.get("__interrupt__") or []
    return interrupts[0].value if interrupts else None


def processing_status(run: WorkflowRun) -> str:
    value = getattr(run, "processing_status", None)
    if value:
        return value
    if run.status in {"approved", "rejected", "partial", "reconciled"}:
        return "complete"
    if run.status == "exception":
        return "awaiting_review" if run.interrupt_payload else "failed"
    return run.status


def legacy_status(processing: str, decision: ReviewDecision | None) -> str:
    return decision.disposition if decision is not None else processing


def review_disposition(
    run: WorkflowRun,
    decision: ReviewDecision | None,
    reconciliation_status: str | None,
) -> str:
    if decision is not None:
        return decision.disposition
    current_processing = processing_status(run)
    if current_processing == "awaiting_review":
        return ReviewDisposition.PENDING
    if current_processing == "complete" and reconciliation_status == "exception":
        return ReviewDisposition.UNKNOWN
    return ReviewDisposition.NOT_REQUIRED


async def reconciliation_status_for_run(
    db: AsyncSession, run_id: str
) -> str | None:
    result = await db.execute(
        select(Shipment.reconciliation_status)
        .join(ReconciliationResult, ReconciliationResult.shipment_id == Shipment.id)
        .where(ReconciliationResult.run_id == run_id)
        .order_by(ReconciliationResult.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


def state_payload(
    run: WorkflowRun,
    decision: ReviewDecision | None,
    reconciliation_status: str | None,
) -> dict[str, Any]:
    current_processing = processing_status(run)
    return {
        "run_id": run.run_id,
        "status": legacy_status(current_processing, decision),
        "processing_status": current_processing,
        "reconciliation_status": reconciliation_status,
        "review_disposition": review_disposition(
            run, decision, reconciliation_status
        ),
        "posting_status": run.posting_status,
        "reviewed_at": decision.decided_at if decision is not None else None,
        "reviewer_id": decision.reviewer_id if decision is not None else None,
    }


async def _locked_run(
    db: AsyncSession, run_id: str
) -> tuple[WorkflowRun | None, ReviewDecision | None]:
    result = await db.execute(
        select(WorkflowRun)
        .where(WorkflowRun.run_id == run_id)
        .with_for_update()
    )
    run = result.scalar_one_or_none()
    if run is None:
        return None, None
    decision_result = await db.execute(
        select(ReviewDecision).where(ReviewDecision.run_id == run_id)
    )
    return run, decision_result.scalar_one_or_none()


async def decide_review(
    db: AsyncSession,
    run_id: str,
    requested_decision: str,
) -> tuple[WorkflowRun | None, ReviewDecision | None, bool]:
    run, existing_decision = await _locked_run(db, run_id)
    if run is None:
        return None, None, False

    if existing_decision is not None:
        if existing_decision.disposition != requested_decision:
            raise BusinessStateConflict(
                "review_decision_conflict",
                "A different review decision already exists",
                {
                    "existing_decision": existing_decision.disposition,
                    "requested_decision": requested_decision,
                },
            )
        await db.commit()
        return run, existing_decision, True

    if processing_status(run) != "awaiting_review":
        raise BusinessStateConflict(
            "workflow_not_awaiting_review",
            "Only an awaiting_review workflow can receive its first decision",
            {"processing_status": processing_status(run)},
        )

    config = graph_config(run_id)
    snapshot = await workflow_graph.aget_state(config)
    values = dict(getattr(snapshot, "values", {}) or {})
    next_nodes = tuple(getattr(snapshot, "next", ()) or ())
    checkpoint_decision = values.get("human_decision")

    if checkpoint_decision in {"approved", "rejected"}:
        if checkpoint_decision != requested_decision:
            raise BusinessStateConflict(
                "review_decision_conflict",
                "The checkpoint contains a different review decision",
                {
                    "existing_decision": checkpoint_decision,
                    "requested_decision": requested_decision,
                },
            )
        result = values
    elif not values or not next_nodes:
        raise BusinessStateConflict(
            "checkpoint_state_conflict",
            "The workflow checkpoint is not paused for review",
        )
    else:
        result = await workflow_graph.ainvoke(
            Command(resume=requested_decision),
            config=config,
        )

    if interrupt_payload(result) is not None:
        raise BusinessStateConflict(
            "checkpoint_state_conflict",
            "The workflow remained interrupted after the decision",
        )
    final_decision = result.get("human_decision")
    if final_decision != requested_decision:
        raise BusinessStateConflict(
            "checkpoint_state_conflict",
            "The completed checkpoint does not contain the requested decision",
            {"checkpoint_decision": final_decision},
        )

    decision = ReviewDecision(
        run_id=run_id,
        disposition=requested_decision,
        reviewer_id=None,
    )
    db.add(decision)
    await db.flush()

    run.processing_status = "complete"
    run.posting_status = (
        "ready_for_posting" if requested_decision == "approved" else "blocked"
    )
    run.status = requested_decision
    await log_audit_event(
        run_id,
        requested_decision,
        payload={
            "decision_id": str(decision.id),
            "human_decision": requested_decision,
            "decided_at": decision.decided_at.isoformat(),
            "reviewer_id": None,
        },
        actor="human",
        db=db,
    )
    await db.commit()
    await db.refresh(run)
    await db.refresh(decision)
    return run, decision, False
