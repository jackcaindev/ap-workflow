from typing import Any

from langgraph.types import interrupt
from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.document import Document
from app.models.workflow_audit_log import WorkflowAuditLog
from app.services.audit import log_audit_event
from app.services.extraction import extract_document
from app.services.reconciliation import reconcile_shipment
from app.services.missing_document_sla import SLA_REASON_CODES
from app.services.shipment import upsert_shipment
from app.services.triage import triage_exception
from app.workflow.state import WorkflowState


def _next_iteration(state: WorkflowState) -> int:
    return state.get("iteration_count", 0) + 1


def _extraction_summary(extraction: dict[str, Any]) -> dict[str, Any]:
    return {
        "doc_type": extraction.get("doc_type"),
        "carrier_name": extraction.get("carrier_name"),
        "load_number": extraction.get("load_number"),
        "total_amount": extraction.get("total_amount"),
    }


async def _log_exception_raised_once(run_id: str, state: WorkflowState) -> None:
    async with AsyncSessionLocal() as db:
        existing = await db.execute(
            select(WorkflowAuditLog.id)
            .where(
                WorkflowAuditLog.run_id == run_id,
                WorkflowAuditLog.event_type == "exception_raised",
            )
            .limit(1)
        )
        if existing.scalar_one_or_none() is not None:
            return

    extraction = state.get("extraction") or {}
    await log_audit_event(
        run_id,
        "exception_raised",
        payload={
            "exception_reason": state.get("exception_reason"),
            "extraction": _extraction_summary(extraction),
        },
    )


async def extract_node(state: WorkflowState) -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        extraction = await extract_document(
            state["file_bytes"],
            state["filename"],
            db,
            document_id=int(state["document_id"]),
        )

    # Nodes return partial state updates because LangGraph owns the canonical
    # state merge. Returning only changed fields keeps reducers like messages
    # meaningful and avoids accidentally overwriting unrelated state.
    await log_audit_event(
        state["run_id"],
        "extracted",
        payload={"extraction": _extraction_summary(extraction.model_dump(mode="json"))},
    )
    return {
        "extraction": extraction.model_dump(mode="json"),
        "status": "extracted",
        "processing_status": "running",
        "messages": ["Extracted document with Claude vision."],
        "iteration_count": _next_iteration(state),
    }


def _reconciliation_exception_reason(exception_reasons: list[str], checks: list[dict]) -> str:
    failed_checks = [
        check
        for check in checks
        if check.get("outcome") == "failed"
        or ("outcome" not in check and check.get("passed") is False)
    ]
    if not failed_checks:
        return ", ".join(exception_reasons)
    details = "; ".join(
        f"{check.get('check_name')}: {check.get('details')}" for check in failed_checks
    )
    return f"{', '.join(exception_reasons)}: {details}"


async def match_node(state: WorkflowState) -> dict[str, Any]:
    extraction = state.get("extraction") or {}
    async with AsyncSessionLocal() as db:
        document = await db.get(Document, int(state["document_id"]))
        if document is None:
            match_result = {
                "matched": False,
                "requires_review": True,
                "reason": "document_not_found",
            }
        else:
            shipment = await upsert_shipment(extraction.get("load_number"), document, extraction, db)
            if shipment is None:
                match_result = {
                    "matched": False,
                    "requires_review": True,
                    "reason": "no_load_number",
                    "missing_docs": [],
                    "exception_reasons": ["no_load_number"],
                }
            else:
                reconciliation = await reconcile_shipment(
                    shipment,
                    db,
                    run_id=state.get("run_id"),
                )
                reviewable_reasons = [
                    reason
                    for reason in reconciliation.exception_reasons
                    if reason not in SLA_REASON_CODES
                ]
                match_result = {
                    # Retained in checkpoint and API payloads as a legacy
                    # projection. Current routing uses requires_review and the
                    # independent business-state dimensions instead.
                    "matched": not reconciliation.exception_reasons,
                    "requires_review": bool(reviewable_reasons),
                    "shipment_id": str(shipment.id),
                    "reconciliation_result_id": str(reconciliation.id),
                    "reconciliation_status": shipment.reconciliation_status,
                    "checks": reconciliation.checks,
                    "missing_docs": reconciliation.missing_docs,
                    "exception_reasons": reconciliation.exception_reasons,
                    "reviewable_exception_reasons": reviewable_reasons,
                }

    if not match_result["requires_review"]:
        return {
            "match_result": match_result,
            "exception_reason": None,
            "status": "matched",
            "processing_status": "running",
            "messages": [
                f"Reconciled shipment state for load number {extraction.get('load_number')}."
            ],
            "iteration_count": _next_iteration(state),
        }

    exception_reasons = match_result.get("reviewable_exception_reasons") or [
        match_result.get("reason")
    ]
    reviewable_checks = [
        check
        for check in match_result.get("checks") or []
        if check.get("reason_code") not in SLA_REASON_CODES
    ]
    exception_reason = _reconciliation_exception_reason(
        [reason for reason in exception_reasons if reason],
        reviewable_checks,
    )
    return {
        "match_result": match_result,
        "exception_reason": exception_reason,
        "status": "exception",
        "processing_status": "running",
        "messages": [f"Shipment reconciliation failed: {exception_reason}."],
        "iteration_count": _next_iteration(state),
    }


async def supervisor_node(state: WorkflowState) -> dict[str, Any]:
    decision = await triage_exception(
        exception_reason=state.get("exception_reason"),
        extraction=state.get("extraction"),
        match_result=state.get("match_result"),
    )
    await log_audit_event(
        state["run_id"],
        "triaged",
        payload=decision.model_dump(mode="json"),
    )
    return {
        "triage_route": decision.route,
        "triage_reasoning": decision.reasoning,
        "triage_confidence": decision.confidence,
        "messages": [f"Triage supervisor routed exception as {decision.route}."],
        "iteration_count": _next_iteration(state),
    }


async def exception_node(state: WorkflowState) -> dict[str, Any]:
    # interrupt() checkpoints the current graph thread and returns control to the
    # caller with this payload. When /workflow/{run_id}/resume sends
    # Command(resume=...), LangGraph re-enters this node and interrupt() returns
    # that resume value, letting the graph continue from the saved checkpoint.
    await _log_exception_raised_once(state["run_id"], state)
    decision = interrupt(
        {
            "reason": state.get("exception_reason"),
            "extraction": state.get("extraction"),
            "triage_route": state.get("triage_route"),
            "triage_reasoning": state.get("triage_reasoning"),
            "triage_confidence": state.get("triage_confidence"),
        }
    )
    return {
        "human_decision": decision,
        "status": "awaiting_review",
        "processing_status": "awaiting_review",
        "review_disposition": decision,
        "messages": ["Paused for human review."],
        "iteration_count": _next_iteration(state),
    }


async def approve_node(state: WorkflowState) -> dict[str, Any]:
    return {
        "status": "approved",
        "review_disposition": "approved",
        "messages": ["Human reviewer approved the exception."],
        "iteration_count": _next_iteration(state),
    }


async def reject_node(state: WorkflowState) -> dict[str, Any]:
    return {
        "status": "rejected",
        "review_disposition": "rejected",
        "messages": ["Human reviewer rejected the exception."],
        "iteration_count": _next_iteration(state),
    }


async def complete_node(state: WorkflowState) -> dict[str, Any]:
    decision = state.get("human_decision")
    unresolved_exception = bool(state.get("exception_reason")) and decision not in {
        "approved",
        "rejected",
    }
    if unresolved_exception:
        await log_audit_event(
            state["run_id"],
            "failed",
            payload={"reason": "unresolved_exception_circuit_breaker"},
        )
        return {
            "status": "failed",
            "processing_status": "failed",
            "posting_status": "not_ready",
            "messages": ["Workflow failed because an exception was not resolved."],
            "iteration_count": _next_iteration(state),
        }

    legacy_status = decision if decision in {"approved", "rejected"} else "complete"
    reconciliation_status = (state.get("match_result") or {}).get("reconciliation_status")
    if decision == "approved" or (decision is None and reconciliation_status == "reconciled"):
        posting_status = "ready_for_posting"
    elif decision == "rejected":
        posting_status = "blocked"
    else:
        posting_status = "not_ready"

    # Review completion is persisted by the resume transaction. Avoid opening a
    # second transaction here while that transaction holds the WorkflowRun row
    # lock; PostgreSQL's FK check on workflow_audit_logs would otherwise wait on
    # the same lock. Non-review runs retain the existing completion audit event.
    if decision not in {"approved", "rejected"}:
        await log_audit_event(
            state["run_id"],
            "completed",
            payload={
                "processing_status": "complete",
                "posting_status": posting_status,
            },
        )
    return {
        "status": legacy_status,
        "processing_status": "complete",
        "posting_status": posting_status,
        "messages": ["Workflow completed."],
        "iteration_count": _next_iteration(state),
    }
