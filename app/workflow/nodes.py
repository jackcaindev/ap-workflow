from typing import Any

from langgraph.types import interrupt

from app.database import AsyncSessionLocal
from app.models.document import Document
from app.services.extraction import extract_document
from app.services.reconciliation import reconcile_shipment
from app.services.shipment import upsert_shipment
from app.workflow.state import WorkflowState


def _next_iteration(state: WorkflowState) -> int:
    return state.get("iteration_count", 0) + 1


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
    return {
        "extraction": extraction.model_dump(mode="json"),
        "status": "extracted",
        "messages": ["Extracted document with Claude vision."],
        "iteration_count": _next_iteration(state),
    }


def _reconciliation_exception_reason(exception_reasons: list[str], checks: list[dict]) -> str:
    failed_checks = [check for check in checks if not check.get("passed")]
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
            match_result = {"matched": False, "reason": "document_not_found"}
        else:
            shipment = await upsert_shipment(extraction.get("load_number"), document, extraction, db)
            if shipment is None:
                match_result = {
                    "matched": False,
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
                match_result = {
                    "matched": not reconciliation.exception_reasons,
                    "shipment_id": str(shipment.id),
                    "reconciliation_result_id": str(reconciliation.id),
                    "reconciliation_status": shipment.reconciliation_status,
                    "checks": reconciliation.checks,
                    "missing_docs": reconciliation.missing_docs,
                    "exception_reasons": reconciliation.exception_reasons,
                }

    if match_result["matched"]:
        return {
            "match_result": match_result,
            "exception_reason": None,
            "status": "matched",
            "messages": [
                f"Reconciled shipment state for load number {extraction.get('load_number')}."
            ],
            "iteration_count": _next_iteration(state),
        }

    exception_reasons = match_result.get("exception_reasons") or [match_result.get("reason")]
    exception_reason = _reconciliation_exception_reason(
        [reason for reason in exception_reasons if reason],
        match_result.get("checks") or [],
    )
    return {
        "match_result": match_result,
        "exception_reason": exception_reason,
        "status": "exception",
        "messages": [f"Shipment reconciliation failed: {exception_reason}."],
        "iteration_count": _next_iteration(state),
    }


def exception_node(state: WorkflowState) -> dict[str, Any]:
    # interrupt() checkpoints the current graph thread and returns control to the
    # caller with this payload. When /workflow/{run_id}/resume sends
    # Command(resume=...), LangGraph re-enters this node and interrupt() returns
    # that resume value, letting the graph continue from the saved checkpoint.
    decision = interrupt(
        {
            "reason": state.get("exception_reason"),
            "extraction": state.get("extraction"),
        }
    )
    return {
        "human_decision": decision,
        "status": "awaiting_review",
        "messages": ["Paused for human review."],
        "iteration_count": _next_iteration(state),
    }


def approve_node(state: WorkflowState) -> dict[str, Any]:
    return {
        "status": "approved",
        "messages": ["Human reviewer approved the exception."],
        "iteration_count": _next_iteration(state),
    }


def reject_node(state: WorkflowState) -> dict[str, Any]:
    return {
        "status": "rejected",
        "messages": ["Human reviewer rejected the exception."],
        "iteration_count": _next_iteration(state),
    }


def complete_node(state: WorkflowState) -> dict[str, Any]:
    return {
        "status": "complete",
        "messages": ["Workflow completed."],
        "iteration_count": _next_iteration(state),
    }
