from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from langgraph.types import Command
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models.document import Document
from app.models.reconciliation_result import ReconciliationResult
from app.models.workflow_run import WorkflowRun
from app.services.extraction import (
    ExtractionError,
    UnsupportedFileTypeError,
    detect_file_type,
)
from app.workflow.graph import workflow_graph


router = APIRouter(prefix="/workflow", tags=["workflow"])


class ResumeRequest(BaseModel):
    decision: Literal["approved", "rejected"]


def _extracted_carrier(extraction: dict | None) -> str | None:
    if not extraction:
        return None
    return extraction.get("carrier_name")


def _extracted_amount(extraction: dict | None) -> float | None:
    if not extraction:
        return None
    if extraction.get("doc_type") != "invoice":
        return None
    amount = extraction.get("total_amount")
    if amount is None:
        return None
    try:
        return float(amount)
    except (TypeError, ValueError):
        return None


def _exception_reason(run: WorkflowRun) -> str | None:
    payload = run.interrupt_payload or {}
    return payload.get("reason") or payload.get("error")


def graph_config(run_id: str) -> dict[str, dict[str, str]]:
    return {"configurable": {"thread_id": run_id}}


def interrupt_payload(result: dict) -> dict | None:
    interrupts = result.get("__interrupt__") or []
    if not interrupts:
        return None
    return interrupts[0].value


async def get_workflow_run(db: AsyncSession, run_id: str) -> WorkflowRun | None:
    result = await db.execute(select(WorkflowRun).where(WorkflowRun.run_id == run_id))
    return result.scalar_one_or_none()


async def persist_run_state(
    db: AsyncSession,
    run: WorkflowRun,
    *,
    status_value: str,
    payload: dict | None = None,
) -> None:
    run.status = status_value
    run.interrupt_payload = payload
    await db.commit()


@router.get("/runs")
async def workflow_runs(db: AsyncSession = Depends(get_session)) -> list[dict]:
    result = await db.execute(
        select(WorkflowRun, Document)
        .join(Document, WorkflowRun.document_id == Document.id)
        .order_by(WorkflowRun.created_at.desc())
        .limit(20)
    )

    runs = []
    for run, document in result.all():
        extraction = document.extracted_data or {}
        total_amount = _extracted_amount(extraction)
        doc_type = extraction.get("doc_type") or document.doc_type
        runs.append(
            {
                "run_id": run.run_id,
                "filename": document.filename,
                "doc_type": doc_type,
                "carrier_name": _extracted_carrier(extraction),
                "amount": total_amount,
                "total_amount": total_amount,
                "status": run.status,
                "created_at": run.created_at,
            }
        )
    return runs


@router.post("/run")
async def run_workflow(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    file_bytes = await file.read()
    try:
        # Validate the extension before creating workflow rows. The extraction
        # node performs the same check, but doing it here avoids leaving a failed
        # run around for input that can never be processed.
        detect_file_type(file.filename or "")
    except UnsupportedFileTypeError as exc:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=str(exc),
        ) from exc

    run_id = str(uuid4())

    document = Document(
        filename=file.filename or "",
        # The extraction node updates doc_type after Claude classifies the file.
        # A placeholder value is needed because the Phase 1 schema made doc_type
        # non-null before classification existed.
        doc_type="unknown",
        status="received",
    )
    db.add(document)
    await db.flush()

    run = WorkflowRun(
        run_id=run_id,
        document_id=document.id,
        status="running",
        interrupt_payload=None,
    )
    db.add(run)
    await db.commit()

    initial_state = {
        "run_id": run_id,
        "document_id": str(document.id),
        "file_bytes": file_bytes,
        "filename": file.filename or "",
        "extraction": None,
        "match_result": None,
        "exception_reason": None,
        "human_decision": None,
        "status": "running",
        "messages": [],
        "iteration_count": 0,
    }

    try:
        result = await workflow_graph.ainvoke(initial_state, config=graph_config(run_id))
    except ExtractionError as exc:
        await persist_run_state(
            db,
            run,
            status_value="failed",
            payload={"error": str(exc)},
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        await persist_run_state(
            db,
            run,
            status_value="failed",
            payload={"error": str(exc)},
        )
        raise
    payload = interrupt_payload(result)
    if payload is not None:
        await persist_run_state(db, run, status_value="awaiting_review", payload=payload)
        return {"run_id": run_id, "status": "awaiting_review"}

    final_status = result.get("status", "complete")
    await persist_run_state(db, run, status_value=final_status)
    return {"run_id": run_id, "status": final_status}


@router.get("/{run_id}")
async def workflow_detail(run_id: str, db: AsyncSession = Depends(get_session)) -> dict:
    result = await db.execute(
        select(WorkflowRun, Document)
        .join(Document, WorkflowRun.document_id == Document.id)
        .where(WorkflowRun.run_id == run_id)
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow run not found")

    run, document = row
    extraction = document.extracted_data or None
    reconciliation_result = await db.execute(
        select(ReconciliationResult)
        .where(ReconciliationResult.run_id == run.run_id)
        .order_by(ReconciliationResult.created_at.desc())
        .limit(1)
    )
    reconciliation = reconciliation_result.scalar_one_or_none()
    match_result = None if reconciliation is None else {
        "matched": not reconciliation.exception_reasons,
        "reconciliation_result_id": str(reconciliation.id),
        "shipment_id": str(reconciliation.shipment_id),
        "checks": reconciliation.checks,
        "missing_docs": reconciliation.missing_docs,
        "exception_reasons": reconciliation.exception_reasons,
    }

    return {
        "run_id": run.run_id,
        "filename": document.filename,
        "status": run.status,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "extraction": extraction,
        "match_result": match_result,
        "exception_reason": _exception_reason(run),
        "interrupt_payload": run.interrupt_payload,
    }


@router.post("/{run_id}/resume")
async def resume_workflow(
    run_id: str,
    request: ResumeRequest,
    db: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    run = await get_workflow_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow run not found")

    result = await workflow_graph.ainvoke(
        Command(resume=request.decision),
        config=graph_config(run_id),
    )
    payload = interrupt_payload(result)
    if payload is not None:
        await persist_run_state(db, run, status_value="awaiting_review", payload=payload)
        return {"run_id": run_id, "status": "awaiting_review"}

    final_status = result.get("status", "complete")
    await persist_run_state(db, run, status_value=final_status)
    return {"run_id": run_id, "status": final_status}


@router.get("/{run_id}/status")
async def workflow_status(run_id: str, db: AsyncSession = Depends(get_session)) -> dict:
    run = await get_workflow_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow run not found")

    response = {"run_id": run_id, "status": run.status}
    if run.status == "awaiting_review":
        response["interrupt_payload"] = run.interrupt_payload
    return response
