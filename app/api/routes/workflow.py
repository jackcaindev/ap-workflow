from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models.document import Document
from app.models.reconciliation_result import ReconciliationResult
from app.models.review_decision import ReviewDecision
from app.models.shipment import Shipment
from app.models.workflow_audit_log import WorkflowAuditLog
from app.models.workflow_run import WorkflowRun
from app.schemas.workflow import ResumeRequest
from app.services.business_state import (
    BusinessStateConflict,
    decide_review,
    reconciliation_status_for_run,
    state_payload,
)
from app.services.extraction import (
    ExtractionError,
    UnsupportedFileTypeError,
    detect_file_type,
)
from app.workflow.graph import workflow_graph


router = APIRouter(prefix="/workflow", tags=["workflow"])


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


def _triage_from_payload(payload: dict | None) -> dict[str, str | float | None]:
    if not payload:
        return {
            "triage_route": None,
            "triage_reasoning": None,
            "triage_confidence": None,
        }
    return {
        "triage_route": payload.get("triage_route"),
        "triage_reasoning": payload.get("triage_reasoning"),
        "triage_confidence": payload.get("triage_confidence"),
    }


def _run_sort_key(run: dict) -> tuple[int, int, object]:
    triage_priority = {
        "escalate_priority": 0,
        "escalate_standard": 1,
        "auto_resolve": 2,
    }
    review_priority = 0 if run.get("processing_status") == "awaiting_review" else 1
    route = run.get("triage_route")
    route_rank = triage_priority.get(route, 3) if route else 3
    return (review_priority, route_rank, run.get("created_at"))


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


async def get_review_decision(db: AsyncSession, run_id: str) -> ReviewDecision | None:
    result = await db.execute(
        select(ReviewDecision).where(ReviewDecision.run_id == run_id)
    )
    return result.scalar_one_or_none()


async def persist_run_state(
    db: AsyncSession,
    run: WorkflowRun,
    *,
    status_value: str,
    processing_status: str | None = None,
    posting_status: str | None = None,
    payload: dict | None = None,
    preserve_payload: bool = False,
) -> None:
    run.status = status_value
    run.processing_status = processing_status or status_value
    if posting_status is not None:
        run.posting_status = posting_status
    if not preserve_payload:
        run.interrupt_payload = payload
    await db.commit()


@router.get("/runs")
async def workflow_runs(db: AsyncSession = Depends(get_session)) -> list[dict]:
    result = await db.execute(
        select(WorkflowRun, Document, ReviewDecision)
        .join(Document, WorkflowRun.document_id == Document.id)
        .outerjoin(ReviewDecision, ReviewDecision.run_id == WorkflowRun.run_id)
        .order_by(WorkflowRun.created_at.desc())
        .limit(20)
    )

    runs = []
    for run, document, decision in result.all():
        extraction = document.extracted_data or {}
        total_amount = _extracted_amount(extraction)
        doc_type = extraction.get("doc_type") or document.doc_type
        reconciliation_status = await reconciliation_status_for_run(db, run.run_id)
        business_state = state_payload(run, decision, reconciliation_status)
        runs.append(
            {
                "run_id": run.run_id,
                "filename": document.filename,
                "doc_type": doc_type,
                "carrier_name": _extracted_carrier(extraction),
                "amount": total_amount,
                "total_amount": total_amount,
                **business_state,
                "created_at": run.created_at,
                **_triage_from_payload(run.interrupt_payload),
            }
        )
    runs.sort(key=_run_sort_key)
    return runs


@router.post("/run")
async def run_workflow(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_session),
) -> dict:
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
        processing_status="running",
        posting_status="not_ready",
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
        "triage_route": None,
        "triage_reasoning": None,
        "triage_confidence": None,
        "human_decision": None,
        "status": "running",
        "processing_status": "running",
        "posting_status": "not_ready",
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
            processing_status="failed",
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
            processing_status="failed",
            payload={"error": str(exc)},
        )
        raise
    payload = interrupt_payload(result)
    if payload is not None:
        await persist_run_state(
            db,
            run,
            status_value="awaiting_review",
            processing_status="awaiting_review",
            posting_status="not_ready",
            payload=payload,
        )
        return state_payload(run, None, await reconciliation_status_for_run(db, run_id))

    final_status = result.get("status", "complete")
    await persist_run_state(
        db,
        run,
        status_value=final_status,
        processing_status=result.get("processing_status", "complete"),
        posting_status=result.get("posting_status", "not_ready"),
        preserve_payload=True,
    )
    return state_payload(run, None, await reconciliation_status_for_run(db, run_id))


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
    decision = await get_review_decision(db, run.run_id)
    match_result = None if reconciliation is None else {
        # Public compatibility projection for older clients. Authoritative
        # decisions use reconciliation, review, and posting state dimensions.
        "matched": not reconciliation.exception_reasons,
        "reconciliation_result_id": str(reconciliation.id),
        "shipment_id": str(reconciliation.shipment_id),
        "checks": reconciliation.checks,
        "missing_docs": reconciliation.missing_docs,
        "exception_reasons": reconciliation.exception_reasons,
    }

    triage = _triage_from_payload(run.interrupt_payload)

    return {
        **state_payload(
            run,
            decision,
            None if reconciliation is None else (
                await db.scalar(
                    select(Shipment.reconciliation_status).where(
                        Shipment.id == reconciliation.shipment_id
                    )
                )
            ),
        ),
        "filename": document.filename,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "extraction": extraction,
        "match_result": match_result,
        "exception_reason": _exception_reason(run),
        "interrupt_payload": run.interrupt_payload,
        **triage,
    }


@router.post("/{run_id}/resume")
async def resume_workflow(
    run_id: str,
    request: ResumeRequest,
    db: AsyncSession = Depends(get_session),
) -> dict:
    try:
        run, decision, idempotent = await decide_review(db, run_id, request.decision)
    except BusinessStateConflict as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": exc.code,
                "message": exc.message,
                **(exc.details or {}),
            },
        ) from exc
    if run is None or decision is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow run not found")
    response = state_payload(
        run,
        decision,
        await reconciliation_status_for_run(db, run_id),
    )
    response["idempotent"] = idempotent
    return response


@router.get("/{run_id}/audit")
async def workflow_audit(run_id: str, db: AsyncSession = Depends(get_session)) -> list[dict]:
    run = await get_workflow_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow run not found")

    result = await db.execute(
        select(WorkflowAuditLog)
        .where(WorkflowAuditLog.run_id == run_id)
        .order_by(WorkflowAuditLog.created_at.asc())
    )
    entries = result.scalars().all()
    return [
        {
            "id": entry.id,
            "run_id": entry.run_id,
            "event_type": entry.event_type,
            "payload": entry.payload,
            "actor": entry.actor,
            "created_at": entry.created_at,
        }
        for entry in entries
    ]


@router.get("/{run_id}/status")
async def workflow_status(run_id: str, db: AsyncSession = Depends(get_session)) -> dict:
    run = await get_workflow_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow run not found")

    decision = await get_review_decision(db, run_id)
    response = state_payload(
        run,
        decision,
        await reconciliation_status_for_run(db, run_id),
    )
    if run.processing_status == "awaiting_review":
        response["interrupt_payload"] = run.interrupt_payload
    return response
