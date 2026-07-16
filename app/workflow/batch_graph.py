import logging
import operator
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Send

from app.services.extraction import ExtractionError


logger = logging.getLogger(__name__)


class BatchState(TypedDict):
    jobs: list[dict]
    # Send branches finish independently, so the reducer combines each job's
    # one-item result list without requiring shared mutable state.
    batch_results: Annotated[list[dict], operator.add]


class ProcessJobState(TypedDict):
    job: dict


def dispatch_jobs(state: BatchState) -> list[Send]:
    # Send creates one parallel branch per document while leaving the durable,
    # independently resumable per-document workflow unchanged.
    return [Send("process_job", {"job": job}) for job in state["jobs"]]


async def process_job_node(state: ProcessJobState) -> dict[str, list[dict]]:
    # Import locally so invoice_worker can import the compiled batch graph
    # without creating a module-level circular dependency.
    from app.services.invoice_worker import _run_workflow_for_job, _summary_result

    job = state["job"]
    filename = job.get("filename") or "unknown"

    try:
        result = await _run_workflow_for_job(job)
    except ExtractionError as exc:
        logger.warning("Extraction failed for invoice batch job %s: %s", filename, exc)
        result = _summary_result(
            filename=filename,
            run_id="",
            status="failed",
            exception_reason=str(exc),
        )
    except Exception as exc:
        logger.exception("Invoice batch job failed for %s", filename)
        result = _summary_result(
            filename=filename,
            run_id="",
            status="failed",
            exception_reason=str(exc),
        )

    return {"batch_results": [result]}


def build_batch_graph() -> CompiledStateGraph:
    graph = StateGraph(BatchState)
    graph.add_node("process_job", process_job_node)
    graph.add_conditional_edges(START, dispatch_jobs, ["process_job"])
    graph.add_edge("process_job", END)
    # Batch orchestration is transient. Each process_job branch invokes the
    # existing document graph, which owns its Postgres checkpointer and thread_id.
    return graph.compile()


batch_graph = build_batch_graph()
