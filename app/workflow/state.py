import operator
from typing import Annotated, TypedDict


class WorkflowState(TypedDict):
    run_id: str
    document_id: str
    file_bytes: bytes
    filename: str
    extraction: dict | None
    match_result: dict | None
    exception_reason: str | None
    triage_route: str | None
    triage_reasoning: str | None
    triage_confidence: float | None
    human_decision: str | None
    status: str
    # LangGraph applies the reducer when multiple nodes update this field, so
    # nodes can append audit messages without re-sending the full message list.
    messages: Annotated[list[str], operator.add]
    iteration_count: int
