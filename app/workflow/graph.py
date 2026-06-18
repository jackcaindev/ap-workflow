from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, StateGraph

from app.core.config import get_settings
from app.workflow.nodes import (
    approve_node,
    complete_node,
    exception_node,
    extract_node,
    match_node,
    reject_node,
)
from app.workflow.state import WorkflowState


RouteName = Literal["exception_node", "approve_node", "reject_node", "complete_node"]


def checkpoint_database_url() -> str:
    # SQLAlchemy uses the asyncpg dialect marker, but the LangGraph PostgresSaver
    # talks through psycopg. Removing the dialect suffix keeps both components on
    # the same DATABASE_URL setting without requiring a second env var.
    return get_settings().DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)


def route_after_match(state: WorkflowState) -> RouteName:
    if state.get("iteration_count", 0) > 5:
        return "complete_node"
    if state.get("exception_reason"):
        return "exception_node"
    return "complete_node"


def route_after_exception(state: WorkflowState) -> RouteName:
    if state.get("iteration_count", 0) > 5:
        return "complete_node"

    decision = state.get("human_decision")
    if decision == "approved":
        return "approve_node"
    if decision == "rejected":
        return "reject_node"
    # In normal operation this branch is not reached before resume because
    # interrupt() pauses the graph inside exception_node. Keeping the self-route
    # makes the conditional explicit and lets the circuit breaker stop bad loops.
    return "exception_node"


def build_state_graph() -> StateGraph:
    graph = StateGraph(WorkflowState)
    graph.add_node("extract_node", extract_node)
    graph.add_node("match_node", match_node)
    graph.add_node("exception_node", exception_node)
    graph.add_node("approve_node", approve_node)
    graph.add_node("reject_node", reject_node)
    graph.add_node("complete_node", complete_node)

    graph.set_entry_point("extract_node")
    graph.add_edge("extract_node", "match_node")
    graph.add_conditional_edges(
        "match_node",
        route_after_match,
        {
            "exception_node": "exception_node",
            "complete_node": "complete_node",
        },
    )
    graph.add_conditional_edges(
        "exception_node",
        route_after_exception,
        {
            "approve_node": "approve_node",
            "reject_node": "reject_node",
            "exception_node": "exception_node",
            "complete_node": "complete_node",
        },
    )
    graph.add_edge("approve_node", "complete_node")
    graph.add_edge("reject_node", "complete_node")
    graph.add_edge("complete_node", END)
    return graph


class WorkflowGraph:
    @asynccontextmanager
    async def _compiled_graph(self) -> AsyncIterator[Any]:
        # PostgresSaver persists checkpoints keyed by thread_id, which is what
        # makes /resume work after an API process restart. The saver connection is
        # opened lazily so importing app.main does not require Postgres to be up.
        async with AsyncPostgresSaver.from_conn_string(checkpoint_database_url()) as saver:
            await saver.setup()
            yield build_state_graph().compile(checkpointer=saver)

    async def ainvoke(self, input_data: Any, config: dict[str, Any]) -> dict[str, Any]:
        async with self._compiled_graph() as graph:
            return await graph.ainvoke(input_data, config=config)

    async def aget_state(self, config: dict[str, Any]) -> Any:
        async with self._compiled_graph() as graph:
            return await graph.aget_state(config)


workflow_graph = WorkflowGraph()
