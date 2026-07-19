from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.workflow_audit_log import WorkflowAuditLog


async def log_audit_event(
    run_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    actor: str = "system",
    db: AsyncSession | None = None,
) -> None:
    statement = (
        insert(WorkflowAuditLog)
        .values(
            run_id=run_id,
            event_type=event_type,
            payload=payload,
            actor=actor,
        )
        .on_conflict_do_nothing(index_elements=["run_id", "event_type"])
    )
    if db is not None:
        await db.execute(statement)
        return

    async with AsyncSessionLocal() as owned_db:
        await owned_db.execute(statement)
        await owned_db.commit()
