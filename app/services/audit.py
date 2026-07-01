from typing import Any

from app.database import AsyncSessionLocal
from app.models.workflow_audit_log import WorkflowAuditLog


async def log_audit_event(
    run_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    actor: str = "system",
) -> None:
    async with AsyncSessionLocal() as db:
        db.add(
            WorkflowAuditLog(
                run_id=run_id,
                event_type=event_type,
                payload=payload,
                actor=actor,
            )
        )
        await db.commit()
