from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models.notification import Notification


router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.get("")
async def notifications(db: AsyncSession = Depends(get_session)) -> list[dict]:
    result = await db.execute(
        select(Notification).order_by(Notification.sent_at.desc()).limit(10)
    )

    return [
        {
            "id": notification.id,
            "sent_at": notification.sent_at,
            "total_count": notification.total_count,
            "complete_count": notification.complete_count,
            "awaiting_review_count": notification.awaiting_review_count,
            "failed_count": notification.failed_count,
            "approved_count": notification.approved_count,
            "rejected_count": notification.rejected_count,
            "ready_for_posting_count": notification.ready_for_posting_count,
        }
        for notification in result.scalars()
    ]
