from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(primary_key=True)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    total_count: Mapped[int] = mapped_column(nullable=False)
    complete_count: Mapped[int] = mapped_column(nullable=False)
    awaiting_review_count: Mapped[int] = mapped_column(nullable=False)
    failed_count: Mapped[int] = mapped_column(nullable=False)
