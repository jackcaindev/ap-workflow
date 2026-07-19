from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.workflow_run import WorkflowRun


class ReviewDecision(Base):
    __tablename__ = "review_decisions"
    __table_args__ = (
        CheckConstraint(
            "disposition IN ('approved', 'rejected')",
            name="ck_review_decisions_disposition",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    run_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("workflow_runs.run_id", ondelete="CASCADE"),
        unique=True,
        index=True,
        nullable=False,
    )
    disposition: Mapped[str] = mapped_column(String(16), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    reviewer_id: Mapped[str | None] = mapped_column(String(255))

    workflow_run: Mapped["WorkflowRun"] = relationship(
        "WorkflowRun", back_populates="review_decision"
    )

