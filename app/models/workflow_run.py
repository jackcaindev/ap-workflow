from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base
from app.models.review_decision import ReviewDecision


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    # API clients resume and inspect workflows by UUID, while the integer
    # primary key remains an internal database concern from the Phase 1 schema.
    run_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    document_id: Mapped[int] = mapped_column(
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # Persisted compatibility projection for existing rows, checkpoints, and
    # API clients. Current business logic uses the dimensions below plus the
    # immutable ReviewDecision relationship.
    status: Mapped[str] = mapped_column(nullable=False, default="pending")
    processing_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", server_default="pending"
    )
    posting_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="not_ready", server_default="not_ready"
    )
    # JSONB lets the workflow pause with structured context for a human approval
    # or retry path without locking the schema to one interrupt payload shape.
    interrupt_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    document = relationship("Document", back_populates="workflow_runs")
    review_decision: Mapped["ReviewDecision | None"] = relationship(
        "ReviewDecision",
        back_populates="workflow_run",
        uselist=False,
        cascade="all, delete-orphan",
    )
