from datetime import datetime
from typing import Any, TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.shipment import Shipment


class ReconciliationResult(Base):
    __tablename__ = "reconciliation_results"
    __table_args__ = (
        Index(
            "uq_reconciliation_results_run_id",
            "run_id",
            unique=True,
            postgresql_where=text("run_id IS NOT NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    shipment_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True),
        ForeignKey("shipments.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    # workflow_runs.run_id is the existing public UUID string for workflow runs;
    # the table's integer primary key remains an internal Phase 1 artifact.
    run_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("workflow_runs.run_id", ondelete="SET NULL"),
        index=True,
    )
    checks: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    missing_docs: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    exception_reasons: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    shipment: Mapped["Shipment"] = relationship(
        "Shipment",
        back_populates="reconciliation_results",
    )
