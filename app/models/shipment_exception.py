from datetime import datetime
from typing import Any, TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.reconciliation_result import ReconciliationResult
    from app.models.shipment import Shipment


class ShipmentException(Base):
    __tablename__ = "shipment_exceptions"
    __table_args__ = (
        UniqueConstraint("shipment_id", "kind", name="uq_shipment_exceptions_shipment_kind"),
        CheckConstraint(
            "kind = 'missing_required_documents'",
            name="ck_shipment_exceptions_kind",
        ),
        CheckConstraint(
            "status IN ('active', 'resolved')",
            name="ck_shipment_exceptions_status",
        ),
        Index("ix_shipment_exceptions_status", "status"),
    )

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    shipment_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True),
        ForeignKey("shipments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    kind: Mapped[str] = mapped_column(
        String(64), nullable=False, default="missing_required_documents"
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    missing_docs: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    reason_codes: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    shipment: Mapped["Shipment"] = relationship("Shipment", back_populates="exceptions")
    events: Mapped[list["ShipmentExceptionEvent"]] = relationship(
        "ShipmentExceptionEvent",
        back_populates="exception",
        cascade="all, delete-orphan",
        order_by="ShipmentExceptionEvent.version",
    )


class ShipmentExceptionEvent(Base):
    __tablename__ = "shipment_exception_events"
    __table_args__ = (
        UniqueConstraint("exception_id", "version", name="uq_shipment_exception_events_version"),
        CheckConstraint(
            "transition IN ('opened', 'changed', 'resolved')",
            name="ck_shipment_exception_events_transition",
        ),
        CheckConstraint(
            "notification_status IN ('pending', 'sending', 'sent', 'failed')",
            name="ck_shipment_exception_events_notification_status",
        ),
        Index("ix_shipment_exception_events_notification_status", "notification_status"),
    )

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    exception_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True),
        ForeignKey("shipment_exceptions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    reconciliation_result_id: Mapped[UUID | None] = mapped_column(
        PostgresUUID(as_uuid=True),
        ForeignKey("reconciliation_results.id", ondelete="SET NULL"),
        index=True,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    transition: Mapped[str] = mapped_column(String(16), nullable=False)
    before_state: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    after_state: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    notification_status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="pending", server_default="pending"
    )
    notification_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    notification_last_error: Mapped[str | None] = mapped_column(Text)
    notification_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    exception: Mapped["ShipmentException"] = relationship(
        "ShipmentException", back_populates="events"
    )
    reconciliation_result: Mapped["ReconciliationResult | None"] = relationship(
        "ReconciliationResult"
    )
