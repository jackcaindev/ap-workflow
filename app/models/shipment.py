from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.document import Document
    from app.models.reconciliation_result import ReconciliationResult


class Shipment(Base):
    __tablename__ = "shipments"
    __table_args__ = (
        CheckConstraint(
            "load_number = upper(regexp_replace(load_number, "
            "'^[[:space:]]+|[[:space:]]+$', '', 'g')) AND load_number <> ''",
            name="ck_shipments_load_number_normalized",
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    load_number: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    carrier_name: Mapped[str | None] = mapped_column(String)
    has_invoice: Mapped[bool] = mapped_column(default=False, server_default="false", nullable=False)
    has_rate_con: Mapped[bool] = mapped_column(default=False, server_default="false", nullable=False)
    has_bol: Mapped[bool] = mapped_column(default=False, server_default="false", nullable=False)
    has_pod: Mapped[bool] = mapped_column(default=False, server_default="false", nullable=False)
    invoice_id: Mapped[int | None] = mapped_column(ForeignKey("documents.id", ondelete="SET NULL"))
    rate_con_id: Mapped[int | None] = mapped_column(ForeignKey("documents.id", ondelete="SET NULL"))
    bol_id: Mapped[int | None] = mapped_column(ForeignKey("documents.id", ondelete="SET NULL"))
    pod_id: Mapped[int | None] = mapped_column(ForeignKey("documents.id", ondelete="SET NULL"))
    reconciliation_status: Mapped[str] = mapped_column(
        String,
        default="pending",
        server_default="pending",
        nullable=False,
    )
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

    documents: Mapped[list["Document"]] = relationship(
        "Document",
        back_populates="shipment",
        foreign_keys="Document.shipment_id",
    )
    invoice: Mapped["Document | None"] = relationship(
        "Document",
        foreign_keys=[invoice_id],
        post_update=True,
    )
    rate_con: Mapped["Document | None"] = relationship(
        "Document",
        foreign_keys=[rate_con_id],
        post_update=True,
    )
    bol: Mapped["Document | None"] = relationship(
        "Document",
        foreign_keys=[bol_id],
        post_update=True,
    )
    pod: Mapped["Document | None"] = relationship(
        "Document",
        foreign_keys=[pod_id],
        post_update=True,
    )
    reconciliation_results: Mapped[list["ReconciliationResult"]] = relationship(
        "ReconciliationResult",
        back_populates="shipment",
        cascade="all, delete-orphan",
    )
