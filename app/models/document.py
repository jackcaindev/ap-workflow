from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class Document(Base):
    __tablename__ = "documents"

    # Integer primary keys are enough for the first internal workflow service and
    # keep the initial migration simple; external document identifiers can be
    # added separately when integration requirements are clearer.
    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(nullable=False)
    # Valid doc_type values are enforced by the Pydantic extraction schema.
    # Keeping the database column as String avoids PostgreSQL enum migrations
    # whenever the document taxonomy changes.
    doc_type: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(nullable=False, default="received")
    raw_text: Mapped[str | None] = mapped_column(Text)
    extracted_data: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    source_type: Mapped[str | None] = mapped_column(String(32))
    source_idempotency_key: Mapped[str | None] = mapped_column(
        String(64), unique=True, index=True
    )
    source_message_id: Mapped[str | None] = mapped_column(String(255))
    source_part_id: Mapped[str | None] = mapped_column(String(255))
    source_enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    content_sha256: Mapped[str | None] = mapped_column(String(64))
    shipment_id: Mapped[UUID | None] = mapped_column(
        PostgresUUID(as_uuid=True),
        ForeignKey("shipments.id", ondelete="SET NULL"),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    workflow_runs = relationship(
        "WorkflowRun",
        back_populates="document",
        cascade="all, delete-orphan",
    )
    shipment = relationship(
        "Shipment",
        back_populates="documents",
        foreign_keys=[shipment_id],
    )
