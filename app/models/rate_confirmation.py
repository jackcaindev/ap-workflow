from datetime import date, datetime
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, Date, DateTime, Float, String, func
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class RateConfirmation(Base):
    __tablename__ = "rate_confirmations"
    __table_args__ = (
        CheckConstraint(
            "load_number = upper(regexp_replace(load_number, "
            "'^[[:space:]]+|[[:space:]]+$', '', 'g')) AND load_number <> ''",
            name="ck_rate_confirmations_load_number_normalized",
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    load_number: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    carrier_name: Mapped[str] = mapped_column(String, nullable=False)
    origin: Mapped[str] = mapped_column(String, nullable=False)
    destination: Mapped[str] = mapped_column(String, nullable=False)
    agreed_rate: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False, default="USD", server_default="USD")
    shipment_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
