from datetime import date, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.load_numbers import normalize_load_number
from app.models.document import Document
from app.models.rate_confirmation import RateConfirmation
from app.services.shipment import rate_confirmation_extraction, upsert_shipment


router = APIRouter(prefix="/rate-confirmations", tags=["rate-confirmations"])


class RateConfirmationCreate(BaseModel):
    load_number: str
    carrier_name: str
    origin: str
    destination: str
    agreed_rate: float = Field(ge=0)
    currency: str = "USD"
    shipment_date: date

    @field_validator("load_number")
    @classmethod
    def normalize_load_number_value(cls, value: str) -> str:
        normalized = normalize_load_number(value)
        assert normalized is not None
        return normalized


class RateConfirmationRead(RateConfirmationCreate):
    id: UUID
    created_at: datetime

    model_config = {"from_attributes": True}


@router.post("", response_model=RateConfirmationRead, status_code=status.HTTP_201_CREATED)
async def create_rate_confirmation(
    body: RateConfirmationCreate,
    db: AsyncSession = Depends(get_session),
) -> RateConfirmation:
    rate_confirmation = RateConfirmation(**body.model_dump())
    db.add(rate_confirmation)
    try:
        await db.flush()
        extraction = rate_confirmation_extraction(rate_confirmation)
        # Shipments store document foreign keys, so manual rate confirmations get
        # a lightweight document row before reusing the normal assembly service.
        document = Document(
            filename=f"manual-rate-confirmation-{rate_confirmation.load_number}.json",
            doc_type="rate_confirmation",
            status="extracted",
            raw_text=None,
            extracted_data=extraction,
        )
        db.add(document)
        await db.flush()
        await upsert_shipment(rate_confirmation.load_number, document, extraction, db)
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Rate confirmation already exists for load_number",
        ) from exc
    await db.refresh(rate_confirmation)
    return rate_confirmation


@router.get("", response_model=list[RateConfirmationRead])
async def list_rate_confirmations(
    db: AsyncSession = Depends(get_session),
) -> list[RateConfirmation]:
    result = await db.execute(
        select(RateConfirmation).order_by(RateConfirmation.created_at.desc())
    )
    return list(result.scalars().all())
