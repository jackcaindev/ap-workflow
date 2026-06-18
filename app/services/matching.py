from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.rate_confirmation import RateConfirmation


RATE_TOLERANCE = 0.05


async def match_invoice(extraction: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    doc_type = extraction.get("doc_type")
    if doc_type and doc_type != "invoice":
        return {
            "matched": True,
            "skipped": True,
            "reason": "not_invoice",
            "doc_type": doc_type,
        }

    load_number = extraction.get("load_number")
    if not load_number:
        return {"matched": False, "reason": "no_load_number"}

    result = await db.execute(
        select(RateConfirmation).where(RateConfirmation.load_number == load_number)
    )
    rate_confirmation = result.scalar_one_or_none()
    if rate_confirmation is None:
        return {
            "matched": False,
            "reason": "no_rate_con_found",
            "load_number": load_number,
        }

    invoiced_amount = float(extraction["total_amount"])
    agreed_rate = float(rate_confirmation.agreed_rate)
    variance = invoiced_amount - agreed_rate

    # Matching tolerates small invoice/rate-confirmation differences because
    # fuel, accessorials, and extraction rounding can create harmless drift.
    # Keeping the 5% variance rule in this service keeps business policy out of
    # the LangGraph node, whose job is only to route workflow state.
    within_tolerance = abs(variance) <= abs(agreed_rate) * RATE_TOLERANCE

    if within_tolerance:
        return {
            "matched": True,
            "rate_con_id": str(rate_confirmation.id),
            "agreed_rate": rate_confirmation.agreed_rate,
            "invoiced_amount": invoiced_amount,
            "variance": variance,
        }

    return {
        "matched": False,
        "reason": "amount_variance",
        "agreed_rate": rate_confirmation.agreed_rate,
        "invoiced_amount": invoiced_amount,
        "variance": variance,
    }
