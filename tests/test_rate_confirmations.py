from sqlalchemy import select

from app.models.document import Document
from app.models.shipment import Shipment


def _payload(load_number: str) -> dict:
    return {
        "load_number": load_number,
        "carrier_name": "ACME FREIGHT",
        "origin": "Chicago, IL",
        "destination": "Dallas, TX",
        "agreed_rate": 1500.0,
        "currency": "USD",
        "shipment_date": "2026-07-19",
    }


async def test_manual_rate_confirmation_commits_normalized_shipment_attachment(
    client,
    db_session,
):
    response = await client.post(
        "/rate-confirmations", json=_payload("  manual-load  ")
    )

    assert response.status_code == 201
    assert response.json()["load_number"] == "MANUAL-LOAD"
    shipment = await db_session.scalar(
        select(Shipment).where(Shipment.load_number == "MANUAL-LOAD")
    )
    assert shipment is not None
    assert shipment.has_rate_con is True
    assert shipment.rate_con_id is not None
    document = await db_session.get(Document, shipment.rate_con_id)
    assert document is not None
    assert document.shipment_id == shipment.id

    duplicate = await client.post(
        "/rate-confirmations", json=_payload("manual-load")
    )
    assert duplicate.status_code == 409
