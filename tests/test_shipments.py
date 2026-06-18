from tests.conftest import create_shipment_with_documents


async def test_list_shipments(client, db_session, seeded_rate_confirmations):
    await create_shipment_with_documents(db_session, load_number="LD-1001")
    await create_shipment_with_documents(
        db_session,
        load_number="LD-2002",
        carrier_name="MIDWEST LOGISTICS",
        invoice_total=2200.0,
        agreed_rate=2200.0,
    )

    response = await client.get("/shipments")

    assert response.status_code == 200
    shipments = response.json()
    assert len(shipments) == 2
    load_numbers = {shipment["load_number"] for shipment in shipments}
    assert load_numbers == {"LD-1001", "LD-2002"}


async def test_shipment_detail_populates_manual_rate_con(
    client,
    db_session,
    seeded_rate_confirmations,
):
    shipment = await create_shipment_with_documents(
        db_session,
        manual_rate_con_only=True,
    )
    rate_con = seeded_rate_confirmations[0]

    response = await client.get(f"/shipments/{shipment.id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["load_number"] == shipment.load_number
    assert payload["has_rate_con"] is True

    rate_con_document = payload["documents"]["rate_con"]
    assert rate_con_document is not None
    assert rate_con_document["filename"] == "Manual Entry"
    assert rate_con_document["doc_type"] == "rate_confirmation"

    extracted = rate_con_document["extracted_data"]
    assert extracted["load_number"] == rate_con.load_number
    assert extracted["carrier_name"] == rate_con.carrier_name
    assert extracted["agreed_rate"] == rate_con.agreed_rate
    assert extracted["origin"] == rate_con.origin
    assert extracted["destination"] == rate_con.destination
    assert extracted["shipment_date"] == rate_con.shipment_date.isoformat()
