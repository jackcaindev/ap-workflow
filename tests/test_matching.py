from app.services.matching import match_invoice


async def test_match_invoice_matched_case(db_session, seeded_rate_confirmations):
    extraction = {
        "doc_type": "invoice",
        "load_number": "LD-1001",
        "total_amount": 1500.0,
    }

    result = await match_invoice(extraction, db_session)

    assert result["matched"] is True
    assert result["agreed_rate"] == 1500.0
    assert result["invoiced_amount"] == 1500.0
    assert result["variance"] == 0.0
    assert "rate_con_id" in result


async def test_match_invoice_amount_variance_failure(db_session, seeded_rate_confirmations):
    extraction = {
        "doc_type": "invoice",
        "load_number": "LD-1001",
        "total_amount": 3000.0,
    }

    result = await match_invoice(extraction, db_session)

    assert result["matched"] is False
    assert result["reason"] == "amount_variance"
    assert result["agreed_rate"] == 1500.0
    assert result["invoiced_amount"] == 3000.0
    assert result["variance"] == 1500.0


async def test_match_invoice_no_load_number(db_session, seeded_rate_confirmations):
    extraction = {
        "doc_type": "invoice",
        "load_number": None,
        "total_amount": 1500.0,
    }

    result = await match_invoice(extraction, db_session)

    assert result == {"matched": False, "reason": "no_load_number"}


async def test_match_invoice_no_rate_con_found(db_session, seeded_rate_confirmations):
    extraction = {
        "doc_type": "invoice",
        "load_number": "LD-UNKNOWN",
        "total_amount": 1500.0,
    }

    result = await match_invoice(extraction, db_session)

    assert result["matched"] is False
    assert result["reason"] == "no_rate_con_found"
    assert result["load_number"] == "LD-UNKNOWN"
