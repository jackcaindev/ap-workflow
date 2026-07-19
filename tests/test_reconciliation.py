from app.services.reconciliation import reconcile_shipment

from tests.conftest import create_shipment_with_documents, old_shipment_timestamp


async def test_reconcile_shipment_fully_reconciled(db_session, seeded_rate_confirmations):
    shipment = await create_shipment_with_documents(db_session)

    result = await reconcile_shipment(shipment, db_session)

    assert shipment.reconciliation_status == "reconciled"
    assert result.exception_reasons == []
    assert result.missing_docs == []
    assert all(check["outcome"] == "passed" for check in result.checks)


async def test_reconcile_shipment_amount_variance_exception(db_session, seeded_rate_confirmations):
    shipment = await create_shipment_with_documents(
        db_session,
        invoice_total=3000.0,
        agreed_rate=1500.0,
    )

    result = await reconcile_shipment(shipment, db_session)

    assert shipment.reconciliation_status == "exception"
    assert "amount_variance" in result.exception_reasons


async def test_reconcile_shipment_missing_pod_exception(db_session, seeded_rate_confirmations):
    shipment = await create_shipment_with_documents(
        db_session,
        include_pod=False,
        created_at=old_shipment_timestamp(),
    )

    result = await reconcile_shipment(shipment, db_session)

    assert shipment.reconciliation_status == "exception"
    assert "missing_required_pod_sla_exceeded" in result.exception_reasons


async def test_reconcile_shipment_partial_docs(db_session, seeded_rate_confirmations):
    shipment = await create_shipment_with_documents(
        db_session,
        include_bol=False,
        include_pod=False,
    )

    result = await reconcile_shipment(shipment, db_session)

    assert shipment.reconciliation_status == "partial"
    assert "bol" in result.missing_docs
    assert "pod" in result.missing_docs
    assert result.exception_reasons == []
    assert all("passed" not in check for check in result.checks)
    not_evaluated = {
        check["check_name"]
        for check in result.checks
        if check["outcome"] == "not_evaluated"
    }
    assert {"bol_pickup_date", "pod_delivery_confirmation", "missing_docs"} <= not_evaluated
