from app.services.notifier import _build_summary_body, _notification_counts


def test_notification_summary_uses_business_dimensions():
    results = [
        {
            "filename": "approved.pdf",
            "run_id": "approved-run",
            "processing_status": "complete",
            "review_disposition": "approved",
            "posting_status": "ready_for_posting",
        },
        {
            "filename": "rejected.pdf",
            "run_id": "rejected-run",
            "processing_status": "complete",
            "review_disposition": "rejected",
            "posting_status": "blocked",
        },
        {
            "filename": "review.pdf",
            "run_id": "review-run",
            "processing_status": "awaiting_review",
            "review_disposition": "pending",
            "posting_status": "not_ready",
        },
    ]

    counts = _notification_counts(results)
    body = _build_summary_body(results)

    assert counts["complete_count"] == 2
    assert counts["approved_count"] == 1
    assert counts["rejected_count"] == 1
    assert counts["ready_for_posting_count"] == 1
    assert counts["awaiting_review_count"] == 1
    assert "Ready for Posting" in body
    assert "Rejected / Blocked" in body

