from enum import StrEnum


class ProcessingStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    AWAITING_REVIEW = "awaiting_review"
    COMPLETE = "complete"
    FAILED = "failed"


class ReconciliationStatus(StrEnum):
    PENDING = "pending"
    PARTIAL = "partial"
    RECONCILED = "reconciled"
    EXCEPTION = "exception"


class ReviewDisposition(StrEnum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class PostingStatus(StrEnum):
    NOT_READY = "not_ready"
    READY_FOR_POSTING = "ready_for_posting"
    POSTING = "posting"
    POSTED = "posted"
    PAYMENT_SCHEDULED = "payment_scheduled"
    PAID = "paid"
    BLOCKED = "blocked"
    FAILED = "failed"

