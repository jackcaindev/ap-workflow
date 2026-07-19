export type ProcessingStatus =
  | "pending"
  | "running"
  | "retrying"
  | "awaiting_review"
  | "complete"
  | "failed";

export type ReviewDisposition = "not_required" | "pending" | "approved" | "rejected" | "unknown";

export type PostingStatus =
  | "not_ready"
  | "ready_for_posting"
  | "posting"
  | "posted"
  | "payment_scheduled"
  | "paid"
  | "blocked"
  | "failed";

export type CheckOutcome = "passed" | "failed" | "not_evaluated";

export type BusinessState = {
  status: string;
  processing_status: ProcessingStatus;
  reconciliation_status: string | null;
  review_disposition: ReviewDisposition;
  posting_status: PostingStatus;
  reviewed_at: string | null;
  reviewer_id: string | null;
};

export function normalizeBusinessState<T extends { status: string }>(value: T): T & BusinessState {
  const raw = value as T & Partial<BusinessState>;
  let legacyProcessing: ProcessingStatus;
  if (["approved", "rejected", "partial", "reconciled", "complete"].includes(raw.status)) {
    legacyProcessing = "complete";
  } else if (["awaiting_review", "exception"].includes(raw.status)) {
    legacyProcessing = "awaiting_review";
  } else if (["pending", "running", "retrying", "failed"].includes(raw.status)) {
    legacyProcessing = raw.status as ProcessingStatus;
  } else {
    legacyProcessing = "pending";
  }
  const processingStatus = raw.processing_status ?? legacyProcessing;
  const reviewDisposition =
    raw.review_disposition ??
    (raw.status === "approved" || raw.status === "rejected"
      ? raw.status
      : processingStatus === "awaiting_review"
        ? "pending"
        : "not_required");
  const postingStatus =
    raw.posting_status ??
    (reviewDisposition === "approved" || raw.status === "reconciled"
      ? "ready_for_posting"
      : reviewDisposition === "rejected"
        ? "blocked"
        : "not_ready");
  return {
    ...value,
    processing_status: processingStatus,
    reconciliation_status: raw.reconciliation_status ?? null,
    review_disposition: reviewDisposition,
    posting_status: postingStatus,
    reviewed_at: raw.reviewed_at ?? null,
    reviewer_id: raw.reviewer_id ?? null,
  };
}

export function checkOutcome(check: {
  outcome?: CheckOutcome;
  passed?: boolean;
  details: string;
}): CheckOutcome {
  if (check.outcome) {
    return check.outcome;
  }
  if (check.passed === false) {
    return "failed";
  }
  if (check.details.startsWith("skipped:") || check.details.includes("within 3-day grace period")) {
    return "not_evaluated";
  }
  return check.passed === true ? "passed" : "not_evaluated";
}
