import { describe, expect, it } from "vitest";

import { checkOutcome, normalizeBusinessState } from "./businessState";


describe("business state compatibility", () => {
  it("preserves explicit multidimensional state", () => {
    const state = normalizeBusinessState({
      status: "approved",
      processing_status: "complete" as const,
      reconciliation_status: "exception",
      review_disposition: "approved" as const,
      posting_status: "ready_for_posting" as const,
      reviewed_at: "2026-07-19T12:00:00Z",
      reviewer_id: null,
    });

    expect(state.processing_status).toBe("complete");
    expect(state.review_disposition).toBe("approved");
    expect(state.posting_status).toBe("ready_for_posting");
  });

  it("adapts legacy reviewed runs", () => {
    const state = normalizeBusinessState({ status: "rejected" });

    expect(state.processing_status).toBe("complete");
    expect(state.review_disposition).toBe("rejected");
    expect(state.posting_status).toBe("blocked");
  });

  it("maps legacy reconciliation statuses onto processing and posting", () => {
    expect(normalizeBusinessState({ status: "reconciled" })).toMatchObject({
      processing_status: "complete",
      posting_status: "ready_for_posting",
      review_disposition: "not_required",
    });
    expect(normalizeBusinessState({ status: "exception" })).toMatchObject({
      processing_status: "awaiting_review",
      review_disposition: "pending",
    });
  });

  it("does not display skipped legacy checks as passed", () => {
    expect(checkOutcome({ passed: true, details: "skipped: amount missing" })).toBe(
      "not_evaluated",
    );
    expect(checkOutcome({ outcome: "failed", details: "variance" })).toBe("failed");
    expect(checkOutcome({ details: "malformed legacy check" })).toBe("not_evaluated");
  });
});
