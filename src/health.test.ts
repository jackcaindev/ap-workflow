import { describe, expect, it } from "vitest";

import { healthDotClass, healthLabel, safeReasonLabel } from "./health";


describe("health presentation", () => {
  it("maps overall states to concise labels and colors", () => {
    expect(healthLabel("ready")).toBe("System ready");
    expect(healthLabel("degraded")).toBe("System degraded");
    expect(healthLabel("unavailable")).toBe("System unavailable");
    expect(healthDotClass("degraded")).toBe("bg-amber-400");
  });

  it("renders only whitelisted reason labels", () => {
    expect(safeReasonLabel("gmail_token_missing")).toBe("not authenticated");
    expect(safeReasonLabel("postgresql://secret@db")).toBe("unavailable");
  });
});
