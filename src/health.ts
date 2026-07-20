export type OverallHealthStatus = "ready" | "degraded" | "unavailable";
export type CapabilityStatus = "available" | "degraded" | "starting" | "unavailable";

export type CapabilityCheck = {
  status: CapabilityStatus;
  reason_code: string | null;
  verification: string;
  last_attempt_at: string | null;
  last_success_at: string | null;
  last_failure_at: string | null;
  last_result_count: number | null;
  stale: boolean;
};

export type ReadinessResponse = {
  status: OverallHealthStatus;
  ready: boolean;
  phase: "starting" | "running" | "stopping";
  observed_at: string;
  dependencies: Record<string, { status: string; reason_code: string | null; latency_ms: number | null }>;
  capabilities: {
    gmail_ingestion: CapabilityCheck;
    claude_processing: CapabilityCheck;
    notifications: CapabilityCheck;
    scheduled_sla_scanning: CapabilityCheck;
  };
};

export function healthLabel(status: OverallHealthStatus | null): string {
  if (status === "ready") return "System ready";
  if (status === "degraded") return "System degraded";
  if (status === "unavailable") return "System unavailable";
  return "System unknown";
}

export function healthDotClass(status: OverallHealthStatus | null): string {
  if (status === "ready") return "bg-emerald-400";
  if (status === "degraded") return "bg-amber-400";
  if (status === "unavailable") return "bg-red-400";
  return "bg-slate-500";
}

export function safeReasonLabel(reason: string | null): string | null {
  if (!reason) return null;
  const labels: Record<string, string> = {
    anthropic_not_configured: "not configured",
    background_success_stale: "stale",
    background_task_exited: "task stopped",
    connection_failed: "connection failed",
    gmail_token_invalid: "token invalid",
    gmail_token_missing: "not authenticated",
    operation_failed: "last attempt failed",
    postgresql_unavailable: "database unavailable",
    probe_timeout: "timed out",
    scan_failed: "last scan failed",
  };
  return labels[reason] ?? "unavailable";
}
