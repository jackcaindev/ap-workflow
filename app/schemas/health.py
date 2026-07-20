from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class OverallStatus(StrEnum):
    READY = "ready"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class ProcessPhase(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"


class CheckStatus(StrEnum):
    AVAILABLE = "available"
    STARTING = "starting"
    UNAVAILABLE = "unavailable"


class CapabilityStatus(StrEnum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    STARTING = "starting"
    UNAVAILABLE = "unavailable"


class VerificationKind(StrEnum):
    CONFIGURATION_ONLY = "configuration_only"
    OBSERVED_RUNTIME = "observed_runtime"
    CONFIGURATION_AND_OBSERVED_RUNTIME = "configuration_and_observed_runtime"


class DependencyCheck(BaseModel):
    status: CheckStatus
    reason_code: str | None = None
    latency_ms: float | None = None


class CapabilityCheck(BaseModel):
    status: CapabilityStatus
    reason_code: str | None = None
    verification: VerificationKind
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_result_count: int | None = None
    stale: bool = False


class DependencyChecks(BaseModel):
    postgresql: DependencyCheck
    checkpoints: DependencyCheck
    redis: DependencyCheck
    invoice_worker: DependencyCheck


class CapabilityChecks(BaseModel):
    gmail_ingestion: CapabilityCheck
    claude_processing: CapabilityCheck
    notifications: CapabilityCheck
    scheduled_sla_scanning: CapabilityCheck


class LivenessResponse(BaseModel):
    status: str = "alive"
    phase: ProcessPhase
    observed_at: datetime


class ReadinessResponse(BaseModel):
    status: OverallStatus
    ready: bool
    phase: ProcessPhase
    observed_at: datetime
    dependencies: DependencyChecks
    capabilities: CapabilityChecks
