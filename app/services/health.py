import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Awaitable, Callable

from google.oauth2.credentials import Credentials
from psycopg import AsyncConnection
from redis.asyncio import Redis
from sqlalchemy import text

from app import database
from app.core.config import Settings
from app.schemas.health import (
    CapabilityCheck,
    CapabilityChecks,
    CapabilityStatus,
    CheckStatus,
    DependencyCheck,
    DependencyChecks,
    LivenessResponse,
    OverallStatus,
    ProcessPhase,
    ReadinessResponse,
    VerificationKind,
)
from app.services.gmail_auth import SCOPES
from app.workflow.graph import checkpoint_database_url


Clock = Callable[[], datetime]
Probe = Callable[[], Awaitable[None]]
CHECKPOINT_TABLES = {
    "checkpoint_migrations",
    "checkpoints",
    "checkpoint_blobs",
    "checkpoint_writes",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class BackgroundOutcome:
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_result_count: int | None = None
    reason_code: str | None = None
    in_progress: bool = False

    def started(self, now: datetime) -> None:
        self.last_attempt_at = now
        self.in_progress = True

    def succeeded(self, now: datetime, count: int | None = None) -> None:
        self.last_success_at = now
        self.last_result_count = count
        self.reason_code = None
        self.in_progress = False

    def failed(self, now: datetime, reason_code: str) -> None:
        self.last_failure_at = now
        self.reason_code = reason_code
        self.in_progress = False


@dataclass
class RuntimeHealth:
    phase: ProcessPhase = ProcessPhase.STARTING
    started_at: datetime = field(default_factory=utc_now)
    invoice_worker_status: CheckStatus = CheckStatus.STARTING
    invoice_worker_reason: str | None = None
    gmail: BackgroundOutcome = field(default_factory=BackgroundOutcome)
    notifications: BackgroundOutcome = field(default_factory=BackgroundOutcome)
    sla_scanner: BackgroundOutcome = field(default_factory=BackgroundOutcome)

    def worker_started(self) -> None:
        self.invoice_worker_status = CheckStatus.AVAILABLE
        self.invoice_worker_reason = None

    def worker_failed(self, reason_code: str = "background_task_exited") -> None:
        self.invoice_worker_status = CheckStatus.UNAVAILABLE
        self.invoice_worker_reason = reason_code


def liveness(runtime: RuntimeHealth, *, clock: Clock = utc_now) -> LivenessResponse:
    return LivenessResponse(phase=runtime.phase, observed_at=clock())


async def probe_postgresql() -> None:
    async with database.engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def probe_checkpoints() -> None:
    connection = await AsyncConnection.connect(checkpoint_database_url())
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = ANY(%s)
                """,
                (list(CHECKPOINT_TABLES),),
            )
            rows = await cursor.fetchall()
            if {row[0] for row in rows} != CHECKPOINT_TABLES:
                raise LookupError("checkpoint schema missing")
            await cursor.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'checkpoint_writes'
                  AND column_name = 'task_path'
                """
            )
            if await cursor.fetchone() is None:
                raise LookupError("checkpoint schema missing")
    finally:
        await connection.close()


async def probe_redis(settings: Settings) -> None:
    client = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=settings.HEALTH_PROBE_TIMEOUT_SECONDS,
        socket_timeout=settings.HEALTH_PROBE_TIMEOUT_SECONDS,
    )
    try:
        await client.ping()
    finally:
        await client.aclose()


def inspect_gmail_token(settings: Settings) -> str | None:
    token_path = Path(settings.GMAIL_TOKEN_PATH)
    if not token_path.is_file() or token_path.stat().st_size == 0:
        return "gmail_token_missing"
    try:
        credentials = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    except (OSError, ValueError, TypeError):
        return "gmail_token_invalid"
    if not credentials.valid and not credentials.refresh_token:
        return "gmail_token_invalid"
    return None


def inspect_anthropic_configuration(settings: Settings) -> str | None:
    key = settings.ANTHROPIC_API_KEY.strip()
    if not key or key == "sk-ant-your-key-here":
        return "anthropic_not_configured"
    return None


async def _bounded_probe(probe: Probe, timeout: float) -> DependencyCheck:
    started = time.monotonic()
    try:
        async with asyncio.timeout(timeout):
            await probe()
    except TimeoutError:
        return DependencyCheck(
            status=CheckStatus.UNAVAILABLE,
            reason_code="probe_timeout",
            latency_ms=round(timeout * 1000, 1),
        )
    except LookupError:
        return DependencyCheck(
            status=CheckStatus.UNAVAILABLE,
            reason_code="checkpoint_schema_missing",
            latency_ms=round((time.monotonic() - started) * 1000, 1),
        )
    except Exception:
        return DependencyCheck(
            status=CheckStatus.UNAVAILABLE,
            reason_code="connection_failed",
            latency_ms=round((time.monotonic() - started) * 1000, 1),
        )
    return DependencyCheck(
        status=CheckStatus.AVAILABLE,
        latency_ms=round((time.monotonic() - started) * 1000, 1),
    )


async def _bounded_configuration_probe(
    probe: Callable[[], str | None], timeout: float, unavailable_code: str
) -> str | None:
    try:
        async with asyncio.timeout(timeout):
            return await asyncio.to_thread(probe)
    except TimeoutError:
        return "probe_timeout"
    except Exception:
        return unavailable_code


def _runtime_capability(
    outcome: BackgroundOutcome,
    *,
    now: datetime,
    stale_after_seconds: float | None,
    verification: VerificationKind,
    configuration_reason: str | None = None,
) -> CapabilityCheck:
    if configuration_reason is not None:
        status = CapabilityStatus.UNAVAILABLE
        reason = configuration_reason
        stale = False
        verification = VerificationKind.CONFIGURATION_ONLY
    else:
        reference = outcome.last_success_at or outcome.last_attempt_at
        stale = bool(
            stale_after_seconds is not None
            and reference is not None
            and (now - reference).total_seconds() > stale_after_seconds
        )
        if outcome.reason_code is not None:
            status = (
                CapabilityStatus.DEGRADED
                if outcome.last_success_at is not None
                else CapabilityStatus.UNAVAILABLE
            )
            reason = outcome.reason_code
        elif stale:
            status = CapabilityStatus.DEGRADED
            reason = "background_success_stale"
        elif outcome.last_success_at is None:
            status = CapabilityStatus.STARTING
            reason = None
        else:
            status = CapabilityStatus.AVAILABLE
            reason = None
    return CapabilityCheck(
        status=status,
        reason_code=reason,
        verification=verification,
        last_attempt_at=outcome.last_attempt_at,
        last_success_at=outcome.last_success_at,
        last_failure_at=outcome.last_failure_at,
        last_result_count=outcome.last_result_count,
        stale=stale,
    )


async def readiness(
    runtime: RuntimeHealth,
    settings: Settings,
    *,
    clock: Clock = utc_now,
    postgresql_probe: Probe | None = None,
    checkpoint_probe: Probe | None = None,
    redis_probe: Probe | None = None,
) -> ReadinessResponse:
    now = clock()
    timeout = settings.HEALTH_PROBE_TIMEOUT_SECONDS
    postgresql_probe = postgresql_probe or probe_postgresql
    checkpoint_probe = checkpoint_probe or probe_checkpoints
    redis_probe = redis_probe or (lambda: probe_redis(settings))
    postgresql, checkpoints, redis, gmail_reason, anthropic_reason = await asyncio.gather(
        _bounded_probe(postgresql_probe, timeout),
        _bounded_probe(checkpoint_probe, timeout),
        _bounded_probe(redis_probe, timeout),
        _bounded_configuration_probe(
            lambda: inspect_gmail_token(settings), timeout, "gmail_token_invalid"
        ),
        _bounded_configuration_probe(
            lambda: inspect_anthropic_configuration(settings),
            timeout,
            "anthropic_not_configured",
        ),
    )
    if postgresql.status is CheckStatus.UNAVAILABLE:
        checkpoints = DependencyCheck(
            status=CheckStatus.UNAVAILABLE,
            reason_code="postgresql_unavailable",
        )
    worker = DependencyCheck(
        status=runtime.invoice_worker_status,
        reason_code=runtime.invoice_worker_reason,
    )
    dependencies = DependencyChecks(
        postgresql=postgresql,
        checkpoints=checkpoints,
        redis=redis,
        invoice_worker=worker,
    )

    gmail_stale = settings.GMAIL_POLL_INTERVAL_SECONDS * settings.HEALTH_STALE_AFTER_MULTIPLIER
    sla_stale = (
        settings.MISSING_DOCUMENT_SCAN_INTERVAL_SECONDS
        * settings.HEALTH_STALE_AFTER_MULTIPLIER
    )
    gmail = _runtime_capability(
        runtime.gmail,
        now=now,
        stale_after_seconds=gmail_stale,
        verification=VerificationKind.CONFIGURATION_AND_OBSERVED_RUNTIME,
        configuration_reason=gmail_reason,
    )
    if gmail_reason is None and runtime.notifications.last_attempt_at is None:
        notifications = CapabilityCheck(
            status=CapabilityStatus.AVAILABLE,
            verification=VerificationKind.CONFIGURATION_ONLY,
        )
    else:
        notifications = _runtime_capability(
            runtime.notifications,
            now=now,
            stale_after_seconds=None,
            verification=VerificationKind.CONFIGURATION_AND_OBSERVED_RUNTIME,
            configuration_reason=gmail_reason,
        )
    sla = _runtime_capability(
        runtime.sla_scanner,
        now=now,
        stale_after_seconds=sla_stale,
        verification=VerificationKind.OBSERVED_RUNTIME,
    )
    if postgresql.status is CheckStatus.UNAVAILABLE:
        sla = sla.model_copy(
            update={
                "status": (
                    CapabilityStatus.DEGRADED
                    if runtime.sla_scanner.last_success_at is not None
                    else CapabilityStatus.UNAVAILABLE
                ),
                "reason_code": "postgresql_unavailable",
            }
        )
    claude = CapabilityCheck(
        status=(
            CapabilityStatus.UNAVAILABLE
            if anthropic_reason
            else CapabilityStatus.AVAILABLE
        ),
        reason_code=anthropic_reason,
        verification=VerificationKind.CONFIGURATION_ONLY,
    )
    capabilities = CapabilityChecks(
        gmail_ingestion=gmail,
        claude_processing=claude,
        notifications=notifications,
        scheduled_sla_scanning=sla,
    )

    dependency_values = (
        dependencies.postgresql,
        dependencies.checkpoints,
        dependencies.redis,
        dependencies.invoice_worker,
    )
    capability_values = (
        capabilities.gmail_ingestion,
        capabilities.claude_processing,
        capabilities.notifications,
        capabilities.scheduled_sla_scanning,
    )
    core_ready = runtime.phase is ProcessPhase.RUNNING and all(
        check.status is CheckStatus.AVAILABLE for check in dependency_values
    )
    if not core_ready:
        overall = OverallStatus.UNAVAILABLE
    elif any(
        capability.status is not CapabilityStatus.AVAILABLE
        for capability in capability_values
    ):
        overall = OverallStatus.DEGRADED
    else:
        overall = OverallStatus.READY
    return ReadinessResponse(
        status=overall,
        ready=core_ready,
        phase=runtime.phase,
        observed_at=now,
        dependencies=dependencies,
        capabilities=capabilities,
    )


def reason_code_for_exception(exc: BaseException) -> str:
    message = str(exc)
    if "No Gmail token found" in message:
        return "gmail_token_missing"
    return "operation_failed"
