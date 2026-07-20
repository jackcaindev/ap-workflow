import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from httpx import ASGITransport, AsyncClient

from app.core.config import Settings
from app.main import app
from app.schemas.health import (
    CheckStatus,
    LivenessResponse,
    OverallStatus,
    ProcessPhase,
    ReadinessResponse,
)
from app.services.health import RuntimeHealth, readiness
from app.services.gmail_poller import poll_inbox


NOW = datetime(2026, 7, 19, 16, 5, tzinfo=UTC)


async def available_probe() -> None:
    return None


def settings(tmp_path: Path, *, anthropic_key: str = "test-key") -> Settings:
    token_path = tmp_path / "token.json"
    token_path.write_text(
        json.dumps(
            {
                "token": "test-access-token",
                "refresh_token": "test-refresh-token",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "scopes": ["https://www.googleapis.com/auth/gmail.modify"],
            }
        ),
        encoding="utf-8",
    )
    return Settings(
        ANTHROPIC_API_KEY=anthropic_key,
        GMAIL_TOKEN_PATH=str(token_path),
        GMAIL_CREDENTIALS_PATH=str(tmp_path / "credentials.json"),
        HEALTH_PROBE_TIMEOUT_SECONDS=0.05,
    )


def healthy_runtime() -> RuntimeHealth:
    runtime = RuntimeHealth(phase=ProcessPhase.RUNNING, started_at=NOW)
    runtime.worker_started()
    for outcome in (runtime.gmail, runtime.notifications, runtime.sla_scanner):
        outcome.started(NOW)
        outcome.succeeded(NOW, 0)
    return runtime


async def collect(runtime: RuntimeHealth, config: Settings, *, clock=lambda: NOW, **kwargs):
    return await readiness(
        runtime,
        config,
        clock=clock,
        postgresql_probe=kwargs.get("postgresql_probe", available_probe),
        checkpoint_probe=kwargs.get("checkpoint_probe", available_probe),
        redis_probe=kwargs.get("redis_probe", available_probe),
    )


async def test_healthy_and_optional_degraded_classification(tmp_path):
    result = await collect(healthy_runtime(), settings(tmp_path))
    assert result.status is OverallStatus.READY
    assert result.ready

    degraded_settings = settings(tmp_path, anthropic_key="")
    Path(degraded_settings.GMAIL_TOKEN_PATH).unlink()
    result = await collect(healthy_runtime(), degraded_settings)
    assert result.status is OverallStatus.DEGRADED
    assert result.ready
    assert result.capabilities.gmail_ingestion.reason_code == "gmail_token_missing"
    assert result.capabilities.gmail_ingestion.verification == "configuration_only"
    assert result.capabilities.claude_processing.reason_code == "anthropic_not_configured"


async def test_core_timeout_is_bounded_and_unavailable(tmp_path):
    async def slow_probe():
        await asyncio.sleep(1)

    started = asyncio.get_running_loop().time()
    result = await collect(
        healthy_runtime(), settings(tmp_path), postgresql_probe=slow_probe
    )
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.2
    assert result.status is OverallStatus.UNAVAILABLE
    assert not result.ready
    assert result.dependencies.postgresql.reason_code == "probe_timeout"
    assert result.dependencies.checkpoints.reason_code == "postgresql_unavailable"


async def test_checkpoint_schema_failure_and_worker_starting_are_core_failures(tmp_path):
    async def missing_schema():
        raise LookupError("secret database detail")

    runtime = healthy_runtime()
    runtime.invoice_worker_status = CheckStatus.STARTING
    result = await collect(
        runtime, settings(tmp_path), checkpoint_probe=missing_schema
    )
    serialized = result.model_dump_json()

    assert result.status is OverallStatus.UNAVAILABLE
    assert result.dependencies.checkpoints.reason_code == "checkpoint_schema_missing"
    assert "secret database detail" not in serialized


async def test_redis_failure_and_stopping_phase_are_core_unavailable(tmp_path):
    async def redis_failure():
        raise RuntimeError("redis://user:secret@private-host/0")

    runtime = healthy_runtime()
    result = await collect(runtime, settings(tmp_path), redis_probe=redis_failure)
    assert not result.ready
    assert result.dependencies.redis.reason_code == "connection_failed"
    assert "private-host" not in result.model_dump_json()

    runtime.phase = ProcessPhase.STOPPING
    stopping = await collect(runtime, settings(tmp_path))
    assert not stopping.ready
    assert stopping.status is OverallStatus.UNAVAILABLE


async def test_background_failure_staleness_and_recovery(tmp_path):
    runtime = healthy_runtime()
    runtime.gmail.failed(NOW + timedelta(minutes=1), "operation_failed")
    failed = await collect(
        runtime,
        settings(tmp_path),
        clock=lambda: NOW + timedelta(minutes=2),
    )
    assert failed.capabilities.gmail_ingestion.status == "degraded"

    runtime.gmail.succeeded(NOW + timedelta(minutes=3), 4)
    recovered = await collect(
        runtime,
        settings(tmp_path),
        clock=lambda: NOW + timedelta(minutes=4),
    )
    assert recovered.capabilities.gmail_ingestion.status == "available"
    assert recovered.capabilities.gmail_ingestion.last_result_count == 4

    stale = await collect(
        runtime,
        settings(tmp_path),
        clock=lambda: NOW + timedelta(minutes=20),
    )
    assert stale.capabilities.gmail_ingestion.stale
    assert stale.capabilities.gmail_ingestion.reason_code == "background_success_stale"


async def test_poll_inbox_records_attempt_success_and_count(monkeypatch):
    class FakeRedis:
        async def aclose(self):
            return None

    monkeypatch.setattr(
        "app.services.gmail_poller.Redis.from_url", lambda *_args, **_kwargs: FakeRedis()
    )
    monkeypatch.setattr(
        "app.services.gmail_poller.get_gmail_service", lambda **_kwargs: object()
    )
    monkeypatch.setattr("app.services.gmail_poller._gmail_account", lambda _service: "demo@example.com")
    monkeypatch.setattr("app.services.gmail_poller._list_unread_messages", lambda _service: [])
    runtime = RuntimeHealth()

    assert await poll_inbox(runtime) == 0
    assert runtime.gmail.last_attempt_at is not None
    assert runtime.gmail.last_success_at is not None
    assert runtime.gmail.last_result_count == 0
    assert runtime.gmail.last_failure_at is None


def extract_readme_example(name: str) -> dict:
    readme = Path("README.md").read_text(encoding="utf-8")
    pattern = rf"<!-- {re.escape(name)}:start -->\s*```json\s*(.*?)\s*```\s*<!-- {re.escape(name)}:end -->"
    matches = re.findall(pattern, readme, flags=re.DOTALL)
    assert len(matches) == 1, f"expected exactly one README example named {name}"
    return json.loads(matches[0])


def test_readme_examples_are_the_public_typed_contract():
    live = LivenessResponse.model_validate(extract_readme_example("health-live"))
    assert live.status == "alive"

    expected = {
        "health-ready-healthy": (OverallStatus.READY, True),
        "health-ready-degraded": (OverallStatus.DEGRADED, True),
        "health-ready-unavailable": (OverallStatus.UNAVAILABLE, False),
    }
    forbidden = ("password", "api_key", "token.json", "postgresql://", "redis://")
    for name, (status, ready) in expected.items():
        raw = extract_readme_example(name)
        parsed = ReadinessResponse.model_validate(raw)
        assert (parsed.status, parsed.ready) == (status, ready)
        serialized = json.dumps(raw).lower()
        assert not any(secret in serialized for secret in forbidden)

    legacy = extract_readme_example("health-legacy")
    assert set(legacy) == {"status", "missing_document_sla_scanner"}


async def test_http_status_contract_and_legacy_shape(monkeypatch):
    healthy = ReadinessResponse.model_validate(
        extract_readme_example("health-ready-healthy")
    )
    unavailable = ReadinessResponse.model_validate(
        extract_readme_example("health-ready-unavailable")
    )
    app.state.runtime_health = RuntimeHealth(phase=ProcessPhase.RUNNING)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        monkeypatch.setattr("app.main.readiness", lambda *_: _async_value(healthy))
        assert (await client.get("/health/ready")).status_code == 200

        monkeypatch.setattr("app.main.readiness", lambda *_: _async_value(unavailable))
        response = await client.get("/health/ready")
        assert response.status_code == 503
        assert response.json()["status"] == "unavailable"

        legacy = await client.get("/health")
        assert legacy.status_code == 200
        assert set(legacy.json()) == {"status", "missing_document_sla_scanner"}


async def _async_value(value):
    return value
