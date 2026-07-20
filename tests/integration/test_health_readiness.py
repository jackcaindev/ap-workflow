import pytest

from app.core.config import get_settings
from app.schemas.health import CheckStatus, ProcessPhase
from app.services.health import RuntimeHealth, readiness


pytestmark = pytest.mark.integration


async def test_real_core_storage_and_queue_probes_are_ready():
    runtime = RuntimeHealth(phase=ProcessPhase.RUNNING)
    runtime.worker_started()

    result = await readiness(runtime, get_settings())

    assert result.ready
    assert result.dependencies.postgresql.status is CheckStatus.AVAILABLE
    assert result.dependencies.checkpoints.status is CheckStatus.AVAILABLE
    assert result.dependencies.redis.status is CheckStatus.AVAILABLE
