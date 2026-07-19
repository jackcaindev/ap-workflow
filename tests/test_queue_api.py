from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from app.api.routes import queue_operations as queue_routes
from app.redis_client import get_redis
from app.schemas.queue_operations import ReplayResponse
from tests.conftest import test_app as _test_app


@pytest.fixture
def queue_redis_override():
    async def override():
        yield object()

    _test_app.dependency_overrides[get_redis] = override
    try:
        yield
    finally:
        _test_app.dependency_overrides.pop(get_redis, None)


async def test_replay_endpoint_returns_created_then_idempotent_status(
    client, monkeypatch, queue_redis_override
):
    request_id = str(uuid4())
    call_count = 0

    async def replay(*_args, **kwargs):
        nonlocal call_count
        call_count += 1
        now = datetime.now(UTC)
        return ReplayResponse(
            dlq_id=kwargs["dlq_id"],
            request_id=kwargs["request_id"],
            created=call_count == 1,
            replay_count=1,
            state="enqueued",
            requested_at=now,
            enqueued_at=now,
            live_stream_id="20-0",
        )

    monkeypatch.setattr(queue_routes, "replay_dlq_entry", replay)
    first = await client.post(
        "/operations/queue/dlq/10-0/replay",
        headers={"Idempotency-Key": request_id},
    )
    second = await client.post(
        "/operations/queue/dlq/10-0/replay",
        headers={"Idempotency-Key": request_id},
    )

    assert first.status_code == 201
    assert first.json()["created"] is True
    assert second.status_code == 200
    assert second.json()["created"] is False
    assert second.json()["live_stream_id"] == first.json()["live_stream_id"]


async def test_replay_endpoint_requires_uuid_idempotency_key(
    client, queue_redis_override
):
    missing = await client.post("/operations/queue/dlq/10-0/replay")
    malformed = await client.post(
        "/operations/queue/dlq/10-0/replay",
        headers={"Idempotency-Key": "not-a-uuid"},
    )
    assert missing.status_code == 422
    assert malformed.status_code == 422


async def test_purge_endpoint_requires_explicit_past_cutoff(
    client, queue_redis_override
):
    missing = await client.post("/operations/queue/dlq/purge", json={})
    future = await client.post(
        "/operations/queue/dlq/purge",
        json={"before": (datetime.now(UTC) + timedelta(days=1)).isoformat()},
    )
    naive = await client.post(
        "/operations/queue/dlq/purge",
        json={"before": "2026-01-01T00:00:00"},
    )
    assert missing.status_code == 422
    assert future.status_code == 422
    assert naive.status_code == 422
