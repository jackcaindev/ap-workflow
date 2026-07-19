from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from app.services.invoice_queue import ensure_consumer_group, read_new_batch
from app.services.queue_operations import (
    DLQNotReplayable,
    ReplayInProgress,
    list_dlq_entries,
    purge_dlq_entries,
    queue_metrics,
    replay_dlq_entry,
)


pytestmark = pytest.mark.integration


async def _add_dlq(redis_client, queue_names, job, *, reason: str = "provider outage"):
    original_fields = {"payload": job.model_dump_json()}
    dlq_id = await redis_client.xadd(
        queue_names.dead_letter_stream,
        {
            "original_stream_id": "1-0",
            "attempt_count": "3",
            "failed_at": datetime.now(UTC).isoformat(),
            "failure_reason": reason,
            "original_fields": json.dumps(original_fields, sort_keys=True),
        },
    )
    return dlq_id, original_fields


async def test_replay_is_atomic_idempotent_and_bypasses_stale_source_dedupe(
    redis_client, queue_names, make_integration_job
):
    job = make_integration_job("atomic-replay")
    dlq_id, original_fields = await _add_dlq(redis_client, queue_names, job)
    stale_stream_id = "1000-0"
    await redis_client.set(
        f"{queue_names.namespace}:dedupe:{job.idempotency_key}", stale_stream_id
    )
    request_id = str(uuid4())

    responses = await asyncio.gather(
        *(
            replay_dlq_entry(
                redis_client,
                dead_letter_stream=queue_names.dead_letter_stream,
                live_stream=queue_names.stream,
                replay_prefix=queue_names.replay_prefix,
                dlq_id=dlq_id,
                request_id=request_id,
            )
            for _ in range(12)
        )
    )

    assert len({response.live_stream_id for response in responses}) == 1
    assert sum(response.created for response in responses) == 1
    assert {response.replay_count for response in responses} == {1}
    live_rows = await redis_client.xrange(queue_names.stream, "-", "+")
    assert len(live_rows) == 1
    live_id, live_fields = live_rows[0]
    assert live_id != stale_stream_id
    assert live_fields == {
        "payload": job.model_dump_json(),
        "replay_dlq_id": dlq_id,
        "replay_request_id": request_id,
    }
    assert json.loads(original_fields["payload"])["idempotency_key"] == job.idempotency_key
    assert await redis_client.xrange(queue_names.dead_letter_stream, dlq_id, dlq_id)
    with pytest.raises(ReplayInProgress):
        await replay_dlq_entry(
            redis_client,
            dead_letter_stream=queue_names.dead_letter_stream,
            live_stream=queue_names.stream,
            replay_prefix=queue_names.replay_prefix,
            dlq_id=dlq_id,
            request_id=str(uuid4()),
        )


async def test_dlq_pagination_is_newest_first_stable_and_bounded(
    redis_client, queue_names, make_integration_job
):
    ids = []
    for index in range(5):
        dlq_id, _ = await _add_dlq(
            redis_client, queue_names, make_integration_job(f"page-{index}")
        )
        ids.append(dlq_id)

    first = await list_dlq_entries(
        redis_client,
        dead_letter_stream=queue_names.dead_letter_stream,
        replay_prefix=queue_names.replay_prefix,
        limit=2,
        cursor=None,
    )
    await _add_dlq(redis_client, queue_names, make_integration_job("new-after-page"))
    second = await list_dlq_entries(
        redis_client,
        dead_letter_stream=queue_names.dead_letter_stream,
        replay_prefix=queue_names.replay_prefix,
        limit=2,
        cursor=first.next_cursor,
    )

    assert [item.dlq_id for item in first.items] == list(reversed(ids[-2:]))
    assert [item.dlq_id for item in second.items] == list(reversed(ids[1:3]))
    assert set(item.dlq_id for item in first.items).isdisjoint(
        item.dlq_id for item in second.items
    )


async def test_malformed_dlq_entry_cannot_replay_and_remains_intact(
    redis_client, queue_names
):
    dlq_id = await redis_client.xadd(
        queue_names.dead_letter_stream,
        {
            "original_stream_id": "2-0",
            "attempt_count": "1",
            "failed_at": datetime.now(UTC).isoformat(),
            "failure_reason": "malformed",
            "original_fields": json.dumps({"payload": "{broken"}),
        },
    )
    before = await redis_client.xrange(queue_names.dead_letter_stream, dlq_id, dlq_id)

    with pytest.raises(DLQNotReplayable):
        await replay_dlq_entry(
            redis_client,
            dead_letter_stream=queue_names.dead_letter_stream,
            live_stream=queue_names.stream,
            replay_prefix=queue_names.replay_prefix,
            dlq_id=dlq_id,
            request_id=str(uuid4()),
        )

    assert await redis_client.xlen(queue_names.stream) == 0
    assert await redis_client.xrange(queue_names.dead_letter_stream, dlq_id, dlq_id) == before


async def test_retention_removes_only_entries_strictly_before_cutoff_and_metadata(
    redis_client, queue_names, make_integration_job
):
    now = datetime.now(UTC)
    old_ms = int((now - timedelta(days=3)).timestamp() * 1000)
    cutoff = now - timedelta(days=2)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    new_ms = int((now - timedelta(days=1)).timestamp() * 1000)
    fields = {
        "attempt_count": "3",
        "failed_at": now.isoformat(),
        "failure_reason": "failed",
        "original_fields": json.dumps(
            {"payload": make_integration_job("retention").model_dump_json()}, sort_keys=True
        ),
    }
    old_id = await redis_client.xadd(
        queue_names.dead_letter_stream,
        {**fields, "original_stream_id": "10-0"},
        id=f"{old_ms}-0",
    )
    boundary_id = await redis_client.xadd(
        queue_names.dead_letter_stream,
        {**fields, "original_stream_id": "11-0"},
        id=f"{cutoff_ms}-0",
    )
    new_id = await redis_client.xadd(
        queue_names.dead_letter_stream,
        {**fields, "original_stream_id": "12-0"},
        id=f"{new_ms}-0",
    )
    await redis_client.hset(f"{queue_names.replay_prefix}:{old_id}", mapping={"x": "1"})
    await redis_client.hset(f"{queue_names.metadata_prefix}:10-0", mapping={"x": "1"})

    count, has_more = await purge_dlq_entries(
        redis_client,
        dead_letter_stream=queue_names.dead_letter_stream,
        replay_prefix=queue_names.replay_prefix,
        metadata_prefix=queue_names.metadata_prefix,
        before=cutoff,
        max_delete=1000,
    )

    assert count == 1
    assert has_more is False
    assert await redis_client.xrange(queue_names.dead_letter_stream, old_id, old_id) == []
    assert await redis_client.xrange(queue_names.dead_letter_stream, boundary_id, boundary_id)
    assert await redis_client.xrange(queue_names.dead_letter_stream, new_id, new_id)
    assert not await redis_client.exists(f"{queue_names.replay_prefix}:{old_id}")
    assert not await redis_client.exists(f"{queue_names.metadata_prefix}:10-0")


async def test_queue_metrics_report_live_pending_oldest_age_and_dlq(
    redis_client, queue_names, make_integration_job
):
    await ensure_consumer_group(
        redis_client, stream=queue_names.stream, group=queue_names.group
    )
    job = make_integration_job("metrics")
    stream_id = await redis_client.xadd(
        queue_names.stream, {"payload": job.model_dump_json()}
    )
    await read_new_batch(
        redis_client,
        stream=queue_names.stream,
        group=queue_names.group,
        consumer="metrics-consumer",
        count=1,
        block_ms=10,
    )
    await _add_dlq(redis_client, queue_names, make_integration_job("metrics-dlq"))
    observed_at = datetime.now(UTC) + timedelta(seconds=2)

    metrics = await queue_metrics(
        redis_client,
        live_stream=queue_names.stream,
        group=queue_names.group,
        dead_letter_stream=queue_names.dead_letter_stream,
        now=observed_at,
    )

    assert metrics.live_stream_length == 1
    assert metrics.pending_count == 1
    assert metrics.dlq_count == 1
    expected_age = observed_at.timestamp() - int(stream_id.split("-", 1)[0]) / 1000
    assert metrics.oldest_pending_age_seconds == pytest.approx(expected_age, abs=0.01)
