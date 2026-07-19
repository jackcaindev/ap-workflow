from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from app.services.invoice_queue import (
    StreamDelivery,
    acknowledge,
    claim_stale_batch,
    dead_letter,
    ensure_consumer_group,
    read_new_batch,
    record_failure,
)
from app.services.queue_operations import replay_dlq_entry


pytestmark = pytest.mark.integration


async def test_aof_restart_preserves_pending_retry_and_dlq_state(
    redis_client,
    queue_names,
    make_integration_job,
    wait_for_condition,
):
    appendonly = await redis_client.config_get("appendonly")
    appendfsync = await redis_client.config_get("appendfsync")
    persistence = await redis_client.info("persistence")
    assert appendonly["appendonly"] == "yes"
    assert appendfsync["appendfsync"] == "always"
    assert persistence["aof_enabled"] == 1

    await ensure_consumer_group(
        redis_client, stream=queue_names.stream, group=queue_names.group
    )
    pending_job = make_integration_job("aof-pending")
    pending_id = await redis_client.xadd(
        queue_names.stream, {"payload": pending_job.model_dump_json()}
    )
    initial = await read_new_batch(
        redis_client,
        stream=queue_names.stream,
        group=queue_names.group,
        consumer="original-owner",
        count=1,
        block_ms=100,
    )
    assert initial[0].stream_id == pending_id

    retry_delivery = (
        await claim_stale_batch(
            redis_client,
            stream=queue_names.stream,
            group=queue_names.group,
            consumer="retry-owner",
            min_idle_ms=0,
            count=1,
        )
    )[0]
    assert retry_delivery.attempt == 2
    await record_failure(
        redis_client,
        metadata_prefix=queue_names.metadata_prefix,
        delivery=retry_delivery,
        reason="deterministic transient failure",
    )
    metadata_key = f"{queue_names.metadata_prefix}:{pending_id}"

    terminal_id = await redis_client.xadd(
        queue_names.stream, {"payload": "{malformed-json"}
    )
    terminal_delivery = (
        await read_new_batch(
            redis_client,
            stream=queue_names.stream,
            group=queue_names.group,
            consumer="dlq-owner",
            count=1,
            block_ms=100,
        )
    )[0]
    assert terminal_delivery.stream_id == terminal_id
    await dead_letter(
        redis_client,
        stream=queue_names.stream,
        group=queue_names.group,
        dead_letter_stream=queue_names.dead_letter_stream,
        delivery=terminal_delivery,
        reason="payload is not valid JSON",
    )

    replay_job = make_integration_job("aof-replay")
    replay_dlq_id = await redis_client.xadd(
        queue_names.dead_letter_stream,
        {
            "original_stream_id": "aof-replay-original-0",
            "attempt_count": "3",
            "failed_at": datetime.now(UTC).isoformat(),
            "failure_reason": "replay after remediation",
            "original_fields": json.dumps(
                {"payload": replay_job.model_dump_json()}, sort_keys=True
            ),
        },
    )
    replay = await replay_dlq_entry(
        redis_client,
        dead_letter_stream=queue_names.dead_letter_stream,
        live_stream=queue_names.stream,
        replay_prefix=queue_names.replay_prefix,
        dlq_id=replay_dlq_id,
        request_id=str(uuid4()),
    )
    replay_metadata_key = f"{queue_names.replay_prefix}:{replay_dlq_id}"
    replay_live_before = await redis_client.xrange(
        queue_names.stream, replay.live_stream_id, replay.live_stream_id
    )
    replay_metadata_before = await redis_client.hgetall(replay_metadata_key)

    source_before = await redis_client.xrange(
        queue_names.stream, pending_id, pending_id
    )
    pending_before = await redis_client.xpending_range(
        queue_names.stream, queue_names.group, pending_id, pending_id, 1
    )
    metadata_before = await redis_client.hgetall(metadata_key)
    dlq_before = await redis_client.xrange(queue_names.dead_letter_stream, "-", "+")
    assert source_before
    assert pending_before[0]["consumer"] == "retry-owner"
    assert pending_before[0]["times_delivered"] == 2
    assert metadata_before["attempt_count"] == "2"
    assert len(dlq_before) == 2
    assert replay_live_before
    assert replay_metadata_before["last_live_stream_id"] == replay.live_stream_id

    compose_file = os.environ["AP_WORKFLOW_INTEGRATION_COMPOSE_FILE"]
    compose_project = os.environ["AP_WORKFLOW_INTEGRATION_COMPOSE_PROJECT"]
    restart = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "-p",
        compose_project,
        "-f",
        compose_file,
        "restart",
        "redis",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await restart.communicate()
    assert restart.returncode == 0, (
        f"Redis restart failed.\nstdout:\n{stdout.decode()}\nstderr:\n{stderr.decode()}"
    )

    port_lookup = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "-p",
        compose_project,
        "-f",
        compose_file,
        "port",
        "redis",
        "6379",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    port_stdout, port_stderr = await port_lookup.communicate()
    assert port_lookup.returncode == 0, port_stderr.decode()
    redis_port = port_stdout.decode().strip().rsplit(":", 1)[-1]
    post_restart_url = f"redis://127.0.0.1:{redis_port}/0"
    # Anonymous host ports can change across a Docker container restart. Update
    # the test process and any later crash subprocesses to the new published port.
    os.environ["REDIS_URL"] = post_restart_url

    post_restart_client: Redis | None = None

    async def redis_ready():
        nonlocal post_restart_client
        candidate = Redis.from_url(post_restart_url, decode_responses=True)
        try:
            if await candidate.ping():
                post_restart_client = candidate
                return True
        except Exception:
            await candidate.aclose()
            return False
        await candidate.aclose()
        return False

    try:
        await wait_for_condition(
            redis_ready,
            timeout=20,
            interval=0.05,
            description="Redis readiness after AOF restart",
        )
    except AssertionError as exc:
        diagnostics = await asyncio.create_subprocess_exec(
            "docker",
            "compose",
            "-p",
            compose_project,
            "-f",
            compose_file,
            "logs",
            "--no-color",
            "redis",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        logs, _ = await diagnostics.communicate()
        raise AssertionError(f"{exc}\nRedis logs:\n{logs.decode()}") from exc

    assert post_restart_client is not None
    client = post_restart_client

    assert await client.xrange(
        queue_names.stream, pending_id, pending_id
    ) == source_before
    pending_after = await client.xpending_range(
        queue_names.stream, queue_names.group, pending_id, pending_id, 1
    )
    assert pending_after[0]["consumer"] == pending_before[0]["consumer"]
    assert pending_after[0]["times_delivered"] == pending_before[0]["times_delivered"]
    assert await client.hgetall(metadata_key) == metadata_before
    assert await client.xrange(
        queue_names.dead_letter_stream, "-", "+"
    ) == dlq_before
    assert await client.xrange(
        queue_names.stream, replay.live_stream_id, replay.live_stream_id
    ) == replay_live_before
    assert await client.hgetall(replay_metadata_key) == replay_metadata_before

    reclaimed = await claim_stale_batch(
        client,
        stream=queue_names.stream,
        group=queue_names.group,
        consumer="post-restart-owner",
        min_idle_ms=0,
        count=1,
    )
    assert reclaimed[0].stream_id == pending_id
    await acknowledge(
        client,
        stream=queue_names.stream,
        group=queue_names.group,
        stream_id=pending_id,
    )
    assert await client.xpending_range(
        queue_names.stream, queue_names.group, "-", "+", 10
    ) == []
    keys = [key async for key in client.scan_iter(f"{queue_names.namespace}*")]
    if keys:
        await client.delete(*keys)
    await client.aclose()
