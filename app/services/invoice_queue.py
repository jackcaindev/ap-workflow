import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.schemas.invoice_job import InvoiceJobEnvelope, PermanentJobError


ENQUEUE_SCRIPT = """
local existing = redis.call('GET', KEYS[2])
if existing then
  return existing
end
local stream_id = redis.call('XADD', KEYS[1], '*', 'payload', ARGV[1])
redis.call('SET', KEYS[2], stream_id, 'EX', ARGV[2])
return stream_id
"""


@dataclass(frozen=True)
class StreamDelivery:
    stream_id: str
    fields: dict[str, str]
    attempt: int = 1


def parse_delivery(delivery: StreamDelivery) -> InvoiceJobEnvelope:
    raw_payload = delivery.fields.get("payload")
    if raw_payload is None:
        raise PermanentJobError("missing payload field")
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise PermanentJobError(f"payload is not valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise PermanentJobError("payload must be a JSON object")
    try:
        return InvoiceJobEnvelope.model_validate(payload)
    except Exception as exc:
        raise PermanentJobError(f"payload validation failed: {exc}") from exc


async def ensure_consumer_group(redis: Redis, *, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def enqueue_job(
    redis: Redis,
    *,
    stream: str,
    dedupe_prefix: str,
    dedupe_ttl_seconds: int,
    job: InvoiceJobEnvelope,
) -> str:
    payload = job.model_dump_json()
    return await redis.eval(
        ENQUEUE_SCRIPT,
        2,
        stream,
        f"{dedupe_prefix}:{job.idempotency_key}",
        payload,
        dedupe_ttl_seconds,
    )


def _flatten_read_response(response: list[Any]) -> list[StreamDelivery]:
    deliveries: list[StreamDelivery] = []
    for _, entries in response:
        for stream_id, fields in entries:
            deliveries.append(StreamDelivery(stream_id=stream_id, fields=fields))
    return deliveries


async def read_new_batch(
    redis: Redis,
    *,
    stream: str,
    group: str,
    consumer: str,
    count: int,
    block_ms: int,
) -> list[StreamDelivery]:
    response = await redis.xreadgroup(
        group,
        consumer,
        streams={stream: ">"},
        count=count,
        block=block_ms,
    )
    return _flatten_read_response(response or [])


async def claim_stale_batch(
    redis: Redis,
    *,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int,
    count: int,
) -> list[StreamDelivery]:
    response = await redis.xautoclaim(
        stream,
        group,
        consumer,
        min_idle_ms,
        start_id="0-0",
        count=count,
    )
    entries = response[1] if response and len(response) > 1 else []
    deliveries: list[StreamDelivery] = []
    for stream_id, fields in entries:
        pending = await redis.xpending_range(stream, group, stream_id, stream_id, 1)
        attempt = int(pending[0].get("times_delivered", 1)) if pending else 1
        deliveries.append(StreamDelivery(stream_id, fields, attempt))
    return deliveries


async def record_failure(
    redis: Redis,
    *,
    metadata_prefix: str,
    delivery: StreamDelivery,
    reason: str,
) -> None:
    await redis.hset(
        f"{metadata_prefix}:{delivery.stream_id}",
        mapping={
            "state": "retrying",
            "attempt_count": delivery.attempt,
            "last_failure_reason": reason[:2000],
            "updated_at": datetime.now(UTC).isoformat(),
        },
    )


async def acknowledge(
    redis: Redis, *, stream: str, group: str, stream_id: str
) -> None:
    async with redis.pipeline(transaction=True) as pipe:
        pipe.xack(stream, group, stream_id)
        pipe.xdel(stream, stream_id)
        await pipe.execute()


async def dead_letter(
    redis: Redis,
    *,
    stream: str,
    group: str,
    dead_letter_stream: str,
    delivery: StreamDelivery,
    reason: str,
) -> None:
    fields = {
        "original_stream_id": delivery.stream_id,
        "attempt_count": str(delivery.attempt),
        "failed_at": datetime.now(UTC).isoformat(),
        "failure_reason": reason[:2000],
        "original_fields": json.dumps(delivery.fields, sort_keys=True),
    }
    async with redis.pipeline(transaction=True) as pipe:
        pipe.xadd(dead_letter_stream, fields)
        pipe.xack(stream, group, delivery.stream_id)
        pipe.xdel(stream, delivery.stream_id)
        await pipe.execute()
