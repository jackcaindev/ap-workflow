import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.schemas.invoice_job import InvoiceJobEnvelope, PermanentJobError
from app.schemas.queue_operations import (
    DLQEntry,
    DLQPage,
    DLQSource,
    QueueMetrics,
    ReplayResponse,
    ReplaySummary,
)
from app.services.invoice_queue import StreamDelivery, parse_delivery


STREAM_ID_RE = re.compile(r"^[0-9]+-[0-9]+$")
NONTERMINAL_REPLAY_STATES = {"enqueued", "processing", "retrying"}
REPLAY_STATES = NONTERMINAL_REPLAY_STATES | {"acknowledged", "dead_lettered"}

REPLAY_SCRIPT = """
local dlq_type = redis.call('TYPE', KEYS[1])['ok']
local live_type = redis.call('TYPE', KEYS[2])['ok']
local meta_type = redis.call('TYPE', KEYS[3])['ok']
if dlq_type ~= 'none' and dlq_type ~= 'stream' then return {'wrong_type'} end
if live_type ~= 'none' and live_type ~= 'stream' then return {'wrong_type'} end
if meta_type ~= 'none' and meta_type ~= 'hash' then return {'wrong_type'} end

local rows = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1])
if #rows == 0 then return {'not_found'} end
local fields = rows[1][2]
local stored_original_fields = nil
for i = 1, #fields, 2 do
  if fields[i] == 'original_fields' then stored_original_fields = fields[i + 1] end
end
if stored_original_fields ~= ARGV[3] then return {'evidence_changed'} end

local action_prefix = 'action:' .. ARGV[2] .. ':'
local existing = redis.call('HGET', KEYS[3], action_prefix .. 'live_stream_id')
if existing then
  return {
    'existing',
    existing,
    redis.call('HGET', KEYS[3], 'replay_count') or '0',
    redis.call('HGET', KEYS[3], action_prefix .. 'requested_at') or '',
    redis.call('HGET', KEYS[3], action_prefix .. 'enqueued_at') or '',
    redis.call('HGET', KEYS[3], action_prefix .. 'state') or 'enqueued'
  }
end

local last_request_id = redis.call('HGET', KEYS[3], 'last_request_id')
if last_request_id and last_request_id ~= ARGV[2] then
  local last_state = redis.call('HGET', KEYS[3], 'action:' .. last_request_id .. ':state')
  if last_state == 'enqueued' or last_state == 'processing' or last_state == 'retrying' then
    return {'in_progress'}
  end
end

local stream_id = redis.call(
  'XADD', KEYS[2], '*',
  'payload', ARGV[4],
  'replay_dlq_id', ARGV[1],
  'replay_request_id', ARGV[2]
)
local count = redis.call('HINCRBY', KEYS[3], 'replay_count', 1)
redis.call(
  'HSET', KEYS[3],
  'last_request_id', ARGV[2],
  'last_replay_requested_at', ARGV[5],
  'last_replay_enqueued_at', ARGV[6],
  'last_live_stream_id', stream_id,
  action_prefix .. 'requested_at', ARGV[5],
  action_prefix .. 'enqueued_at', ARGV[6],
  action_prefix .. 'live_stream_id', stream_id,
  action_prefix .. 'state', 'enqueued'
)
return {'created', stream_id, tostring(count), ARGV[5], ARGV[6], 'enqueued'}
"""

PURGE_BATCH_SCRIPT = """
local dlq_type = redis.call('TYPE', KEYS[1])['ok']
if dlq_type == 'none' then return {0, 0} end
if dlq_type ~= 'stream' then return {'wrong_type'} end

local rows = redis.call('XRANGE', KEYS[1], '-', '(' .. ARGV[1], 'COUNT', ARGV[2])
for _, row in ipairs(rows) do
  local dlq_id = row[1]
  local fields = row[2]
  local original_stream_id = nil
  for i = 1, #fields, 2 do
    if fields[i] == 'original_stream_id' then original_stream_id = fields[i + 1] end
  end
  redis.call('XDEL', KEYS[1], dlq_id)
  redis.call('DEL', ARGV[3] .. ':' .. dlq_id)
  if original_stream_id then
    redis.call('DEL', ARGV[4] .. ':' .. original_stream_id)
  end
end
local remaining = redis.call('XRANGE', KEYS[1], '-', '(' .. ARGV[1], 'COUNT', 1)
return {#rows, #remaining}
"""


class QueueOperationError(RuntimeError):
    code = "queue_operation_error"


class DLQNotFound(QueueOperationError):
    code = "dlq_not_found"


class DLQNotReplayable(QueueOperationError):
    code = "not_replayable"


class ReplayInProgress(QueueOperationError):
    code = "replay_in_progress"


@dataclass(frozen=True)
class DecodedDLQEvidence:
    original_fields_json: str
    original_fields: dict[str, str]
    job: InvoiceJobEnvelope


def validate_stream_id(value: str) -> str:
    if not STREAM_ID_RE.fullmatch(value):
        raise ValueError("cursor must be a Redis stream ID such as 1752940000000-0")
    return value


def decode_dlq_evidence(fields: dict[str, str]) -> DecodedDLQEvidence:
    raw_original_fields = fields.get("original_fields")
    if raw_original_fields is None:
        raise DLQNotReplayable("missing original_fields")
    try:
        original_fields = json.loads(raw_original_fields)
    except (json.JSONDecodeError, TypeError) as exc:
        raise DLQNotReplayable("original_fields is not valid JSON") from exc
    if not isinstance(original_fields, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in original_fields.items()
    ):
        raise DLQNotReplayable("original_fields must be a string-to-string object")
    try:
        job = parse_delivery(StreamDelivery("dlq-validation", original_fields))
    except PermanentJobError as exc:
        raise DLQNotReplayable(str(exc)) from exc
    return DecodedDLQEvidence(raw_original_fields, original_fields, job)


def _optional_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _replay_summary(metadata: dict[str, str]) -> ReplaySummary:
    request_id = metadata.get("last_request_id")
    action_prefix = f"action:{request_id}:" if request_id else None
    try:
        replay_count = int(metadata.get("replay_count", "0"))
    except ValueError:
        replay_count = 0
    raw_state = metadata.get(f"{action_prefix}state") if action_prefix else None
    return ReplaySummary(
        count=replay_count,
        last_request_id=request_id,
        last_requested_at=_optional_datetime(metadata.get("last_replay_requested_at")),
        last_enqueued_at=_optional_datetime(metadata.get("last_replay_enqueued_at")),
        last_live_stream_id=metadata.get("last_live_stream_id"),
        state=raw_state if raw_state in REPLAY_STATES else None,
        workflow_processing_status=(
            metadata.get(f"{action_prefix}workflow_processing_status")
            if action_prefix
            else None
        ),
    )


def _recover_payload_identity(fields: dict[str, str]) -> dict[str, str | None]:
    recovered = {
        "filename": None,
        "gmail_account": None,
        "message_id": None,
        "mime_part_id": None,
        "idempotency_key": None,
    }
    try:
        original_fields = json.loads(fields.get("original_fields", ""))
        payload = json.loads(original_fields.get("payload", ""))
    except (AttributeError, json.JSONDecodeError, TypeError):
        return recovered
    if not isinstance(payload, dict):
        return recovered
    for key in recovered:
        value = payload.get(key)
        recovered[key] = value if isinstance(value, str) else None
    return recovered


def dlq_entry_from_fields(
    dlq_id: str, fields: dict[str, str], metadata: dict[str, str]
) -> DLQEntry:
    evidence: DecodedDLQEvidence | None = None
    replay_block_reason: str | None = None
    try:
        evidence = decode_dlq_evidence(fields)
    except DLQNotReplayable as exc:
        replay_block_reason = str(exc)

    try:
        attempt_count = int(fields.get("attempt_count", "0"))
    except ValueError:
        attempt_count = 0
    job = evidence.job if evidence else None
    recovered = _recover_payload_identity(fields)
    return DLQEntry(
        dlq_id=dlq_id,
        original_stream_id=fields.get("original_stream_id"),
        failure_reason=fields.get("failure_reason"),
        attempt_count=attempt_count,
        failed_at=_optional_datetime(fields.get("failed_at")),
        filename=job.filename if job else recovered["filename"],
        source=DLQSource(
            gmail_account=job.gmail_account if job else recovered["gmail_account"],
            message_id=job.message_id if job else recovered["message_id"],
            mime_part_id=job.mime_part_id if job else recovered["mime_part_id"],
            idempotency_key=job.idempotency_key if job else recovered["idempotency_key"],
        ),
        replayable=evidence is not None,
        replay_block_reason=replay_block_reason,
        replay=_replay_summary(metadata),
    )


async def list_dlq_entries(
    redis: Redis,
    *,
    dead_letter_stream: str,
    replay_prefix: str,
    limit: int,
    cursor: str | None,
) -> DLQPage:
    maximum = f"({validate_stream_id(cursor)}" if cursor else "+"
    rows = await redis.xrevrange(
        dead_letter_stream, max=maximum, min="-", count=limit + 1
    )
    visible_rows = (rows or [])[:limit]
    async with redis.pipeline(transaction=False) as pipe:
        for dlq_id, _ in visible_rows:
            pipe.hgetall(f"{replay_prefix}:{dlq_id}")
        metadata_rows = await pipe.execute() if visible_rows else []
    items = [
        dlq_entry_from_fields(dlq_id, fields, metadata)
        for (dlq_id, fields), metadata in zip(visible_rows, metadata_rows, strict=True)
    ]
    next_cursor = visible_rows[-1][0] if len(rows or []) > limit else None
    return DLQPage(items=items, next_cursor=next_cursor)


async def replay_dlq_entry(
    redis: Redis,
    *,
    dead_letter_stream: str,
    live_stream: str,
    replay_prefix: str,
    dlq_id: str,
    request_id: str,
    now: datetime | None = None,
) -> ReplayResponse:
    validate_stream_id(dlq_id)
    rows = await redis.xrange(dead_letter_stream, min=dlq_id, max=dlq_id, count=1)
    if not rows:
        raise DLQNotFound("DLQ entry not found")
    evidence = decode_dlq_evidence(rows[0][1])
    timestamp = (now or datetime.now(UTC)).isoformat()
    response = await redis.eval(
        REPLAY_SCRIPT,
        3,
        dead_letter_stream,
        live_stream,
        f"{replay_prefix}:{dlq_id}",
        dlq_id,
        request_id,
        evidence.original_fields_json,
        evidence.job.model_dump_json(),
        timestamp,
        timestamp,
    )
    outcome = response[0]
    if outcome == "not_found":
        raise DLQNotFound("DLQ entry not found")
    if outcome == "in_progress":
        raise ReplayInProgress("another replay action is still in progress")
    if outcome == "evidence_changed":
        raise QueueOperationError("DLQ evidence changed during replay validation")
    if outcome == "wrong_type":
        raise QueueOperationError("a configured queue key has the wrong Redis type")
    if outcome not in {"created", "existing"}:
        raise QueueOperationError(f"unexpected replay result: {outcome}")
    replay_state = response[5]
    if replay_state not in REPLAY_STATES:
        raise QueueOperationError(f"unexpected replay state: {replay_state}")
    return ReplayResponse(
        dlq_id=dlq_id,
        request_id=request_id,
        created=outcome == "created",
        replay_count=int(response[2]),
        state=replay_state,
        requested_at=_optional_datetime(response[3]) or datetime.now(UTC),
        enqueued_at=_optional_datetime(response[4]) or datetime.now(UTC),
        live_stream_id=response[1],
    )


async def queue_metrics(
    redis: Redis,
    *,
    live_stream: str,
    group: str,
    dead_letter_stream: str,
    now: datetime | None = None,
) -> QueueMetrics:
    observed_at = now or datetime.now(UTC)
    live_length, dlq_count = await redis.xlen(live_stream), await redis.xlen(
        dead_letter_stream
    )
    pending_count = 0
    oldest_pending_age_seconds: float | None = None
    try:
        groups = await redis.xinfo_groups(live_stream)
    except ResponseError:
        groups = []
    if any(info.get("name") == group for info in groups):
        pending = await redis.xpending(live_stream, group)
        pending_count = int(pending.get("pending", 0))
        oldest_id = pending.get("min")
        if pending_count and oldest_id:
            oldest_ms = int(str(oldest_id).split("-", 1)[0])
            oldest_pending_age_seconds = max(
                0.0, observed_at.timestamp() - oldest_ms / 1000
            )
    return QueueMetrics(
        live_stream_length=int(live_length),
        pending_count=pending_count,
        oldest_pending_age_seconds=oldest_pending_age_seconds,
        dlq_count=int(dlq_count),
        observed_at=observed_at,
    )


async def purge_dlq_entries(
    redis: Redis,
    *,
    dead_letter_stream: str,
    replay_prefix: str,
    metadata_prefix: str,
    before: datetime,
    max_delete: int,
    batch_size: int = 100,
) -> tuple[int, bool]:
    cutoff_id = f"{int(before.timestamp() * 1000)}-0"
    purged_count = 0
    has_more = False
    while purged_count < max_delete:
        limit = min(batch_size, max_delete - purged_count)
        response = await redis.eval(
            PURGE_BATCH_SCRIPT,
            1,
            dead_letter_stream,
            cutoff_id,
            limit,
            replay_prefix,
            metadata_prefix,
        )
        if response[0] == "wrong_type":
            raise QueueOperationError("the configured DLQ key has the wrong Redis type")
        removed, remaining = int(response[0]), bool(int(response[1]))
        purged_count += removed
        has_more = remaining
        if removed < limit or not remaining:
            break
    return purged_count, has_more
