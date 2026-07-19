from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


ReplayState = Literal[
    "enqueued",
    "processing",
    "retrying",
    "acknowledged",
    "dead_lettered",
]


class DLQSource(BaseModel):
    gmail_account: str | None = None
    message_id: str | None = None
    mime_part_id: str | None = None
    idempotency_key: str | None = None


class ReplaySummary(BaseModel):
    count: int = 0
    last_request_id: str | None = None
    last_requested_at: datetime | None = None
    last_enqueued_at: datetime | None = None
    last_live_stream_id: str | None = None
    state: ReplayState | None = None
    workflow_processing_status: str | None = None


class DLQEntry(BaseModel):
    dlq_id: str
    original_stream_id: str | None = None
    failure_reason: str | None = None
    attempt_count: int = 0
    failed_at: datetime | None = None
    filename: str | None = None
    source: DLQSource
    replayable: bool
    replay_block_reason: str | None = None
    replay: ReplaySummary


class DLQPage(BaseModel):
    items: list[DLQEntry]
    next_cursor: str | None = None


class ReplayResponse(BaseModel):
    dlq_id: str
    request_id: str
    created: bool
    replay_count: int
    state: ReplayState
    requested_at: datetime
    enqueued_at: datetime
    live_stream_id: str


class QueueMetrics(BaseModel):
    live_stream_length: int
    pending_count: int
    oldest_pending_age_seconds: float | None
    dlq_count: int
    observed_at: datetime


class PurgeRequest(BaseModel):
    before: datetime


class PurgeResponse(BaseModel):
    before: datetime
    purged_count: int = Field(ge=0)
    has_more: bool
