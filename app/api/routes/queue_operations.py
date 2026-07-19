from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import Settings, get_settings
from app.redis_client import get_redis
from app.schemas.queue_operations import (
    DLQPage,
    PurgeRequest,
    PurgeResponse,
    QueueMetrics,
    ReplayResponse,
)
from app.services.queue_operations import (
    DLQNotFound,
    DLQNotReplayable,
    QueueOperationError,
    ReplayInProgress,
    list_dlq_entries,
    purge_dlq_entries,
    queue_metrics,
    replay_dlq_entry,
)


router = APIRouter(
    prefix="/operations/queue",
    tags=["queue operations"],
)


def _service_unavailable(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={"code": "redis_unavailable", "message": str(exc)},
    )


@router.get(
    "/dlq",
    response_model=DLQPage,
    summary="List retained dead-letter entries (trusted local demo)",
)
async def list_dead_letters(
    limit: int = Query(default=25, ge=1, le=100),
    cursor: str | None = Query(default=None),
    redis: Redis = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> DLQPage:
    try:
        return await list_dlq_entries(
            redis,
            dead_letter_stream=settings.INVOICE_DEAD_LETTER_STREAM,
            replay_prefix=settings.INVOICE_DLQ_REPLAY_PREFIX,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
    except RedisError as exc:
        raise _service_unavailable(exc) from exc


@router.post(
    "/dlq/{dlq_id}/replay",
    response_model=ReplayResponse,
    summary="Replay one retained dead-letter entry (trusted local demo)",
)
async def replay_dead_letter(
    dlq_id: str,
    response: Response,
    idempotency_key: UUID = Header(alias="Idempotency-Key"),
    redis: Redis = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> ReplayResponse:
    try:
        result = await replay_dlq_entry(
            redis,
            dead_letter_stream=settings.INVOICE_DEAD_LETTER_STREAM,
            live_stream=settings.INVOICE_STREAM,
            replay_prefix=settings.INVOICE_DLQ_REPLAY_PREFIX,
            dlq_id=dlq_id,
            request_id=str(idempotency_key),
        )
    except (ValueError, DLQNotFound) as exc:
        code = "dlq_not_found" if isinstance(exc, DLQNotFound) else "invalid_dlq_id"
        http_status = (
            status.HTTP_404_NOT_FOUND
            if isinstance(exc, DLQNotFound)
            else status.HTTP_422_UNPROCESSABLE_CONTENT
        )
        raise HTTPException(
            status_code=http_status,
            detail={"code": code, "message": str(exc)},
        ) from exc
    except DLQNotReplayable as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc
    except ReplayInProgress as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc
    except QueueOperationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc
    except RedisError as exc:
        raise _service_unavailable(exc) from exc
    response.status_code = status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    return result


@router.get(
    "/metrics",
    response_model=QueueMetrics,
    summary="Read queue health metrics (trusted local demo)",
)
async def read_queue_metrics(
    redis: Redis = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> QueueMetrics:
    try:
        return await queue_metrics(
            redis,
            live_stream=settings.INVOICE_STREAM,
            group=settings.INVOICE_CONSUMER_GROUP,
            dead_letter_stream=settings.INVOICE_DEAD_LETTER_STREAM,
        )
    except RedisError as exc:
        raise _service_unavailable(exc) from exc


@router.post(
    "/dlq/purge",
    response_model=PurgeResponse,
    summary="Explicitly purge old dead-letter entries (trusted local demo)",
)
async def purge_dead_letters(
    body: PurgeRequest,
    redis: Redis = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> PurgeResponse:
    if body.before.tzinfo is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="before must include a timezone",
        )
    before = body.before.astimezone(UTC)
    if before >= datetime.now(UTC):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="before must be in the past",
        )
    try:
        purged_count, has_more = await purge_dlq_entries(
            redis,
            dead_letter_stream=settings.INVOICE_DEAD_LETTER_STREAM,
            replay_prefix=settings.INVOICE_DLQ_REPLAY_PREFIX,
            metadata_prefix=settings.INVOICE_METADATA_PREFIX,
            before=before,
            max_delete=settings.INVOICE_DLQ_RETENTION_MAX_DELETE,
        )
    except QueueOperationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc
    except RedisError as exc:
        raise _service_unavailable(exc) from exc
    return PurgeResponse(before=before, purged_count=purged_count, has_more=has_more)
