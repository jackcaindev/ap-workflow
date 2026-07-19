from collections.abc import AsyncIterator

from redis.asyncio import Redis

from app.core.config import get_settings


async def get_redis() -> AsyncIterator[Redis]:
    settings = get_settings()
    redis = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )
    try:
        yield redis
    finally:
        await redis.aclose()
