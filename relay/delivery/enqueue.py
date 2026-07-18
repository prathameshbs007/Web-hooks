import uuid
import zlib

import redis.asyncio as aioredis

from relay.config import get_settings

_redis: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(get_settings().redis_url, decode_responses=True)
    return _redis


async def close_redis() -> None:
    """Close the shared client (worker shutdown / test teardown)."""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


def shard_for(endpoint_id: uuid.UUID) -> int:
    # crc32 (not Python hash()) so the shard assignment is stable across
    # processes and restarts — a delivery always lands on the same stream.
    return zlib.crc32(endpoint_id.bytes) % get_settings().stream_shards


def stream_key(shard: int) -> str:
    return f"relay:deliveries:{shard}"


async def enqueue_delivery(
    delivery_id: uuid.UUID, event_id: uuid.UUID, endpoint_id: uuid.UUID
) -> None:
    """XADD one delivery onto its endpoint's shard stream."""
    await get_redis().xadd(
        stream_key(shard_for(endpoint_id)),
        {
            "delivery_id": str(delivery_id),
            "event_id": str(event_id),
            "endpoint_id": str(endpoint_id),
        },
    )
