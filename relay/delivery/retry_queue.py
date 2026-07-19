"""The `relay:retries` ZSET: deliveries waiting for their next attempt.

Score is the epoch second the delivery becomes due. The scheduler moves due
members back onto their shard stream; the ZREM and the XADD happen inside one
Lua script so two schedulers can never both re-enqueue the same delivery.
"""

import time
import uuid

from relay.delivery.enqueue import get_redis, shard_for

RETRY_ZSET = "relay:retries"
MEMBER_SEPARATOR = "|"

# ZREM returning 1 means *this* call won the member; only that caller XADDs.
# Running as a script makes the pair atomic without a distributed lock.
MOVE_DUE_LUA = """
local zset = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local due = redis.call('ZRANGEBYSCORE', zset, '-inf', now, 'LIMIT', 0, limit)
local moved = 0
for _, member in ipairs(due) do
  if redis.call('ZREM', zset, member) == 1 then
    local parts = {}
    for part in string.gmatch(member, '[^|]+') do
      parts[#parts + 1] = part
    end
    redis.call(
      'XADD', 'relay:deliveries:' .. parts[4], '*',
      'delivery_id', parts[1],
      'event_id', parts[2],
      'endpoint_id', parts[3]
    )
    moved = moved + 1
  end
end
return moved
"""


def encode_member(delivery_id: uuid.UUID, event_id: uuid.UUID, endpoint_id: uuid.UUID) -> str:
    """Pack everything the scheduler needs to rebuild the stream entry.

    The shard is baked in so the scheduler never has to consult Postgres.
    """
    return MEMBER_SEPARATOR.join(
        [str(delivery_id), str(event_id), str(endpoint_id), str(shard_for(endpoint_id))]
    )


async def schedule_retry(
    delivery_id: uuid.UUID,
    event_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    delay_seconds: float,
    now: float | None = None,
) -> float:
    """ZADD the delivery with score = when it becomes due. Returns that score."""
    due_at = (time.time() if now is None else now) + delay_seconds
    await get_redis().zadd(RETRY_ZSET, {encode_member(delivery_id, event_id, endpoint_id): due_at})
    return due_at


async def move_due_to_streams(limit: int = 100, now: float | None = None) -> int:
    """Re-enqueue every retry whose time has come. Returns how many moved."""
    script = get_redis().register_script(MOVE_DUE_LUA)
    return int(
        await script(
            keys=[RETRY_ZSET], args=[time.time() if now is None else now, limit]
        )
    )


async def retry_queue_depth() -> int:
    return int(await get_redis().zcard(RETRY_ZSET))


async def remove_from_retry_queue(
    delivery_id: uuid.UUID, event_id: uuid.UUID, endpoint_id: uuid.UUID
) -> bool:
    """Drop a scheduled retry (used when a dead delivery is replayed)."""
    removed = await get_redis().zrem(
        RETRY_ZSET, encode_member(delivery_id, event_id, endpoint_id)
    )
    return bool(removed)
