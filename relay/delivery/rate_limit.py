"""Per-tenant rate limiting and concurrency caps (spec Section 7.4).

Both live in Redis so every worker process shares one budget. Both use Lua:
check-then-act across a network round trip would let N workers each see room
and collectively blow past the cap.
"""

import uuid

from relay.delivery.enqueue import get_redis

# Bucket keeps its own clock so refill is computed server-side; passing the
# time in from each worker would make the limit sensitive to clock skew.
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local burst = rate  -- one second of headroom; enough to absorb jitter

local state = redis.call('HMGET', key, 'tokens', 'updated_at')
local tokens = tonumber(state[1])
local updated_at = tonumber(state[2])
if tokens == nil then
  tokens = burst
  updated_at = now
end

local elapsed = math.max(0, now - updated_at)
tokens = math.min(burst, tokens + elapsed * rate)

local granted = 0
if tokens >= requested then
  tokens = tokens - requested
  granted = 1
end

redis.call('HSET', key, 'tokens', tokens, 'updated_at', now)
-- Idle tenants shouldn't keep state around forever.
redis.call('EXPIRE', key, 60)
return granted
"""

# Slots are held in a sorted set keyed by expiry, not a counter: a worker that
# dies mid-delivery would leak a counter increment forever, whereas a stale
# ZSET member ages out and is swept on the next acquire.
ACQUIRE_SLOT_LUA = """
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local token = ARGV[3]
local ttl = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) >= cap then
  return 0
end
redis.call('ZADD', key, now + ttl, token)
redis.call('EXPIRE', key, ttl * 2)
return 1
"""

# Safety net: a slot is force-released this long after acquisition even if the
# holder never returns it. Must exceed the delivery timeout.
SLOT_TTL_SECONDS = 60


def bucket_key(tenant_id: uuid.UUID) -> str:
    return f"relay:ratelimit:{tenant_id}"


def inflight_key(tenant_id: uuid.UUID) -> str:
    return f"relay:inflight:{tenant_id}"


async def take_token(tenant_id: uuid.UUID, rate_per_sec: int, now: float) -> bool:
    """Try to spend one delivery token. False means the tenant is over budget."""
    script = get_redis().register_script(TOKEN_BUCKET_LUA)
    granted = await script(keys=[bucket_key(tenant_id)], args=[rate_per_sec, now, 1])
    return bool(granted)


async def acquire_slot(tenant_id: uuid.UUID, max_inflight: int, now: float) -> str | None:
    """Reserve an in-flight slot. Returns a release token, or None if at cap."""
    token = uuid.uuid4().hex
    script = get_redis().register_script(ACQUIRE_SLOT_LUA)
    granted = await script(
        keys=[inflight_key(tenant_id)], args=[max_inflight, now, token, SLOT_TTL_SECONDS]
    )
    return token if granted else None


async def release_slot(tenant_id: uuid.UUID, token: str) -> None:
    await get_redis().zrem(inflight_key(tenant_id), token)


async def inflight_count(tenant_id: uuid.UUID, now: float) -> int:
    redis = get_redis()
    await redis.zremrangebyscore(inflight_key(tenant_id), "-inf", now)
    return int(await redis.zcard(inflight_key(tenant_id)))
