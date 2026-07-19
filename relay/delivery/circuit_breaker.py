"""Per-endpoint circuit breaker (spec Section 7.5).

States: closed -> open -> half_open -> (closed | open).

Opens on 10 consecutive failures, or >50% failure rate over a 5-minute sliding
window with at least 20 attempts. Both the outcome recording and the state
decision happen inside one Lua script: a check-then-write split would let two
workers observe "closed" simultaneously and disagree about the transition.
"""

import json
import uuid

from relay.delivery.enqueue import get_redis

BREAKER_EVENTS_CHANNEL = "relay:breaker-events"

CONSECUTIVE_FAILURE_THRESHOLD = 10
WINDOW_SECONDS = 300
WINDOW_MIN_ATTEMPTS = 20
FAILURE_RATE_THRESHOLD = 0.5
HALF_OPEN_PROBE_INTERVAL_SECONDS = 600
AUTO_DISABLE_AFTER_SECONDS = 72 * 3600


def state_key(endpoint_id: uuid.UUID) -> str:
    return f"relay:breaker:{endpoint_id}"


def attempts_key(endpoint_id: uuid.UUID) -> str:
    return f"relay:breaker:{endpoint_id}:attempts"


def failures_key(endpoint_id: uuid.UUID) -> str:
    return f"relay:breaker:{endpoint_id}:failures"


# KEYS: state, attempts, failures
# ARGV: now, success(1|0), consecutive_threshold, window_s, min_attempts,
#       rate_threshold, member, channel, endpoint_id
RECORD_OUTCOME_LUA = """
local state_key, attempts_key, failures_key = KEYS[1], KEYS[2], KEYS[3]
local now = tonumber(ARGV[1])
local success = tonumber(ARGV[2]) == 1
local consec_threshold = tonumber(ARGV[3])
local window = tonumber(ARGV[4])
local min_attempts = tonumber(ARGV[5])
local rate_threshold = tonumber(ARGV[6])
local member = ARGV[7]
local channel = ARGV[8]
local endpoint_id = ARGV[9]

local state = redis.call('HGET', state_key, 'state') or 'closed'
local cutoff = now - window

-- Sliding window: trim, then record this attempt.
redis.call('ZREMRANGEBYSCORE', attempts_key, '-inf', cutoff)
redis.call('ZREMRANGEBYSCORE', failures_key, '-inf', cutoff)
redis.call('ZADD', attempts_key, now, member)
redis.call('EXPIRE', attempts_key, window * 2)
if not success then
  redis.call('ZADD', failures_key, now, member)
  redis.call('EXPIRE', failures_key, window * 2)
end

local consecutive = tonumber(redis.call('HGET', state_key, 'consecutive') or '0')
if success then
  consecutive = 0
else
  consecutive = consecutive + 1
end
redis.call('HSET', state_key, 'consecutive', consecutive)

local new_state = state
if success then
  -- A success in half_open means the endpoint recovered: resume the backlog.
  if state == 'half_open' or state == 'open' then
    new_state = 'closed'
  end
else
  if state == 'half_open' then
    -- The probe failed; go back to open and restart the cooldown.
    new_state = 'open'
  elseif state == 'closed' then
    local attempts = redis.call('ZCARD', attempts_key)
    local failures = redis.call('ZCARD', failures_key)
    local rate_tripped = attempts >= min_attempts
      and (failures / attempts) > rate_threshold
    if consecutive >= consec_threshold or rate_tripped then
      new_state = 'open'
    end
  end
end

if new_state ~= state then
  redis.call('HSET', state_key, 'state', new_state)
  if new_state == 'open' then
    redis.call('HSET', state_key, 'opened_at', now, 'last_probe_at', now)
  elseif new_state == 'closed' then
    redis.call('HDEL', state_key, 'opened_at', 'last_probe_at')
    redis.call('HSET', state_key, 'consecutive', 0)
    -- Recovered: clear history so old failures can't retrip it immediately.
    redis.call('DEL', attempts_key, failures_key)
  end
  redis.call('PUBLISH', channel, cjson.encode({
    endpoint_id = endpoint_id,
    from_state = state,
    to_state = new_state,
    at = now,
    consecutive = consecutive
  }))
end

return {new_state, tostring(consecutive), tostring(new_state ~= state)}
"""

# KEYS: state
# ARGV: now, probe_interval
# Returns {decision, state}: allow | deny | probe
ALLOW_REQUEST_LUA = """
local state_key = KEYS[1]
local now = tonumber(ARGV[1])
local probe_interval = tonumber(ARGV[2])

local state = redis.call('HGET', state_key, 'state') or 'closed'
if state == 'closed' then
  return {'allow', state}
end

if state == 'open' then
  local last_probe = tonumber(redis.call('HGET', state_key, 'last_probe_at') or '0')
  if now - last_probe >= probe_interval then
    -- Promote to half_open and let exactly this caller through. Recording the
    -- probe time here is what stops a second worker also being let through.
    redis.call('HSET', state_key, 'state', 'half_open', 'last_probe_at', now)
    return {'probe', 'half_open'}
  end
  return {'deny', state}
end

-- half_open: a probe is already in flight, everyone else waits.
return {'deny', state}
"""


async def record_outcome(endpoint_id: uuid.UUID, success: bool, now: float) -> dict:
    """Record an attempt outcome and return the resulting breaker state."""
    script = get_redis().register_script(RECORD_OUTCOME_LUA)
    state, consecutive, transitioned = await script(
        keys=[state_key(endpoint_id), attempts_key(endpoint_id), failures_key(endpoint_id)],
        args=[
            now,
            1 if success else 0,
            CONSECUTIVE_FAILURE_THRESHOLD,
            WINDOW_SECONDS,
            WINDOW_MIN_ATTEMPTS,
            FAILURE_RATE_THRESHOLD,
            uuid.uuid4().hex,
            BREAKER_EVENTS_CHANNEL,
            str(endpoint_id),
        ],
    )
    return {
        "state": state,
        "consecutive": int(consecutive),
        "transitioned": transitioned == "true",
    }


async def allow_request(endpoint_id: uuid.UUID, now: float) -> tuple[str, str]:
    """Return (decision, state) where decision is allow | probe | deny."""
    script = get_redis().register_script(ALLOW_REQUEST_LUA)
    decision, state = await script(
        keys=[state_key(endpoint_id)], args=[now, HALF_OPEN_PROBE_INTERVAL_SECONDS]
    )
    return decision, state


async def get_state(endpoint_id: uuid.UUID) -> dict:
    raw = await get_redis().hgetall(state_key(endpoint_id))
    return {
        "state": raw.get("state", "closed"),
        "consecutive": int(raw.get("consecutive", 0)),
        "opened_at": float(raw["opened_at"]) if raw.get("opened_at") else None,
        "last_probe_at": float(raw["last_probe_at"]) if raw.get("last_probe_at") else None,
    }


async def reset(endpoint_id: uuid.UUID) -> None:
    """Force the breaker closed (used by DLQ replay and by tests)."""
    await get_redis().delete(
        state_key(endpoint_id), attempts_key(endpoint_id), failures_key(endpoint_id)
    )


async def should_auto_disable(endpoint_id: uuid.UUID, now: float) -> bool:
    """True once an endpoint has been continuously open for 72h."""
    state = await get_state(endpoint_id)
    if state["state"] == "closed" or state["opened_at"] is None:
        return False
    return (now - state["opened_at"]) >= AUTO_DISABLE_AFTER_SECONDS


def parse_event(payload: str) -> dict:
    return json.loads(payload)
