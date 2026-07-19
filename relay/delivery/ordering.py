"""Strict per-endpoint FIFO for `ordering='ordered'` endpoints (spec Section 7.3).

Design: ingestion RPUSHes the delivery onto a per-endpoint list and XADDs a
wakeup to the stream. A worker takes a per-endpoint lock and processes only the
list *head*, one at a time.

Head-of-line blocking is the deliberate tradeoff: a failing delivery stays at
the head and blocks everything behind it until it succeeds or dies, because
the alternative — skipping ahead — is exactly what "ordered" promises not to
do. See the ordering section in the README.
"""

import uuid

from relay.delivery.enqueue import get_redis

# Long enough to outlast a delivery (10s timeout) plus DB writes, short enough
# that a crashed worker's lock frees up quickly.
LOCK_TTL_MS = 30_000

# Release only if we still own the lock; a lock that expired mid-delivery may
# already belong to someone else, and deleting it blindly would break mutual
# exclusion.
RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def queue_key(endpoint_id: uuid.UUID) -> str:
    return f"relay:ordered:{endpoint_id}"


def lock_key(endpoint_id: uuid.UUID) -> str:
    return f"relay:lock:endpoint:{endpoint_id}"


async def enqueue_ordered(endpoint_id: uuid.UUID, delivery_id: uuid.UUID) -> None:
    await get_redis().rpush(queue_key(endpoint_id), str(delivery_id))


async def acquire_lock(endpoint_id: uuid.UUID) -> str | None:
    """SET NX PX. Returns an ownership token, or None if another worker holds it."""
    token = uuid.uuid4().hex
    acquired = await get_redis().set(lock_key(endpoint_id), token, nx=True, px=LOCK_TTL_MS)
    return token if acquired else None


async def release_lock(endpoint_id: uuid.UUID, token: str) -> None:
    script = get_redis().register_script(RELEASE_LOCK_LUA)
    await script(keys=[lock_key(endpoint_id)], args=[token])


async def peek_head(endpoint_id: uuid.UUID) -> uuid.UUID | None:
    """The delivery that must go next. Peek, not pop: a failure keeps its place."""
    head = await get_redis().lindex(queue_key(endpoint_id), 0)
    return uuid.UUID(head) if head else None


async def pop_head(endpoint_id: uuid.UUID, delivery_id: uuid.UUID) -> bool:
    """Remove the head once it is settled (delivered or dead)."""
    removed = await get_redis().lrem(queue_key(endpoint_id), 1, str(delivery_id))
    return bool(removed)


async def queue_depth(endpoint_id: uuid.UUID) -> int:
    return int(await get_redis().llen(queue_key(endpoint_id)))
