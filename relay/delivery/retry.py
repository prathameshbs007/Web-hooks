"""Retry policy: what counts as retryable, and how long to wait.

Pure functions — no I/O — so the policy is unit-testable in isolation.
"""

import random

# attempt_number -> base delay before the NEXT attempt (Section 7.2).
BACKOFF_SCHEDULE: dict[int, int] = {
    1: 5,
    2: 30,
    3: 2 * 60,
    4: 10 * 60,
    5: 30 * 60,
    6: 2 * 60 * 60,
    7: 5 * 60 * 60,
}

JITTER_MIN = 0.8
JITTER_MAX = 1.2

# A 4xx that isn't 408/429 means the request itself is wrong — retrying an
# unchanged body cannot fix it. We still allow a few attempts in case the
# receiver was briefly misconfigured, then give up well short of MAX_ATTEMPTS.
TERMINAL_4XX_MAX_ATTEMPTS = 3

# Never honour an absurd Retry-After; a receiver asking for a week would
# otherwise pin the delivery in the queue indefinitely.
RETRY_AFTER_CAP_SECONDS = 60 * 60

RETRYABLE_4XX = {408, 429}


def is_terminal_status(http_status: int | None) -> bool:
    """True for 4xx responses that won't be fixed by retrying the same payload."""
    if http_status is None:
        return False
    return 400 <= http_status < 500 and http_status not in RETRYABLE_4XX


def max_attempts_for(http_status: int | None, configured_max: int) -> int:
    if is_terminal_status(http_status):
        return min(TERMINAL_4XX_MAX_ATTEMPTS, configured_max)
    return configured_max


def apply_jitter(delay: float, rng: random.Random | None = None) -> float:
    """Spread retries so a fleet of failed deliveries doesn't thunder together."""
    r = rng or random
    return delay * r.uniform(JITTER_MIN, JITTER_MAX)


def parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header given in delta-seconds. Capped at 1h."""
    if not value:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        # HTTP-date form is legal but rare from webhook receivers; ignoring it
        # falls back to normal backoff rather than failing the delivery.
        return None
    if seconds < 0:
        return None
    return min(seconds, RETRY_AFTER_CAP_SECONDS)


def next_delay_seconds(
    attempt_number: int,
    *,
    http_status: int | None = None,
    retry_after: str | None = None,
    configured_max: int = 7,
    rng: random.Random | None = None,
) -> float | None:
    """Seconds to wait before the next attempt, or None if the delivery is dead.

    `attempt_number` is the attempt that just failed (1-based).
    """
    if attempt_number >= max_attempts_for(http_status, configured_max):
        return None

    # A 429 receiver told us exactly how long to wait — honour it over our
    # own schedule, since it reflects the receiver's real capacity.
    if http_status == 429:
        explicit = parse_retry_after(retry_after)
        if explicit is not None:
            return explicit

    base = BACKOFF_SCHEDULE.get(attempt_number)
    if base is None:
        return None
    return apply_jitter(base, rng)
