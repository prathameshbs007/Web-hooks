"""Unit tests for the retry policy (spec Section 7.2): schedule, jitter, terminal 4xx."""

import random

import pytest

from relay.delivery.retry import (
    BACKOFF_SCHEDULE,
    JITTER_MAX,
    JITTER_MIN,
    RETRY_AFTER_CAP_SECONDS,
    TERMINAL_4XX_MAX_ATTEMPTS,
    apply_jitter,
    is_terminal_status,
    max_attempts_for,
    next_delay_seconds,
    parse_retry_after,
)


def test_backoff_schedule_matches_spec():
    assert BACKOFF_SCHEDULE == {
        1: 5,
        2: 30,
        3: 120,
        4: 600,
        5: 1800,
        6: 7200,
        7: 18000,
    }


@pytest.mark.parametrize("attempt", sorted(BACKOFF_SCHEDULE))
def test_jitter_stays_within_bounds(attempt):
    """1000 draws must all land inside [0.8x, 1.2x] of the base delay."""
    base = BACKOFF_SCHEDULE[attempt]
    rng = random.Random(attempt)
    for _ in range(1000):
        delay = next_delay_seconds(attempt, http_status=500, configured_max=8, rng=rng)
        assert base * JITTER_MIN <= delay <= base * JITTER_MAX


def test_jitter_actually_varies():
    rng = random.Random(7)
    draws = {apply_jitter(100, rng) for _ in range(50)}
    assert len(draws) > 40, "jitter should spread retries, not return a constant"


@pytest.mark.parametrize(
    ("status", "terminal"),
    [
        (400, True),
        (401, True),
        (403, True),
        (404, True),
        (422, True),
        (408, False),  # request timeout — retryable
        (429, False),  # rate limited — retryable
        (500, False),
        (503, False),
        (None, False),  # transport error
    ],
)
def test_terminal_4xx_classification(status, terminal):
    assert is_terminal_status(status) is terminal


def test_terminal_4xx_gives_up_after_three_attempts():
    assert max_attempts_for(400, 7) == TERMINAL_4XX_MAX_ATTEMPTS == 3
    assert next_delay_seconds(1, http_status=400, configured_max=7) is not None
    assert next_delay_seconds(2, http_status=400, configured_max=7) is not None
    # third failure exhausts a terminal 4xx → dead
    assert next_delay_seconds(3, http_status=400, configured_max=7) is None


def test_retryable_statuses_use_full_attempt_budget():
    assert max_attempts_for(500, 7) == 7
    assert max_attempts_for(429, 7) == 7
    assert next_delay_seconds(6, http_status=500, configured_max=7) is not None
    # seventh failure exhausts the budget → dead
    assert next_delay_seconds(7, http_status=500, configured_max=7) is None


def test_transport_failure_uses_full_budget():
    """A timeout has no http_status; it must not be treated as terminal."""
    assert next_delay_seconds(1, http_status=None, configured_max=7) is not None
    assert next_delay_seconds(7, http_status=None, configured_max=7) is None


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("45", 45.0),
        ("0", 0.0),
        ("999999", RETRY_AFTER_CAP_SECONDS),  # capped at 1h
        ("-5", None),
        ("not-a-number", None),
        ("Wed, 21 Oct 2015 07:28:00 GMT", None),  # HTTP-date form ignored
        (None, None),
        ("", None),
    ],
)
def test_parse_retry_after(header, expected):
    assert parse_retry_after(header) == expected


def test_429_honours_retry_after_over_schedule():
    assert next_delay_seconds(1, http_status=429, retry_after="45", configured_max=7) == 45.0


def test_429_without_retry_after_falls_back_to_schedule():
    delay = next_delay_seconds(1, http_status=429, retry_after=None, configured_max=7)
    assert BACKOFF_SCHEDULE[1] * JITTER_MIN <= delay <= BACKOFF_SCHEDULE[1] * JITTER_MAX


def test_retry_after_capped_at_one_hour():
    assert next_delay_seconds(1, http_status=429, retry_after="86400", configured_max=7) == 3600


def test_non_429_ignores_retry_after():
    """Only 429 means 'the receiver told us when to come back'."""
    delay = next_delay_seconds(1, http_status=503, retry_after="45", configured_max=7)
    assert delay != 45.0
