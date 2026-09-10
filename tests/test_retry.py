"""Retry policy: what gets retried, how long it backs off, when it gives up."""

import random

import pytest

from src.errors import PermanentError, TransientError, ValidationError
from src.retry import RetryPolicy


class Recorder:
    """Captures sleep durations instead of actually waiting."""

    def __init__(self):
        self.slept: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.slept.append(seconds)


def policy(**kwargs) -> tuple[RetryPolicy, Recorder]:
    recorder = Recorder()
    defaults = dict(
        max_attempts=4,
        base_delay=1.0,
        max_delay=10.0,
        multiplier=2.0,
        jitter_ratio=0.0,   # deterministic unless a test asks for jitter
        sleep=recorder,
        rng=random.Random(1234),
    )
    defaults.update(kwargs)
    return RetryPolicy(**defaults), recorder


# --- success paths ----------------------------------------------------------

def test_succeeds_first_try_without_sleeping():
    p, rec = policy()
    result, attempts = p.execute(lambda: "ok")
    assert (result, attempts) == ("ok", 1)
    assert rec.slept == []


def test_recovers_after_transient_failures():
    p, rec = policy()
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("sink down")
        return "recovered"

    result, attempts = p.execute(flaky)
    assert result == "recovered"
    assert attempts == 3
    assert len(rec.slept) == 2  # slept before attempt 2 and attempt 3


# --- backoff shape ----------------------------------------------------------

def test_backoff_is_exponential():
    p, _ = policy(base_delay=1.0, multiplier=2.0, max_delay=100.0)
    assert [p.delay_for(n) for n in (1, 2, 3, 4)] == [1.0, 2.0, 4.0, 8.0]


def test_backoff_is_capped_at_max_delay():
    p, _ = policy(base_delay=1.0, multiplier=10.0, max_delay=5.0)
    assert [p.delay_for(n) for n in (1, 2, 3, 9)] == [1.0, 5.0, 5.0, 5.0]


def test_jitter_stays_within_ratio_and_varies():
    p, _ = policy(base_delay=10.0, multiplier=1.0, max_delay=10.0, jitter_ratio=0.3)
    delays = [p.delay_for(1) for _ in range(200)]
    assert all(7.0 <= d <= 13.0 for d in delays)
    assert len(set(delays)) > 100, "jitter should spread retries, not cluster them"


def test_jitter_never_produces_a_negative_delay():
    p, _ = policy(base_delay=0.01, multiplier=1.0, max_delay=0.01, jitter_ratio=5.0)
    assert all(p.delay_for(1) >= 0.0 for _ in range(500))


def test_actual_sleeps_follow_the_backoff_schedule():
    p, rec = policy(base_delay=0.5, multiplier=2.0, max_attempts=4)

    with pytest.raises(TransientError):
        p.execute(lambda: (_ for _ in ()).throw(TransientError("always down")))

    assert rec.slept == [0.5, 1.0, 2.0]  # three sleeps between four attempts


# --- giving up --------------------------------------------------------------

def test_raises_last_transient_error_when_attempts_are_exhausted():
    p, rec = policy(max_attempts=3)
    attempts = {"n": 0}

    def always_fails():
        attempts["n"] += 1
        raise TransientError(f"failure {attempts['n']}")

    with pytest.raises(TransientError, match="failure 3"):
        p.execute(always_fails)

    assert attempts["n"] == 3
    assert len(rec.slept) == 2  # no sleep after the final attempt


def test_max_attempts_of_one_means_no_retry():
    p, rec = policy(max_attempts=1)
    calls = {"n": 0}

    def once():
        calls["n"] += 1
        raise TransientError("down")

    with pytest.raises(TransientError):
        p.execute(once)
    assert calls["n"] == 1
    assert rec.slept == []


def test_rejects_zero_max_attempts():
    with pytest.raises(ValueError, match="at least 1"):
        RetryPolicy(max_attempts=0)


# --- permanent errors bypass retry entirely ---------------------------------

def test_permanent_error_is_not_retried():
    p, rec = policy()
    calls = {"n": 0}

    def poison():
        calls["n"] += 1
        raise PermanentError("malformed record")

    with pytest.raises(PermanentError):
        p.execute(poison)

    assert calls["n"] == 1, "a permanent failure must not consume the retry budget"
    assert rec.slept == []


def test_validation_error_is_treated_as_permanent():
    p, rec = policy()
    with pytest.raises(ValidationError):
        p.execute(lambda: (_ for _ in ()).throw(ValidationError("negative price")))
    assert rec.slept == []


# --- observer ---------------------------------------------------------------

def test_on_retry_observer_sees_every_retry():
    p, _ = policy(max_attempts=3, base_delay=1.0, multiplier=2.0)
    seen = []

    def observe(attempt, delay, error):
        seen.append((attempt, delay, str(error)))

    with pytest.raises(TransientError):
        p.execute(lambda: (_ for _ in ()).throw(TransientError("boom")), on_retry=observe)

    assert [s[0] for s in seen] == [1, 2]
    assert [s[1] for s in seen] == [1.0, 2.0]
    assert all(s[2] == "boom" for s in seen)


def test_observer_is_not_called_on_success():
    p, _ = policy()
    seen = []
    p.execute(lambda: "fine", on_retry=lambda **kw: seen.append(kw))
    assert seen == []
