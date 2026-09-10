"""Retry policy: exponential backoff with full-jitter.

Backoff is exponential so a struggling downstream service gets progressively
more room to recover. Jitter is applied because without it every consumer
instance that failed at the same moment retries at the same moment, and the
retry storm re-creates the outage it was meant to ride out.

Only :class:`~src.errors.TransientError` is retried. A
:class:`~src.errors.PermanentError` propagates on the first attempt so the
caller can route it to the DLQ without burning the backoff budget.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Protocol, TypeVar

from .errors import PermanentError, TransientError

T = TypeVar("T")


class RetryObserver(Protocol):
    """Called after each failed attempt that will be retried."""

    def __call__(self, attempt: int, delay: float, error: Exception) -> None: ...


@dataclass
class RetryPolicy:
    """Bounded exponential backoff with proportional jitter.

    ``max_attempts`` counts the first try, so ``max_attempts=4`` means one
    initial attempt plus three retries.

    ``sleep`` and ``rng`` are injectable so tests can assert on the delay
    sequence without actually waiting.
    """

    max_attempts: int = 4
    base_delay: float = 0.25
    max_delay: float = 4.0
    multiplier: float = 2.0
    jitter_ratio: float = 0.3
    sleep: Callable[[float], None] = time.sleep
    rng: random.Random = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.rng is None:
            self.rng = random.Random()

    def delay_for(self, attempt: int) -> float:
        """Backoff before retry number ``attempt`` (1 = after the first failure).

        Exponential growth is capped at ``max_delay`` *before* jitter, then
        jitter spreads the retry over +/- ``jitter_ratio`` of that value. The
        result is never negative.
        """
        raw = self.base_delay * (self.multiplier ** (attempt - 1))
        capped = min(raw, self.max_delay)
        spread = capped * self.jitter_ratio
        return max(0.0, capped + self.rng.uniform(-spread, spread))

    def execute(
        self,
        operation: Callable[[], T],
        on_retry: RetryObserver | None = None,
    ) -> tuple[T, int]:
        """Run ``operation``, retrying transient failures.

        Returns ``(result, attempts_used)``.

        Raises the last :class:`TransientError` once attempts are exhausted, or
        any :class:`PermanentError` immediately.
        """
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                return operation(), attempt
            except PermanentError:
                # Not retryable by definition — let the caller DLQ it now.
                raise
            except TransientError as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
                delay = self.delay_for(attempt)
                if on_retry is not None:
                    on_retry(attempt=attempt, delay=delay, error=exc)
                self.sleep(delay)

        assert last_error is not None  # only reachable after a TransientError
        raise last_error

    @classmethod
    def from_config(cls) -> "RetryPolicy":
        from . import config

        return cls(
            max_attempts=config.RETRY_MAX_ATTEMPTS,
            base_delay=config.RETRY_BASE_DELAY_SECONDS,
            max_delay=config.RETRY_MAX_DELAY_SECONDS,
            multiplier=config.RETRY_MULTIPLIER,
            jitter_ratio=config.RETRY_JITTER_RATIO,
        )
