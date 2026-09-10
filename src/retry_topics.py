"""Non-blocking retry via tiered retry topics.

The blocking retry in :mod:`src.retry` sleeps inside the message handler. That
is simple and keeps ordering, but while it sleeps the consumer processes
nothing else from that partition — one slow record holds up every record behind
it. This is head-of-line blocking, and it is the reason Uber moved off
client-level retries in their reprocessing architecture.

The alternative used here republishes the failed record to a retry topic and
commits the original offset straight away, so the main partition keeps moving:

    orders ──fail──► orders.retry.1 ──fail──► orders.retry.2
                          (2s)                     (6s)
                                                     │
                                          fail ──► orders.retry.3 ──fail──► orders.DLQ
                                                     (15s)                   (terminal)

Each tier's records carry the wall-clock time they become eligible. A tier
consumer that polls a record too early rewinds and pauses that partition rather
than sleeping, so the other partitions it owns keep being served.

The delay is per tier, so every record on a given retry topic has the same
delay applied. That makes each retry topic monotonically ordered by due time,
which is what lets a consumer pause on the head record: nothing behind it can
be due sooner.

The trade-off is real and worth stating: a retried record rejoins the stream
later than records that never failed, so global ordering is lost. Blocking
retry preserves order and sacrifices throughput; topic retry does the reverse.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

# --- header names -----------------------------------------------------------
H_ATTEMPT = "retry-attempt"
H_NOT_BEFORE = "retry-not-before-ms"
H_FIRST_FAILED_AT = "retry-first-failed-at"
H_ORIGINAL_TOPIC = "retry-original-topic"
H_ORIGINAL_PARTITION = "retry-original-partition"
H_ORIGINAL_OFFSET = "retry-original-offset"
H_LAST_ERROR = "retry-last-error"

#: Times this record has been replayed out of the DLQ by an operator. Distinct
#: from `retry-attempt`, which counts automatic tier hops within one journey.
H_REPLAY_COUNT = "x-replay-count"


@dataclass(frozen=True)
class RetryTier:
    """One rung of the retry ladder."""

    index: int  # 1-based; also the value written to the retry-attempt header
    topic: str
    delay_seconds: float


def build_tiers(prefix: str, delays: list[float]) -> list[RetryTier]:
    return [
        RetryTier(index=i, topic=f"{prefix}.{i}", delay_seconds=float(d))
        for i, d in enumerate(delays, start=1)
    ]


def decode_headers(headers) -> dict[str, str]:
    """Kafka headers as a plain dict of str -> str."""
    out: dict[str, str] = {}
    for key, value in headers or []:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "replace")
        out[key] = value
    return out


def _int_header(headers: dict[str, str], name: str, default: int = 0) -> int:
    try:
        return int(headers[name])
    except (KeyError, TypeError, ValueError):
        return default


def read_attempt(headers) -> int:
    """Which tier this record is currently on; 0 if it is from the main topic."""
    return _int_header(decode_headers(headers), H_ATTEMPT, 0)


def read_replay_count(headers) -> int:
    return _int_header(decode_headers(headers), H_REPLAY_COUNT, 0)


def read_not_before_ms(headers) -> int:
    """Epoch-ms this record becomes eligible; 0 means immediately."""
    return _int_header(decode_headers(headers), H_NOT_BEFORE, 0)


class RetryRouter:
    """Decides where a record goes after a transient failure.

    Routing is driven by which topic the record arrived on, not by a counter
    the record carries, so a record cannot skip tiers or loop back to an
    earlier one by presenting a forged header.
    """

    def __init__(self, tiers: list[RetryTier], dlq_topic: str) -> None:
        self.tiers = tiers
        self.dlq_topic = dlq_topic
        self._by_topic = {t.topic: t for t in tiers}

    @property
    def topics(self) -> list[str]:
        return [t.topic for t in self.tiers]

    def tier_for_topic(self, topic: str) -> RetryTier | None:
        """The tier a record was consumed from, or None for the main topic."""
        return self._by_topic.get(topic)

    def next_destination(self, current_topic: str) -> RetryTier | None:
        """Where a record from ``current_topic`` goes next.

        ``None`` means the ladder is exhausted and the record belongs in the
        DLQ. A record from the main topic starts at tier 1.
        """
        current = self._by_topic.get(current_topic)
        if current is None:
            return self.tiers[0] if self.tiers else None
        nxt = current.index + 1  # tiers are 1-based and contiguous
        return next((t for t in self.tiers if t.index == nxt), None)

    def build_headers(
        self,
        msg,
        tier: RetryTier,
        error: Exception,
        now_ms: int | None = None,
    ) -> list[tuple[str, bytes]]:
        """Headers for republishing ``msg`` onto ``tier``.

        Original provenance is written once, on the first hop, and carried
        unchanged after that — so a record sitting on tier 3 still points back
        at the offset it entered the system on.
        """
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        existing = decode_headers(msg.headers())

        # Keep anything the producer set, minus the retry bookkeeping we are
        # about to rewrite.
        headers = [
            (k, v.encode("utf-8", "replace"))
            for k, v in existing.items()
            if not k.startswith("retry-")
        ]

        first_failed = existing.get(H_FIRST_FAILED_AT)
        headers += [
            (H_ATTEMPT, str(tier.index).encode()),
            (H_NOT_BEFORE, str(now_ms + int(tier.delay_seconds * 1000)).encode()),
            (H_FIRST_FAILED_AT, (first_failed or str(now_ms)).encode()),
            (H_ORIGINAL_TOPIC,
             (existing.get(H_ORIGINAL_TOPIC) or msg.topic()).encode()),
            (H_ORIGINAL_PARTITION,
             (existing.get(H_ORIGINAL_PARTITION) or str(msg.partition())).encode()),
            (H_ORIGINAL_OFFSET,
             (existing.get(H_ORIGINAL_OFFSET) or str(msg.offset())).encode()),
            (H_LAST_ERROR, str(error)[:900].encode("utf-8", "replace")),
        ]
        return headers
