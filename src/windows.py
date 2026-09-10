"""Tumbling-window aggregation over order prices.

The cumulative average in :mod:`src.aggregator` answers "what is the average
order value since this consumer started". On a long-running stream that number
stops being interesting: after a million records, a sudden shift in pricing
barely moves it, and the state it summarises only ever grows.

A tumbling window answers the more useful question — "what is the average
*right now*" — by bucketing records into fixed, non-overlapping intervals:

    window [0s..10s)   window [10s..20s)   window [20s..30s)
    ├─────────────────┤├─────────────────┤├─────────────────┤
     avg 231.40          avg 198.02          avg 402.11

Windows are keyed on **event time** — the timestamp Kafka recorded when the
producer published the record — not on when this consumer happened to read it.
Processing time would make the buckets depend on consumer lag, so a backlog
being drained would pile hours of orders into one window.

State stays bounded, which is the property that makes this usable on an
unbounded stream: once a window closes it is emitted and evicted, so memory is
proportional to the number of *open* windows, not to the length of the stream.

A window closes when the watermark — the highest event time seen so far —
passes the window's end plus a grace period. The grace period exists because
records can arrive slightly out of order across partitions; a record that turns
up after its window has already closed is counted as late rather than silently
folded into the wrong bucket.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .aggregator import RunningStats


def clock(epoch_ms: int) -> str:
    """Local wall-clock time, for labels a human can match to the run."""
    return datetime.fromtimestamp(epoch_ms / 1000).strftime("%H:%M:%S")


@dataclass
class Window:
    """One tumbling interval, half-open: ``[start_ms, end_ms)``."""

    start_ms: int
    end_ms: int
    stats: RunningStats = field(default_factory=RunningStats)

    @property
    def label(self) -> str:
        return f"[{clock(self.start_ms)} .. {clock(self.end_ms)})"

    def snapshot(self) -> dict:
        return {
            "windowStartMs": self.start_ms,
            "windowEndMs": self.end_ms,
            "windowSeconds": round((self.end_ms - self.start_ms) / 1000, 3),
            **self.stats.snapshot(),
        }


class TumblingWindowAggregator:
    """Fixed-size, non-overlapping windows over event time."""

    def __init__(self, window_seconds: float, grace_seconds: float = 0.0) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_ms = int(window_seconds * 1000)
        self.grace_ms = int(max(0.0, grace_seconds) * 1000)

        self._open: dict[int, Window] = {}
        self.closed: list[Window] = []
        self.late_records = 0
        self.watermark_ms = 0

    # --- window maths -------------------------------------------------------
    def window_start_for(self, timestamp_ms: int) -> int:
        """Floor an event time to its window boundary."""
        return (int(timestamp_ms) // self.window_ms) * self.window_ms

    # --- ingestion ----------------------------------------------------------
    def add(self, timestamp_ms: int, price: float) -> list[Window]:
        """Fold one record in; returns any windows that closed as a result.

        Closing is driven by the incoming record's timestamp advancing the
        watermark, so a window emits as soon as the stream has moved past it
        rather than on a wall-clock timer.
        """
        timestamp_ms = int(timestamp_ms)
        self.watermark_ms = max(self.watermark_ms, timestamp_ms)

        start = self.window_start_for(timestamp_ms)
        if start not in self._open:
            # Already emitted and evicted: this record is late beyond grace.
            if any(w.start_ms == start for w in self.closed):
                self.late_records += 1
                return []
            self._open[start] = Window(start_ms=start, end_ms=start + self.window_ms)

        self._open[start].stats.add(price)
        return self._close_expired()

    def _close_expired(self) -> list[Window]:
        """Emit and evict every window the watermark has moved past."""
        cutoff = self.watermark_ms - self.grace_ms
        expired = [w for w in self._open.values() if w.end_ms <= cutoff]
        for window in sorted(expired, key=lambda w: w.start_ms):
            del self._open[window.start_ms]
            self.closed.append(window)
        return sorted(expired, key=lambda w: w.start_ms)

    def flush(self) -> list[Window]:
        """Close every remaining open window, for end of stream."""
        remaining = sorted(self._open.values(), key=lambda w: w.start_ms)
        self._open.clear()
        self.closed.extend(remaining)
        return remaining

    # --- reporting ----------------------------------------------------------
    @property
    def open_windows(self) -> list[Window]:
        return sorted(self._open.values(), key=lambda w: w.start_ms)

    @property
    def open_window_count(self) -> int:
        """Size of the retained state - the number that must stay bounded."""
        return len(self._open)

    def format_recent(self, limit: int = 5) -> str:
        """Last few closed windows, for the live console display."""
        recent = self.closed[-limit:]
        if not recent:
            return "  (no window has closed yet)"

        lines = [
            f"  {'window':<26}{'count':>7}{'avg price':>13}{'min':>10}{'max':>10}"
        ]
        for w in recent:
            s = w.stats
            lines.append(
                f"  {w.label:<26}{s.count:>7}{s.mean:>13.2f}"
                f"{s.minimum:>10.2f}{s.maximum:>10.2f}"
            )
        if self.late_records:
            lines.append(f"  late records dropped: {self.late_records}")
        return "\n".join(lines)
