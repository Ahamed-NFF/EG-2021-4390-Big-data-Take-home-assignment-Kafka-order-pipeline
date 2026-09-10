"""Real-time aggregation of order prices.

The running average is computed with Welford's online algorithm rather than
``sum / count``. Both give the same answer on small demo data, but Welford
updates the mean incrementally and never holds a growing sum, so it stays
accurate on an unbounded stream of float32 prices where a naive running sum
loses low-order bits once it grows large. That property is the point of a
*streaming* aggregate: state stays O(1) per key regardless of stream length.

Aggregates are kept globally and per product, so the consumer can answer both
"what is the average order value right now" and "which product line is
driving it".
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class RunningStats:
    """O(1) online mean/variance over a stream of values."""

    count: int = 0
    mean: float = 0.0
    _m2: float = 0.0  # sum of squared deviations from the running mean
    total: float = 0.0
    minimum: float = math.inf
    maximum: float = -math.inf

    def add(self, value: float) -> None:
        value = float(value)
        self.count += 1
        # Welford: shift the mean by the residual scaled by the new count.
        delta = value - self.mean
        self.mean += delta / self.count
        self._m2 += delta * (value - self.mean)
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    @property
    def variance(self) -> float:
        """Sample variance; 0.0 until there are at least two observations."""
        return self._m2 / (self.count - 1) if self.count > 1 else 0.0

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def snapshot(self) -> dict:
        return {
            "count": self.count,
            "average": round(self.mean, 4),
            "total": round(self.total, 4),
            "min": round(self.minimum, 4) if self.count else None,
            "max": round(self.maximum, 4) if self.count else None,
            "stddev": round(self.stddev, 4),
        }


@dataclass
class OrderAggregator:
    """Running price statistics, overall and broken down by product."""

    overall: RunningStats = field(default_factory=RunningStats)
    by_product: dict[str, RunningStats] = field(default_factory=dict)

    def add(self, order: dict) -> float:
        """Fold one order into the aggregates; returns the new running average."""
        price = float(order["price"])
        product = str(order["product"])

        self.overall.add(price)
        self.by_product.setdefault(product, RunningStats()).add(price)
        return self.overall.mean

    @property
    def running_average(self) -> float:
        return self.overall.mean

    @property
    def processed_count(self) -> int:
        return self.overall.count

    def snapshot(self) -> dict:
        """Serialisable view of current state, for logging or republishing."""
        return {
            "overall": self.overall.snapshot(),
            "by_product": {
                product: stats.snapshot()
                for product, stats in sorted(self.by_product.items())
            },
        }

    def format_table(self) -> str:
        """Human-readable block for the live console demo."""
        o = self.overall
        lines = [
            f"  RUNNING AVERAGE : {o.mean:>10.2f}   (n={o.count})",
            f"  total={o.total:.2f}  min={o.minimum:.2f}  "
            f"max={o.maximum:.2f}  stddev={o.stddev:.2f}",
            "  " + "-" * 52,
            f"  {'product':<12}{'count':>8}{'avg price':>14}{'total':>16}",
        ]
        for product, stats in sorted(self.by_product.items()):
            lines.append(
                f"  {product:<12}{stats.count:>8}{stats.mean:>14.2f}{stats.total:>16.2f}"
            )
        return "\n".join(lines)
