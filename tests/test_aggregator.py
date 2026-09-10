"""Running-average aggregation: correctness, incrementality, per-product split."""

import math
import random
import statistics

import pytest

from src.aggregator import OrderAggregator, RunningStats


def order(order_id, product, price):
    return {"orderId": str(order_id), "product": product, "price": price}


# --- RunningStats -----------------------------------------------------------

def test_empty_stats_have_no_average():
    s = RunningStats()
    assert s.count == 0
    assert s.mean == 0.0
    assert s.snapshot()["min"] is None


def test_single_value():
    s = RunningStats()
    s.add(42.0)
    assert s.count == 1
    assert s.mean == 42.0
    assert s.minimum == s.maximum == 42.0
    assert s.variance == 0.0  # undefined for n=1; reported as 0


def test_mean_matches_statistics_module():
    values = [12.5, 99.0, 3.25, 47.75, 60.0, 8.125]
    s = RunningStats()
    for v in values:
        s.add(v)
    assert s.mean == pytest.approx(statistics.fmean(values))
    assert s.total == pytest.approx(sum(values))
    assert s.minimum == min(values)
    assert s.maximum == max(values)


def test_variance_and_stddev_match_statistics_module():
    values = [10.0, 20.0, 30.0, 40.0, 55.0]
    s = RunningStats()
    for v in values:
        s.add(v)
    assert s.variance == pytest.approx(statistics.variance(values))
    assert s.stddev == pytest.approx(statistics.stdev(values))


def test_average_is_correct_after_every_single_update():
    """It is a *running* average: correct at each step, not just at the end."""
    rng = random.Random(7)
    values, s = [], RunningStats()
    for _ in range(200):
        v = rng.uniform(1, 1000)
        values.append(v)
        s.add(v)
        assert s.mean == pytest.approx(statistics.fmean(values), rel=1e-9)


def test_welford_stays_accurate_where_a_naive_running_sum_drifts():
    """Large offset, tiny spread — the case a naive sum/count loses to rounding."""
    base = 1e8
    values = [base + i * 0.25 for i in range(1000)]
    s = RunningStats()
    for v in values:
        s.add(v)
    assert s.mean == pytest.approx(statistics.fmean(values), rel=1e-12)


def test_state_is_constant_size_regardless_of_stream_length():
    """O(1) memory is what makes this usable on an unbounded stream."""
    short, long_ = RunningStats(), RunningStats()
    for _ in range(10):
        short.add(1.0)
    for _ in range(100_000):
        long_.add(1.0)
    assert vars(short).keys() == vars(long_).keys()
    assert long_.mean == pytest.approx(1.0)


def test_handles_negative_and_zero_values_arithmetically():
    """The aggregator does not police business rules; validation does that."""
    s = RunningStats()
    for v in (-10.0, 0.0, 10.0):
        s.add(v)
    assert s.mean == pytest.approx(0.0)
    assert s.minimum == -10.0


# --- OrderAggregator --------------------------------------------------------

def test_add_returns_the_new_running_average():
    agg = OrderAggregator()
    assert agg.add(order(1, "Item1", 10.0)) == pytest.approx(10.0)
    assert agg.add(order(2, "Item1", 20.0)) == pytest.approx(15.0)
    assert agg.add(order(3, "Item2", 60.0)) == pytest.approx(30.0)


def test_tracks_each_product_separately():
    agg = OrderAggregator()
    for o in [order(1, "Item1", 10.0), order(2, "Item2", 100.0),
              order(3, "Item1", 30.0), order(4, "Item2", 200.0)]:
        agg.add(o)

    assert agg.by_product["Item1"].mean == pytest.approx(20.0)
    assert agg.by_product["Item2"].mean == pytest.approx(150.0)
    assert agg.by_product["Item1"].count == agg.by_product["Item2"].count == 2
    assert agg.running_average == pytest.approx(85.0)


def test_per_product_counts_sum_to_the_overall_count():
    rng = random.Random(3)
    agg = OrderAggregator()
    for i in range(500):
        agg.add(order(i, f"Item{rng.randint(1, 5)}", rng.uniform(1, 100)))

    assert sum(s.count for s in agg.by_product.values()) == agg.processed_count == 500
    assert sum(s.total for s in agg.by_product.values()) == pytest.approx(agg.overall.total)


def test_accepts_string_prices_from_a_decoded_record():
    agg = OrderAggregator()
    agg.add({"orderId": "1", "product": "Item1", "price": "25.5"})
    assert agg.running_average == pytest.approx(25.5)


def test_snapshot_is_json_serialisable_and_shaped_for_republishing():
    import json

    agg = OrderAggregator()
    agg.add(order(1, "Item1", 10.0))
    agg.add(order(2, "Item2", 20.0))

    snap = agg.snapshot()
    assert set(snap) == {"overall", "by_product"}
    assert snap["overall"]["count"] == 2
    assert snap["overall"]["average"] == pytest.approx(15.0)
    assert set(snap["by_product"]) == {"Item1", "Item2"}
    json.loads(json.dumps(snap))  # must round-trip for the aggregates topic


def test_snapshot_of_an_empty_aggregator_is_safe():
    snap = OrderAggregator().snapshot()
    assert snap["overall"]["count"] == 0
    assert snap["by_product"] == {}


def test_format_table_reports_every_product():
    agg = OrderAggregator()
    agg.add(order(1, "Item1", 10.0))
    agg.add(order(2, "Item2", 20.0))

    table = agg.format_table()
    assert "RUNNING AVERAGE" in table
    assert "Item1" in table and "Item2" in table
    assert "15.00" in table


def test_averages_are_finite_for_realistic_float32_prices():
    rng = random.Random(11)
    agg = OrderAggregator()
    for i in range(2000):
        agg.add(order(i, "Item1", round(rng.uniform(5.0, 500.0), 2)))
    assert math.isfinite(agg.running_average)
    assert 5.0 <= agg.running_average <= 500.0
