"""Tumbling windows: bucketing, closing, eviction, late data."""

import statistics

import pytest

from src.windows import TumblingWindowAggregator

WINDOW = 10.0  # seconds
MS = 1000


def agg(window_seconds=WINDOW, grace_seconds=0.0):
    return TumblingWindowAggregator(window_seconds, grace_seconds)


# --- bucketing --------------------------------------------------------------

def test_window_start_floors_to_the_boundary():
    a = agg()
    assert a.window_start_for(0) == 0
    assert a.window_start_for(1 * MS) == 0
    assert a.window_start_for(9_999) == 0
    assert a.window_start_for(10 * MS) == 10 * MS
    assert a.window_start_for(19_999) == 10 * MS


def test_windows_are_half_open_so_boundaries_do_not_double_count():
    a = agg()
    a.add(9_999, 1.0)   # belongs to [0, 10000)
    a.add(10_000, 2.0)  # belongs to [10000, 20000)
    a.flush()
    starts = [w.start_ms for w in a.closed]
    assert starts == [0, 10 * MS]
    assert all(w.stats.count == 1 for w in a.closed)


def test_records_in_the_same_window_aggregate_together():
    a = agg()
    for price in (10.0, 20.0, 60.0):
        a.add(5 * MS, price)
    (window,) = a.flush()
    assert window.stats.count == 3
    assert window.stats.mean == pytest.approx(30.0)


def test_rejects_non_positive_window():
    with pytest.raises(ValueError, match="must be positive"):
        TumblingWindowAggregator(0)


# --- closing and eviction ---------------------------------------------------

def test_window_closes_when_the_watermark_passes_its_end():
    a = agg()
    assert a.add(1 * MS, 10.0) == []      # window [0,10s) still open
    closed = a.add(11 * MS, 20.0)         # watermark 11s > 10s
    assert [w.start_ms for w in closed] == [0]
    assert closed[0].stats.mean == pytest.approx(10.0)


def test_closing_evicts_state_so_memory_stays_bounded():
    """The whole reason to window rather than accumulate forever."""
    a = agg()
    for i in range(500):
        a.add(i * MS, 1.0)
    # 500 seconds of data at a 10s window = 50 windows, but only the current
    # one is still open; the rest have been emitted and dropped.
    assert a.open_window_count == 1
    assert len(a.closed) == 49


def test_grace_period_keeps_a_window_open_for_late_arrivals():
    a = agg(grace_seconds=3.0)
    a.add(1 * MS, 10.0)                   # -> window [0,10s)

    # Watermark 11s, minus 3s grace, is 8s — still inside window [0,10s).
    assert a.add(11 * MS, 20.0) == [], "grace should hold [0,10s) open"

    # So this straggler still lands in its own window rather than being dropped.
    assert a.add(2 * MS, 30.0) == []

    closed = a.add(14 * MS, 40.0)         # 14s - 3s grace = 11s, past the end
    assert [w.start_ms for w in closed] == [0]
    assert closed[0].stats.count == 2     # 10.0 and the late 30.0
    assert closed[0].stats.mean == pytest.approx(20.0)
    assert a.late_records == 0            # it arrived within grace, so not late


def test_without_grace_the_same_straggler_is_dropped():
    a = agg(grace_seconds=0.0)
    a.add(1 * MS, 10.0)
    a.add(11 * MS, 20.0)                  # closes [0,10s) immediately
    a.add(2 * MS, 30.0)                   # too late now
    assert a.late_records == 1


def test_record_after_its_window_closed_is_counted_as_late():
    a = agg()
    a.add(1 * MS, 10.0)
    a.add(25 * MS, 20.0)                  # closes [0,10s)
    assert a.late_records == 0

    a.add(2 * MS, 99.0)                   # far too late
    assert a.late_records == 1
    closed_zero = [w for w in a.closed if w.start_ms == 0][0]
    assert closed_zero.stats.count == 1, "late record must not mutate a closed window"


def test_out_of_order_within_an_open_window_is_accepted():
    a = agg()
    a.add(8 * MS, 10.0)
    a.add(3 * MS, 20.0)                   # earlier, but same window
    (window,) = a.flush()
    assert window.stats.count == 2


def test_watermark_never_goes_backwards():
    a = agg()
    a.add(50 * MS, 1.0)
    a.add(2 * MS, 1.0)
    assert a.watermark_ms == 50 * MS


# --- flush ------------------------------------------------------------------

def test_flush_closes_every_open_window():
    a = agg()
    a.add(1 * MS, 10.0)
    a.add(11 * MS, 20.0)                  # closes the first, opens the second
    remaining = a.flush()
    assert [w.start_ms for w in remaining] == [10 * MS]
    assert a.open_window_count == 0


def test_flush_on_an_empty_aggregator_is_safe():
    assert agg().flush() == []


# --- correctness of the numbers ---------------------------------------------

def test_window_averages_match_the_statistics_module():
    a = agg()
    first = [12.5, 99.0, 3.25]
    second = [47.75, 60.0]
    for p in first:
        a.add(1 * MS, p)
    for p in second:
        a.add(12 * MS, p)
    a.flush()

    by_start = {w.start_ms: w for w in a.closed}
    assert by_start[0].stats.mean == pytest.approx(statistics.fmean(first))
    assert by_start[10 * MS].stats.mean == pytest.approx(statistics.fmean(second))


def test_snapshot_is_json_serialisable_with_window_bounds():
    import json

    a = agg()
    a.add(1 * MS, 10.0)
    (window,) = a.flush()
    snap = window.snapshot()

    assert snap["windowStartMs"] == 0
    assert snap["windowEndMs"] == 10 * MS
    assert snap["windowSeconds"] == pytest.approx(10.0)
    assert snap["count"] == 1
    json.loads(json.dumps(snap))


def test_cumulative_and_windowed_answer_different_questions():
    """A late price shift barely moves the cumulative mean but dominates a window."""
    a = agg()
    for _ in range(100):
        a.add(1 * MS, 10.0)          # window [0,10s): all cheap
    for _ in range(5):
        a.add(11 * MS, 1000.0)       # window [10,20s): all expensive
    a.flush()

    by_start = {w.start_ms: w for w in a.closed}
    cumulative = (100 * 10.0 + 5 * 1000.0) / 105
    assert by_start[0].stats.mean == pytest.approx(10.0)
    assert by_start[10 * MS].stats.mean == pytest.approx(1000.0)
    assert cumulative == pytest.approx(57.14, abs=0.01)


def test_format_recent_is_safe_before_any_window_closes():
    assert "no window" in agg().format_recent()


def test_format_recent_lists_closed_windows():
    a = agg()
    a.add(1 * MS, 10.0)
    a.add(11 * MS, 20.0)
    a.add(21 * MS, 30.0)
    text = a.format_recent()
    assert "avg price" in text
    assert "10.00" in text
