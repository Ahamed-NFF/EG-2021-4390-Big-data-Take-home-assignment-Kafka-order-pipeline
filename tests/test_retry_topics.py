"""Retry-topic routing, header bookkeeping, and loop prevention."""

import pytest

from src.retry_topics import (
    H_ATTEMPT,
    H_FIRST_FAILED_AT,
    H_LAST_ERROR,
    H_NOT_BEFORE,
    H_ORIGINAL_OFFSET,
    H_ORIGINAL_PARTITION,
    H_ORIGINAL_TOPIC,
    H_REPLAY_COUNT,
    RetryRouter,
    build_tiers,
    decode_headers,
    read_attempt,
    read_not_before_ms,
    read_replay_count,
)

PREFIX = "orders.retry"
DELAYS = [2.0, 6.0, 15.0]


class FakeMessage:
    """Minimal stand-in for a confluent_kafka Message."""

    def __init__(self, topic="orders", partition=1, offset=42, headers=None):
        self._topic, self._partition = topic, partition
        self._offset, self._headers = offset, headers or []

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def headers(self):
        return self._headers


@pytest.fixture
def router():
    return RetryRouter(build_tiers(PREFIX, DELAYS), dlq_topic="orders.DLQ")


# --- tier construction ------------------------------------------------------

def test_tiers_are_named_and_numbered_from_one():
    tiers = build_tiers(PREFIX, DELAYS)
    assert [t.topic for t in tiers] == [
        "orders.retry.1", "orders.retry.2", "orders.retry.3"]
    assert [t.index for t in tiers] == [1, 2, 3]
    assert [t.delay_seconds for t in tiers] == DELAYS


def test_delays_increase_so_a_struggling_service_gets_more_room():
    delays = [t.delay_seconds for t in build_tiers(PREFIX, DELAYS)]
    assert delays == sorted(delays)


# --- routing ----------------------------------------------------------------

def test_main_topic_failure_starts_at_tier_one(router):
    assert router.next_destination("orders").topic == "orders.retry.1"


def test_each_tier_escalates_to_the_next(router):
    assert router.next_destination("orders.retry.1").topic == "orders.retry.2"
    assert router.next_destination("orders.retry.2").topic == "orders.retry.3"


def test_last_tier_has_nowhere_left_to_go(router):
    assert router.next_destination("orders.retry.3") is None, "exhausted -> DLQ"


def test_ladder_is_terminal_and_never_loops_back(router):
    """A record must not be able to cycle back to an earlier tier."""
    seen, topic = [], "orders"
    for _ in range(10):
        tier = router.next_destination(topic)
        if tier is None:
            break
        assert tier.topic not in seen, "revisited a tier - that is a loop"
        seen.append(tier.topic)
        topic = tier.topic
    assert seen == ["orders.retry.1", "orders.retry.2", "orders.retry.3"]


def test_tier_lookup_distinguishes_main_from_retry(router):
    assert router.tier_for_topic("orders") is None
    assert router.tier_for_topic("orders.retry.2").index == 2


def test_routing_ignores_a_forged_attempt_header(router):
    """Routing is driven by the topic, so a header cannot skip tiers."""
    msg = FakeMessage(topic="orders", headers=[(H_ATTEMPT, b"3")])
    assert router.next_destination(msg.topic()).topic == "orders.retry.1"


def test_router_with_no_tiers_sends_straight_to_dlq():
    empty = RetryRouter(build_tiers(PREFIX, []), dlq_topic="orders.DLQ")
    assert empty.next_destination("orders") is None
    assert empty.topics == []


# --- header bookkeeping -----------------------------------------------------

def test_headers_record_the_tier_and_when_it_becomes_due(router):
    tier = router.next_destination("orders")
    headers = decode_headers(
        router.build_headers(FakeMessage(), tier, RuntimeError("boom"), now_ms=1_000_000)
    )
    assert headers[H_ATTEMPT] == "1"
    assert headers[H_NOT_BEFORE] == str(1_000_000 + 2_000)  # +2s tier delay
    assert headers[H_LAST_ERROR] == "boom"


def test_first_hop_captures_the_original_provenance(router):
    msg = FakeMessage(topic="orders", partition=2, offset=99)
    tier = router.next_destination("orders")
    headers = decode_headers(router.build_headers(msg, tier, RuntimeError("x"),
                                                  now_ms=1_000))
    assert headers[H_ORIGINAL_TOPIC] == "orders"
    assert headers[H_ORIGINAL_PARTITION] == "2"
    assert headers[H_ORIGINAL_OFFSET] == "99"
    assert headers[H_FIRST_FAILED_AT] == "1000"


def test_provenance_survives_every_later_hop(router):
    """On tier 3 the record must still point at where it entered the system."""
    msg = FakeMessage(topic="orders", partition=2, offset=99)
    now = 1_000

    for expected_tier in (1, 2, 3):
        tier = router.next_destination(msg.topic())
        raw = router.build_headers(msg, tier, RuntimeError("x"), now_ms=now)
        headers = decode_headers(raw)
        assert headers[H_ATTEMPT] == str(expected_tier)
        # Original coordinates and first-failure time never change.
        assert headers[H_ORIGINAL_TOPIC] == "orders"
        assert headers[H_ORIGINAL_PARTITION] == "2"
        assert headers[H_ORIGINAL_OFFSET] == "99"
        assert headers[H_FIRST_FAILED_AT] == "1000"

        now += 5_000
        msg = FakeMessage(topic=tier.topic, partition=0, offset=7, headers=raw)


def test_stale_retry_headers_are_replaced_not_duplicated(router):
    msg = FakeMessage(
        topic="orders.retry.1",
        headers=[(H_ATTEMPT, b"1"), (H_NOT_BEFORE, b"111"), ("content-type", b"avro")],
    )
    tier = router.next_destination(msg.topic())
    raw = router.build_headers(msg, tier, RuntimeError("x"), now_ms=500_000)

    assert [k for k, _ in raw].count(H_ATTEMPT) == 1
    assert [k for k, _ in raw].count(H_NOT_BEFORE) == 1
    assert decode_headers(raw)[H_NOT_BEFORE] == str(500_000 + 6_000)


def test_producer_headers_are_carried_through(router):
    msg = FakeMessage(headers=[("content-type", b"application/avro")])
    tier = router.next_destination("orders")
    headers = decode_headers(router.build_headers(msg, tier, RuntimeError("x")))
    assert headers["content-type"] == "application/avro"


def test_due_time_grows_with_the_tier_delay(router):
    now = 1_000_000
    due = []
    for topic in ("orders", "orders.retry.1", "orders.retry.2"):
        tier = router.next_destination(topic)
        h = decode_headers(router.build_headers(FakeMessage(), tier,
                                                RuntimeError("x"), now_ms=now))
        due.append(int(h[H_NOT_BEFORE]) - now)
    assert due == [2_000, 6_000, 15_000]


# --- header parsing is defensive -------------------------------------------

def test_missing_headers_read_as_zero():
    assert read_attempt(None) == 0
    assert read_not_before_ms([]) == 0
    assert read_replay_count(None) == 0


def test_malformed_headers_do_not_crash_the_consumer():
    """A hand-edited or corrupt header must not take down the pipeline."""
    junk = [(H_ATTEMPT, b"not-a-number"), (H_NOT_BEFORE, b""),
            (H_REPLAY_COUNT, b"3.7")]
    assert read_attempt(junk) == 0
    assert read_not_before_ms(junk) == 0
    assert read_replay_count(junk) == 0


def test_replay_count_is_read_back():
    assert read_replay_count([(H_REPLAY_COUNT, b"2")]) == 2


def test_decode_headers_handles_bytes_and_str():
    decoded = decode_headers([("a", b"1"), ("b", "2")])
    assert decoded == {"a": "1", "b": "2"}
