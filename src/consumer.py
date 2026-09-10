"""Order consumer: Avro decode -> validate -> retry -> aggregate, DLQ on failure.

Run:
    python -m src.consumer                          # non-blocking retry topics
    python -m src.consumer --retry-mode blocking    # in-process backoff

Per-record pipeline, and where each failure goes:

    bytes
      |
      +-- Avro decode ......... fails -> DeserializationError (permanent) -> DLQ
      |
      +-- business validation . fails -> ValidationError      (permanent) -> DLQ
      |
      +-- downstream sink ..... fails -> TransientError -> retry (see below)
      |                                    exhausted        -> DLQ
      |
      +-- fold into cumulative average + tumbling window
      |
      +-- commit offset

Two retry strategies are implemented, selected with ``--retry-mode``:

``topics`` (default)
    Non-blocking. The record is republished to the next retry topic and the
    original offset is committed immediately, so the partition keeps moving.
    Records that are not yet due cause their partition to be rewound and
    paused, never slept on. Throughput is preserved; global ordering is not.

``blocking``
    In-process exponential backoff with jitter. Ordering within the partition
    is preserved, but nothing else on that partition is processed while a
    record backs off - head-of-line blocking.

Delivery semantics are **at-least-once**. Auto-commit is disabled and the
offset is committed only once the record has been processed, republished to a
retry topic, or parked in the DLQ - and in the last two cases only after the
broker has *acknowledged* that write. A crash mid-retry replays the record
instead of losing it. The trade is that a crash between a successful process
and the commit replays a record that was already counted, so the aggregate is
not exactly-once; a real deployment would key the sink idempotently or use
transactions to close that gap.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from . import config, retry_topics
from .aggregator import OrderAggregator
from .avro_codec import default_codec
from .errors import DlqWriteError, PermanentError, TransientError, ValidationError
from .retry import RetryPolicy
from .retry_topics import RetryRouter, build_tiers
from .windows import TumblingWindowAggregator

MAX_PLAUSIBLE_PRICE = 1_000_000.0


def validate_order(order: dict) -> None:
    """Business rules Avro cannot express.

    Avro guarantees ``price`` is a 32-bit float; it cannot say the float must
    be positive and finite. These checks are what separate a schema-valid
    record from a *usable* one, and failing them is permanent by nature.
    """
    order_id = order.get("orderId", "")
    if not isinstance(order_id, str) or not order_id.strip():
        raise ValidationError("orderId is empty")

    product = order.get("product", "")
    if not isinstance(product, str) or not product.strip():
        raise ValidationError(f"product is empty for order {order_id}")

    price = order.get("price")
    if price is None or not math.isfinite(float(price)):
        raise ValidationError(f"price is not a finite number for order {order_id}")
    if float(price) <= 0:
        raise ValidationError(
            f"price must be positive, got {float(price):.2f} for order {order_id}"
        )
    if float(price) > MAX_PLAUSIBLE_PRICE:
        raise ValidationError(
            f"price {float(price):.2f} exceeds sanity ceiling for order {order_id}"
        )


class SimulatedSink:
    """Stands in for a downstream system that is occasionally unavailable.

    A real consumer would write to a warehouse, cache, or HTTP API here. Those
    fail intermittently, which is exactly the condition retry exists for, so
    the demo reproduces it with a configurable failure probability.
    """

    def __init__(self, failure_rate: float, rng: random.Random) -> None:
        self.failure_rate = failure_rate
        self.rng = rng
        self.calls = 0
        self.injected_failures = 0

    def write(self, order: dict) -> None:
        self.calls += 1
        if self.rng.random() < self.failure_rate:
            self.injected_failures += 1
            raise TransientError(
                f"downstream sink unavailable while writing order {order['orderId']}"
            )


class Stats:
    def __init__(self) -> None:
        self.consumed = 0
        self.from_main = 0
        self.from_retry = 0
        self.processed = 0
        self.retried = 0          # records that needed more than one attempt
        self.retry_attempts = 0   # extra attempts spent (blocking mode)
        self.tier_hops = 0        # records pushed onto a retry topic (topic mode)
        self.recovered = 0        # failed at least once, then succeeded
        self.deferred = 0         # times a partition was paused for a delay
        self.dlq_deserialization = 0
        self.dlq_validation = 0
        self.dlq_retry_exhausted = 0

    @property
    def dlq_total(self) -> int:
        return (
            self.dlq_deserialization
            + self.dlq_validation
            + self.dlq_retry_exhausted
        )


class OrderConsumer:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.codec = default_codec()
        self.rng = random.Random(args.seed)
        self.aggregator = OrderAggregator()
        self.windows = TumblingWindowAggregator(
            window_seconds=args.window_seconds,
            grace_seconds=config.WINDOW_GRACE_SECONDS,
        )
        self.stats = Stats()
        self.retry_policy = RetryPolicy(
            max_attempts=args.max_attempts,
            base_delay=config.RETRY_BASE_DELAY_SECONDS,
            max_delay=config.RETRY_MAX_DELAY_SECONDS,
            multiplier=config.RETRY_MULTIPLIER,
            jitter_ratio=config.RETRY_JITTER_RATIO,
            rng=self.rng,
        )
        self.router = RetryRouter(
            tiers=build_tiers(config.RETRY_TOPIC_PREFIX, config.RETRY_TIER_DELAYS),
            dlq_topic=args.dlq_topic,
        )
        self.sink = SimulatedSink(args.transient_failure_rate, self.rng)

        consumer_conf = config.consumer_config(args.group)
        consumer_conf["bootstrap.servers"] = args.bootstrap_servers
        self.consumer = Consumer(consumer_conf)

        producer_conf = config.producer_config()
        producer_conf["bootstrap.servers"] = args.bootstrap_servers
        # One producer serves the DLQ, the retry topics and the aggregates.
        self.producer = Producer(producer_conf)

        # (topic, partition) -> epoch-ms at which it may be resumed.
        self._paused_until: dict[tuple[str, int], int] = {}
        self._main_drained = False
        self.running = True

        # Timing. The interesting number is how long the *main* topic takes to
        # drain: that is what head-of-line blocking actually costs, and it is
        # where the two retry modes differ. Total runtime is not comparable,
        # because the tier delays are deliberately longer than the in-process
        # backoffs and happen in the background either way.
        self._first_message_at: float | None = None
        self._main_drained_at: float | None = None

    # --- guaranteed writes --------------------------------------------------
    def produce_confirmed(self, topic, key, value, headers, what: str) -> None:
        """Produce and block until the broker acknowledges, or raise.

        Every write that lets the consumer move past a record - the DLQ and the
        retry topics both - has to be confirmed before the offset is committed.
        A fire-and-forget produce followed by a commit means a broker-side
        rejection silently destroys the record, which is the single worst
        failure mode this design can have.
        """
        outcome: dict[str, object] = {"error": None, "delivered": False}

        def on_delivery(err, _msg) -> None:
            outcome["error"] = err
            outcome["delivered"] = err is None

        try:
            self.producer.produce(topic=topic, key=key, value=value,
                                  headers=headers, on_delivery=on_delivery)
        except BufferError:
            self.producer.flush(timeout=10)
            self.producer.produce(topic=topic, key=key, value=value,
                                  headers=headers, on_delivery=on_delivery)

        remaining = self.producer.flush(timeout=15)
        if remaining:
            raise DlqWriteError(
                f"{what}: {remaining} message(s) still queued after flush timeout"
            )
        if outcome["error"] is not None:
            raise DlqWriteError(f"{what}: broker rejected the write: {outcome['error']}")
        if not outcome["delivered"]:
            raise DlqWriteError(f"{what}: no delivery confirmation received")

    # --- DLQ ----------------------------------------------------------------
    def send_to_dlq(self, msg, error: Exception, stage: str, attempts: int) -> None:
        """Park a failed record on the DLQ, preserving the original bytes.

        The value is the *untouched* original payload rather than a rewritten
        envelope, so a record can be replayed onto the main topic verbatim once
        the underlying problem is fixed. Diagnostics ride in headers, which
        keeps the payload replay-safe.
        """
        headers = list(msg.headers() or [])
        headers += [
            ("dlq-error-type", type(error).__name__.encode()),
            ("dlq-error-message", str(error)[:900].encode("utf-8", "replace")),
            ("dlq-failure-stage", stage.encode()),
            ("dlq-attempts", str(attempts).encode()),
            ("dlq-original-topic", msg.topic().encode()),
            ("dlq-original-partition", str(msg.partition()).encode()),
            ("dlq-original-offset", str(msg.offset()).encode()),
            ("dlq-failed-at", datetime.now(timezone.utc).isoformat().encode()),
        ]
        self.produce_confirmed(self.args.dlq_topic, msg.key(), msg.value(),
                               headers, what="DLQ write")

    # --- retry topics -------------------------------------------------------
    def send_to_retry_tier(self, msg, error: Exception) -> bool:
        """Republish onto the next retry tier. False means the ladder is spent."""
        tier = self.router.next_destination(msg.topic())
        if tier is None:
            return False

        headers = self.router.build_headers(msg, tier, error)
        self.produce_confirmed(tier.topic, msg.key(), msg.value(), headers,
                               what=f"retry tier {tier.index} write")
        self.stats.tier_hops += 1
        return True

    # --- aggregates ---------------------------------------------------------
    def publish_aggregate(self) -> None:
        """Republish the cumulative aggregate so downstream jobs can consume it."""
        snapshot = self.aggregator.snapshot()
        snapshot["emittedAt"] = datetime.now(timezone.utc).isoformat()
        self.producer.produce(
            topic=self.args.aggregate_topic,
            key=b"orders-global",
            value=json.dumps(snapshot).encode("utf-8"),
            headers=[("content-type", b"application/json")],
        )
        self.producer.poll(0)

    def publish_windows(self, closed) -> None:
        """Emit each closed window, keyed so compaction keeps one per window."""
        for window in closed:
            payload = window.snapshot()
            payload["emittedAt"] = datetime.now(timezone.utc).isoformat()
            self.producer.produce(
                topic=self.args.aggregate_topic,
                key=f"window-{window.start_ms}".encode(),
                value=json.dumps(payload).encode("utf-8"),
                headers=[("content-type", b"application/json"),
                         ("aggregate-type", b"tumbling-window")],
            )
        if closed:
            self.producer.poll(0)

    # --- per-record ---------------------------------------------------------
    def event_time_ms(self, msg) -> int:
        """Kafka's record timestamp, falling back to now if unset."""
        _, ts = msg.timestamp()
        return int(ts) if ts and ts > 0 else int(time.time() * 1000)

    def handle(self, msg) -> None:
        self.stats.consumed += 1
        from_retry = self.router.tier_for_topic(msg.topic()) is not None
        if from_retry:
            self.stats.from_retry += 1
        else:
            self.stats.from_main += 1

        ref = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"

        # 1. Deserialise. Any failure here is permanent: bad bytes stay bad.
        try:
            order = self.codec.decode(msg.value())
        except PermanentError as exc:
            self.stats.dlq_deserialization += 1
            self.send_to_dlq(msg, exc, stage="deserialize", attempts=1)
            print(f"  [DLQ] {ref} deserialize failed -> {exc}")
            return

        order_id = order["orderId"]

        # 2. Validate. Also permanent - the record itself is wrong.
        try:
            validate_order(order)
        except PermanentError as exc:
            self.stats.dlq_validation += 1
            self.send_to_dlq(msg, exc, stage="validate", attempts=1)
            print(f"  [DLQ] #{order_id} validation failed -> {exc}")
            return

        # 3. Hand to the downstream sink.
        if self.args.retry_mode == "blocking":
            attempts = self._process_blocking(msg, order, order_id)
        else:
            attempts = self._process_via_retry_topics(msg, order, order_id)
        if attempts is None:
            return  # routed to a retry tier or the DLQ

        # 4. Fold into the aggregates.
        self._record_success(msg, order, order_id, attempts, from_retry)

    def _process_blocking(self, msg, order: dict, order_id: str) -> int | None:
        """In-process backoff. Returns attempts used, or None if DLQ'd."""

        def on_retry(attempt: int, delay: float, error: Exception) -> None:
            self.stats.retry_attempts += 1
            print(f"  [RETRY] #{order_id} attempt {attempt}/"
                  f"{self.retry_policy.max_attempts} failed ({error}); "
                  f"backing off {delay:.2f}s")

        try:
            _, attempts = self.retry_policy.execute(
                lambda: self.sink.write(order), on_retry=on_retry
            )
            return attempts
        except TransientError as exc:
            self.stats.retry_attempts += 1
            self.stats.retried += 1
            self.stats.dlq_retry_exhausted += 1
            self.send_to_dlq(msg, exc, stage="process",
                             attempts=self.retry_policy.max_attempts)
            print(f"  [DLQ] #{order_id} exhausted "
                  f"{self.retry_policy.max_attempts} attempts -> {exc}")
            return None

    def _process_via_retry_topics(self, msg, order: dict, order_id: str) -> int | None:
        """One attempt only; failures move to the next tier. None if not done."""
        tier = self.router.tier_for_topic(msg.topic())
        attempt_number = (tier.index + 1) if tier else 1

        try:
            self.sink.write(order)
            return attempt_number
        except TransientError as exc:
            if self.send_to_retry_tier(msg, exc):
                nxt = self.router.next_destination(msg.topic())
                self.stats.retried += 1
                print(f"  [RETRY] #{order_id} failed ({exc}); "
                      f"-> {nxt.topic} in {nxt.delay_seconds:.0f}s "
                      "(partition keeps moving)")
            else:
                self.stats.dlq_retry_exhausted += 1
                self.send_to_dlq(msg, exc, stage="process", attempts=attempt_number)
                print(f"  [DLQ] #{order_id} exhausted all "
                      f"{len(self.router.tiers)} retry tiers -> {exc}")
            return None

    def _record_success(self, msg, order, order_id, attempts, from_retry) -> None:
        running_avg = self.aggregator.add(order)
        closed = self.windows.add(self.event_time_ms(msg), float(order["price"]))
        self.stats.processed += 1

        if attempts > 1 or from_retry:
            self.stats.recovered += 1
            note = f"  (recovered on attempt {attempts})"
        else:
            note = ""

        print(
            f"  [OK]  #{order_id:<6} {order['product']:<6} "
            f"price={float(order['price']):>9.2f} | "
            f"running avg={running_avg:>9.2f} over {self.aggregator.processed_count}"
            f"{note}"
        )

        if closed:
            self.publish_windows(closed)
            for w in closed:
                print(f"  [WINDOW CLOSED] {w.label}  n={w.stats.count}  "
                      f"avg={w.stats.mean:.2f}  "
                      f"(open windows retained: {self.windows.open_window_count})")

        if self.stats.processed % self.args.aggregate_every == 0:
            self.publish_aggregate()
            print("\n" + self.aggregator.format_table() + "\n")

    # --- delay handling -----------------------------------------------------
    def defer_if_not_due(self, msg) -> bool:
        """Pause a retry partition whose head record is not yet eligible.

        Rewind to this offset and pause the partition rather than sleeping.
        Sleeping would stall every other partition this consumer owns, which is
        the exact problem the retry-topic pattern exists to avoid.

        Pausing on the head record is sound because every record on a given
        retry topic carries the same delay, so the topic is ordered by due time
        and nothing behind this record can be due sooner.
        """
        not_before = retry_topics.read_not_before_ms(msg.headers())
        now_ms = int(time.time() * 1000)
        if not_before <= now_ms:
            return False

        tp_key = (msg.topic(), msg.partition())
        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        self.consumer.pause([TopicPartition(msg.topic(), msg.partition())])
        self._paused_until[tp_key] = not_before
        self.stats.deferred += 1

        wait = (not_before - now_ms) / 1000
        print(f"  [DEFER] {msg.topic()}[{msg.partition()}] not due for "
              f"{wait:.1f}s - partition paused, others keep flowing")
        return True

    def resume_due_partitions(self) -> None:
        now_ms = int(time.time() * 1000)
        due = [tp for tp, at in self._paused_until.items() if at <= now_ms]
        for topic, partition in due:
            self.consumer.resume([TopicPartition(topic, partition)])
            del self._paused_until[(topic, partition)]
            print(f"  [RESUME] {topic}[{partition}] is due")

    # --- main loop ----------------------------------------------------------
    def subscription(self) -> list[str]:
        if self.args.retry_mode == "topics":
            return [self.args.topic] + self.router.topics
        return [self.args.topic]

    def run(self) -> int:
        def stop(signum, frame):  # noqa: ARG001
            self.running = False
            print("\n[consumer] Ctrl+C received, shutting down cleanly...")

        signal.signal(signal.SIGINT, stop)

        topics = self.subscription()
        self.consumer.subscribe(topics)
        self.print_banner(topics)

        last_message_at = time.monotonic()
        exit_code = 0

        try:
            while self.running:
                self.resume_due_partitions()
                msg = self.consumer.poll(timeout=0.5)

                if msg is None:
                    if self._should_stop_when_idle(last_message_at):
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                last_message_at = time.monotonic()
                if self._first_message_at is None:
                    self._first_message_at = last_message_at

                # A retry record that is not yet eligible is rewound, not slept on.
                if self.router.tier_for_topic(msg.topic()) and self.defer_if_not_due(msg):
                    continue

                self.handle(msg)

                # At-least-once: commit only after the record is processed, sent
                # to a retry tier, or DLQ'd - and only after that write was
                # acknowledged. Synchronous so a commit failure is visible here.
                self.consumer.commit(message=msg, asynchronous=False)

                if self._main_quota_reached():
                    self._drain_main_topic()

        except DlqWriteError as exc:
            # The offset was deliberately not committed, so the broker will
            # redeliver this record. Stopping is correct: if the DLQ is
            # unwritable, every later failure is unparkable too.
            print(f"\n[consumer] FATAL: {exc}", file=sys.stderr)
            print("[consumer] offset NOT committed - the record will be "
                  "redelivered on restart. No data lost.", file=sys.stderr)
            exit_code = 3
        except KafkaException as exc:
            print(f"[consumer] fatal Kafka error: {exc}", file=sys.stderr)
            exit_code = 2
        finally:
            self.publish_windows(self.windows.flush())
            if self.aggregator.processed_count:
                self.publish_aggregate()
            self.producer.flush(timeout=10)
            self.consumer.close()

        self.print_report()
        return exit_code

    def _main_quota_reached(self) -> bool:
        return bool(self.args.max_messages) and not self._main_drained \
            and self.stats.from_main >= self.args.max_messages

    def _drain_main_topic(self) -> None:
        """Stop taking new orders but finish the retry work already in flight."""
        self._main_drained = True
        self._main_drained_at = time.monotonic()
        if self.args.retry_mode != "topics":
            self.running = False
            print(f"[consumer] reached --max-messages {self.args.max_messages}, stopping.")
            return

        main_partitions = [tp for tp in self.consumer.assignment()
                           if tp.topic == self.args.topic]
        if main_partitions:
            self.consumer.pause(main_partitions)
        print(f"\n[consumer] reached --max-messages {self.args.max_messages} on "
              f"'{self.args.topic}'; draining retry tiers before stopping.\n")

    def _should_stop_when_idle(self, last_message_at: float) -> bool:
        """Idle means no messages *and* nothing waiting on a timer."""
        if self._paused_until:
            return False
        idle = time.monotonic() - last_message_at
        if self._main_drained:
            if idle > self.args.drain_timeout:
                print(f"[consumer] retry tiers drained after {idle:.0f}s idle, stopping.")
                return True
            return False
        if self.args.idle_timeout and idle > self.args.idle_timeout:
            print(f"[consumer] idle for {idle:.0f}s, stopping.")
            return True
        return False

    # --- reporting ----------------------------------------------------------
    def print_banner(self, topics: list[str]) -> None:
        print("=" * 74)
        print("  ORDER CONSUMER  (Avro + retry + DLQ + running average + windows)")
        print("=" * 74)
        print(f"  broker            : {self.args.bootstrap_servers}")
        print(f"  subscribed topics : {', '.join(topics)}")
        print(f"  dlq topic         : {self.args.dlq_topic}")
        print(f"  aggregate topic   : {self.args.aggregate_topic}")
        print(f"  consumer group    : {self.args.group}")
        print(f"  schema fingerprint: {self.codec.fingerprint_hex}")
        print(f"  retry mode        : {self.args.retry_mode}")
        if self.args.retry_mode == "topics":
            ladder = " -> ".join(
                f"{t.topic}({t.delay_seconds:.0f}s)" for t in self.router.tiers
            )
            print(f"  retry ladder      : {ladder} -> {self.args.dlq_topic}")
            print("                      non-blocking: the partition is never slept on")
        else:
            print(f"                      up to {self.args.max_attempts} attempts, "
                  f"base {config.RETRY_BASE_DELAY_SECONDS}s, "
                  f"x{config.RETRY_MULTIPLIER}, cap {config.RETRY_MAX_DELAY_SECONDS}s, "
                  f"jitter +/-{config.RETRY_JITTER_RATIO:.0%}")
            print("                      blocking: ordering kept, partition stalls")
        print(f"  tumbling window   : {self.args.window_seconds:.0f}s "
              f"(grace {config.WINDOW_GRACE_SECONDS:.0f}s)")
        print(f"  injected transient failure rate: "
              f"{self.args.transient_failure_rate:.0%}")
        print("=" * 74)
        print("  waiting for messages (Ctrl+C to stop)...\n")

    def print_report(self) -> None:
        s = self.stats
        print("\n" + "=" * 74)
        print("  FINAL REPORT")
        print("=" * 74)
        print(f"  retry mode              : {self.args.retry_mode}")
        print(f"  consumed                : {s.consumed} "
              f"({s.from_main} from '{self.args.topic}', "
              f"{s.from_retry} from retry tiers)")
        print(f"  processed successfully  : {s.processed}")
        print(f"    of which recovered    : {s.recovered} "
              "(failed at least once, then succeeded)")
        if self.args.retry_mode == "topics":
            print(f"  pushed to a retry tier  : {s.tier_hops}")
            print(f"  partitions paused       : {s.deferred} "
                  "(deferred without blocking others)")
        else:
            print(f"  total retry attempts    : {s.retry_attempts}")
        print(f"  sent to DLQ             : {s.dlq_total}")
        print(f"    deserialization fail  : {s.dlq_deserialization}")
        print(f"    validation fail       : {s.dlq_validation}")
        print(f"    retries exhausted     : {s.dlq_retry_exhausted}")

        # Reconciliation is against main-topic intake: a record entering from
        # 'orders' must end up either processed or in the DLQ. Retry-tier reads
        # are re-reads of records already counted, so they are excluded.
        accounted = s.processed + s.dlq_total
        ok = accounted == s.from_main
        print(f"  accounted for           : {accounted}/{s.from_main} from main topic "
              f"{'OK - no records lost' if ok else 'MISMATCH'}")

        if self._first_message_at is not None:
            end = self._main_drained_at or time.monotonic()
            drain = end - self._first_message_at
            rate = s.from_main / drain if drain > 0 else 0.0
            print("-" * 74)
            print(f"  main topic drain time   : {drain:.1f}s "
                  f"({rate:.1f} orders/s)")
            print("    ^ the head-of-line cost. In blocking mode this includes "
                  "every backoff\n      sleep; in topics mode the backoff happens "
                  "off the critical path.")

        if self.aggregator.processed_count:
            print("-" * 74)
            print("  CUMULATIVE")
            print(self.aggregator.format_table())
            print("-" * 74)
            print(f"  TUMBLING WINDOWS ({self.args.window_seconds:.0f}s each, "
                  f"{len(self.windows.closed)} closed)")
            print(self.windows.format_recent(limit=8))
        print("=" * 74)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m src.consumer",
        description="Consume Avro orders with retry, DLQ and running-average aggregation.",
    )
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--dlq-topic", default=config.TOPIC_DLQ)
    p.add_argument("--aggregate-topic", default=config.TOPIC_AGGREGATES)
    p.add_argument("--group", default=config.CONSUMER_GROUP)
    p.add_argument("--retry-mode", choices=("topics", "blocking"), default="topics",
                   help="topics = non-blocking retry topics (default); "
                        "blocking = in-process exponential backoff")
    p.add_argument("--max-attempts", type=int, default=config.RETRY_MAX_ATTEMPTS,
                   help="blocking mode: total attempts per record, including the first")
    p.add_argument("--transient-failure-rate", type=float,
                   default=config.TRANSIENT_FAILURE_RATE,
                   help="probability the simulated sink raises a transient error")
    p.add_argument("--window-seconds", type=float, default=config.WINDOW_SECONDS,
                   help="width of the tumbling aggregation window")
    p.add_argument("--aggregate-every", type=int, default=config.AGGREGATE_PUBLISH_EVERY,
                   help="publish a cumulative snapshot every N processed records")
    p.add_argument("--max-messages", type=int, default=0,
                   help="stop after N records from the main topic (0 = unbounded)")
    p.add_argument("--idle-timeout", type=float, default=0.0,
                   help="stop after N seconds with no messages (0 = never)")
    p.add_argument("--drain-timeout", type=float, default=8.0,
                   help="seconds of retry-tier silence before stopping, once the "
                        "main topic quota is reached")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for a reproducible run")
    p.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return OrderConsumer(parse_args(argv)).run()


if __name__ == "__main__":
    raise SystemExit(main())
