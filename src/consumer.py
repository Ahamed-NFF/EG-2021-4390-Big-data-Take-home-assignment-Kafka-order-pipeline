"""Order consumer: Avro decode -> validate -> retry -> aggregate, DLQ on failure.

Run:
    python -m src.consumer

Per-record pipeline, and where each failure goes:

    bytes
      |
      +-- Avro decode ......... fails -> DeserializationError (permanent) -> DLQ
      |
      +-- business validation . fails -> ValidationError      (permanent) -> DLQ
      |
      +-- downstream sink ..... fails -> TransientError -> retry w/ backoff
      |                                    exhausted        -> DLQ
      |
      +-- fold into running average -> publish aggregate snapshot
      |
      +-- commit offset

Delivery semantics are **at-least-once**. Auto-commit is disabled and the
offset is committed only once the record has either been processed or safely
parked in the DLQ (with the DLQ produce flushed first). A crash mid-retry
therefore replays the record instead of losing it. The trade is that a crash
between a successful process and the commit replays a record that was already
counted, so the aggregate is not exactly-once — a real deployment would key the
sink idempotently or use transactions to close that gap.
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

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from . import config
from .aggregator import OrderAggregator
from .avro_codec import default_codec
from .errors import PermanentError, TransientError, ValidationError
from .retry import RetryPolicy

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
        self.processed = 0
        self.retried = 0          # records that needed >1 attempt
        self.retry_attempts = 0   # total extra attempts spent
        self.recovered = 0        # records that failed then succeeded on retry
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
        self.stats = Stats()
        self.retry_policy = RetryPolicy(
            max_attempts=args.max_attempts,
            base_delay=config.RETRY_BASE_DELAY_SECONDS,
            max_delay=config.RETRY_MAX_DELAY_SECONDS,
            multiplier=config.RETRY_MULTIPLIER,
            jitter_ratio=config.RETRY_JITTER_RATIO,
            rng=self.rng,
        )
        self.sink = SimulatedSink(args.transient_failure_rate, self.rng)

        consumer_conf = config.consumer_config(args.group)
        consumer_conf["bootstrap.servers"] = args.bootstrap_servers
        self.consumer = Consumer(consumer_conf)

        producer_conf = config.producer_config()
        producer_conf["bootstrap.servers"] = args.bootstrap_servers
        # One producer serves both the DLQ and the aggregates topic.
        self.producer = Producer(producer_conf)

        self.running = True

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

        self.producer.produce(
            topic=self.args.dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=headers,
        )
        # Flush before the caller commits the offset. Committing first would
        # risk acknowledging a record whose DLQ copy never reached the broker,
        # which is the one way this design could actually lose data.
        self.producer.flush(timeout=10)

    # --- aggregates ---------------------------------------------------------
    def publish_aggregate(self) -> None:
        """Republish the running aggregate so downstream jobs can consume it."""
        snapshot = self.aggregator.snapshot()
        snapshot["emittedAt"] = datetime.now(timezone.utc).isoformat()
        self.producer.produce(
            topic=self.args.aggregate_topic,
            key=b"orders-global",
            value=json.dumps(snapshot).encode("utf-8"),
            headers=[("content-type", b"application/json")],
        )
        self.producer.poll(0)

    # --- per-record ---------------------------------------------------------
    def handle(self, msg) -> None:
        self.stats.consumed += 1
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

        # 2. Validate. Also permanent — the record itself is wrong.
        try:
            validate_order(order)
        except PermanentError as exc:
            self.stats.dlq_validation += 1
            self.send_to_dlq(msg, exc, stage="validate", attempts=1)
            print(f"  [DLQ] #{order_id} validation failed -> {exc}")
            return

        # 3. Hand to the downstream sink, retrying transient failures.
        def on_retry(attempt: int, delay: float, error: Exception) -> None:
            self.stats.retry_attempts += 1
            print(f"  [RETRY] #{order_id} attempt {attempt}/"
                  f"{self.retry_policy.max_attempts} failed ({error}); "
                  f"backing off {delay:.2f}s")

        try:
            _, attempts = self.retry_policy.execute(
                lambda: self.sink.write(order), on_retry=on_retry
            )
        except TransientError as exc:
            self.stats.retry_attempts += 1
            self.stats.retried += 1
            self.stats.dlq_retry_exhausted += 1
            self.send_to_dlq(
                msg, exc, stage="process", attempts=self.retry_policy.max_attempts
            )
            print(f"  [DLQ] #{order_id} exhausted "
                  f"{self.retry_policy.max_attempts} attempts -> {exc}")
            return

        if attempts > 1:
            self.stats.retried += 1
            self.stats.recovered += 1

        # 4. Fold into the running aggregate.
        running_avg = self.aggregator.add(order)
        self.stats.processed += 1

        recovered = f"  (recovered after {attempts} attempts)" if attempts > 1 else ""
        print(
            f"  [OK]  #{order_id:<6} {order['product']:<6} "
            f"price={float(order['price']):>9.2f} | "
            f"running avg={running_avg:>9.2f} over {self.aggregator.processed_count}"
            f"{recovered}"
        )

        # 5. Republish the aggregate periodically.
        if self.stats.processed % self.args.aggregate_every == 0:
            self.publish_aggregate()
            print("\n" + self.aggregator.format_table() + "\n")

    # --- main loop ----------------------------------------------------------
    def run(self) -> int:
        def stop(signum, frame):  # noqa: ARG001
            self.running = False
            print("\n[consumer] Ctrl+C received, shutting down cleanly...")

        signal.signal(signal.SIGINT, stop)

        self.consumer.subscribe([self.args.topic])

        print("=" * 72)
        print("  ORDER CONSUMER  (Avro + retry + DLQ + running average)")
        print("=" * 72)
        print(f"  broker            : {self.args.bootstrap_servers}")
        print(f"  source topic      : {self.args.topic}")
        print(f"  dlq topic         : {self.args.dlq_topic}")
        print(f"  aggregate topic   : {self.args.aggregate_topic}")
        print(f"  consumer group    : {self.args.group}")
        print(f"  schema fingerprint: {self.codec.fingerprint_hex}")
        print(f"  retry policy      : up to {self.args.max_attempts} attempts, "
              f"base {config.RETRY_BASE_DELAY_SECONDS}s, "
              f"x{config.RETRY_MULTIPLIER}, cap {config.RETRY_MAX_DELAY_SECONDS}s, "
              f"jitter +/-{config.RETRY_JITTER_RATIO:.0%}")
        print(f"  injected transient failure rate: "
              f"{self.args.transient_failure_rate:.0%}")
        print("=" * 72)
        print("  waiting for messages (Ctrl+C to stop)...\n")

        last_message_at = time.monotonic()
        exit_code = 0

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    idle = time.monotonic() - last_message_at
                    if self.args.idle_timeout and idle > self.args.idle_timeout:
                        print(f"[consumer] idle for {idle:.0f}s, stopping.")
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                last_message_at = time.monotonic()
                self.handle(msg)

                # At-least-once: commit only after the record is processed or
                # DLQ'd. Synchronous so a commit failure is visible here.
                self.consumer.commit(message=msg, asynchronous=False)

                if self.args.max_messages and self.stats.consumed >= self.args.max_messages:
                    print(f"[consumer] reached --max-messages "
                          f"{self.args.max_messages}, stopping.")
                    break

        except KafkaException as exc:
            print(f"[consumer] fatal Kafka error: {exc}", file=sys.stderr)
            exit_code = 2
        finally:
            if self.aggregator.processed_count:
                self.publish_aggregate()
            self.producer.flush(timeout=10)
            self.consumer.close()

        self.print_report()
        return exit_code

    def print_report(self) -> None:
        s = self.stats
        print("\n" + "=" * 72)
        print("  FINAL REPORT")
        print("=" * 72)
        print(f"  consumed                : {s.consumed}")
        print(f"  processed successfully  : {s.processed}")
        print(f"    of which recovered    : {s.recovered} "
              "(failed at least once, then succeeded on retry)")
        print(f"  total retry attempts    : {s.retry_attempts}")
        print(f"  sent to DLQ             : {s.dlq_total}")
        print(f"    deserialization fail  : {s.dlq_deserialization}")
        print(f"    validation fail       : {s.dlq_validation}")
        print(f"    retries exhausted     : {s.dlq_retry_exhausted}")
        accounted = s.processed + s.dlq_total
        print(f"  accounted for           : {accounted}/{s.consumed} "
              f"{'OK - no records lost' if accounted == s.consumed else 'MISMATCH'}")
        if self.aggregator.processed_count:
            print("-" * 72)
            print(self.aggregator.format_table())
        print("=" * 72)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m src.consumer",
        description="Consume Avro orders with retry, DLQ and running-average aggregation.",
    )
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--dlq-topic", default=config.TOPIC_DLQ)
    p.add_argument("--aggregate-topic", default=config.TOPIC_AGGREGATES)
    p.add_argument("--group", default=config.CONSUMER_GROUP)
    p.add_argument("--max-attempts", type=int, default=config.RETRY_MAX_ATTEMPTS,
                   help="total attempts per record, including the first")
    p.add_argument("--transient-failure-rate", type=float,
                   default=config.TRANSIENT_FAILURE_RATE,
                   help="probability the simulated sink raises a transient error")
    p.add_argument("--aggregate-every", type=int, default=config.AGGREGATE_PUBLISH_EVERY,
                   help="publish an aggregate snapshot every N processed records")
    p.add_argument("--max-messages", type=int, default=0,
                   help="stop after consuming N records (0 = unbounded)")
    p.add_argument("--idle-timeout", type=float, default=0.0,
                   help="stop after N seconds with no messages (0 = never)")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for a reproducible run")
    p.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return OrderConsumer(parse_args(argv)).run()


if __name__ == "__main__":
    raise SystemExit(main())
