"""Order producer: generates orders, Avro-encodes them, publishes to Kafka.

Run:
    python -m src.producer --count 100 --rate 5

The producer also injects faults on purpose, because the consumer's retry and
DLQ paths are only demonstrable if something actually fails:

    --corrupt-rate  fraction of records written as non-Avro bytes
                    -> consumer cannot deserialise -> permanent -> DLQ
    --invalid-rate  fraction of records with a negative price
                    -> decodes fine, fails validation -> permanent -> DLQ

Transient failures are injected on the consumer side, since they model a
downstream sink being unavailable rather than a bad message.
"""

from __future__ import annotations

import argparse
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

from . import config
from .avro_codec import default_codec


class DeliveryTracker:
    """Counts broker acknowledgements delivered through the async callback."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0
        self.errors: list[str] = []

    def callback(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            self.errors.append(str(err))
            print(f"  [DELIVERY FAILED] {err}", file=sys.stderr)
        else:
            self.delivered += 1


def build_order(order_id: int, rng: random.Random) -> dict:
    """A valid Order matching schemas/order.avsc."""
    return {
        "orderId": str(order_id),
        "product": rng.choice(config.PRODUCTS),
        # Avro `float` is 32-bit; round to cents so the demo output is readable.
        "price": round(rng.uniform(config.PRICE_MIN, config.PRICE_MAX), 2),
    }


def corrupt_payload(rng: random.Random) -> bytes:
    """Bytes that are not Avro single-object encoded.

    The magic bytes are wrong, so the consumer rejects this at the header check
    without even attempting a decode.
    """
    return bytes(rng.randrange(256) for _ in range(rng.randint(12, 32)))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m src.producer",
        description="Publish Avro-encoded order messages to Kafka.",
    )
    p.add_argument("--count", type=int, default=100,
                   help="number of messages to publish (0 = run until Ctrl+C)")
    p.add_argument("--rate", type=float, default=5.0,
                   help="messages per second (0 = as fast as possible)")
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--start-id", type=int, default=1001,
                   help="first orderId to emit")
    p.add_argument("--corrupt-rate", type=float, default=config.CORRUPT_MESSAGE_RATE,
                   help="fraction of messages emitted as non-Avro bytes")
    p.add_argument("--invalid-rate", type=float, default=config.INVALID_MESSAGE_RATE,
                   help="fraction of messages emitted with a negative price")
    p.add_argument("--seed", type=int, default=None,
                   help="RNG seed, for a reproducible run")
    p.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)

    codec = default_codec()
    conf = config.producer_config()
    conf["bootstrap.servers"] = args.bootstrap_servers
    producer = Producer(conf)
    tracker = DeliveryTracker()

    running = True

    def stop(signum, frame):  # noqa: ARG001
        nonlocal running
        running = False
        print("\n[producer] Ctrl+C received, flushing in-flight messages...")

    signal.signal(signal.SIGINT, stop)

    print("=" * 72)
    print("  ORDER PRODUCER")
    print("=" * 72)
    print(f"  broker            : {args.bootstrap_servers}")
    print(f"  topic             : {args.topic}")
    print(f"  schema            : {codec.schema_path.name}")
    print(f"  schema fingerprint: {codec.fingerprint_hex}")
    print(f"  target count      : {args.count or 'unbounded'}")
    print(f"  rate              : {args.rate or 'max'} msg/s")
    print(f"  fault injection   : corrupt={args.corrupt_rate:.0%} "
          f"invalid={args.invalid_rate:.0%}")
    print("=" * 72)

    sent = {"valid": 0, "corrupt": 0, "invalid": 0}
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    order_id = args.start_id
    published = 0

    try:
        while running and (args.count == 0 or published < args.count):
            roll = rng.random()
            order = build_order(order_id, rng)

            if roll < args.corrupt_rate:
                kind = "corrupt"
                payload = corrupt_payload(rng)
                detail = f"{len(payload)} random bytes"
            elif roll < args.corrupt_rate + args.invalid_rate:
                kind = "invalid"
                order["price"] = -abs(order["price"])
                payload = codec.encode(order)
                detail = f"{order['product']:<6} price={order['price']:>9.2f}"
            else:
                kind = "valid"
                payload = codec.encode(order)
                detail = f"{order['product']:<6} price={order['price']:>9.2f}"

            headers = [
                ("content-type", b"application/avro"),
                ("avro-schema-fingerprint", codec.fingerprint_hex.encode()),
                ("produced-at", datetime.now(timezone.utc).isoformat().encode()),
                ("injected-fault", kind.encode() if kind != "valid" else b"none"),
            ]

            try:
                producer.produce(
                    topic=args.topic,
                    key=order["orderId"].encode(),
                    value=payload,
                    headers=headers,
                    on_delivery=tracker.callback,
                )
            except BufferError:
                # Local queue is full: drain acknowledgements and retry once.
                producer.poll(0.5)
                producer.produce(
                    topic=args.topic,
                    key=order["orderId"].encode(),
                    value=payload,
                    headers=headers,
                    on_delivery=tracker.callback,
                )

            sent[kind] += 1
            published += 1
            flag = "" if kind == "valid" else f"  <-- INJECTED {kind.upper()}"
            print(f"  -> #{order['orderId']:<6} {detail}{flag}")

            # Serve delivery callbacks without blocking the send loop.
            producer.poll(0)
            if interval:
                time.sleep(interval)

            order_id += 1

    except KafkaException as exc:
        print(f"[producer] fatal Kafka error: {exc}", file=sys.stderr)
        return 2
    finally:
        remaining = producer.flush(timeout=30)
        if remaining:
            print(f"[producer] WARNING: {remaining} message(s) not delivered "
                  "before flush timeout", file=sys.stderr)

    print("=" * 72)
    print(f"  published        : {published}")
    print(f"    valid          : {sent['valid']}")
    print(f"    corrupt bytes  : {sent['corrupt']}   (expected in DLQ)")
    print(f"    negative price : {sent['invalid']}   (expected in DLQ)")
    print(f"  broker acked     : {tracker.delivered}")
    print(f"  delivery failed  : {tracker.failed}")
    print("=" * 72)

    return 0 if tracker.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
