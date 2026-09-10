"""Inspect and replay the Dead Letter Queue.

    python -m src.dlq_tool inspect
    python -m src.dlq_tool replay --error-type TransientError --dry-run
    python -m src.dlq_tool replay --error-type TransientError

A DLQ that nobody can read is just a slower way of dropping messages, so the
queue ships with an operator tool. ``inspect`` decodes the diagnostic headers
written by the consumer and, where the payload is still valid Avro, shows the
order that failed. ``replay`` re-publishes records onto the main topic.

Replay defaults to transient failures only, and that default is the important
part: retry-exhausted records failed because the *environment* was down, so
replaying them once it recovers is likely to succeed. Records that failed
deserialisation or validation are broken in themselves — replaying them just
loops them back into the DLQ, so selecting them requires saying so explicitly.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from . import config
from .avro_codec import default_codec

REPLAYABLE_BY_DEFAULT = {"TransientError"}


def header_map(msg) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in msg.headers() or []:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "replace")
        out[key] = value
    return out


def drain(bootstrap: str, topic: str, group: str, timeout: float) -> list:
    """Read a topic from the beginning without committing offsets."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    consumer.subscribe([topic])

    messages: list = []
    idle = 0.0
    try:
        while idle < timeout:
            msg = consumer.poll(0.5)
            if msg is None:
                idle += 0.5
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    idle += 0.5
                    continue
                raise KafkaException(msg.error())
            idle = 0.0
            messages.append(msg)
    finally:
        consumer.close()
    return messages


def cmd_inspect(args) -> int:
    messages = drain(args.bootstrap_servers, args.dlq_topic,
                     f"dlq-inspector-{args.group_suffix}", args.timeout)

    print("=" * 78)
    print(f"  DEAD LETTER QUEUE: {args.dlq_topic}   ({len(messages)} record(s))")
    print("=" * 78)

    if not messages:
        print("  empty — nothing has failed permanently.")
        return 0

    codec = default_codec()
    by_type: Counter[str] = Counter()
    by_stage: Counter[str] = Counter()

    for msg in messages:
        h = header_map(msg)
        err_type = h.get("dlq-error-type", "?")
        stage = h.get("dlq-failure-stage", "?")
        by_type[err_type] += 1
        by_stage[stage] += 1

        key = msg.key().decode("utf-8", "replace") if msg.key() else "<no key>"
        print(f"\n  DLQ offset {msg.offset()}  key={key}")
        print(f"    error      : {err_type} @ {stage} stage")
        print(f"    reason     : {h.get('dlq-error-message', '?')}")
        print(f"    attempts   : {h.get('dlq-attempts', '?')}")
        print(f"    origin     : {h.get('dlq-original-topic', '?')}"
              f"[{h.get('dlq-original-partition', '?')}]"
              f"@{h.get('dlq-original-offset', '?')}")
        print(f"    failed at  : {h.get('dlq-failed-at', '?')}")

        value = msg.value()
        print(f"    payload    : {len(value) if value else 0} bytes", end="")
        try:
            order = codec.decode(value)
            print(f" -> decodes to {order}")
        except Exception as exc:
            preview = value[:16].hex() if value else ""
            print(f" -> UNDECODABLE ({type(exc).__name__}); first bytes: {preview}")

    print("\n" + "-" * 78)
    print("  by error type :", dict(by_type))
    print("  by stage      :", dict(by_stage))
    replayable = sum(c for t, c in by_type.items() if t in REPLAYABLE_BY_DEFAULT)
    print(f"  replayable    : {replayable} of {len(messages)} "
          "(transient failures only; the rest need a fix, not a retry)")
    print("=" * 78)
    return 0


def cmd_replay(args) -> int:
    messages = drain(args.bootstrap_servers, args.dlq_topic,
                     f"dlq-replayer-{args.group_suffix}", args.timeout)
    if not messages:
        print("DLQ is empty; nothing to replay.")
        return 0

    wanted = set(args.error_type) if args.error_type else REPLAYABLE_BY_DEFAULT
    selected = [m for m in messages
                if header_map(m).get("dlq-error-type", "?") in wanted]

    print(f"DLQ holds {len(messages)} record(s); "
          f"{len(selected)} match error type(s) {sorted(wanted)}.")

    skipped = len(messages) - len(selected)
    if skipped:
        print(f"Skipping {skipped} record(s) whose failure a replay cannot fix.")

    if not selected:
        return 0

    if args.dry_run:
        for msg in selected:
            h = header_map(msg)
            key = msg.key().decode("utf-8", "replace") if msg.key() else "<no key>"
            print(f"  would replay key={key} "
                  f"({h.get('dlq-error-type')}: {h.get('dlq-error-message', '')[:60]})")
        print(f"\nDry run — nothing published. Re-run without --dry-run to replay.")
        return 0

    conf = config.producer_config()
    conf["bootstrap.servers"] = args.bootstrap_servers
    producer = Producer(conf)

    replayed = 0
    for msg in selected:
        # Keep only the original headers; strip dlq-* diagnostics so the record
        # looks like a fresh publish, then mark it as a replay for traceability.
        headers = [(k, v) for k, v in (msg.headers() or [])
                   if not k.startswith("dlq-")]
        headers.append(("replayed-from-dlq", b"true"))
        headers.append(("replay-source-offset", str(msg.offset()).encode()))

        producer.produce(topic=args.target_topic, key=msg.key(),
                         value=msg.value(), headers=headers)
        replayed += 1
        producer.poll(0)

    remaining = producer.flush(timeout=30)
    if remaining:
        print(f"WARNING: {remaining} replayed message(s) not acknowledged",
              file=sys.stderr)
        return 1

    print(f"Replayed {replayed} record(s) to '{args.target_topic}'.")
    print("Note: records stay in the DLQ (Kafka topics are append-only). "
          "Re-running replay would publish them again.")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m src.dlq_tool",
                                description="Inspect and replay the DLQ.")
    p.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    p.add_argument("--dlq-topic", default=config.TOPIC_DLQ)
    p.add_argument("--timeout", type=float, default=3.0,
                   help="seconds of silence before the read is considered complete")
    p.add_argument("--group-suffix", default="1",
                   help="vary to re-read the DLQ from the start in a fresh group")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("inspect", help="print every DLQ record with its diagnostics")

    replay = sub.add_parser("replay", help="re-publish DLQ records to the main topic")
    replay.add_argument("--target-topic", default=config.TOPIC_ORDERS)
    replay.add_argument("--error-type", action="append",
                        help="error type to replay (repeatable); "
                             "default: TransientError only")
    replay.add_argument("--dry-run", action="store_true",
                        help="list what would be replayed without publishing")

    args = p.parse_args(argv)
    return cmd_inspect(args) if args.command == "inspect" else cmd_replay(args)


if __name__ == "__main__":
    raise SystemExit(main())
