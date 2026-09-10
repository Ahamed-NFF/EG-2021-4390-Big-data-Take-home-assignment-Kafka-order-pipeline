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
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from . import config
from .avro_codec import default_codec
from .retry_topics import H_REPLAY_COUNT, read_replay_count

REPLAYABLE_BY_DEFAULT = {"TransientError"}


def header_map(msg) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in msg.headers() or []:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "replace")
        out[key] = value
    return out


class DlqReader:
    """Reads the DLQ, optionally remembering how far it got.

    ``inspect`` is a viewer: it never commits, so it always shows the whole
    queue. ``replay`` does commit, and that is what stops it re-sending records
    it already handled on an earlier invocation.

    That distinction matters more than it looks. Kafka topics are append-only,
    so replaying a record does not remove it from the DLQ. Without a committed
    position, every ``replay`` run would resend the entire queue from the
    beginning, the per-record budget would never be reached, and the tool would
    itself become the infinite loop the budget exists to prevent.
    """

    def __init__(self, bootstrap: str, topic: str, group: str, timeout: float) -> None:
        self.timeout = timeout
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        })
        self.consumer.subscribe([topic])
        self._last: list = []

    def __enter__(self) -> "DlqReader":
        return self

    def __exit__(self, *exc) -> None:
        self.consumer.close()

    def read(self) -> list:
        messages: list = []
        idle = 0.0
        while idle < self.timeout:
            msg = self.consumer.poll(0.5)
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
        self._last = messages
        return messages

    def commit(self) -> None:
        """Mark everything read as handled. Call only after a successful replay."""
        if not self._last:
            return
        # Commit the furthest offset seen on each partition.
        furthest: dict[tuple[str, int], object] = {}
        for msg in self._last:
            key = (msg.topic(), msg.partition())
            current = furthest.get(key)
            if current is None or msg.offset() > current.offset():
                furthest[key] = msg
        for msg in furthest.values():
            self.consumer.commit(message=msg, asynchronous=False)


def cmd_inspect(args) -> int:
    # A viewer: never commits, so it always shows the entire queue.
    with DlqReader(args.bootstrap_servers, args.dlq_topic,
                   f"dlq-inspector-{args.group_suffix}", args.timeout) as reader:
        messages = reader.read()

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
        replays = read_replay_count(msg.headers())
        print(f"\n  DLQ offset {msg.offset()}  key={key}")
        print(f"    error      : {err_type} @ {stage} stage")
        print(f"    reason     : {h.get('dlq-error-message', '?')}")
        print(f"    attempts   : {h.get('dlq-attempts', '?')}")
        print(f"    replays    : {replays}/{config.MAX_REPLAYS} budget used")
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
    right_type = [m for m in messages
                  if header_map(m).get("dlq-error-type") in REPLAYABLE_BY_DEFAULT]
    replayable = [m for m in right_type
                  if read_replay_count(m.headers()) < config.MAX_REPLAYS]
    print(f"  replayable    : {len(replayable)} of {len(messages)} "
          "(transient failures only; the rest need a fix, not a retry)")
    spent = len(right_type) - len(replayable)
    if spent:
        print(f"  budget spent  : {spent} transient record(s) have used all "
              f"{config.MAX_REPLAYS} replays and need manual attention")

    # Age of the oldest record beats queue depth as an alerting signal: depth
    # has to be re-tuned every time traffic volume changes, whereas "something
    # has been stuck here for 20 minutes" means the same thing at any scale.
    timestamps = [ts for _, ts in (m.timestamp() for m in messages) if ts and ts > 0]
    if timestamps:
        age_seconds = (time.time() * 1000 - min(timestamps)) / 1000
        print(f"  queue depth   : {len(messages)}")
        print(f"  oldest record : {_format_age(age_seconds)} old"
              f"{'   <-- ALERT' if age_seconds > args.alert_age_seconds else ''}")
    print("=" * 78)
    return 0


def _format_age(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"


def cmd_replay(args) -> int:
    group = f"dlq-replayer-{args.group_suffix}"
    with DlqReader(args.bootstrap_servers, args.dlq_topic, group, args.timeout) as reader:
        messages = reader.read()
        if not messages:
            print("Nothing new in the DLQ to replay.")
            print(f"(Group '{group}' has already handled everything before this "
                  "point; use --group-suffix to re-read from the start.)")
            return 0

        code = _replay_messages(args, messages)

        # Only advance the position once the replay actually succeeded, and
        # never on a dry run - otherwise a preview would silently consume the
        # records it was only supposed to describe.
        if code == 0 and not args.dry_run:
            reader.commit()
            print(f"Replay position committed for group '{group}'.")
        return code


def _replay_messages(args, messages) -> int:
    wanted = set(args.error_type) if args.error_type else REPLAYABLE_BY_DEFAULT
    by_type = [m for m in messages
               if header_map(m).get("dlq-error-type", "?") in wanted]

    # Enforce a cumulative replay budget. Replaying a record that fails again
    # puts it straight back in the DLQ, so without a ceiling
    # replay -> fail -> DLQ -> replay is an unbounded loop that never
    # converges. The count rides on the record itself, so the budget survives
    # across separate invocations of this tool.
    selected = [m for m in by_type if read_replay_count(m.headers()) < args.max_replays]
    exhausted = len(by_type) - len(selected)

    print(f"DLQ holds {len(messages)} record(s); "
          f"{len(by_type)} match error type(s) {sorted(wanted)}.")

    wrong_type = len(messages) - len(by_type)
    if wrong_type:
        print(f"Skipping {wrong_type} record(s) whose failure a replay cannot fix.")
    if exhausted:
        print(f"Skipping {exhausted} record(s) that already used their "
              f"{args.max_replays}-replay budget - these need a human, not another retry.")

    if not selected:
        return 0

    if args.dry_run:
        for msg in selected:
            h = header_map(msg)
            key = msg.key().decode("utf-8", "replace") if msg.key() else "<no key>"
            used = read_replay_count(msg.headers())
            print(f"  would replay key={key} (replay {used + 1}/{args.max_replays}) "
                  f"({h.get('dlq-error-type')}: {h.get('dlq-error-message', '')[:60]})")
        print("\nDry run — nothing published. Re-run without --dry-run to replay.")
        return 0

    conf = config.producer_config()
    conf["bootstrap.servers"] = args.bootstrap_servers
    producer = Producer(conf)

    failures: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            failures.append(str(err))

    replayed = 0
    for msg in selected:
        # Strip both the dlq-* diagnostics and the retry-* bookkeeping. Leaving
        # the retry headers on would carry a stale due-time and tier number
        # back onto the main topic, so the record would skip retry tiers it has
        # not actually used. A replayed record starts its ladder afresh.
        headers = [(k, v) for k, v in (msg.headers() or [])
                   if not k.startswith("dlq-") and not k.startswith("retry-")]
        headers.append(("replayed-from-dlq", b"true"))
        headers.append(("replay-source-offset", str(msg.offset()).encode()))
        headers.append((H_REPLAY_COUNT, str(read_replay_count(msg.headers()) + 1).encode()))

        producer.produce(topic=args.target_topic, key=msg.key(),
                         value=msg.value(), headers=headers, on_delivery=on_delivery)
        replayed += 1
        producer.poll(0)

    remaining = producer.flush(timeout=30)
    if remaining or failures:
        print(f"WARNING: {remaining} message(s) unflushed, {len(failures)} rejected: "
              f"{failures[:3]}", file=sys.stderr)
        return 1

    print(f"Replayed {replayed} record(s) to '{args.target_topic}'.")
    print("Records stay in the DLQ (Kafka topics are append-only), but each "
          "replay increments a counter carried on the record, so a record that "
          "keeps failing runs out of budget instead of cycling forever.")
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
    p.add_argument("--alert-age-seconds", type=float, default=300.0,
                   help="flag the DLQ if its oldest record is older than this")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("inspect", help="print every DLQ record with its diagnostics")

    replay = sub.add_parser("replay", help="re-publish DLQ records to the main topic")
    replay.add_argument("--target-topic", default=config.TOPIC_ORDERS)
    replay.add_argument("--error-type", action="append",
                        help="error type to replay (repeatable); "
                             "default: TransientError only")
    replay.add_argument("--max-replays", type=int, default=config.MAX_REPLAYS,
                        help="cumulative replay budget per record "
                             f"(default {config.MAX_REPLAYS}); prevents an "
                             "endless replay/fail/DLQ loop")
    replay.add_argument("--dry-run", action="store_true",
                        help="list what would be replayed without publishing")

    args = p.parse_args(argv)
    return cmd_inspect(args) if args.command == "inspect" else cmd_replay(args)


if __name__ == "__main__":
    raise SystemExit(main())
