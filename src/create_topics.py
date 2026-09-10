"""Create the three topics this pipeline needs (idempotent).

Run:
    python -m src.create_topics

Auto-creation is deliberately not relied on: it produces topics with whatever
the broker defaults happen to be, and the DLQ in particular needs a longer
retention than a normal topic — a message parked there is waiting for a human,
and a week-long default would quietly delete the evidence.
"""

from __future__ import annotations

import argparse
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from . import config
from .retry_topics import build_tiers

THIRTY_DAYS_MS = str(30 * 24 * 60 * 60 * 1000)


def topic_specs(partitions: int, replication: int) -> list[NewTopic]:
    tiers = build_tiers(config.RETRY_TOPIC_PREFIX, config.RETRY_TIER_DELAYS)

    specs = [
        NewTopic(
            config.TOPIC_ORDERS,
            num_partitions=partitions,
            replication_factor=replication,
            config={"cleanup.policy": "delete"},
        ),
    ]

    # One topic per retry tier. Every record on a given tier carries the same
    # delay, which keeps each tier ordered by due time - that is what lets a
    # consumer pause on the head record instead of scanning ahead.
    specs += [
        NewTopic(
            tier.topic,
            num_partitions=partitions,
            replication_factor=replication,
            config={"cleanup.policy": "delete"},
        )
        for tier in tiers
    ]

    specs += [
        NewTopic(
            config.TOPIC_DLQ,
            # Matches the source partition count. Replay re-publishes with the
            # original key, and keeping the counts aligned is what preserves
            # per-key ordering through a DLQ round trip. Order ids happen to be
            # unique here, so it makes no practical difference today - but it
            # would the moment a key could repeat.
            num_partitions=partitions,
            replication_factor=replication,
            config={"cleanup.policy": "delete", "retention.ms": THIRTY_DAYS_MS},
        ),
        NewTopic(
            config.TOPIC_AGGREGATES,
            num_partitions=1,
            replication_factor=replication,
            # Compacted: only the latest snapshot per key matters. Cumulative
            # snapshots share one key so old ones collapse; each window gets
            # its own key so the window history is retained.
            config={"cleanup.policy": "compact"},
        ),
    ]
    return specs


def _delete_existing(admin: AdminClient, names: list[str]) -> None:
    """Drop the topics so a demo starts from empty offsets."""
    if not names:
        return
    print("Recreating - deleting existing topics first:")
    for topic, future in admin.delete_topics(names, operation_timeout=30).items():
        try:
            future.result(timeout=30)
            print(f"  - {topic:<20} deleted")
        except Exception as exc:
            print(f"  ! {topic:<20} delete failed: {exc}", file=sys.stderr)

    # Deletion is asynchronous in the metadata layer: creating a topic again
    # too quickly races the controller and fails with TOPIC_ALREADY_EXISTS.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        time.sleep(1.0)
        remaining = set(admin.list_topics(timeout=15).topics) & set(names)
        if not remaining:
            return
    print("  ! timed out waiting for deletion to propagate", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m src.create_topics")
    p.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    p.add_argument("--partitions", type=int, default=config.TOPIC_PARTITIONS)
    p.add_argument("--replication", type=int, default=config.TOPIC_REPLICATION)
    p.add_argument("--recreate", action="store_true",
                   help="delete the topics first, for a clean demo run "
                        "(not supported on Windows - see --force)")
    p.add_argument("--force", action="store_true",
                   help="allow --recreate on Windows despite the known risk")
    args = p.parse_args(argv)

    if args.recreate and sys.platform == "win32" and not args.force:
        print(
            "Refusing to delete topics on Windows.\n"
            "\n"
            "Deleting a topic makes the broker unlink log segment and index\n"
            "files that are still memory-mapped. Windows does not allow that,\n"
            "and the broker treats the resulting IOException as fatal and shuts\n"
            "itself down (KAFKA-1194). Clear the data directory instead:\n"
            "\n"
            "    .\\scripts\\reset-kafka.ps1\n"
            "\n"
            "Pass --force to override.",
            file=sys.stderr,
        )
        return 2

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})

    try:
        existing = set(admin.list_topics(timeout=15).topics)
    except Exception as exc:
        print(f"Cannot reach broker at {args.bootstrap_servers}: {exc}", file=sys.stderr)
        print("Start Kafka first:  .\\scripts\\start-kafka.ps1", file=sys.stderr)
        return 2

    if args.recreate:
        ours = [t.topic for t in topic_specs(args.partitions, args.replication)]
        _delete_existing(admin, [t for t in ours if t in existing])
        existing = set(admin.list_topics(timeout=15).topics)

    wanted = topic_specs(args.partitions, args.replication)
    to_create = [t for t in wanted if t.topic not in existing]

    for t in wanted:
        if t.topic in existing:
            print(f"  = {t.topic:<20} already exists")

    if not to_create:
        print("\nAll topics present.")
        return 0

    failures = 0
    for topic, future in admin.create_topics(to_create).items():
        try:
            future.result(timeout=30)
            print(f"  + {topic:<20} created")
        except Exception as exc:
            failures += 1
            print(f"  ! {topic:<20} FAILED: {exc}", file=sys.stderr)

    print("\nDone." if not failures else f"\n{failures} topic(s) failed.")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
