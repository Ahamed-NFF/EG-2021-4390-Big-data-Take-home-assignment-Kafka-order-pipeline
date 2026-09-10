"""Central configuration.

Every value can be overridden with an environment variable, so the same code
runs against a local broker, a lab broker, or a marker's machine without edits.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = PROJECT_ROOT / "schemas"
ORDER_SCHEMA_PATH = SCHEMA_DIR / "order.avsc"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


# --- Broker -----------------------------------------------------------------
BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- Topics -----------------------------------------------------------------
TOPIC_ORDERS = _env("KAFKA_TOPIC_ORDERS", "orders")
TOPIC_DLQ = _env("KAFKA_TOPIC_DLQ", "orders.DLQ")
TOPIC_AGGREGATES = _env("KAFKA_TOPIC_AGGREGATES", "orders.aggregates")

TOPIC_PARTITIONS = _env_int("KAFKA_TOPIC_PARTITIONS", 3)
TOPIC_REPLICATION = _env_int("KAFKA_TOPIC_REPLICATION", 1)

# --- Consumer group ---------------------------------------------------------
CONSUMER_GROUP = _env("KAFKA_CONSUMER_GROUP", "order-processing-group")

# --- Retry policy -----------------------------------------------------------
# Total attempts for a transient failure, including the first try.
RETRY_MAX_ATTEMPTS = _env_int("RETRY_MAX_ATTEMPTS", 4)
RETRY_BASE_DELAY_SECONDS = _env_float("RETRY_BASE_DELAY_SECONDS", 0.25)
RETRY_MAX_DELAY_SECONDS = _env_float("RETRY_MAX_DELAY_SECONDS", 4.0)
RETRY_MULTIPLIER = _env_float("RETRY_MULTIPLIER", 2.0)
RETRY_JITTER_RATIO = _env_float("RETRY_JITTER_RATIO", 0.3)

# --- Fault injection (demo only) --------------------------------------------
# Probability the simulated downstream sink raises a *transient* error.
TRANSIENT_FAILURE_RATE = _env_float("TRANSIENT_FAILURE_RATE", 0.20)
# Probability the producer emits bytes that are not valid Avro.
CORRUPT_MESSAGE_RATE = _env_float("CORRUPT_MESSAGE_RATE", 0.05)
# Probability the producer emits a schema-valid but business-invalid order.
INVALID_MESSAGE_RATE = _env_float("INVALID_MESSAGE_RATE", 0.05)

# --- Aggregation ------------------------------------------------------------
# How often the running aggregate snapshot is published to TOPIC_AGGREGATES.
AGGREGATE_PUBLISH_EVERY = _env_int("AGGREGATE_PUBLISH_EVERY", 10)

# --- Demo data --------------------------------------------------------------
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]
PRICE_MIN = _env_float("PRICE_MIN", 5.0)
PRICE_MAX = _env_float("PRICE_MAX", 500.0)


def producer_config() -> dict:
    """Durable, exactly-once-per-partition producer settings.

    `enable.idempotence` makes retries inside librdkafka safe: the broker
    de-duplicates by producer id + sequence number, so a retried batch cannot
    create a duplicate record.
    """
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "retry.backoff.ms": 200,
        "delivery.timeout.ms": 30000,
        "linger.ms": 5,
        "compression.type": "snappy",
    }


def consumer_config(group_id: str | None = None) -> dict:
    """At-least-once consumer settings.

    Auto-commit is off: the offset is committed only after the record has been
    processed *or* parked in the DLQ, so a crash mid-retry replays the record
    rather than silently dropping it.
    """
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id or CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # Retry backoff happens between polls, so give the group generous
        # headroom before the coordinator considers this member dead.
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    }
