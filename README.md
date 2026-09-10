# Kafka Order Pipeline — Avro, Retry, DLQ and Real-Time Aggregation

A Kafka producer/consumer pair that publishes purchase orders as Avro-encoded
messages and processes them with a running price average, bounded retries for
transient failures, and a dead letter queue for everything that cannot be
processed.

Built for the Big Data assignment. Python 3.12, Apache Kafka 4.1.2 in KRaft
mode, `confluent-kafka` (librdkafka) and `fastavro`.

---

## What it does

```
                    ┌──────────────────────────────────────────┐
                    │  producer.py                             │
                    │  build order → Avro encode → publish     │
                    │  (+ deliberately injects bad messages)   │
                    └────────────────────┬─────────────────────┘
                                         │  Avro single-object encoded bytes
                                         ▼
                              ┌─────────────────────┐
                              │  topic: orders      │  3 partitions
                              │  key = orderId      │
                              └──────────┬──────────┘
                                         ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │  consumer.py                                                       │
    │                                                                    │
    │   1. Avro decode ─────── fails ──► DeserializationError ─┐         │
    │        │                             (permanent)         │         │
    │        ▼                                                 │         │
    │   2. validate ────────── fails ──► ValidationError ──────┤         │
    │        │                             (permanent)         │         │
    │        ▼                                                 │         │
    │   3. downstream sink ─── fails ──► TransientError                  │
    │        │                             → retry, backoff + jitter     │
    │        │                             → exhausted ───────┤          │
    │        ▼                                                 │         │
    │   4. fold into running average (Welford, O(1) state)     │         │
    │        │                                                 │         │
    │   5. commit offset  ◄────────────────────────────────────┘         │
    └───────┬──────────────────────────────────────────┬─────────────────┘
            ▼                                          ▼
  ┌─────────────────────┐                   ┌─────────────────────┐
  │ topic: orders.      │                   │ topic: orders.DLQ   │
  │        aggregates   │                   │ original bytes +    │
  │ JSON snapshots,     │                   │ diagnostic headers, │
  │ log-compacted       │                   │ 30-day retention    │
  └─────────────────────┘                   └──────────┬──────────┘
                                                       ▼
                                            ┌─────────────────────┐
                                            │ dlq_tool.py         │
                                            │ inspect / replay    │
                                            └─────────────────────┘
```

### Requirements coverage

| Requirement | Where |
|---|---|
| Produce and consume order messages | [src/producer.py](src/producer.py), [src/consumer.py](src/consumer.py) |
| Avro serialization | [src/avro_codec.py](src/avro_codec.py), [schemas/order.avsc](schemas/order.avsc) |
| Real-time aggregation (running average of prices) | [src/aggregator.py](src/aggregator.py) |
| Retry logic for temporary failures | [src/retry.py](src/retry.py) |
| Dead Letter Queue for permanent failures | `send_to_dlq` in [src/consumer.py](src/consumer.py), [src/dlq_tool.py](src/dlq_tool.py) |
| Live demonstration | [scripts/demo.ps1](scripts/demo.ps1), transcript in [docs/demo-output.txt](docs/demo-output.txt) |
| Git repository | this repo |

---

## Quick start (Windows)

```powershell
# 1. Install Python dependencies
python -m pip install -r requirements.txt

# 2. Download and install Kafka to C:\kafka  (~130 MB, one time)
.\scripts\setup-kafka.ps1

# 3. Run the whole thing
.\scripts\demo.ps1 -Reset
```

`demo.ps1` starts the broker, creates the topics, produces 60 orders, consumes
them with retry and DLQ handling, then prints the DLQ contents. Stop the broker
afterwards with `.\scripts\stop-kafka.ps1`.

### Running it as a live two-terminal demo

More convincing to watch, because the consumer's running average updates while
the producer is still publishing:

```powershell
# once
.\scripts\start-kafka.ps1
python -m src.create_topics

# terminal 1 — consumer, leave it running
python -m src.consumer

# terminal 2 — producer, slow enough to follow along
python -m src.producer --count 200 --rate 3

# terminal 2 — afterwards
python -m src.dlq_tool inspect
python -m src.dlq_tool replay --dry-run
```

### Useful flags

```powershell
# heavier fault injection, to force more DLQ traffic
python -m src.producer --count 100 --corrupt-rate 0.15 --invalid-rate 0.15

# make the downstream sink fail half the time
python -m src.consumer --transient-failure-rate 0.5 --max-attempts 6

# reproducible run
python -m src.producer --count 60 --seed 42
```

Every setting also reads from an environment variable — see
[src/config.py](src/config.py).

---

## Avro serialization

[schemas/order.avsc](schemas/order.avsc) is the schema from the assignment
brief:

| Field | Type | Description |
|---|---|---|
| `orderId` | string | Unique identifier for the order (e.g. `"1001"`) |
| `product` | string | Name of the purchased item (e.g. `"Item1"`) |
| `price` | float | Price of the product (randomised) |

Messages use **Avro single-object encoding**:

```
+--------+--------+---------------------------+------------------------+
|  0xC3  |  0x01  | CRC-64-AVRO fingerprint   |  Avro binary datum     |
|        |        | (8 bytes, little-endian)  |  (no inline schema)    |
+--------+--------+---------------------------+------------------------+
 <---- 2-byte marker ---->  <---- 8 bytes ---->  <-- remaining bytes -->
```

Ten bytes of header ride along with each record instead of the schema itself,
which is the reason to use Avro on a high-volume topic at all: an order encodes
to **25 bytes** (10 header + 15 datum), against 55 for the equivalent JSON and
577 for the schema document.

The fingerprint identifies which schema *wrote* the bytes. The consumer looks
it up in a small fingerprint-keyed registry and hands both the writer's and its
own reader schema to the decoder, so Avro's schema-resolution rules apply.
[schemas/order_v2.avsc](schemas/order_v2.avsc) adds two defaulted fields to
exercise that: `tests/test_avro_codec.py` proves a v2 reader decodes v1 bytes
(backward compatible) and a v1 reader ignores the added fields (forward
compatible).

This replaces Confluent Schema Registry, which would otherwise be a second
service to run. The trade-off is real and worth stating: a fingerprint says
*which* schema wrote a record but cannot enforce that new versions stay
compatible before they are published. A production deployment should use a
registry with a compatibility policy.

---

## Failure handling

The retry/DLQ decision comes down to one question: **would running this record
again produce a different result?** The error taxonomy in
[src/errors.py](src/errors.py) answers it explicitly rather than inferring it
from error strings.

| Failure | Class | Routing | Why |
|---|---|---|---|
| Payload is not valid Avro | `DeserializationError` (permanent) | DLQ immediately | Bad bytes stay bad |
| Negative / zero / non-finite price, blank id | `ValidationError` (permanent) | DLQ immediately | The record itself is wrong |
| Downstream sink unavailable | `TransientError` | retry, then DLQ | The input is fine; the environment failed |

Retrying a poison record is not harmless. It delays every later record on the
same partition while attempts burn down, and it ends up in the DLQ regardless —
so permanent failures skip the retry budget entirely.

### Retry policy

Exponential backoff with proportional jitter, capped:

```
attempt 1 fails → wait 0.25s (±30%)
attempt 2 fails → wait 0.50s (±30%)
attempt 3 fails → wait 1.00s (±30%)
attempt 4 fails → give up, send to DLQ
```

Backoff is exponential so a struggling service gets progressively more room.
Jitter matters more than it looks: without it, every consumer that failed at the
same moment retries at the same moment, and the synchronised retry storm
re-creates the outage it was meant to ride out.

Backoff happens between polls, so `max.poll.interval.ms` is set to 5 minutes —
comfortably above the worst-case retry time — to keep the coordinator from
deciding the consumer has died mid-retry and rebalancing the partition away.

### Dead letter queue

DLQ records keep the **original bytes untouched** as the message value, with
diagnostics in Kafka headers:

| Header | Contents |
|---|---|
| `dlq-error-type` | `DeserializationError`, `ValidationError`, `TransientError` |
| `dlq-error-message` | the specific reason |
| `dlq-failure-stage` | `deserialize`, `validate` or `process` |
| `dlq-attempts` | how many tries it got |
| `dlq-original-topic` / `-partition` / `-offset` | exact provenance |
| `dlq-failed-at` | UTC timestamp |

Keeping the payload byte-identical is what makes replay possible; rewriting it
into a JSON envelope would mean it could never be re-published as-is.

`python -m src.dlq_tool inspect` prints all of it and decodes the payload where
it still can. `replay` re-publishes to the main topic, and **defaults to
transient failures only** — those failed because the environment was down, so
replaying once it recovers should succeed. Deserialization and validation
failures would simply loop straight back into the DLQ, so replaying them takes
an explicit `--error-type`.

---

## Real-time aggregation

The consumer maintains a running average of prices overall and per product,
updated on every message and printed live:

```
  [OK]  #1032   Item1  price=   301.48 | running avg=   252.95 over 41  (recovered after 2 attempts)

  RUNNING AVERAGE :     252.25   (n=50)
  total=12612.58  min=6.61  max=486.08  stddev=158.85
  ----------------------------------------------------
  product        count     avg price           total
  Item1             15        248.27         3724.11
  Item2             10        248.08         2480.81
```

The average uses **Welford's online algorithm** rather than `sum / count`. Both
agree on demo-sized data, but Welford updates the mean incrementally and never
carries a growing sum, so it stays accurate on an unbounded stream of float32
prices where a naive running total loses low-order bits as it grows. State stays
O(1) per key no matter how long the stream runs, which is what makes it a
streaming aggregate rather than a batch one.

Snapshots are republished as JSON to the log-compacted `orders.aggregates`
topic every 10 records, so downstream consumers can read the current average
without recomputing it.

---

## Delivery semantics

**Producer — no duplicates on retry.** `enable.idempotence=True` with
`acks=all`. When librdkafka retries a batch, the broker de-duplicates on
producer id and sequence number, so an internal retry cannot silently double a
record.

**Consumer — at-least-once.** Auto-commit is off. The offset is committed only
after the record has been processed *or* written to the DLQ, and the DLQ produce
is flushed before the commit. Committing first would risk acknowledging a record
whose DLQ copy never reached the broker, which is the one way this design could
actually lose data.

The honest limitation: a crash between a successful process and the commit
replays a record that was already counted, so the aggregate is at-least-once,
not exactly-once. Closing that gap needs an idempotent sink keyed on `orderId`,
or Kafka transactions spanning the sink write and the offset commit. For a
running average over a demo stream the replay window is not worth that
complexity, but it is a real property of the system, not an oversight.

Every run ends with a reconciliation line, which is the check that matters:

```
  consumed                : 60
  processed successfully  : 50
    of which recovered    : 13 (failed at least once, then succeeded on retry)
  total retry attempts    : 22
  sent to DLQ             : 10
    deserialization fail  : 3
    validation fail       : 6
    retries exhausted     : 1
  accounted for           : 60/60 OK - no records lost
```

---

## Tests

```powershell
python -m pytest
```

68 unit tests, no broker required — the pieces worth testing in isolation are
pure functions:

- **[tests/test_avro_codec.py](tests/test_avro_codec.py)** — round-trip, wire
  format against the Avro spec, compactness vs JSON, every deserialization
  failure mode, and both directions of schema evolution.
- **[tests/test_retry.py](tests/test_retry.py)** — the exact backoff sequence
  (with `sleep` and the RNG injected, so nothing actually waits), jitter bounds,
  exhaustion behaviour, and that permanent errors never consume retry budget.
- **[tests/test_aggregator.py](tests/test_aggregator.py)** — running average
  checked against `statistics.fmean` after *every* update, the float-precision
  case that motivates Welford, and O(1) state.
- **[tests/test_validation.py](tests/test_validation.py)** — each business rule,
  and that `ValidationError` really is classified permanent.

---

## Repository layout

```
├── schemas/
│   ├── order.avsc              Avro schema from the assignment brief
│   └── order_v2.avsc           evolved schema, used by the tests
├── src/
│   ├── config.py               all settings, env-var overridable
│   ├── errors.py               transient vs permanent taxonomy
│   ├── avro_codec.py           single-object encoding + schema registry
│   ├── retry.py                exponential backoff with jitter
│   ├── aggregator.py           Welford running statistics
│   ├── producer.py             order generator + fault injection
│   ├── consumer.py             decode → validate → retry → aggregate → DLQ
│   ├── create_topics.py        topic provisioning
│   └── dlq_tool.py             DLQ inspect / replay
├── tests/                      68 unit tests
├── scripts/
│   ├── setup-kafka.ps1         download + install Kafka
│   ├── start-kafka.ps1         format storage, start broker
│   ├── stop-kafka.ps1          stop broker
│   ├── reset-kafka.ps1         wipe all data, start clean
│   └── demo.ps1                full end-to-end demonstration
├── config/
│   └── kraft-server.properties.template
└── docs/
    └── demo-output.txt         captured transcript of a full run
```

---

## Running Kafka on Windows 11 — notes

Kafka's Windows scripts are less maintained than the Linux ones, and three
things broke before this ran. They cost real time, so they are written down.

**1. `The input line is too long.`**
`kafka-run-class.bat` builds the JVM classpath by expanding every jar in `libs/`
into a single command line. With ~100 jars, an install path of any length pushes
it past the 8191-character Windows limit and *every* Kafka command fails.
Installing under `C:\kafka` instead of inside this project (path:
`…\Desktop\Big Data\Assignment\…`) keeps the classpath well inside the limit.
`setup-kafka.ps1` refuses an install path containing a space for the same
reason.

**2. `'wmic' is not recognized`**
`kafka-server-start.bat` sizes the JVM heap by shelling out to `wmic`, which
Windows 11 has removed. The fallback branch it lands in is harmless, but
`start-kafka.ps1` sets `KAFKA_HEAP_OPTS` up front to skip the dead call.
`kafka-server-stop.bat` also depends on `wmic` and is a genuine no-op on Windows
11, so `stop-kafka.ps1` finds the broker through CIM instead — matching on
`kafka.Kafka` in the command line, so unrelated Java processes are never
touched.

**3. Deleting a topic kills the broker.**
Deleting a topic makes Kafka unlink log segment and index files that are still
memory-mapped. Windows does not permit that, and the broker treats the
resulting `IOException` as fatal and shuts down — [KAFKA-1194], open since 2014.
This is why a clean run is a *data directory* reset (`reset-kafka.ps1`: stop,
wipe, re-format, restart) rather than a topic delete, and why
`create_topics.py --recreate` refuses to run on Windows without `--force`.

[KAFKA-1194]: https://issues.apache.org/jira/browse/KAFKA-1194

Also worth knowing: `Start-Transcript` does not capture output from child
processes, so `demo.ps1 -Transcript` re-runs itself as a child process and tees
the merged stream instead. The broker is launched through a generated `.cmd`
rather than a quoted argument list, because `cmd /c "a.bat" "b.properties"` hits
cmd's quote-stripping rule and the config path never arrives.

---

## Requirements

- **Python 3.10+** (`match` on `|` type syntax; developed on 3.12)
- **Java 17+** — required by Kafka 4.x (developed on Java 21)
- **Apache Kafka 4.1.2** — installed by `scripts/setup-kafka.ps1`

No ZooKeeper: Kafka 4.x removed it, and the broker runs as a single KRaft node
that is both controller and broker.

No Docker required.
