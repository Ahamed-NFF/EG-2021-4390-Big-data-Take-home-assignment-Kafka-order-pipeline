# Kafka Order Pipeline — Avro, Tiered Retry, DLQ and Windowed Aggregation

A Kafka producer/consumer pair that publishes purchase orders as Avro-encoded
messages and processes them with real-time aggregation, two interchangeable
retry strategies, and a dead letter queue with a bounded replay budget.

Built for the Big Data assignment. Python 3.12, Apache Kafka 4.1.2 in KRaft
mode, `confluent-kafka` (librdkafka) and `fastavro`. No ZooKeeper, no Docker.

---

## Architecture

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
    │   3. downstream sink ─── fails ──► TransientError ──► retry        │
    │        │                                                 │         │
    │        ▼                                                 │         │
    │   4. aggregate: cumulative average + tumbling windows    │         │
    │        │                                                 │         │
    │   5. commit offset  ◄────────────────────────────────────┘         │
    └───────┬───────────────────────┬──────────────────────────┬─────────┘
            ▼                       ▼                          ▼
  ┌───────────────────┐   ┌──────────────────────┐   ┌───────────────────┐
  │ orders.aggregates │   │  retry ladder        │   │  orders.DLQ       │
  │ cumulative snaps  │   │  orders.retry.1 (2s) │   │  original bytes + │
  │ + closed windows  │   │  orders.retry.2 (6s) │   │  diagnostics,     │
  │ log-compacted     │   │  orders.retry.3 (15s)│   │  30-day retention │
  └───────────────────┘   └───────┬──────────────┘   └─────────┬─────────┘
                                  │ exhausted                  ▼
                                  └───────────────►  ┌───────────────────┐
                                                     │ dlq_tool.py       │
                                                     │ inspect / replay  │
                                                     │ (bounded budget)  │
                                                     └───────────────────┘
```

### Requirements coverage

| Requirement | Where |
|---|---|
| Produce and consume order messages | [src/producer.py](src/producer.py), [src/consumer.py](src/consumer.py) |
| Avro serialization | [src/avro_codec.py](src/avro_codec.py), [schemas/order.avsc](schemas/order.avsc) |
| Real-time aggregation (running average of prices) | [src/aggregator.py](src/aggregator.py), [src/windows.py](src/windows.py) |
| Retry logic for temporary failures | [src/retry.py](src/retry.py) (blocking), [src/retry_topics.py](src/retry_topics.py) (non-blocking) |
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

# 4. Measure the two retry strategies against each other
.\scripts\compare-retry-modes.ps1
```

Stop the broker afterwards with `.\scripts\stop-kafka.ps1`.

### Running it as a live two-terminal demo

More convincing to watch, because the running average and the window
boundaries update while the producer is still publishing:

```powershell
# once
.\scripts\start-kafka.ps1
python -m src.create_topics

# terminal 1 — consumer, leave it running
python -m src.consumer --window-seconds 5

# terminal 2 — producer, slow enough to follow along
python -m src.producer --count 200 --rate 3

# terminal 2 — afterwards
python -m src.dlq_tool inspect
python -m src.dlq_tool replay --dry-run
```

### Useful flags

```powershell
# blocking retry instead of retry topics
python -m src.consumer --retry-mode blocking --max-attempts 6

# heavier fault injection, to force more DLQ traffic
python -m src.producer --count 100 --corrupt-rate 0.15 --invalid-rate 0.15

# make the downstream sink fail half the time
python -m src.consumer --transient-failure-rate 0.5
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
exercise that: the tests prove a v2 reader decodes v1 bytes (backward
compatible) and a v1 reader ignores the added fields (forward compatible).

This replaces Confluent Schema Registry, which would otherwise be a second
service to run. The trade-off is worth stating: a fingerprint says *which*
schema wrote a record but cannot enforce that new versions stay compatible
before they are published. A production deployment should use a registry with a
compatibility policy — [Karapace](https://github.com/aiven/karapace) is a
Confluent-API-compatible one that installs from pip if a JVM service is
unwelcome.

---

## Failure handling

The routing decision comes down to one question: **would running this record
again produce a different result?** The taxonomy in [src/errors.py](src/errors.py)
answers it explicitly rather than inferring it from error strings.

| Failure | Class | Routing | Why |
|---|---|---|---|
| Payload is not valid Avro | `DeserializationError` (permanent) | DLQ immediately | Bad bytes stay bad |
| Negative / zero / non-finite price, blank id | `ValidationError` (permanent) | DLQ immediately | The record itself is wrong |
| Downstream sink unavailable | `TransientError` | retry, then DLQ | The input is fine; the environment failed |

Retrying a poison record is not harmless. It delays every later record on the
same partition while attempts burn down, and it ends up in the DLQ regardless —
so permanent failures skip the retry path entirely.

### Two retry models, measured

Both are implemented and selectable with `--retry-mode`, because the choice
between them is a genuine trade-off rather than one being simply better.

**`blocking`** — in-process exponential backoff with jitter:

```
attempt 1 fails → wait 0.25s (±30%)
attempt 2 fails → wait 0.50s (±30%)
attempt 3 fails → wait 1.00s (±30%)
attempt 4 fails → give up, send to DLQ
```

Jitter matters more than it looks: without it, every consumer that failed at
the same moment retries at the same moment, and the synchronised retry storm
re-creates the outage it was meant to ride out.

**`topics`** (default) — the record is republished to the next retry topic and
the original offset is committed immediately:

```
orders ──fail──► orders.retry.1 ──fail──► orders.retry.2 ──fail──► orders.retry.3 ──fail──► orders.DLQ
                      (2s)                     (6s)                     (15s)              (terminal)
```

Each record carries the wall-clock time it becomes eligible. A tier consumer
that polls a record too early **rewinds and pauses that partition** rather than
sleeping, so every other partition it owns keeps being served. Pausing on the
head record is sound because every record on a given tier has the same delay
applied, which keeps each tier ordered by due time — nothing behind the head
can be due sooner.

`scripts/compare-retry-modes.ps1` runs both over identical input. A
representative result, 60 orders with a 30% transient failure rate:

| retry mode | main topic drain | throughput | processed | recovered | DLQ | accounted |
|---|---|---|---|---|---|---|
| `blocking` | 12.1 s | 5.0 orders/s | 54 | 17 | 6 | 60/60 |
| `topics`   | **1.0 s** | **61.0 orders/s** | 55 | 21 | 5 | 60/60 |

Drain time is the honest metric here. Total wall-clock is not comparable: the
tier delays (2s/6s/15s) are deliberately longer than the in-process backoffs,
and in topic mode they elapse in the background where they cost nothing. What
head-of-line blocking actually costs is the time the main topic spends stalled
behind a record that is sleeping, and that is what the 12× gap measures.

The price of the faster mode is ordering. A retried record rejoins the stream
later than records that never failed, so global ordering is lost. Blocking
retry preserves order and sacrifices throughput; topic retry does the reverse.
Neither loses records — both reconcile 60/60.

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
| `x-replay-count` | replays used out of the budget |

Keeping the payload byte-identical is what makes replay possible. It also
avoids a circularity trap: a record that failed *deserialization* cannot be
re-serialized to write it somewhere, so the DLQ has to accept raw bytes.

`python -m src.dlq_tool inspect` prints all of it, decodes the payload where it
still can, and reports **queue depth plus the age of the oldest record**. Age is
the better alerting signal — a depth threshold has to be re-tuned every time
traffic volume changes, whereas "something has been stuck here for 20 minutes"
means the same thing at any scale.

`replay` re-publishes to the main topic and **defaults to transient failures
only** — those failed because the environment was down, so replaying once it
recovers should succeed. Deserialization and validation failures would loop
straight back into the DLQ, so replaying them takes an explicit `--error-type`.

Two mechanisms stop replay becoming an infinite loop:

- Each replay increments `x-replay-count` on the record. Once a record has used
  its budget (default 3) the tool refuses it and says so.
- The replayer **commits its offsets**. Kafka topics are append-only, so
  replaying does not remove anything from the DLQ; without a committed position
  every run would resend the whole queue from the beginning and the budget
  would never be reached.

Verified by cycling records through replay four times: rounds 1–3 replayed,
round 4 refused with *"Skipping 3 record(s) that already used their 3-replay
budget"*.

---

## Real-time aggregation

Two aggregates run side by side because they answer different questions.

**Cumulative** — a running average over everything seen, using **Welford's
online algorithm** rather than `sum / count`. Both agree on demo-sized data, but
Welford updates the mean incrementally and never carries a growing sum, so it
stays accurate on an unbounded stream of float32 prices where a naive running
total loses low-order bits as it grows.

**Tumbling windows** — fixed, non-overlapping intervals over **event time** (the
timestamp Kafka recorded at publish, not when the consumer got round to
reading it; processing time would make the buckets depend on consumer lag).

```
  [OK]  #1032   Item1  price=   301.48 | running avg=   252.95 over 41  (recovered on attempt 2)
  [WINDOW CLOSED] [13:56:12 .. 13:56:14)  n=9  avg=245.95  (open windows retained: 1)
```

The cumulative average is the one that stops being interesting on a long
stream: after a million records a sudden shift in pricing barely moves it.
Windows also keep **state bounded** — once a window closes it is emitted and
evicted, so memory is proportional to the number of *open* windows, not to the
length of the stream. That is the property that makes it usable indefinitely,
and there is a test asserting it (500 seconds of data leaves exactly one window
open).

A window closes when the watermark — the highest event time seen — passes its
end plus a grace period. The grace period exists because records arrive
slightly out of order across partitions; a straggler that turns up after its
window closed is counted as late rather than folded into the wrong bucket.

Both kinds of snapshot are published to the log-compacted `orders.aggregates`
topic. Cumulative snapshots share one key so old ones collapse away; each window
gets its own key so the window history is retained.

---

## Delivery semantics

**Producer — no duplicates on retry.** `enable.idempotence=True` with
`acks=all`. When librdkafka retries a batch, the broker de-duplicates on
producer id and sequence number, so an internal retry cannot silently double a
record.

**Consumer — at-least-once.** Auto-commit is off. The offset is committed only
after the record has been processed, republished to a retry tier, or written to
the DLQ — and in the last two cases only once **the broker has acknowledged
that write**. That acknowledgement is not decoration: a fire-and-forget produce
followed by a commit means a broker-side rejection silently destroys the
record, which is the worst failure this design can have. If the write cannot be
confirmed the consumer raises, leaves the offset uncommitted, and stops:

```
[consumer] FATAL: DLQ write: broker rejected the write: ...
[consumer] offset NOT committed - the record will be redelivered on restart. No data lost.
```

Stopping is the right response. If the DLQ is unwritable, every subsequent
failure is unparkable too.

The honest limitation: a crash between a successful process and the commit
replays a record that was already counted, so the aggregate is at-least-once,
not exactly-once. Closing that gap needs an idempotent sink keyed on `orderId`,
or Kafka transactions spanning the sink write and the offset commit
(`init_transactions` / `send_offsets_to_transaction` with `read_committed`
consumers). For a running average over a demo stream the replay window is not
worth that complexity, but it is a real property of the system, not an
oversight.

Every run ends with a reconciliation line, which is the check that matters:

```
  consumed                : 85 (60 from 'orders', 25 from retry tiers)
  processed successfully  : 55
    of which recovered    : 21 (failed at least once, then succeeded)
  pushed to a retry tier  : 25
  partitions paused       : 6 (deferred without blocking others)
  sent to DLQ             : 5
  accounted for           : 60/60 from main topic OK - no records lost
```

Reconciliation is against main-topic intake. Retry-tier reads are re-reads of
records already counted, so including them would double-count.

---

## Tests

```powershell
python -m pytest
```

105 unit tests, no broker required — the pieces worth testing in isolation are
pure functions:

- **[tests/test_avro_codec.py](tests/test_avro_codec.py)** — round-trip, wire
  format against the Avro spec, compactness vs JSON, every deserialization
  failure mode, and both directions of schema evolution.
- **[tests/test_retry.py](tests/test_retry.py)** — the exact backoff sequence
  (with `sleep` and the RNG injected, so nothing actually waits), jitter bounds,
  exhaustion, and that permanent errors never consume retry budget.
- **[tests/test_retry_topics.py](tests/test_retry_topics.py)** — tier
  escalation, that the ladder is terminal and cannot loop, that routing is
  topic-driven so a forged header cannot skip tiers, provenance surviving every
  hop, and that malformed headers cannot crash the consumer.
- **[tests/test_windows.py](tests/test_windows.py)** — bucketing and half-open
  boundaries, closing on watermark, **state eviction**, grace-period behaviour
  with and without, and late-record accounting.
- **[tests/test_aggregator.py](tests/test_aggregator.py)** — running average
  checked against `statistics.fmean` after *every* update, and the
  float-precision case that motivates Welford.
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
│   ├── retry.py                blocking exponential backoff with jitter
│   ├── retry_topics.py         non-blocking tiered retry topics
│   ├── aggregator.py           Welford cumulative statistics
│   ├── windows.py              tumbling windows over event time
│   ├── producer.py             order generator + fault injection
│   ├── consumer.py             decode → validate → retry → aggregate → DLQ
│   ├── create_topics.py        topic provisioning
│   └── dlq_tool.py             DLQ inspect / replay with budget
├── tests/                      105 unit tests
├── scripts/
│   ├── setup-kafka.ps1         download + install Kafka
│   ├── start-kafka.ps1         format storage, start broker
│   ├── stop-kafka.ps1          stop broker
│   ├── reset-kafka.ps1         wipe all data, start clean
│   ├── demo.ps1                full end-to-end demonstration
│   └── compare-retry-modes.ps1 measured blocking vs non-blocking
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
into a single command line. With ~100 jars, a long install path pushes it past
the 8191-character Windows limit and *every* Kafka command fails. Installing
under `C:\kafka` instead of inside this project (path:
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

- **Python 3.10+** (developed on 3.12)
- **Java 17+** — required by Kafka 4.x (developed on Java 21)
- **Apache Kafka 4.1.2** — installed by `scripts/setup-kafka.ps1`

No ZooKeeper: Kafka 4.x removed it, and the broker runs as a single KRaft node
that is both controller and broker. No Docker required.

---

## References

The retry and DLQ design follows established practice rather than being
invented here.

- [Uber Engineering — Building Reliable Reprocessing and Dead Letter Queues with Apache Kafka](https://www.uber.com/us/en/blog/reliable-reprocessing/) — the tiered retry-topic architecture, and why client-level blocking retries clog batch processing.
- [Factor House — Dead letter queues in Kafka: patterns and pitfalls](https://factorhouse.io/articles/dead-letter-queues-kafka/) — DLQ header conventions, the poison-pill loop, silent data loss when the DLQ producer fails, and age-of-oldest-record as an alerting signal.
- [Spring Kafka — How the retry topic pattern works](https://docs.spring.io/spring-kafka/reference/retrytopic/how-the-pattern-works.html) — the pause/resume-until-due mechanism reimplemented here in Python.
- [Confluent — Defining windows in Kafka Streams](https://developer.confluent.io/courses/kafka-streams/windowing/) — window types, and why unwindowed aggregations accumulate without bound.
- [Confluent — Transactions in Apache Kafka](https://www.confluent.io/blog/transactions-apache-kafka/) and [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) — what closing the at-least-once gap would require.
- [Apache Avro specification — single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding) — the wire format used on the topic.
