<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/kafko-wordmark.png">
    <img src="docs/kafko-wordmark-light.png" alt="kafko" width="320">
  </picture>
</p>

> **Trademark notice:** Apache Kafka and Kafka are trademarks of the [Apache Software Foundation](https://www.apache.org/). **kafko** is an independent open-source project and is not affiliated with or endorsed by the Apache Software Foundation or Confluent Inc.

# kafko

An in-process log with Kafka-like semantics for Rust. Topics, partitions, offset-based reads, replay, retention, compaction — all without a broker, a network hop, or a JVM.

`kafko` exists for use cases where your data never needs to leave the process: embedded event sourcing, edge buffers, durable in-process pub/sub, deterministic integration tests without Docker or a broker, single-binary services that want a real log instead of a `VecDeque<T>` under a mutex. SQLite is to PostgreSQL what `kafko` is to Kafka.

## What kafko is

A single Rust crate providing:

- **Topics with partitions** — name a stream, append records, read them back by offset
- **Persistent segments** — records go to disk in framed `[len][crc32][ts][key_len][key][val_len][val]` form; segments rotate by size
- **Offset-based reads** — consumers maintain their own cursor, can seek freely, can replay from anywhere
- **Retention** — drop segments by age or total bytes
- **Compression** — none / lz4 / zstd, configured per topic
- **Compaction** — key-based dedup of the active log (v0.2)
- **Crash recovery** — CRC verification on read, torn-tail truncate on startup
- **Async API on `tokio`** — `Producer::send().await` resolves only after the record is durably appended to the OS file
- **Single-writer-per-partition invariant** — no global mutex on the hot path

The killer use case isn't "replace Kafka." It's **testing log-shaped application code in-process**: open a `Kafko` in the same test binary, call the produce/consume/seek APIs directly, and get offset-aware integration tests without containers, brokers, or flake.

## What kafko is NOT

- Not a competitor to real Kafka — no distribution, no replication, no Kafka wire-protocol
- Not a queue (queues consume = remove; logs are append-only with replay)
- Not a substitute for RabbitMQ-style routing (different category)
- Not for sub-microsecond hot paths (use a matching-engine WAL pattern with `io_uring` + `O_DIRECT` for that)

## Quickstart

```toml
[dependencies]
kafko = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
bytes = "1"
```

```rust
use bytes::Bytes;
use kafko::Kafko;

#[tokio::main]
async fn main() -> kafko::Result<()> {
    let broker = Kafko::open("./data").await?;
    broker.create_topic("orders").await?;

    // Produce
    let producer = broker.producer_for("orders").await?;
    let offset = producer.send(None, Bytes::from("order-1")).await?;
    println!("appended at offset {offset}");

    // Consume from the beginning
    let mut consumer = broker.consumer_for("orders").await?;
    consumer.seek(0);
    let record = consumer.next_record().await?;
    println!("read: {:?}", record.value());

    Ok(())
}
```

### Per-topic compression

```rust
use kafko::{Compression, Kafko, LogConfig};

let broker = Kafko::open("./data").await?;
broker
    .create_topic_with_config(
        "metrics",
        LogConfig { compression: Compression::Zstd, ..Default::default() },
    )
    .await?;
```

## Architecture

One broker object, many cheap handles. Each partition has its own writer task that exclusively owns the active segment file. **No global mutex on the hot path.**

```
                ┌─────────────────────────────────────┐
                │  Kafko (Arc<KafkoInner>)            │
                │  - Topic registry (RwLock)          │
                │  - HashMap<(topic,part), Handle>    │
                └────────┬────────────────────────────┘
                         │ Arc::clone (cheap)
        ┌────────────────┼────────────────┐
        │                │                │
   Producer         Producer         Consumer
        │                │                │
        │   send via per-partition inbox  │
        └────────────────▼────────────────┘
              ┌──────────┴──────────┐
              ▼                     ▼
       Partition writer task    Partition writer task
       (single mpsc owner)      (single mpsc owner)
              │                     │
              ▼                     ▼
       orders-0/ segments      payments-0/ segments
```

## Benchmarks

All numbers from a single machine, both systems running in Linux Docker containers, both load tests running **inside their respective containers** (`oha` inside the kafko container; `kafka-producer-perf-test.sh` inside the Kafka container). Container loopback only — no Windows TCP, no port forwarding. Reproducible from `scripts/kafko_docker_bench.ps1` and `scripts/kafka_bench_unbatched.ps1`.

### Methodology — apples-to-apples

Both systems configured to send **one record per network request** with 16 concurrent producers:

| | kafko | Kafka |
|---|---|---|
| Image | Debian slim + kafko_http + oha | `apache/kafka:3.7.0` + perf-test |
| Server config | axum 0.7 + kafko, port 9091 | KRaft single-node, 8 io/net threads, 16 MiB max msg, 1 GiB JVM heap |
| Client config | oha, `-c 16`, one HTTP request per record | `linger.ms=0`, `batch.size=size+1024`, `max.in.flight=1`, `acks=1`, 16 parallel `kafka-producer-perf-test.sh` processes |
| Durability | record in OS file at `send().await` resolution | `acks=1` — leader has appended to page cache |
| Payload | all-zero bytes | all-zero bytes |
| Compression codecs | none / lz4 / zstd (per-topic) | none / lz4 / zstd (`compression.type`) |

### Throughput — records/sec (16 concurrent producers, wall-clock aggregate)

| Size | kafko **none** | Kafka **none** | Δ | kafko **lz4** | Kafka **lz4** | Δ | kafko **zstd** | Kafka **zstd** | Δ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 B    | **102,786** | 69,179 | 1.49× | **102,828** | 63,782 | 1.61× | **98,696** | 50,888 | 1.94× |
| 256 B   | **102,294** | 39,791 | 2.57× | **105,551** | 36,941 | 2.86× | **93,268** | 29,068 | 3.21× |
| 512 B   | **87,777**  | 32,059 | 2.74× | **102,598** | 25,409 | 4.04× | **95,384** | 19,007 | 5.02× |
| 1 KiB   | **77,431**  | 22,001 | 3.52× | **101,841** | 19,975 | 5.10× | **97,153** | 14,539 | 6.68× |
| 4 KiB   | **27,568**  | 7,025  | 3.92× | **94,154**  | 5,614  | 16.8× | **93,917** | 4,145  | 22.7× |
| 1 MiB   | **1,818**   | 261    | 6.97× | **6,518**   | 242    | 26.9× | **4,758**  | 210    | 22.7× |

kafko wins every cell. At small records, the win is 1.5-3×; at large records with compression, it grows to **22-27×**.

### Payload throughput — MiB/s committed

| Size | kafko none | Kafka none | kafko lz4 | Kafka lz4 | kafko zstd | Kafka zstd |
|---|---:|---:|---:|---:|---:|---:|
| 64 B    | 6.3   | 4.2  | 6.3   | 3.9  | 6.0   | 3.1  |
| 256 B   | 25.0  | 9.7  | 25.8  | 9.0  | 22.8  | 7.1  |
| 1 KiB   | 75.6  | 21.5 | 99.5  | 19.5 | 94.9  | 14.2 |
| 4 KiB   | 107.7 | 27.4 | **367.8** | 21.9 | **366.9** | 16.2 |
| **1 MiB** | **1818** | 261 | **6518** | 242 | **4758** | 210 |

### Latency — p50 (median)

| Size | kafko p50 | Kafka p50 |
|---|---:|---:|
| 64 B    | **0.15 ms** | 2,834 ms |
| 256 B   | **0.15 ms** | 5,605 ms |
| 512 B   | **0.17 ms** | 9,758 ms |
| 1 KiB   | **0.20 ms** | 12,171 ms |
| 4 KiB   | **0.57 ms** | 2,069 ms |
| 1 MiB   | **9.1 ms**  | 404 ms |

The Kafka latency values are dominated by **client-side queue saturation**: with `max.in.flight=1` and `linger.ms=0`, the Java producer cannot avoid pile-up when records arrive faster than the broker can ack one-at-a-time. kafko's `oha` is strictly synchronous per connection (send → wait → receive → next), so its p50 is honest HTTP round-trip time. This is exactly the "Kafka assumes you batch" story — without batching, **Kafka's producer-side architecture forces queueing that kafko's simpler request-response shape sidesteps.**

### Honest framing

Apache Kafka is designed for **batched, throughput-oriented workloads** — `linger.ms` and `batch.size` are not optional in production. With its default-tuned client batching (50 ms linger, 128 KiB batches), Kafka reaches ~1.15M rec/s at 64 B by amortizing one network call across ~2,000 records.

When both systems are configured for the same workload — **one record per network request** — kafko outperforms Kafka by 1.5-27× across every record size at a fraction of the latency.

Once kafko gains a `send_batch` API (planned, v0.2), the expectation is that it matches or beats batched Kafka on small-record throughput while preserving the latency lead.

## v0.1 — what's in

- Single partition per topic
- Single consumer per topic
- File-based segments with CRC32 integrity
- Crash recovery on startup (torn-tail truncate)
- Time- and size-based retention
- Producer + Consumer async API on `tokio`
- Per-topic compression (none / lz4 / zstd)
- HTTP server adapter (`--features http-server`) for testing/benchmarking

## v0.2 — roadmap

- `Producer::send_batch` + framed `POST /produce_batch` for opportunistic batching (no `linger.ms` window)
- Multi-partition with key-based routing
- Consumer groups with independent committed offsets
- Log compaction (key-based dedup)
- Configurable fsync policy (`EveryRecord` / `EveryBatch` / `EveryNms` / `Never`)
- Headers / record metadata
- Per-topic config persistence (currently a topic's compression is set at creation but not persisted across restarts)

## Not on the roadmap

- Kafka wire-protocol compatibility (different category of tool)
- Distributed replication (kafko is in-process by design — if you need replication, use Kafka)
- Schema registry, Connect, Streams ecosystem (out of scope)

## Building and benchmarking

```bash
# Standard build
cargo build --release

# Build with the HTTP server binary
cargo build --release --bin kafko_http --features http-server

# Run all tests
cargo test --all-features

# Run kafko storage benchmarks
cargo bench

# Reproduce the apples-to-apples Kafka comparison (Windows PowerShell)
.\scripts\kafko_docker_bench.ps1
.\scripts\kafka_bench_unbatched.ps1
```

## License

Licensed under **MIT OR Apache-2.0**, at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
