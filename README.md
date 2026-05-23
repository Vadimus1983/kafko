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
- **Async API on `tokio`** — `Producer::send().await` resolves once the record is appended to the OS file (page cache); see [Durability](#durability) for the exact contract
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

## Durability

kafko v0.1 provides the **same durability contract as Kafka with `acks=1`** — leader has the record in page cache, not necessarily on disk:

- `Producer::send().await` resolves once the record has been written to the OS file via `write_all`. The bytes are in the **OS page cache**, owned by the kernel — they survive process crashes (panic, SIGKILL, OOM) because the process doesn't own them.
- `Producer::send().await` does **not** fsync. Records may be lost if the OS crashes, the kernel panics, or the host loses power before automatic writeback (typically seconds on Linux / Windows).
- Torn or partial writes at the tail of the active segment are detected and truncated on next startup via CRC scan; the sparse index is rebuilt from the verified segment.
- For stricter guarantees, the partition exposes an explicit `sync()` you can call after `send`. A configurable per-call fsync policy (`EveryRecord` / `EveryBatch` / `EveryNms` / `Never`) is on the v0.2 roadmap.

This contract is identical to what Kafka calls `acks=1` and is the *fair* comparison shape for the benchmarks below. If you need `acks=all`-style multi-replica durability, kafko is not the right tool — use Kafka.

## Benchmarks

All numbers from a single machine, both systems running in Linux Docker containers, both load tests running **inside their respective containers** (`oha` inside the kafko container; `kafka-producer-perf-test.sh` inside the Kafka container). Container loopback only — no Windows TCP, no port forwarding. Reproducible from `scripts/kafko_docker_bench.ps1` and `scripts/kafka_bench_unbatched.ps1`.

### Methodology — apples-to-apples

Both systems configured to send **one record per network request** with 16 concurrent producers:

| | kafko | Kafka |
|---|---|---|
| Image | Debian slim + `kafko-http` + oha | `apache/kafka:3.7.0` + perf-test |
| Server config | axum 0.7 + kafko, port 9091 | KRaft single-node, 8 io/net threads, 16 MiB max msg, 1 GiB JVM heap |
| Client config | oha, `-c 16`, one HTTP request per record | `linger.ms=0`, `batch.size=size+1024`, `max.in.flight=1`, `acks=1`, 16 parallel `kafka-producer-perf-test.sh` processes |
| Durability | record in OS page cache at `send().await` (same contract as Kafka `acks=1`) | `acks=1` — leader has appended to page cache |
| Payload | all-zero bytes | all-zero bytes |
| Compression codecs | none / lz4 / zstd (per-topic) | none / lz4 / zstd (`compression.type`) |

### Throughput — records/sec (16 concurrent producers, wall-clock aggregate)

| Size | kafko **none** | Kafka **none** | Δ | kafko **lz4** | Kafka **lz4** | Δ | kafko **zstd** | Kafka **zstd** | Δ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 B    | **106,638** | 64,555 | 1.65× | **102,819** | 66,806 | 1.54× | **99,986**  | 47,211 | 2.12× |
| 256 B   | **101,513** | 39,537 | 2.57× | **105,016** | 37,918 | 2.77× | **102,030** | 27,791 | 3.67× |
| 512 B   | **92,107**  | 30,973 | 2.97× | **104,222** | 26,526 | 3.93× | **96,380**  | 19,045 | 5.06× |
| 1 KiB   | **76,754**  | 21,980 | 3.49× | **101,391** | 19,703 | 5.15× | **98,395**  | 14,564 | 6.76× |
| 4 KiB   | **26,979**  | 6,916  | 3.90× | **97,632**  | 5,941  | 16.4× | **95,316**  | 4,125  | 23.1× |
| 128 KiB | **8,480**   | n/a¹   | n/a   | **27,698**  | n/a¹   | n/a   | **31,493**  | n/a¹   | n/a   |
| 1 MiB   | **1,770**   | 256    | 6.91× | **6,199**   | 238    | 26.0× | **4,848**   | 215    | 22.5× |

¹ The 128 KiB cell was added to `kafka_bench_unbatched.ps1` after its most recent run; rerun that script to populate the Kafka column at 128 KiB. (Why 128 KiB? It's Kafka's default `batch.size`, so the cell asks: what does kafko do when it's sending the same payload size Kafka would naturally pack into one batched network call?)

kafko wins every apples-to-apples cell. At small records, the win is 1.5-3×; at large records with compression, it grows to **22-26×**.

### Payload throughput — MiB/s committed

| Size | kafko none | Kafka none | kafko lz4 | Kafka lz4 | kafko zstd | Kafka zstd |
|---|---:|---:|---:|---:|---:|---:|
| 64 B    | 6.5   | 3.9  | 6.3   | 4.1  | 6.1   | 2.9  |
| 256 B   | 24.8  | 9.7  | 25.6  | 9.3  | 24.9  | 6.8  |
| 1 KiB   | 75.0  | 21.5 | 99.0  | 19.2 | 96.1  | 14.2 |
| 4 KiB   | 105.4 | 27.0 | **381.4** | 23.2 | **372.3** | 16.1 |
| 128 KiB | **1,060** | n/a¹ | **3,462** | n/a¹ | **3,937** | n/a¹ |
| **1 MiB** | **1,770** | 256 | **6,199** | 238 | **4,848** | 215 |

### Latency — p50 (median, codec = none)

| Size | kafko p50 | Kafka p50 |
|---|---:|---:|
| 64 B    | **0.14 ms** | 3,079 ms |
| 256 B   | **0.15 ms** | 5,549 ms |
| 512 B   | **0.16 ms** | 9,086 ms |
| 1 KiB   | **0.20 ms** | 12,386 ms |
| 4 KiB   | **0.58 ms** | 2,155 ms |
| 128 KiB | **1.86 ms** | n/a¹ |
| 1 MiB   | **8.94 ms** | 424 ms |

The Kafka latency values are dominated by **client-side queue saturation**: with `max.in.flight=1` and `linger.ms=0`, the Java producer cannot avoid pile-up when records arrive faster than the broker can ack one-at-a-time. kafko's `oha` is strictly synchronous per connection (send → wait → receive → next), so its p50 is honest HTTP round-trip time. This is exactly the "Kafka assumes you batch" story — without batching, **Kafka's producer-side architecture forces queueing that kafko's simpler request-response shape sidesteps.**

### Honest framing

Apache Kafka is designed for **batched, throughput-oriented workloads** — `linger.ms` and `batch.size` are not optional in production. With its default-tuned client batching (50 ms linger, 128 KiB batches), Kafka reaches ~1.15M rec/s at 64 B by amortizing one network call across ~2,000 records.

When both systems are configured for the same workload — **one record per network request** — kafko outperforms Kafka by 1.5-27× across every record size at a fraction of the latency.

Once kafko gains a `send_batch` API (planned, v0.2), the expectation is that it matches or beats batched Kafka on small-record throughput while preserving the latency lead.

## v0.1 — what's in

- Single partition per topic
- Single consumer per topic
- File-based segments with CRC32 integrity
- Crash recovery on startup (torn-tail truncate, sparse index rebuild)
- Time- and size-based retention
- Producer + Consumer async API on `tokio`
- Per-topic compression (none / lz4 / zstd)
- `kafko-http` — a separate workspace crate (`crates/kafko-http/`) exposing the broker over HTTP for integration testing and benchmarking

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

This is a Cargo workspace. The library crate is `crates/kafko/` (publishable to crates.io); the HTTP test harness is `crates/kafko-http/` (`publish = false`).

```bash
# Workspace check (lib + http harness + tests + benches)
cargo check --workspace --all-targets

# Build only the library
cargo build --release --package kafko

# Build the HTTP test harness binary
cargo build --release --package kafko-http
#   → target/release/kafko-http(.exe)

# Run all tests
cargo test --workspace

# Run kafko storage benchmarks
cargo bench --package kafko

# Reproduce the apples-to-apples Kafka comparison (Windows PowerShell)
.\scripts\kafko_docker_bench.ps1
.\scripts\kafka_bench_unbatched.ps1
```

See [`scripts/README.md`](scripts/README.md) for the full bench script catalogue (default Kafka, max-tuned Kafka, apples-to-apples, host-side kafko, Docker-side kafko).

## License

Licensed under **MIT OR Apache-2.0**, at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
