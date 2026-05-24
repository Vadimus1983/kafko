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

### Graceful shutdown

`Kafko::shutdown().await` is a real durability boundary: every partition's writer task drains its inbox, fsyncs the active segment, and exits before the call returns. Any record that was acked to a producer before `shutdown` was called is on disk by the time `shutdown` resolves.

Host applications that care about durability across `SIGTERM` / `SIGINT` / `docker stop` should install a signal handler that drives `shutdown().await` to completion before exiting:

```rust,no_run
tokio::signal::ctrl_c().await.ok();
broker.shutdown().await?;
```

`SIGKILL`, OS panic, and power loss bypass userspace and cannot be intercepted; the recovery path on the next `Kafko::open` handles torn tails via CRC scan, but any record whose page-cache bytes had not yet been written back by the kernel may be lost. Letting the broker drop without calling `shutdown` releases the data-directory lock but does NOT guarantee a final fsync.

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
| 64 B    | **139,037** | 64,555 | 2.15× | **139,077** | 66,806 | 2.08× | **125,207** | 47,211 | 2.65× |
| 256 B   | **137,057** | 39,537 | 3.47× | **138,972** | 37,918 | 3.66× | **125,603** | 27,791 | 4.52× |
| 512 B   | **134,196** | 30,973 | 4.33× | **137,229** | 26,526 | 5.17× | **126,404** | 19,045 | 6.64× |
| 1 KiB   | **134,435** | 21,980 | 6.12× | **136,338** | 19,703 | 6.92× | **123,555** | 14,564 | 8.48× |
| 4 KiB   | **38,774**  | 6,916  | 5.61× | **130,602** | 5,941  | 22.0× | **117,388** | 4,125  | 28.5× |
| 128 KiB | **13,113**  | n/a    | n/a   | **57,842**  | n/a    | n/a   | **44,259**  | n/a    | n/a   |
| 1 MiB   | **818**     | 256    | 3.20× | **6,193**   | 238    | 26.0× | **4,834**   | 215    | 22.5× |

kafko wins every apples-to-apples cell. At small records, the win is 2-3×; at large records with compression, it grows to **22-28×**. The 128 KiB Kafka column is empty because that cell wasn't part of `kafka_bench_unbatched.ps1`'s matrix; re-run that script if you need it populated.

### Payload throughput — MiB/s committed

| Size | kafko none | Kafka none | kafko lz4 | Kafka lz4 | kafko zstd | Kafka zstd |
|---|---:|---:|---:|---:|---:|---:|
| 64 B    | 8.5   | 3.9  | 8.5   | 4.1  | 7.6   | 2.9  |
| 256 B   | 33.5  | 9.7  | 33.9  | 9.3  | 30.7  | 6.8  |
| 1 KiB   | 131.3 | 21.5 | 133.1 | 19.2 | 120.7 | 14.2 |
| 4 KiB   | 151.5 | 27.0 | **510.2** | 23.2 | **458.5** | 16.1 |
| 128 KiB | **1,639** | n/a  | **7,230** | n/a  | **5,532** | n/a  |
| **1 MiB** | **818** | 256 | **6,193** | 238 | **4,834** | 215 |

### Latency — p50 (median, codec = none)

| Size | kafko p50 | Kafka p50 |
|---|---:|---:|
| 64 B    | **0.11 ms** | 3,079 ms |
| 256 B   | **0.11 ms** | 5,549 ms |
| 512 B   | **0.11 ms** | 9,086 ms |
| 1 KiB   | **0.11 ms** | 12,386 ms |
| 4 KiB   | **0.14 ms** | 2,155 ms |
| 128 KiB | **1.21 ms** | n/a |
| 1 MiB   | **9.22 ms** | 424 ms |

The Kafka latency values are dominated by **client-side queue saturation**: with `max.in.flight=1` and `linger.ms=0`, the Java producer cannot avoid pile-up when records arrive faster than the broker can ack one-at-a-time. kafko's `oha` is strictly synchronous per connection (send → wait → receive → next), so its p50 is honest HTTP round-trip time. This is exactly the "Kafka assumes you batch" story — without batching, **Kafka's producer-side architecture forces queueing that kafko's simpler request-response shape sidesteps.**

### Library hot path (in-process, no HTTP)

The tables above measure kafko via the HTTP test harness so the comparison vs Kafka is honest. For users who plan to embed kafko directly (the killer use case — `Producer::send().await` from inside the same process), the library-only numbers are higher because there is no HTTP, axum, or `oha` overhead in the path.

Measured by `crates/kafko-bench` (in-process, 16 concurrent `tokio::spawn` producers sharing one `Producer`):

| Size | none rec/s | lz4 rec/s | zstd rec/s |
|---|---:|---:|---:|
| 64 B    | 1,122,798 | 1,323,455 |   689,843 |
| 256 B   |   904,190 | 1,382,915 |   692,955 |
| 1 KiB   |   560,482 | 1,158,094 |   676,439 |
| 4 KiB   |   253,589 | 1,043,108 |   622,537 |
| 128 KiB |    22,245 |    95,816 |   131,684 |
| 1 MiB   |     3,264 |    13,402 |     7,432 |

Same machine, same workload semantics, no network. Small-record cells nearly 10× the HTTP-path numbers — that's the cost of HTTP serialization, TCP setup, and axum routing per request. Library users skip all of it.

Snapshots and full methodology in `crates/kafko-bench/baselines/`.

### Codec note — LZ4 per-call allocation

LZ4 (`Compression::Lz4`) allocates a fresh **8 KiB hash table on every record encode** (16 KiB on records larger than 64 KiB). This is a property of [`lz4_flex` 0.11](https://crates.io/crates/lz4_flex), kafko's LZ4 dependency: the public block-compress API does not expose a way to reuse the internal hash table across calls. In the in-process bench above, **~1.2 GB of the 1.3 GB total heap traffic** comes from this single source.

The allocations are short-lived (freed immediately after each call) and don't visibly hurt throughput — LZ4 is still the rec/s leader at small records. But for memory-constrained or allocator-sensitive deployments, **zstd is the allocation-free codec on the write path**: `zstd::bulk::Compressor` is held in a thread-local and reuses its internal state across calls, so zstd's per-record heap footprint is essentially zero.

This is fixable only at the dependency level — either when `lz4_flex` exposes a stateful block-compressor API, or by vendoring a slim equivalent into kafko.

### Honest framing

Apache Kafka is designed for **batched, throughput-oriented workloads** — `linger.ms` and `batch.size` are not optional in production. With its default-tuned client batching (50 ms linger, 128 KiB batches), Kafka reaches ~1.15M rec/s at 64 B by amortizing one network call across ~2,000 records.

When both systems are configured for the same workload — **one record per network request** — kafko outperforms Kafka by 2-28× across every record size at a fraction of the latency. The widest gaps appear on **medium and large records with compression**, where Kafka's per-batch overhead dominates and kafko's single-syscall write path runs essentially full-speed.

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
