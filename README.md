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

<p align="center">
  <img src="docs/kafko-integration.svg" alt="kafko in-process pipeline — producer → topic → consumer+producer → topic → consumer chain inside a single Rust process, persisted to disk" width="900">
</p>

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

`SIGKILL`, OS panic, and power loss bypass userspace and cannot be intercepted; the recovery path on the next `Kafko::open` handles torn tails via CRC scan, but any record whose page-cache bytes had not yet been written back by the kernel may be lost.

**Drop-without-shutdown fallback.** If you let the broker go out of scope without calling `shutdown()`, kafko's `Drop` impl runs the same graceful shutdown as a best-effort fallback:

- On a **multi-thread tokio runtime** (the default `#[tokio::main]`), Drop uses `block_in_place` + `block_on` to drive every partition's writer task to completion before returning. Durability is identical to explicit `shutdown()`; you just lose the ability to observe any error it might have returned.
- On a **current-thread runtime**, Drop can't safely block — it spawns the cleanup detached and may not complete before runtime teardown. Call `shutdown().await` explicitly in this case.
- With **no reachable tokio runtime**, Drop releases the directory lock and lets the writer tasks die with their host runtime.

This contract is identical to what Kafka calls `acks=1`. If you need `acks=all`-style multi-replica durability, kafko is not the right tool — use Kafka.

## Benchmarks

All numbers measured on a single machine. Two complementary views: the **HTTP path** (kafko exposed via `kafko-http` over Docker container loopback, driven by `oha`) and the **library hot path** (in-process via `Producer::send().await` from `crates/kafko-bench`). The first matters when kafko is behind a network listener; the second matters when it's embedded.

Reproducible from `scripts/kafko_docker_bench.ps1` (HTTP) and `cargo run --release -p kafko-bench` (in-process).

### Methodology

| | HTTP path | In-process |
|---|---|---|
| Driver | `oha` (in container), 16 concurrent connections, one HTTP request per record | 16 `tokio::spawn` tasks each calling `Producer::send().await` in a loop |
| Server | axum 0.7 + kafko on port 9091 | (none — in-process) |
| Durability | record in OS file (page cache) at `send().await` | same |
| Payload | all-zero bytes | all-zero bytes |
| Compression codecs | none / lz4 / zstd (per-topic) | same |
| Runtime | `multi_thread`, default worker count (one per logical CPU) | `multi_thread, worker_threads = 4` |

### HTTP path — records/sec (16 concurrent producers, wall-clock aggregate)

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 139,037 | 139,077 | 125,207 |
| 256 B   | 137,057 | 138,972 | 125,603 |
| 512 B   | 134,196 | 137,229 | 126,404 |
| 1 KiB   | 134,435 | 136,338 | 123,555 |
| 4 KiB   |  38,774 | 130,602 | 117,388 |
| 128 KiB |  13,113 |  57,842 |  44,259 |
| 1 MiB   |     818 |   6,193 |   4,834 |

### HTTP path — MiB/s committed

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    |     8.5 |     8.5 |     7.6 |
| 256 B   |    33.5 |    33.9 |    30.7 |
| 1 KiB   |   131.3 |   133.1 |   120.7 |
| 4 KiB   |   151.5 | **510.2** | **458.5** |
| 128 KiB | **1,639** | **7,230** | **5,532** |
| 1 MiB   |     818 |   6,193 |   4,834 |

### HTTP path — latency p50 (codec = none)

| Size | p50 |
|---|---:|
| 64 B    | 0.11 ms |
| 256 B   | 0.11 ms |
| 512 B   | 0.11 ms |
| 1 KiB   | 0.11 ms |
| 4 KiB   | 0.14 ms |
| 128 KiB | 1.21 ms |
| 1 MiB   | 9.22 ms |

Latency is `oha`'s synchronous-per-connection request-response time (send → wait → receive → next), so this is honest end-to-end HTTP RTT through the kafko stack including write-to-page-cache.

### Library hot path — records/sec (in-process, no HTTP)

For users who plan to embed kafko directly — the killer use case — the library-only numbers are higher because there is no HTTP, axum, or `oha` overhead in the path.

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 1,122,798 | 1,323,455 |   689,843 |
| 256 B   |   904,190 | 1,382,915 |   692,955 |
| 1 KiB   |   560,482 | 1,158,094 |   676,439 |
| 4 KiB   |   253,589 | 1,043,108 |   622,537 |
| 128 KiB |    22,245 |    95,816 |   131,684 |
| 1 MiB   |     3,264 |    13,402 |     7,432 |

Small-record cells are nearly 10× the HTTP-path numbers — that's the cost of HTTP serialization, TCP setup, and axum routing per request. Library users skip all of it.

Function-level timing + allocation snapshots and full methodology in `crates/kafko-bench/baselines/`.

### Library hot path — `send_batch` vs single `send` (v0.1.1, 256 B records)

Same in-process path as above, but driven by `cargo bench -p kafko --bench send_batch` so a single producer either calls `send_batch(N)` once or loops N single `send()` calls. The gap is the mpsc round-trip cost saved per record.

| Records per call (N) | `send_batch(N)` | Loop of N × `send()` | **Speedup** |
|---:|---:|---:|---:|
| 1 | 273 K rec/s | 272 K rec/s | 1.0× |
| 8 | 635 K rec/s | 261 K rec/s | **2.4×** |
| 32 | 1.34 M rec/s | 241 K rec/s | **5.6×** |
| 128 | 2.26 M rec/s | 245 K rec/s | **9.2×** |
| 1024 | **2.53 M rec/s** | 252 K rec/s | **10.0×** |

The single-`send` floor of ~250 K rec/s is the mpsc actor round-trip (~4 µs per record); batching saves `(N − 1)` of those round-trips and lowers to one `Log::append_batch` call. The curve flattens by N = 128 and is fully amortized by N = 1024.

#### `send_batch` with compression (256 B all-zero values)

| Compression | N = 1 | N = 128 | N = 1024 |
|---|---:|---:|---:|
| None | 273 K | 2.26 M | 2.53 M |
| **Lz4** | 254 K | 2.46 M | **3.34 M** |
| Zstd | 240 K | 1.02 M | 1.13 M |

**Lz4 is faster than None at large batches** because the all-zero payload compresses ~96 %, so the writer task spends proportionally less time on disk I/O. With genuinely random / incompressible data, expect Lz4 to track None within ±10 %.

Reproduce: `cargo bench -p kafko --bench send_batch -- --baseline v0_1_1` (compares against the pinned v0.1.1 baseline under `target/criterion/`).

### Codec note — LZ4 per-call allocation

LZ4 (`Compression::Lz4`) allocates a fresh **8 KiB hash table on every record encode** (16 KiB on records larger than 64 KiB). This is a property of [`lz4_flex` 0.11](https://crates.io/crates/lz4_flex), kafko's LZ4 dependency: the public block-compress API does not expose a way to reuse the internal hash table across calls. In the in-process bench above, **~1.2 GB of the 1.3 GB total heap traffic** comes from this single source.

The allocations are short-lived (freed immediately after each call) and don't visibly hurt throughput — LZ4 is still the rec/s leader at small records. But for memory-constrained or allocator-sensitive deployments, **zstd is the allocation-free codec on the write path**: `zstd::bulk::Compressor` is held in a thread-local and reuses its internal state across calls, so zstd's per-record heap footprint is essentially zero.

This is fixable only at the dependency level — either when `lz4_flex` exposes a stateful block-compressor API, or by vendoring a slim equivalent into kafko.

## Performance recipes — pick once, ship it

Default config is **already near-optimum for single-producer workloads** — the `preset_configs/throughput_oriented` bench buys only ~4 % over `LogConfig::default()`. So the recipes below mostly differ in **which API you call** (`send` vs `send_batch`) and **which codec you pick**, not in `LogConfig` tuning.

All throughput numbers are 256 B records, single producer, in-process. Larger records hit MiB/s ceilings sooner — see the size matrices above.

| Goal | API | Compression | `LogConfig` | Expected throughput | Per-record latency |
|---|---|---|---|---|---|
| **Max throughput, compressible payloads** | `send_batch(N≥128)` | `Lz4` | `default()` | **~3.3 M rec/s** | amortized over batch |
| **Max throughput, incompressible payloads** | `send_batch(N≥128)` | `None` | `default()` | **~2.5 M rec/s** | amortized over batch |
| **Disk-efficient (best compression ratio)** | `send_batch(N≥32)` | `Zstd` | `default()` | ~1 M rec/s | amortized over batch |
| **Lowest single-record latency** | `send()` | `None` | `default()` | ~250 K rec/s | **~4 µs / send** |
| **Many concurrent producers, no batch API** | `send()` × N tasks | `None` or `Lz4` | bump `batch_max_bytes` to 1 MiB | scales with concurrency until disk caps | ~4 µs / send |

### Recipe 1 — Max throughput (compressible data)

```rust
use bytes::Bytes;
use kafko::{Compression, Kafko, LogConfig};

let broker = Kafko::open("./data").await?;
broker
    .create_topic_with_config(
        "events",
        LogConfig { compression: Compression::Lz4, ..Default::default() },
    )
    .await?;

let producer = broker.producer_for("events").await?;

// Stage 128+ records per call. The mpsc-round-trip cost amortizes ~10× vs a loop of send().
let batch: Vec<(Option<Bytes>, Bytes)> = (0..1024)
    .map(|i| (None, Bytes::from(format!("event-{i}"))))
    .collect();
let offsets = producer.send_batch(batch).await?;
```

Expect **~3.3 M rec/s** for redundant / structured payloads (e.g., JSON, logs, protobuf with shared schemas). Lz4 is genuinely free here — the disk-I/O saved more than pays for the compression CPU.

### Recipe 2 — Max throughput (incompressible data)

Same shape, but pick `Compression::None` (or `Lz4` — it tracks None within ±10 % when data won't compress, but with the per-call LZ4 hash-table allocation overhead, `None` is preferable):

```rust
broker
    .create_topic_with_config(
        "events",
        LogConfig { compression: Compression::None, ..Default::default() },
    )
    .await?;
// ...same send_batch(N≥128) loop as Recipe 1
```

Expect **~2.5 M rec/s** for already-compressed payloads (encrypted blobs, JPEG/MP4 frames, random IDs).

### Recipe 3 — Lowest single-record latency

When you can't batch — interactive request handling, event-by-event ingestion from an upstream stream — use `send()` directly:

```rust
let producer = broker.producer_for("events").await?;
let offset = producer.send(None, Bytes::from("one event")).await?;
```

Floor: ~**4 µs per send** (mpsc → writer-task → fsync-to-page-cache → reply), ~250 K rec/s per producer. **Compression doesn't help here** — at this latency scale the codec cost dominates over disk savings.

### Recipe 4 — Many concurrent producers

When you have N producer tasks all hitting the same topic, kafko's writer task already **coalesces concurrent appends into batched disk writes** (the "natural batching" path). You don't need to write `send_batch` glue — just call `send()` in each task and bump the natural-batch ceiling:

```rust
broker
    .create_topic_with_config(
        "events",
        LogConfig {
            // Default is 64 KiB; raise to 1 MiB so more sends coalesce into each disk write.
            batch_max_bytes: 1024 * 1024,
            batch_max_records: 8192,
            ..Default::default()
        },
    )
    .await?;

let producer = broker.producer_for("events").await?;
// Spawn N tasks each calling producer.clone().send(...) in a loop.
```

Throughput scales with concurrency until disk bandwidth caps. The hotpath profiler shows ~6 sends coalesced per disk write under default `batch_max_bytes`; raising it lets that grow.

### What NOT to tune

The `config_sweep` bench data settled these — don't waste time on:

- **`segment_size_threshold`** — anywhere between 1 MiB and 256 MiB performs within noise. The default 1 GiB is fine for almost any workload; only drop it if you genuinely need smaller files on disk.
- **`index_interval`** — the default 4 KiB is the sweet spot. Smaller (≤ 1 KiB) measurably hurts because of constant index writes; larger (≥ 32 KiB) doesn't help.
- **`segment_size_threshold` below 1 MiB** — the `small_footprint` preset in `config_sweep` is **32 % slower** than default because rotation pressure dominates. Only worth it if you're disk-constrained AND read-heavy on cold data.

## v0.1 — what's in

- Single partition per topic
- Single consumer per topic
- File-based segments with CRC32 integrity
- Crash recovery on startup (torn-tail truncate, sparse index rebuild)
- Time- and size-based retention
- Producer + Consumer async API on `tokio`
- Per-topic compression (none / lz4 / zstd)
- `Producer::send_batch` for atomic, single-round-trip batched appends (v0.1.1)
- `kafko-http` — a separate workspace crate (`crates/kafko-http/`) exposing the broker over HTTP for integration testing and benchmarking

## v0.2 — roadmap

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

# Run kafko storage micro-benchmarks (criterion)
cargo bench --package kafko

# Reproduce the HTTP-path bench (Windows PowerShell, requires Docker)
.\scripts\kafko_docker_bench.ps1

# Reproduce the in-process library bench
cargo run --release -p kafko-bench
```

See [`scripts/README.md`](scripts/README.md) for the full bench-script catalogue.

## Fuzzing

The `fuzz/` directory holds cargo-fuzz targets for the wire-format trust boundary. It is deliberately outside the main workspace because libFuzzer requires a nightly toolchain and the cargo-fuzz CLI, which are overkill for the regular build and test cycle.

```bash
# One-time setup
cargo install cargo-fuzz
rustup toolchain install nightly

# Run a target (continues until Ctrl-C; corpus + crash artifacts persist under fuzz/)
cargo +nightly fuzz run decode_record

# Reproduce a saved crash artifact
cargo +nightly fuzz run decode_record fuzz/artifacts/decode_record/<artifact-id>
```

Existing targets:

- `decode_record` — feeds arbitrary bytes to `Record::decode`. Catches the truncation, CRC-mismatch, and invalid-length paths; rarely reaches deeper code because random bytes almost never pass the CRC.
- `decode_record_structured` — hand-builds a wire-format record with chosen flag/key/value bytes and a **valid CRC**, so the decoder reaches the flag-parse and decompress paths that the unstructured target almost never exercises. The decompressor wrappers (lz4_flex, zstd) must not panic on adversarial payloads.
- `recovery_torn_tail` — feeds arbitrary bytes as the active segment file (`00…0.log`) and runs `Log::open`. Recovery must find the longest valid prefix, truncate the rest, and produce a Log where every offset in `[0, next_offset)` reads back successfully.
- `sparse_index_parse` — feeds arbitrary bytes as the `00…0.index` file and exercises both `SparseIndex::open` and `lookup(u64)` against the parsed result. Neither must panic regardless of how malformed the entry table is.
- `log_op_sequence` — model-based fuzzer that generates an `Arbitrary` sequence of `(append, sync, read, retention)` operations against a fresh `Log` with `Arbitrary` `LogConfig` (small thresholds force rotation pressure). Checks state-machine invariants: offsets are monotonic, no panic on any input, no committed-record corruption.

## License

Licensed under **MIT OR Apache-2.0**, at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
