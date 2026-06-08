<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/Vadimus1983/kafko/main/docs/kafko-wordmark.png">
    <img src="https://raw.githubusercontent.com/Vadimus1983/kafko/main/docs/kafko-wordmark-light.png" alt="kafko" width="320">
  </picture>
</p>

> **Trademark notice:** Apache Kafka and Kafka are trademarks of the [Apache Software Foundation](https://www.apache.org/). **kafko** is an independent open-source project and is not affiliated with or endorsed by the Apache Software Foundation or Confluent Inc.

# kafko

An in-process log with Kafka-like semantics for Rust. Topics, partitions with key-based routing, offset-based reads, replay, retention, resumable group consumers — all without a broker, a network hop, or a JVM.

`kafko` exists for use cases where your data never needs to leave the process: embedded event sourcing, edge buffers, durable in-process pub/sub, deterministic integration tests without Docker or a broker, single-binary services that want a real log instead of a `VecDeque<T>` under a mutex. SQLite is to PostgreSQL what `kafko` is to Kafka.

<p align="center">
  <img src="https://raw.githubusercontent.com/Vadimus1983/kafko/main/docs/kafko-integration.svg" alt="kafko in-process pipeline — producer → topic → consumer+producer → topic → consumer chain inside a single Rust process, persisted to disk" width="900">
</p>

## What kafko is

A single Rust crate providing:

- **Topics with partitions** — name a stream, route records to partitions by key (`hash(key) % N`), read them back by offset
- **Persistent segments** — records go to disk in framed `[len][crc32][ts][key_len][key][val_len][val]` form; segments rotate by size
- **Offset-based reads** — consumers maintain their own cursor, can seek freely, can replay from anywhere
- **Resumable consumers** — consumer groups commit per-partition offsets and resume where they left off across restarts
- **Retention** — drop segments by age or total bytes
- **Compression** — none / lz4 / zstd, configured per topic
- **Crash recovery** — CRC verification on read, torn-tail truncate on startup
- **Async API on `tokio`** — `Producer::send().await` resolves once the record is appended to the OS file (page cache); see the [project README](https://github.com/Vadimus1983/kafko#durability) for the full durability contract
- **Single-writer-per-partition invariant** — no global mutex on the hot path

The killer use case isn't "replace Kafka." It's **testing log-shaped application code in-process**: open a `Kafko` in the same test binary, call the produce/consume/seek APIs directly, and get offset-aware integration tests without containers, brokers, or flake.

## Quickstart

```toml
[dependencies]
kafko = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
bytes = "1"
```

To use a compression codec, opt in via Cargo features — see
[Compression features](#compression-features):

```toml
kafko = { version = "0.3", features = ["compression-lz4"] }
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
    let pos = producer.send(None, Bytes::from("order-1")).await?;
    println!("appended at partition {} offset {}", pos.partition(), pos.offset());

    // Consume from the beginning
    let mut consumer = broker.consumer_for("orders").await?;
    consumer.seek_all(0);
    let record = consumer.next_record().await?;
    println!("read: {:?}", record.value());

    Ok(())
}
```

### Partitions and key-based routing

A topic can have multiple partitions. Producers route each record to a partition
by key (`hash(key) % partition_count`), so records sharing a key keep their order;
keyless records spread round-robin. Order is guaranteed **within** a partition,
not across — that's the trade-off that lets partitions' writers run in parallel.
A single `Consumer` reads all partitions merged into one stream.

```rust
use bytes::Bytes;
use kafko::Kafko;

# async fn run() -> kafko::Result<()> {
let broker = Kafko::open("./data").await?;
broker.create_topic_with_partitions("events", 8).await?;

let producer = broker.producer_for("events").await?;
// All records for "user-42" land on the same partition, in order.
let pos = producer.send(Some(Bytes::from("user-42")), Bytes::from("clicked")).await?;
println!("partition {} offset {}", pos.partition(), pos.offset());

// One consumer drains every partition (use next_with_position to see which).
let mut consumer = broker.consumer_for("events").await?;
let record = consumer.next_record().await?;
# Ok(())
# }
```

Default partition count is 1; single-partition topics are a total-order FIFO and
behave exactly as in 0.2 (only the `send` return type changed — see below).

### Resumable consumers (committed offsets)

A consumer bound to a named **group** persists its position and resumes from it
across restarts, instead of re-reading from offset 0. Distinct groups on the same
topic keep independent positions.

```rust
use kafko::Kafko;

# async fn run() -> kafko::Result<()> {
let broker = Kafko::open("./data").await?;
broker.create_topic("orders").await?;

let mut consumer = broker.consumer_for_group("orders", "billing").await?;
let record = consumer.next_record().await?;
// ... process the record ...
consumer.commit().await?;   // durably persist progress (at-least-once)
# Ok(())
# }
```

Commit *after* processing for at-least-once delivery: a crash between processing
and `commit` replays from the last commit. `consumer_for` (no group) stays
anonymous — it reads from 0 and `commit()` is a no-op. Sharing one group across
multiple live consumers (partition assignment + rebalancing) is a later feature.

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

### Compression features

LZ4 and Zstd are opt-in via Cargo features, so a default `cargo add kafko`
pulls in no compression dependencies. Pick what you need:

| Feature | Adds to deps | Enables variant |
|---|---|---|
| _(default)_ | _(nothing)_ | `Compression::None` only |
| `compression-lz4` | `lz4_flex 0.13` | `Compression::Lz4` |
| `compression-zstd` | `zstd 0.13` | `Compression::Zstd` |
| `compression-all` | both above | both |

`Compression::Lz4` and `Compression::Zstd` are visible in the public API
regardless of features — a build without the matching codec returns
`KafkoError::CompressionUnavailable(codec)` instead of mis-decoding bytes,
so a reader built without (e.g.) LZ4 can still detect and gracefully reject
segments written by an LZ4-enabled producer. Call `Compression::is_available()`
for a runtime check.

## What's in (v0.3.0)

- Multi-partition topics with key-based routing (`hash(key) % partitions`) and
  parallel per-partition writers; keyless records spread round-robin
- Merged consumer that reads all of a topic's partitions as one stream
- Resumable consumers: `consumer_for_group` + `commit` persist per-partition
  committed offsets, so a group continues where it left off across restarts
- File-based segments with CRC32 integrity
- Crash recovery on startup (torn-tail truncate, sparse index rebuild)
- Time- and size-based retention
- Producer + Consumer async API on `tokio`
- Per-topic compression (none / lz4 / zstd), opt-in via the
  `compression-lz4` / `compression-zstd` / `compression-all` Cargo features
- LZ4 hot-path allocation amortized to one 8 KiB workspace per encoder thread
  via lz4_flex 0.13's `compress_into_with_table` (down from one alloc per record)
- Data-directory lockfile — concurrent `Kafko::open` on the same dir fails fast with `KafkoError::AlreadyOpen`
- Writer-task panic recovery — typed `KafkoError::PartitionPanicked` instead of generic `Closed`
- Graceful shutdown via explicit `shutdown().await` or `Drop` fallback
- `Producer::send_batch` for single-round-trip batched appends (per-partition atomic)

## Roadmap

- Consumer groups: multi-member partition assignment + rebalancing (committed
  offsets / resumable consumers already shipped)
- Log compaction (key-based dedup)
- Configurable fsync policy (`EveryRecord` / `EveryBatch` / `EveryNms` / `Never`)
- Headers / record metadata
- Per-topic `LogConfig` persistence (partition count already persists via the
  on-disk layout; compression / segment / retention settings still fall back to
  the broker default on reopen)

## Benchmarks, recipes, full docs

The [project README on GitHub](https://github.com/Vadimus1983/kafko) carries the full bench matrices, performance-tuning recipes, durability contract, architecture diagram, codec notes, and the v0.2 architectural details. The [CHANGELOG](https://github.com/Vadimus1983/kafko/blob/main/CHANGELOG.md) tracks per-version changes.

## License

Licensed under **MIT OR Apache-2.0**, at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
