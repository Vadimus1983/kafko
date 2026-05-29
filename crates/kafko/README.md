<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/Vadimus1983/kafko/main/docs/kafko-wordmark.png">
    <img src="https://raw.githubusercontent.com/Vadimus1983/kafko/main/docs/kafko-wordmark-light.png" alt="kafko" width="320">
  </picture>
</p>

> **Trademark notice:** Apache Kafka and Kafka are trademarks of the [Apache Software Foundation](https://www.apache.org/). **kafko** is an independent open-source project and is not affiliated with or endorsed by the Apache Software Foundation or Confluent Inc.

# kafko

An in-process log with Kafka-like semantics for Rust. Topics, partitions, offset-based reads, replay, retention, compaction — all without a broker, a network hop, or a JVM.

`kafko` exists for use cases where your data never needs to leave the process: embedded event sourcing, edge buffers, durable in-process pub/sub, deterministic integration tests without Docker or a broker, single-binary services that want a real log instead of a `VecDeque<T>` under a mutex. SQLite is to PostgreSQL what `kafko` is to Kafka.

<p align="center">
  <img src="https://raw.githubusercontent.com/Vadimus1983/kafko/main/docs/kafko-integration.svg" alt="kafko in-process pipeline — producer → topic → consumer+producer → topic → consumer chain inside a single Rust process, persisted to disk" width="900">
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
- **Async API on `tokio`** — `Producer::send().await` resolves once the record is appended to the OS file (page cache); see the [project README](https://github.com/Vadimus1983/kafko#durability) for the full durability contract
- **Single-writer-per-partition invariant** — no global mutex on the hot path

The killer use case isn't "replace Kafka." It's **testing log-shaped application code in-process**: open a `Kafko` in the same test binary, call the produce/consume/seek APIs directly, and get offset-aware integration tests without containers, brokers, or flake.

## Quickstart

```toml
[dependencies]
kafko = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
bytes = "1"
```

To use a compression codec, opt in via Cargo features — see
[Compression features](#compression-features):

```toml
kafko = { version = "0.2", features = ["compression-lz4"] }
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

## What's in (v0.2.0)

- Single partition per topic
- Single consumer per topic
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
- `Producer::send_batch` for atomic, single-round-trip batched appends

## Roadmap

- Multi-partition with key-based routing
- Consumer groups with independent committed offsets
- Log compaction (key-based dedup)
- Configurable fsync policy (`EveryRecord` / `EveryBatch` / `EveryNms` / `Never`)
- Headers / record metadata
- Per-topic config persistence

## Benchmarks, recipes, full docs

The [project README on GitHub](https://github.com/Vadimus1983/kafko) carries the full bench matrices, performance-tuning recipes, durability contract, architecture diagram, codec notes, and the v0.2 architectural details. The [CHANGELOG](https://github.com/Vadimus1983/kafko/blob/main/CHANGELOG.md) tracks per-version changes.

## License

Licensed under **MIT OR Apache-2.0**, at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
