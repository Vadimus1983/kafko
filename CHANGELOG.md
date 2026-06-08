# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Resumable consumers via committed offsets (consumer groups, part 1).**
  `Kafko::consumer_for_group(topic, group)` returns a consumer that resumes from
  the group's durably committed position instead of offset 0 — the
  "continue where it stopped after a restart" behaviour. `Consumer::commit()`
  persists the current per-partition read position (atomic temp + fsync + rename
  to `<topic>/offsets/<group>`, CRC-framed); `Consumer::committed(partition)` and
  `Consumer::group()` introspect. Distinct groups on a topic keep independent
  positions (durable pub/sub fan-out). A torn/corrupt offset file degrades to
  "start from 0", never an error. `consumer_for` stays anonymous (reads from 0;
  `commit()` is a no-op). New error `KafkoError::InvalidGroupName`.
  Single active consumer per group for now; multi-member partition assignment +
  rebalancing is a later slice.

## [0.3.0] — 2026-06-07

Multi-partition topics with key-based routing. A topic can now own N partitions,
each with its own writer task and log, so producers write in parallel. Records
route by key (`hash(key) % partitions`) so same-key records keep their order;
keyless records spread round-robin. A single consumer reads all partitions merged
into one stream. Ordering is guaranteed **within** a partition; cross-partition
order is intentionally undefined — the standard Kafka contract. Default partition
count is 1, so single-partition topics behave as before.

### Added

- `Kafko::create_topic_with_partitions(name, count)` and
  `create_topic_with_config_and_partitions(name, cfg, count)` — create a topic
  with `count` partitions. `count == 0` returns `KafkoError::InvalidPartitionCount`.
- `Kafko::partition_count(name) -> Option<u32>`.
- `Topic` — public type owning a topic's partitions and its routing (FNV-1a key
  hash + round-robin). Obtained via `Kafko::topic`.
- `RecordPosition { partition, offset }` — returned by all `Producer` send
  methods.
- `Producer::send_to(partition, key, value)` for explicit partition targeting,
  and `Producer::partition_count()`.
- `Consumer::next_with_position()` (record + its `RecordPosition`),
  `from_topic`/`from_topic_at`, `seek_all(offset)`, `seek(partition, offset)`,
  `position(partition)`, `partition_count()`.

### Changed (breaking)

- **`Producer::send` / `send_record` now return `RecordPosition`** instead of a
  bare `u64` offset (an offset is only meaningful within a partition). `send_batch`
  / `send_batch_records` return `Vec<RecordPosition>`. Read `.offset()` /
  `.partition()` on the result.
- **`send_batch` atomicity is now per-partition.** A batch whose records route to
  different partitions is atomic within each partition, not across them. For a
  single-partition topic this is unchanged (one fully-atomic append).
- **On-disk layout changed** to `<dir>/<topic>/<partition>/<segments>`. Data
  directories written by kafko <= 0.2 (segments directly under the topic dir) will
  not open — `Kafko::open` returns `KafkoError::InvalidTopicLayout`. There is no
  automatic migration.
- **`Kafko::topic` returns `Arc<Topic>`** (was `Arc<Partition>`).
- `Producer::new` takes `Arc<Topic>`; `Consumer` is built from a `Topic`
  (`from_topic` / `from_topic_at`) and `Consumer::seek` now takes a partition
  index (use `seek_all` for the old whole-stream behaviour).

### Internal

- Each topic shares one `tokio::sync::Notify` across its partitions; a merged
  consumer parks on it and wakes on any partition's progress. No new dependency.

## [0.2.0] — 2026-05-29

### Performance

- **LZ4 hot-path allocation reduced by ~99.997%.** `Compression::Lz4`'s encode
  path now amortizes its internal hash table across calls via lz4_flex 0.13's
  new `compress_into_with_table` API and a per-thread `CompressTable`. The
  per-record 8 KiB hash-table allocation that dominated heap traffic on
  LZ4-heavy workloads (~1.2 GiB cumulative across 300 K records on lz4_flex
  0.11) is now amortized to a single 8 KiB allocation per encoder thread for
  the process lifetime. Measured: **24.9 KiB total across 100 000 LZ4 sends**
  in the kafko-bench `lz4_sequential` scenario (0.10% of total process
  allocation, down from 93%). LZ4 sequential throughput now tracks no-codec
  sequential throughput within 3% (164 K vs 160 K rec/s on the same path).

### Added

- **Cargo features for compression codecs.** Two new opt-in features —
  `compression-lz4` and `compression-zstd`, plus the convenience
  `compression-all` — make `lz4_flex` and `zstd` optional dependencies. A
  default `cargo add kafko` no longer pulls either codec into the dep tree.
  `Compression::Lz4` and `Compression::Zstd` remain visible in the public API
  regardless of feature flags so on-disk records written by another build are
  detectable and produce a friendly `KafkoError::CompressionUnavailable`
  rather than mis-decoding.
- `KafkoError::CompressionUnavailable(Compression)` — error variant returned
  when encoding or decoding a record under a codec whose Cargo feature is not
  enabled. Display message names the missing feature.
- `Compression::is_available() -> bool` — runtime feature detection for
  callers that want to fall back gracefully between codecs.
- `kafko-bench` `lz4_sequential` scenario — mirrors the `sequential` scenario
  under `Compression::Lz4` so the hotpath alloc table can attribute
  `compression::compress` heap traffic to the LZ4 path specifically. Gated
  behind the `compression-lz4` feature.
- `[package.metadata.docs.rs]` — docs.rs builds with `all-features` so both
  codec variants are documented in full on the rendered crate page.

### Changed (breaking)

- **`Record::encode_with` signature.** Previously `-> usize`; now
  `-> Result<usize>`. The error case is `KafkoError::CompressionUnavailable`,
  returned when encoding under a codec whose feature is not enabled. Callers
  using `Producer::send` / `Producer::send_batch` are unaffected — the
  Producer API was already `Result`-returning and propagates the new error
  naturally. Callers using `Record::encode_with` directly must add `?` or
  `.unwrap()`. `Record::encode()` (the `Compression::None` convenience) is
  unchanged.
- **Default Cargo features.** Previously `lz4_flex` and `zstd` were
  unconditional dependencies; now both are gated behind opt-in features. A
  `cargo update kafko` from `0.1.1` to `0.2.0` for a downstream pinned at
  `^0.1` will **silently drop LZ4/Zstd support** until features are added.
  To preserve v0.1.1 behaviour, change the dependency to:
  ```toml
  kafko = { version = "0.2", features = ["compression-all"] }
  ```

### Internal

- `lz4_flex` bumped from 0.11 to 0.13. The 0.11 line is in maintenance mode;
  0.13 ships the same security fixes as 0.11.6 plus the
  `compress_into_with_table` API that enables the per-thread hash-table reuse
  win above. Wire format unchanged; existing on-disk segments read back
  identically.
- New `LZ4_TABLE: RefCell<lz4_flex::block::CompressTable>` thread-local in
  `crates/kafko/src/compression.rs` alongside the existing
  `ZSTD_COMPRESSOR`/`ZSTD_DECOMPRESSOR` thread-locals.

### Docs

- README: new "Compression features" section explaining the opt-in feature
  set and the `CompressionUnavailable` error contract.
- README: "Codec note — LZ4 per-call allocation" rewritten as "Codec
  allocation profile" — both codecs are now alloc-free on the write path
  after thread warm-up.
- README: send-batch-with-compression table refreshed against the v0.2.0
  build with `compression-all` enabled.
- Memory `project_lz4_flex_alloc.md` flipped from "known unfixable
  limitation" to "shipped via lz4_flex 0.13 upstream API".

## [0.1.1] — 2026-05-25

### Security

- **Fixed: LZ4 decompress OOM (DoS).** Records with `Compression::Lz4` whose
  payload had an adversarial 4-byte LE size prefix could coerce
  `lz4_flex::decompress_size_prepended` into a ~4 GiB `Vec::with_capacity`
  call **before** the compressed bytes were validated, OOM-crashing the host
  process. Triggerable by any read of a corrupt or adversarial segment file.
  Fixed by bounding the claimed decompressed size at 16 MiB before delegating
  to `lz4_flex` (matching the existing Zstd cap). Discovered via the new
  cargo-fuzz `decode_record_structured` target. **v0.1.0 is yanked.**

### Added

- `Producer::send_batch(Vec<(Option<Bytes>, Bytes)>)` and
  `Producer::send_batch_records(Vec<Record>)` — atomic, single-mpsc-round-trip
  batched appends. **~10× throughput vs a loop of `send()` at N = 1024**
  (2.53 M rec/s vs 252 K rec/s, criterion-measured, 256 B records, in-process).
- `Partition::append_batch(Vec<Record>) -> Result<Vec<u64>>` — the actor
  primitive underlying `Producer::send_batch`.
- Two new criterion benches: `send_batch` (batch-size × codec sweep),
  `config_sweep` (`segment_size_threshold` / `index_interval` / preset configs).
- Cargo-fuzz scaffold under `fuzz/` with five targets: `decode_record`,
  `decode_record_structured`, `recovery_torn_tail`, `sparse_index_parse`,
  `log_op_sequence`. WSL2 setup documented in the README.
- Proptest coverage expanded to all three compression codecs, the
  `SparseIndex::lookup` invariant, `Log` append-then-read invariant, and
  `Log` rotation under random `LogConfig`.
- "Performance recipes — pick once, ship it" section in both READMEs:
  decision table + four code recipes + "what NOT to tune" guidance.

### Fixed

- **`Segment::would_overflow` no longer reports overflow on an empty segment.**
  Previously the first append (or batch) whose wire size exceeded
  `segment_size_threshold` against a fresh empty log triggered rotation,
  which then tried to create a new segment at `next_offset = 0` and failed
  with `ErrorKind::AlreadyExists`. The size cap is now correctly a soft
  target on segments with content; empty segments always accept the first
  write. Reachable under default config only at > 1 GiB single records;
  reachable under any custom small-threshold config.

### Changed

- Internal constant `ZSTD_DECOMPRESS_MAX_SIZE` renamed to `DECOMPRESS_MAX_SIZE`
  and now applies uniformly to LZ4 and Zstd.
- README: `Producer::send_batch` moved from v0.2 roadmap to v0.1 list. The
  `kafko-http` `/produce_batch` bullet was dropped from the roadmap — it's
  an improvement for the measurement harness, not the library.

## [0.1.0] — 2026-05-24

### YANKED

This version contains the LZ4 OOM DoS bug described above. **Upgrade to
0.1.1.** The yank does not break existing `Cargo.lock`-pinned consumers;
it only redirects new dependency resolutions.

### Added

- Initial release: in-process log with Kafka-like semantics for Rust.
- Single partition per topic.
- File-based segments with CRC32 integrity.
- Crash recovery on startup (torn-tail truncate, sparse index rebuild).
- Time- and size-based retention.
- Producer + Consumer async API on `tokio`.
- Per-topic compression (none / lz4 / zstd).
- Data-directory advisory lock — concurrent `Kafko::open` on the same dir
  fails fast with `KafkoError::AlreadyOpen`.
- Writer-task panic recovery — typed `KafkoError::PartitionPanicked` instead
  of generic `Closed`.
- Graceful shutdown via explicit `shutdown().await` or `Drop` fallback.

[0.2.0]: https://github.com/Vadimus1983/kafko/releases/tag/v0.2.0
[0.1.1]: https://github.com/Vadimus1983/kafko/releases/tag/v0.1.1
[0.1.0]: https://github.com/Vadimus1983/kafko/releases/tag/v0.1.0
