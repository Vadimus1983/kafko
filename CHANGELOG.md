# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
adheres to [Semantic Versioning](https://semver.org/).

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
