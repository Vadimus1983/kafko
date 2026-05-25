# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
adheres to [Semantic Versioning](https://semver.org/).

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

[0.1.1]: https://github.com/Vadimus1983/kafko/releases/tag/v0.1.1
[0.1.0]: https://github.com/Vadimus1983/kafko/releases/tag/v0.1.0
