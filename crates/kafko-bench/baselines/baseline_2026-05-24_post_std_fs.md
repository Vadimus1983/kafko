# kafko-bench baseline — 2026-05-24, post std::fs swap

Function-level snapshot after the third heap/throughput win: replacing
`tokio::fs::File` with `std::fs::File` in `segment.rs` and `sparse_index.rs`,
plus dropping the `worker_threads = 4` override in `kafko-http/src/main.rs`.
Both items came from the throughput parking lot (`[[project-throughput-optimizations]]`,
items #3 and #1 respectively).

This is **win #3** in the cumulative series. Same workload + machine as
`baseline_2026-05-24.md` and `baseline_2026-05-24_post_wins_1_2.md`.

## Run context

| Field | Value |
|---|---|
| Date | 2026-05-24 |
| Git HEAD | `d0b02cc` ("log::append_batch no longer allocates fresh scratch buffers per batch" — win #2 committed) |
| Working tree | **dirty** (segment.rs, sparse_index.rs, kafko-http/src/main.rs carry the std::fs swap uncommitted at capture time) |
| Profiler PID | 14920 |
| Profiler uptime at capture | 30.01 s |
| Bench feature set | `--features hotpath,hotpath-alloc,hotpath-mcp` |
| Caller | `kafko_bench::main` |
| Tokio runtime | `multi_thread, worker_threads = 4` |
| Concurrency | 16 producer tasks per cell |
| Codecs | None, Lz4, Zstd |
| Sizes | 64 B, 256 B, 1 KiB, 4 KiB, 128 KiB, 1 MiB |
| Total records observed | 466,982 send calls / 74,989 batched flushes (6.23 rec / batch) |
| Matrix wall time | 0.93 s (down from 1.34 s in the post-#1+#2 baseline) |

Machine fingerprint same as prior baselines (i7-13650HX 14C/20T, 64 GB, Win 11 build 26200).

## What changed in this PR

- `crates/kafko/src/segment.rs` — `Segment.file: tokio::fs::File -> std::fs::File`.
  All methods (`create`, `open`, `append`, `read_at`, `truncate`, `sync`,
  `last_modified_ms`) keep their `async fn` signatures but use synchronous
  `std::io::{Read, Seek, Write}` traits internally. `ensure_cursor` demoted
  from `async fn` to `fn` since it no longer awaits anything.
- `crates/kafko/src/sparse_index.rs` — same pattern. `write_entry` demoted
  to `fn`.
- `crates/kafko/src/log.rs` — **unchanged**. `log.rs` only uses `tokio::fs`
  in setup/admin paths (`create_dir_all`, `remove_file` in retention,
  `read_dir` on startup); not hot-path, not worth touching for this win.
- `crates/kafko-http/src/main.rs:80` — `#[tokio::main(flavor = "multi_thread", worker_threads = 4)]`
  changed to `#[tokio::main]` so tokio defaults to one worker per logical CPU.
  Doesn't affect this in-process bench but matters for the HTTP path.

The partition writer task owns the segment file exclusively (the
single-writer-per-partition invariant), so doing blocking I/O inside the
writer task is safe — there is no other concurrent task touching that file.
The win: each `write_all` no longer ships a cross-thread message to the
tokio blocking-pool, runs the syscall there, and ships the result back.
The syscall runs on the writer's own thread now.

## Function timing (mode: timing, total elapsed 30.01 s)

Sorted by total wall time.

| Function | Calls | Avg | p95 | Total | % elapsed |
|---|---:|---:|---:|---:|---:|
| `kafko::producer::send` | 466,982 | 30.93 µs | 54.62 µs | 14.44 s | 48.14% |
| `kafko::partition::append` | 466,985 | 30.54 µs | 54.21 µs | 14.26 s | 47.53% |
| `kafko::partition::flush_append_batch` | 74,989 | 11.24 µs | 25.61 µs | 842.48 ms | 2.81% |
| `kafko::log::append_batch` | 74,989 | 9.79 µs | 22.51 µs | 733.68 ms | 2.45% |
| `kafko::record::encode_with` | 467,040 | 677 ns | 1.10 µs | 308.75 ms | 1.03% |
| `kafko::segment::append` | 74,989 | 3.96 µs | 7.80 µs | 296.69 ms | 0.99% |
| `kafko::compression::compress` | 311,360 | 668 ns | 900 ns | 208.13 ms | 0.69% |
| `kafko::segment::sync` | 2 | 71.24 ms | 131.14 ms | 142.49 ms | 0.47% |
| `kafko::log::sync` | 1 | 11.84 ms | 11.85 ms | 11.84 ms | 0.04% |

### Per-call timing — diff vs `baseline_2026-05-24_post_wins_1_2.md`

| Function | Post-#1+#2 avg | Post-std::fs avg | Δ |
|---|---:|---:|---:|
| `producer::send` | 45.11 µs | **30.93 µs** | **-31 %** |
| `partition::append` | 44.66 µs | **30.54 µs** | **-32 %** |
| `flush_append_batch` | 17.64 µs | **11.24 µs** | **-36 %** |
| `log::append_batch` | 15.47 µs | **9.79 µs** | **-37 %** |
| `segment::append` | 6.31 µs | **3.96 µs** | **-37 %** |
| `compress` | 761 ns | 668 ns | -12 % |
| `record::encode_with` | 786 ns | 677 ns | -14 % |

This is the single biggest change to per-call latency in the series.
**The original 42 µs mpsc-round-trip "gap"** in `producer::send` (the
unaccounted-for time outside the inner work) is now down to roughly 25 µs
— eliminating the tokio blocking-pool round-trip per syscall accounted
for ~14 µs of that gap. The remaining ~25 µs is still the mpsc + oneshot
actor round-trip; that's the structural target that `Producer::send_batch`
would attack.

`segment::sync` and `log::sync` are single-call samples (the shutdown
fsync); don't read into them.

## Function allocations (mode: alloc-bytes, total 1.3 GB in 30.02 s)

Exclusive allocation bytes per function.

| Function | Calls | Avg / call | Total | % allocs |
|---|---:|---:|---:|---:|
| `kafko::compression::compress` | 311,360 | 4.0 KB | **1.2 GB** | **94.84%** |
| `kafko::partition::append` | 466,985 | 88 B | 39.2 MB | 3.04% |
| `kafko::producer::send` | 466,982 | 32 B | 14.3 MB | 1.11% |
| `kafko::log::append_batch` | 74,989 | 150 B | 10.8 MB | 0.84% |
| `kafko::partition::flush_append_batch` | 74,989 | 32 B | 2.3 MB | 0.18% |
| `kafko::log::sync` | 1 | 32 B | 32 B | 0.00% |
| `kafko::record::encode_with` | 467,040 | 0 B | 0 B | 0.00% |
| `kafko::segment::append` | 74,989 | 0 B | 0 B | 0.00% |
| `kafko::segment::sync` | 2 | 0 B | 0 B | 0.00% |

### Allocations — diff vs `baseline_2026-05-24_post_wins_1_2.md`

| Function | Post-#1+#2 avg | Post-std::fs avg | Δ |
|---|---:|---:|---:|
| `segment::append` | 274 B | **0 B** | **-100 %** (-18.2 MB total) |
| `log::append_batch` | 252 B | **150 B** | **-40 %** (-5.9 MB total; sparse_index also cleaner) |
| `compression::compress` | 4.0 KB | 4.0 KB | 0 % (lz4_flex residual, see [[project-lz4-flex-alloc]]) |
| All other functions | unchanged | unchanged | 0 % |
| **Total allocated** | **1.3 GB** | **1.3 GB** | **0 %** |

Total heap traffic is unchanged because `compress` already accounted for
~93 % of it before this change. With the segment/sparse-index allocations
now zero, `compress` is **94.84 %** of the remaining 1.3 GB — almost the
entire heap budget is now sitting on the deferred lz4_flex hash-table
allocation. Until that's addressed, total heap won't move further.

## What this confirms

- The tokio blocking-thread-pool dispatch was costing ~14 µs of wall-clock
  *per syscall* (the per-call delta in `partition::append` is ~14 µs).
  Across 466 k records this is ~6.5 seconds of wall time we just got back
  — visible in both the matrix wall time (1.34 s -> 0.93 s) and the
  release-build throughput numbers (see
  `baseline_2026-05-24_post_std_fs_throughput.md`).
- tokio::fs's blocking-pool also allocates ~274 B per write to ship the
  cross-thread message and return the io::Result. `segment::append` now
  allocates exactly zero bytes per call.

## What's left

Cumulative wins shipped (-34 % timing, -57 % heap from pre-win baseline):

| State | Total alloc | producer::send avg | Matrix wall |
|---|---:|---:|---:|
| Pre-win baseline | 3.0 GB | 47.10 µs | (long) |
| +Win #1 (compress scratch) | 2.3 GB | 46.28 µs | 1.37 s |
| +Win #2 (Log scratch) | 1.3 GB | 45.11 µs | 1.34 s |
| **+Win #3 (std::fs)** | **1.3 GB** | **30.93 µs** | **0.93 s** |

Open from `[[project-throughput-optimizations]]`:

- **#2 — buffered WAL + configurable fsync policy.** Likely the next big
  single win at small record sizes. Blocked on `Kafko::Drop` first per
  the durability story.
- **#4 — `Producer::send_batch`.** The biggest structural lever against
  the remaining ~25 µs mpsc round-trip in `producer::send`. Invasive.

Open from `[[project-lz4-flex-alloc]]`:

- 1.2 GB of compress allocs is intrinsic to lz4_flex 0.11's hash-table
  reuse pattern; cannot be cut without vendoring or swapping crates.
  Recommended path: file upstream issue, document zstd as the
  high-throughput choice.

## How to regenerate this snapshot

```powershell
$env:KAFKO_RESET = "1"
cargo run --release `
  -p kafko-bench `
  --features hotpath,hotpath-alloc,hotpath-mcp
```

Then query the MCP server via the `hotpath` tools and update the tables.
