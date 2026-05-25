# kafko-bench throughput baseline — 2026-05-24

Pre-optimization wall-clock numbers for the same 6 sizes x 3 codecs x 16-concurrency
matrix used by the function-timing snapshot. **Release build, no `hotpath` features**
— this is the clean apples-to-apples reference for diffing post-change throughput.

## Run context

| Field | Value |
|---|---|
| Date | 2026-05-24 |
| Git HEAD | `d747aca1c8daa973daea82559a5ff58a33fccf6e` (`d747aca`) |
| Working tree | dirty (same state as `baseline_2026-05-24.md`) |
| Build | `cargo run --release -p kafko-bench` (no features) |
| Total matrix wall time | 1.03 s |
| Data dir | reset before run (`KAFKO_RESET=1`) |
| Tokio runtime | `multi_thread, worker_threads = 4` |
| Concurrency | 16 producer tasks per cell |
| Machine | i7-13650HX 14C/20T, 64 GB, Win 11 build 26200 (see `baseline_2026-05-24.md`) |

## Throughput — records / sec

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 579,957 | 771,152 | 537,938 |
| 256 B   | 611,691 | 810,222 | 555,899 |
| 1 KiB   | 368,385 | 749,833 | 566,537 |
| 4 KiB   |  89,652 | 504,972 | 514,528 |
| 128 KiB |  19,375 |  44,114 |  96,363 |
| 1 MiB   |   2,957 |  13,953 |   6,574 |

## Throughput — MiB / s

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    |    35.4 |    47.1 |    32.8 |
| 256 B   |   149.3 |   197.8 |   135.7 |
| 1 KiB   |   359.8 |   732.3 |   553.3 |
| 4 KiB   |   350.2 | 1,972.5 | 2,009.9 |
| 128 KiB | 2,421.8 | 5,514.2 |12,045.4 |
| 1 MiB   | 2,957.4 |13,952.8 | 6,574.3 |

## Per-cell elapsed (raw)

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 0.086 s | 0.065 s | 0.093 s |
| 256 B   | 0.082 s | 0.062 s | 0.090 s |
| 1 KiB   | 0.136 s | 0.067 s | 0.088 s |
| 4 KiB   | 0.056 s | 0.010 s | 0.010 s |
| 128 KiB | 0.026 s | 0.011 s | 0.005 s |
| 1 MiB   | 0.065 s | 0.014 s | 0.029 s |

Record counts per cell (16 tasks * `per_task` from `target_records()` in `main.rs`):
50,000 records for sizes < 4 KiB; 4,992 at 4 KiB; 496 at 128 KiB; 192 at 1 MiB.

## Caveat — small-cell timings are noisy

Several cells finish in **5-15 ms**. At those durations a single scheduler hiccup
or context-switch tail moves the throughput by tens of percent. Specifically:

- 4 KiB / lz4 / zstd: 10 ms each
- 128 KiB: 5-26 ms across codecs
- 1 MiB / lz4: 14 ms

For tracking a ~5% perf change, these cells will be **noise-dominated**. The
small-record cells (50 k records taking 60-140 ms) are tighter. If we need
trustworthy deltas on the large-record cells post-change, bump `target_records()`
in `crates/kafko-bench/src/main.rs` so each large cell takes 200 ms+, or run the
matrix 5x and take the median.

The small-record cells are good enough as-is for the planned heap-allocation
wins (#1 compress buffer reuse, #2 append_batch scratch reuse). Those changes
are not expected to move large-record throughput meaningfully -- the win is in
allocation count, and compress is only 0.31% of elapsed at the profiled scale.

## How to regenerate

```powershell
$env:KAFKO_RESET = "1"
cargo run --release -p kafko-bench
```

Save the per-cell `rec/s` and `MiB/s` lines into the tables above, replacing the
current values. Keep the dated filename so the prior baseline stays on disk for
diffing.

## Why these numbers differ wildly from the README's bench tables

| Aspect | README bench tables | This baseline |
|---|---|---|
| Path | HTTP (`oha` -> axum -> kafko) | direct library call (`Producer::send`) |
| Environment | Linux Docker container | Windows host process |
| Concurrency model | 16 HTTP connections | 16 tokio tasks sharing one Producer |
| Per-record overhead | HTTP request + response | `Arc::clone(Producer) + mpsc + oneshot` |
| Purpose | Apples-to-apples vs Kafka | Library hot path only |

The two tables are **not interchangeable** -- a regression visible in this file
may not show in the HTTP tables (because the HTTP overhead floods it), and vice
versa. They serve different purposes:

- **This baseline** -- track library-level changes (compression, log, segment, partition)
- **README tables** -- track external positioning vs Kafka and HTTP-path changes

Both should be diffed before declaring a perf win, but for the planned wins #1/#2
this baseline is the primary signal.
