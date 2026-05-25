# kafko-bench throughput baseline — 2026-05-24, post std::fs swap

Release-build, no hotpath features. Direct apples-to-apples diff against
`baseline_2026-05-24_post_wins_1_2_throughput.md`.

## Run context

| Field | Value |
|---|---|
| Date | 2026-05-24 |
| Git HEAD | `d0b02cc` (wins #1 + #2 committed; win #3 working-tree change) |
| Working tree | dirty (segment.rs, sparse_index.rs, kafko-http/src/main.rs — std::fs swap + drop of worker_threads = 4 override) |
| Build | `cargo run --release -p kafko-bench` (no features) |
| Total matrix wall time | 0.69 s |
| Data dir | reset before run (`KAFKO_RESET=1`) |
| Tokio runtime | `multi_thread, worker_threads = 4` |
| Concurrency | 16 producer tasks per cell |
| Machine | i7-13650HX 14C/20T, 64 GB, Win 11 build 26200 |

## Throughput — records / sec

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 1,122,798 | 1,323,455 |   689,843 |
| 256 B   |   904,190 | 1,382,915 |   692,955 |
| 1 KiB   |   560,482 | 1,158,094 |   676,439 |
| 4 KiB   |   253,589 | 1,043,108 |   622,537 |
| 128 KiB |    22,245 |    95,816 |   131,684 |
| 1 MiB   |     3,264 |    13,402 |     7,432 |

## Throughput — MiB / s

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    |    68.5 |    80.8 |    42.1 |
| 256 B   |   220.7 |   337.6 |   169.2 |
| 1 KiB   |   547.3 | 1,131.0 |   660.6 |
| 4 KiB   |   990.6 | 4,074.6 | 2,431.8 |
| 128 KiB | 2,780.7 |11,977.0 |16,460.5 |
| 1 MiB   | 3,263.5 |13,402.5 | 7,432.2 |

## Per-cell elapsed (raw)

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 0.045 s | 0.038 s | 0.072 s |
| 256 B   | 0.055 s | 0.036 s | 0.072 s |
| 1 KiB   | 0.089 s | 0.043 s | 0.074 s |
| 4 KiB   | 0.020 s | 0.005 s | 0.008 s |
| 128 KiB | 0.022 s | 0.005 s | 0.004 s |
| 1 MiB   | 0.059 s | 0.014 s | 0.026 s |

Record counts per cell (16 tasks * `per_task` from `target_records()`):
50,000 records for sizes < 4 KiB; 4,992 at 4 KiB; 496 at 128 KiB; 192 at 1 MiB.

## Δ vs `baseline_2026-05-24_post_wins_1_2_throughput.md` (cumulative win #3)

| Size | none Δ | lz4 Δ | zstd Δ |
|---|---:|---:|---:|
| 64 B    | **+73%** | **+85%** | +19% |
| 256 B   | **+65%** | **+80%** | +19% |
| 1 KiB   | **+66%** | **+54%** | +19% |
| 4 KiB   | **+160%** | **+55%** | +16% |
| 128 KiB | +19% | +35% | +30% |
| 1 MiB   | -7% (noise) | -7% (noise) | +6% |

Δ vs original pre-win baseline (`baseline_2026-05-24_throughput.md`, cumulative
across all three wins):

| Size | none Δ | lz4 Δ | zstd Δ |
|---|---:|---:|---:|
| 64 B    | **+94%** | **+72%** | +28% |
| 256 B   | **+48%** | **+71%** | +25% |
| 1 KiB   | +52% | **+54%** | +19% |
| 4 KiB   | **+183%** | **+107%** | +21% |
| 128 KiB | +15% | **+117%** | +37% |
| 1 MiB   | +10% | -4% | +13% |

The 4 KiB / none cell going from 89,652 -> 253,589 rec/s (**+183%**) is
emblematic: that's the cell where tokio::fs's blocking-pool overhead
dominated single-record cost. lz4 small-record cells nearly doubling
(64 B / 256 B: +71-72%) is the same mechanism plus the heap-pressure
reduction from wins #1 + #2 compounding.

## Read carefully — what's noise vs signal

The cells that ran in <15 ms remain noise-dominated as in prior baselines
(4 KiB / lz4 / zstd at ~5-8 ms; 128 KiB at 4-5 ms; 1 MiB / lz4 at 14 ms).
For these cells, single-run deltas of less than ~30 % could go either way.

But the **direction is unmistakable across the whole matrix**, and the
cells with 50 ms+ runtimes (most of the small-record None / Lz4 cells)
are far above noise. **Win #3 is the biggest single throughput change
across the entire optimization series.**

## Mechanism

`tokio::fs::File::write_all(...).await` was sending every disk write
through tokio's blocking-thread-pool dispatch:

1. Wake a thread in the pool
2. Send the syscall + payload over a cross-thread channel
3. Run the syscall there (`write_all` blocks until kernel returns)
4. Send the io::Result back via another cross-thread channel
5. Wake the original task

That cross-thread round-trip cost ~14 us per call in the hotpath snapshot.
With ~74 k batch flushes in the bench matrix, that's ~1 s of overhead
just in the round-trip. Eliminated entirely by `std::fs::File::write_all`
running the syscall directly on the writer's own thread.

Safety: the partition writer task is the only thing that touches the
segment file (the single-writer-per-partition invariant); blocking the
writer's thread on a fast syscall doesn't starve anyone — the writer
*is* the work, and other tokio tasks run on the other three workers.

## What this baseline does NOT tell us

- HTTP-path throughput. README's apples-to-apples vs Kafka uses
  `oha` -> axum -> kafko-http -> kafko. The std::fs swap will help the
  HTTP path too (same `tokio::fs` round-trip per record), but not measured
  here. To get HTTP numbers, run `kafko_docker_bench.ps1`.
- Tail latency. p95 numbers in `baseline_2026-05-24_post_std_fs.md` are
  the relevant source.

## How to regenerate

```powershell
$env:KAFKO_RESET = "1"
cargo run --release -p kafko-bench
```
