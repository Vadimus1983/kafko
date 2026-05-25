# kafko-bench throughput baseline — 2026-05-24, post wins #1 + #2

Release-build, no hotpath features. Direct apples-to-apples diff against
`baseline_2026-05-24_throughput.md` (the pre-win snapshot).

## Run context

| Field | Value |
|---|---|
| Date | 2026-05-24 |
| Git HEAD | `ca77494` (win #1 committed; win #2 working-tree change on `log.rs`) |
| Working tree | dirty (`crates/kafko/src/log.rs` — win #2 scratch-field refactor) |
| Build | `cargo run --release -p kafko-bench` (no features) |
| Total matrix wall time | 0.98 s |
| Data dir | reset before run (`KAFKO_RESET=1`) |
| Tokio runtime | `multi_thread, worker_threads = 4` |
| Concurrency | 16 producer tasks per cell |
| Machine | i7-13650HX 14C/20T, 64 GB, Win 11 build 26200 |

## Throughput — records / sec

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 647,405 | 717,003 | 578,699 |
| 256 B   | 548,346 | 767,969 | 582,189 |
| 1 KiB   | 337,148 | 750,013 | 568,024 |
| 4 KiB   |  97,561 | 670,814 | 538,331 |
| 128 KiB |  18,670 |  71,048 | 101,214 |
| 1 MiB   |   3,519 |  14,406 |   6,985 |

## Throughput — MiB / s

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    |    39.5 |    43.8 |    35.3 |
| 256 B   |   133.9 |   187.5 |   142.1 |
| 1 KiB   |   329.2 |   732.4 |   554.7 |
| 4 KiB   |   381.1 | 2,620.4 | 2,102.9 |
| 128 KiB | 2,333.8 | 8,881.0 |12,651.8 |
| 1 MiB   | 3,519.3 |14,405.8 | 6,985.3 |

## Per-cell elapsed (raw)

| Size | none | lz4 | zstd |
|---|---:|---:|---:|
| 64 B    | 0.077 s | 0.070 s | 0.086 s |
| 256 B   | 0.091 s | 0.065 s | 0.086 s |
| 1 KiB   | 0.148 s | 0.067 s | 0.088 s |
| 4 KiB   | 0.051 s | 0.007 s | 0.009 s |
| 128 KiB | 0.027 s | 0.007 s | 0.005 s |
| 1 MiB   | 0.055 s | 0.013 s | 0.027 s |

Record counts per cell (16 tasks × `per_task` from `target_records()` in `main.rs`):
50,000 records for sizes < 4 KiB; 4,992 at 4 KiB; 496 at 128 KiB; 192 at 1 MiB.

## Δ vs `baseline_2026-05-24_throughput.md` (pre-win)

| Size | none Δ | lz4 Δ | zstd Δ |
|---|---:|---:|---:|
| 64 B    | +11.6% | -7.0%  | +7.6% |
| 256 B   | -10.4% | -5.2%  | +4.7% |
| 1 KiB   | -8.5%  |  0%    |  0%   |
| 4 KiB   | +8.8%  | +32.8% | +4.6% |
| 128 KiB | -3.6%  | **+61%** | +5.0% |
| 1 MiB   | +19%   | +3.2%  | +6.3% |

**Read carefully** — most of these deltas are below the single-run noise floor.

Cells where the post-win number is reliably better (cell ran 50 ms+ AND the Δ
is meaningfully above noise):
- **128 KiB / lz4: +61%** — 44,114 → 71,048 rec/s. Probably real; this is a
  compression-heavy cell where the per-call `compress` allocation cut shows up
  most clearly.
- **4 KiB / lz4: +32.8%** — 504,972 → 670,814 rec/s. Also a compression-heavy
  cell. Likely real, but the 7 ms cell duration limits confidence.
- **zstd** column shows +5% consistently across all sizes — small but consistent
  enough to look like signal rather than noise.

Cells where the regression is almost certainly noise:
- 256 B / none: ran in 91 ms post-win vs 82 ms pre-win → -10% throughput.
  Single context switch's worth.
- 1 KiB / none: similar story.

The cells with 5-15 ms run times (4 KiB lz4/zstd, 128 KiB all codecs, 1 MiB lz4)
remain noise-dominated as noted in the pre-win baseline. The throughput gains
*should* be real given win #1 + #2 cut 1.7 GB of heap traffic on this workload,
but a clean signal requires longer-running cells. See the "if we need trustworthy
deltas" note in the pre-win file.

## What this baseline does NOT tell us

- It measures the **library hot path only**. The README's apples-to-apples vs
  Kafka uses `oha` over HTTP through `kafko-http`; that comparison isn't
  affected by win #1 / #2 in any visible way at these record sizes, because
  HTTP overhead dominates. Re-running `kafko_docker_bench.ps1` is *not*
  required to validate wins #1 + #2.
- It does not measure tail-latency. The hotpath p95 numbers in
  `baseline_2026-05-24_post_wins_1_2.md` are the relevant source for that.

## How to regenerate

```powershell
$env:KAFKO_RESET = "1"
cargo run --release -p kafko-bench
```

Save the per-cell `rec/s` and `MiB/s` lines into the tables above, replacing
the current values. Keep the dated filename so the prior baseline stays on
disk for diffing.
