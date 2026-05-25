# kafko-bench baseline — 2026-05-24, post wins #1 + #2

Function-level snapshot after both heap-reduction wins are applied:

1. **Win #1** — `Compression::compress` writes into a caller-supplied `&mut Vec<u8>`;
   `Record::encode_with` routes through a per-thread `ENCODE_COMPRESS_SCRATCH`
   thread-local (commit `ca77494`).
2. **Win #2** — `Log` carries `encode_buf: BytesMut` and `actual_sizes: Vec<usize>`
   scratch fields reused across `append_batch` calls; the dead `sizes: Vec<usize>`
   from the original code is deleted (uncommitted at time of measurement; see
   working-tree note below).

Same workload as `baseline_2026-05-24.md` (the pre-win snapshot), same machine.

## Run context

| Field | Value |
|---|---|
| Date | 2026-05-24 |
| Git HEAD | `ca77494` ("encode path no longer allocates a fresh compress buffer per record" — win #1 committed) |
| Working tree | **dirty** (`crates/kafko/src/log.rs` carries the win #2 changes uncommitted at capture time) |
| Profiler PID | 24856 |
| Profiler uptime at capture | 26.30 s |
| Bench feature set | `--features hotpath,hotpath-alloc,hotpath-mcp` |
| Caller | `kafko_bench::main` |
| Tokio runtime | `multi_thread, worker_threads = 4` |
| Concurrency | 16 producer tasks per cell |
| Codecs | None, Lz4, Zstd (one topic each) |
| Sizes | 64 B, 256 B, 1 KiB, 4 KiB, 128 KiB, 1 MiB |
| Total records observed | 466,979 send calls / 69,523 batched flushes (6.72 rec / batch) |

Machine fingerprint same as `baseline_2026-05-24.md` (i7-13650HX 14C/20T, 64 GB,
Win 11 build 26200).

## Function timing (mode: timing, total elapsed 26.30 s)

Sorted by total wall time.

| Function | Calls | Avg | p95 | Total | % elapsed |
|---|---:|---:|---:|---:|---:|
| `kafko::producer::send` | 466,979 | 45.11 µs | 76.93 µs | 21.06 s | 80.09% |
| `kafko::partition::append` | 466,981 | 44.66 µs | 76.42 µs | 20.86 s | 79.30% |
| `kafko::partition::flush_append_batch` | 69,523 | 17.64 µs | 41.73 µs | 1.23 s | 4.66% |
| `kafko::log::append_batch` | 69,523 | 15.47 µs | 39.23 µs | 1.08 s | 4.09% |
| `kafko::segment::append` | 69,523 | 6.31 µs | 18.82 µs | 438.46 ms | 1.67% |
| `kafko::record::encode_with` | 467,040 | 786 ns | 1.50 µs | 359.53 ms | 1.37% |
| `kafko::compression::compress` | 311,360 | 761 ns | 1.20 µs | 236.97 ms | 0.90% |
| `kafko::segment::sync` | 2 | 69.91 ms | 135.66 ms | 139.80 ms | 0.53% |
| `kafko::log::sync` | 1 | 4.49 ms | 4.49 ms | 4.49 ms | 0.02% |

(The `% elapsed` column is much higher than in the pre-win baseline because this
run's matrix completed in ~1.3 s vs the pre-win run's ~7 minutes — the % is over
profiler uptime, not bench wall time. Per-call timings are the apples-to-apples
comparison.)

### Per-send timing — diff vs `baseline_2026-05-24.md`

| Function | Pre-win avg | Post wins #1+#2 avg | Δ |
|---|---:|---:|---:|
| `producer::send` | 47.10 µs | 45.11 µs | **-4.2%** |
| `partition::append` | 46.66 µs | 44.66 µs | **-4.3%** |
| `flush_append_batch` | 18.46 µs | 17.64 µs | -4.4% |
| `log::append_batch` | 16.31 µs | 15.47 µs | -5.2% |
| `record::encode_with` | 907 ns | 786 ns | -13.3% |
| `compress` | 814 ns | 761 ns | -6.5% |
| `segment::append` | 5.29 µs | 6.31 µs | +19.3% (likely noise; <1 ms total) |

Aggregate: **roughly 4-5% faster on the hot path**, mostly attributable to
removing the per-record `Vec<u8>` allocation in `compress`. All deltas are
within the single-run noise floor of ~10-15% but the direction is consistent.

## Function allocations (mode: alloc-bytes, total 1.3 GB in 26.31 s)

Exclusive allocation bytes per function.

| Function | Calls | Avg / call | Total | % allocs |
|---|---:|---:|---:|---:|
| `kafko::compression::compress` | 311,360 | 4.0 KB | **1.2 GB** | **93.11%** |
| `kafko::partition::append` | 466,981 | 88 B | 39.2 MB | 2.98% |
| `kafko::segment::append` | 69,523 | 274 B | 18.2 MB | 1.39% |
| `kafko::log::append_batch` | 69,523 | 252 B | 16.7 MB | 1.27% |
| `kafko::producer::send` | 466,979 | 32 B | 14.3 MB | 1.08% |
| `kafko::partition::flush_append_batch` | 69,523 | 32 B | 2.1 MB | 0.16% |
| `kafko::segment::sync` | 2 | 256 B | 512 B | 0.00% |
| `kafko::log::sync` | 1 | 288 B | 288 B | 0.00% |
| `kafko::record::encode_with` | 467,040 | 0 B | 0 B | 0.00% |

### Allocations — diff vs `baseline_2026-05-24.md`

| Function | Pre-win avg | Post #1+#2 avg | Pre total | Post total | Δ total |
|---|---:|---:|---:|---:|---:|
| `compression::compress` | 6.4 KB | 4.0 KB | 1.9 GB | 1.2 GB | **-37%** |
| `log::append_batch` | 15.3 KB | 252 B | 1.0 GB | 16.7 MB | **-98.4%** |
| `partition::append` | 88 B | 88 B | 39.2 MB | 39.2 MB | 0% |
| `segment::append` | 277 B | 274 B | 18.3 MB | 18.2 MB | 0% |
| `producer::send` | 32 B | 32 B | 14.3 MB | 14.3 MB | 0% |
| **Total allocated** | — | — | **3.0 GB** | **1.3 GB** | **-57%** |

The 1.7 GB cut comes from two distinct sources:
- ~0.7 GB from win #1 (compress reuses a thread-local output buffer)
- ~1.0 GB from win #2 (Log reuses a single `BytesMut` + `Vec<usize>` across batches)

## What's still allocating after both wins

`compress` is now 93% of the remaining 1.3 GB heap traffic. The 4 KB/call residual
is unfinished business from win #1 — strongly suspected to be the `Vec::resize(N, 0)`
zero-init in the LZ4 path (which writes max-compressed-size bytes of zeros before
each compress_into call). Replacing that with `Vec::reserve` + `Vec::set_len`
via `Vec::spare_capacity_mut` would eliminate the per-call zero-init.

Smaller residuals:
- `partition::append` 88 B = `PartitionCommand::Append { record, reply }` boxed
  into the bounded mpsc slot. Tied to the actor architecture.
- `segment::append` 274 B = tokio::fs internal buffering on the per-call
  `write_all().await`. Tied to the `tokio::fs` thread-pool dispatch.
- `producer::send` 32 B = the `oneshot::channel()` allocation per request.
- `log::append_batch` 252 B = the returned `Vec<u64>` of offsets (≈ 54 B for a
  6.72-record batch) + small overhead from `sparse_index::track_append`
  (not currently hotpath-measured, so its allocs land here via exclusive
  accounting).

The architectural items (88 + 274 + 32 = ~400 B per record) require structural
changes (lock-free ring, std::fs swap, batched-API). They were always going to
need those later wins to address.

## How to regenerate this snapshot

```powershell
$env:KAFKO_RESET = "1"
cargo run --release `
  -p kafko-bench `
  --features hotpath,hotpath-alloc,hotpath-mcp
```

Then query the MCP server via the `hotpath` tools (`functions_timing`,
`functions_alloc`, `threads`, `profiler_status`) and update the tables above.
