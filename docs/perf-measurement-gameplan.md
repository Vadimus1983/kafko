# kafko perf measurement gameplan

How to collect the data needed to decide the next throughput-optimization
target (BufWriter / lz4_flex alloc fix / multi-partition / io_uring / etc.)
without guessing. Three scenarios, one process per scenario, hotpath produces
per-function timing + allocation tables at exit.

## Why three scenarios

Each scenario isolates one variable while holding everything else constant
(256 B value, no key, `Compression::None`, default `LogConfig`):

| Scenario | What varies | What it answers |
|---|---|---|
| `sequential` | 1 task, 100 000 `producer.send()` in a tight loop | **Gap 1.** Where does the per-send cost go? mpsc-send vs writer-loop vs write() syscall vs index update. |
| `concurrent` | 16 long-lived tasks each doing 6 250 sends | **Gap 2.** Does natural batching coalesce contention into bigger flushes, or does contention dominate? Compare `flush_append_batch` mean per call (sequential) vs per call (concurrent). |
| `batch` | 1 task, ~97 calls of `send_batch(1024)` | **Reference ceiling.** Shows what kafko can do when mpsc round-trip is fully amortized. The cost floor that no further optimization can touch. |

Each scenario uses its own process so hotpath's counters reflect only that
access pattern — they're not reset between scenarios within a process.

## How to run

```powershell
.\scripts\kafko_hotpath_matrix.ps1
```

Output lands under `scripts/tmp/hotpath_<timestamp>/`:

- `sequential.txt`, `concurrent.txt`, `batch.txt` — full hotpath tables
- `summary.txt` — one-line throughput per scenario

## How to read the timing table

Look for these specific function rows in each scenario's `timing` table:

| Function | What it measures | Why it matters |
|---|---|---|
| `producer::send` | Full outer call including timestamp + Record construction | Overhead vs `partition::append` = `Producer` layer cost |
| `partition::append` | mpsc-send + writer work + oneshot reply | The full per-send latency the caller observes |
| `flush_append_batch` | Writer-side time only (one call per natural batch) | Subtracting from `partition::append` mean gives **mpsc+oneshot round-trip cost** |
| `log::append_batch` | Storage-layer time only | Subtracting from `flush_append_batch` gives writer-task dispatch overhead |
| `segment::append` | The `write()` syscall + cursor management | The actual disk I/O cost |
| `sparse_index::track_append` | Index update (occasional disk write) | Should be small; non-trivial only when index entries written |
| `record::encode_with` | Codec + CRC + buffer fill | Should be small at 256 B; grows with payload |
| `compression::compress` | Per-codec compression call | N/A for `Compression::None`; non-zero only for LZ4/Zstd scenarios |

### Smoke-run reference numbers (2026-05-26, Windows, release build)

From the initial smoke run (sequential, 256 B, no compression):

```
partition::append              5.43 µs   (full path)
  flush_append_batch           3.83 µs   (writer-side)
    log::append_batch          3.12 µs
      segment::append          2.23 µs   <-- single largest line item (41%)
      sparse_index::track_append  302 ns
      record::encode_with        114 ns
  delta (mpsc+oneshot)          ~1.60 µs (29%)
```

This is the breakdown that answers Gap 1. The `write()` syscall is the
largest single cost, but the mpsc+oneshot round-trip is comparable. Both are
addressable; neither dominates.

## Decision rule

After running the matrix, pick the next optimization based on:

| If the table shows... | The next win is... |
|---|---|
| `segment::append` >= 30% of `partition::append` total | **BufWriter / write coalescing.** Buffering across appends amortizes the write() syscall to near-zero per record. |
| `partition::append` - `flush_append_batch` >= 25% of `partition::append` | **LMAX-style ring or batched producer API.** The mpsc+oneshot round-trip is meaningful. `send_batch` already addresses this; expanding its use upstream may suffice. |
| `compression::compress` alloc bytes dominates the alloc table on the LZ4 scenario | **lz4_flex alloc fix.** Documented in `project_lz4_flex_alloc.md`; either vendor a slim path or swap crates. |
| `partition::append` total time is small but throughput is low | **Multi-partition.** You're CPU-bound at a single writer task; horizontal scaling. |
| `record::encode_with` mean >= 1 µs | **Encode-path optimization.** Inline payload, `IoSlice` writev, reduce memcpy. |
| `sparse_index::track_append` total >= 5% | **Sparse-index batching.** Buffer index entries and write once per batch. |

Two or more of these can be true at once. Pick the largest %, ship it, re-measure.

## Why io_uring is not on this decision list

The matrix doesn't surface a signal io_uring would address until *after*
BufWriter has reduced syscall count to near-zero per record. With BufWriter,
the question becomes "how cheap is the *one* `write()` per N records?" — and
that's still ~200 ns in the page-cache case. io_uring saves at most that ~200
ns per batch syscall, on Linux only. The matrix should be re-run after
BufWriter lands to see whether io_uring then has a measurable target.

## Gap 3 (HTTP path) — separate harness

Library-internal numbers (this matrix) don't capture axum / hyper / TCP
overhead. For end-to-end HTTP measurement use the existing samply script:

```powershell
.\scripts\kafko_http_samply_bench.ps1
```

That produces a samply flame graph showing where time goes across the full
HTTP request path. The kafko library functions will appear in that profile
already-symbolicated; compare their relative shape to the per-function table
this matrix produces to decide whether the HTTP layer is masking or amplifying
library costs.

## Files involved

- `scripts/kafko_hotpath_matrix.ps1` — the runner, one process per scenario
- `crates/kafko-bench/src/main.rs` — scenario implementations
- `crates/kafko/src/{record,segment,log,partition,producer,sparse_index,compression}.rs` — `#[cfg_attr(feature = "hotpath", hotpath::measure)]` annotations on the measured functions
- `crates/kafko/Cargo.toml` — `hotpath` feature (off by default; published crate is unaffected)
- `crates/kafko-bench/Cargo.toml` — feature passthroughs (`hotpath`, `hotpath-alloc`, `hotpath-mcp`)
