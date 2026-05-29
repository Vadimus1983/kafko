# scripts/

Benchmark and load-test scripts for `kafko`. Nothing here is shipped to crates.io — these are reproducibility artifacts for the apples-to-apples comparison against Apache Kafka.

Run all scripts **from the project root** (e.g. `.\scripts\kafko_docker_bench.ps1`), not from inside `scripts/`. Paths inside the scripts assume the project root is the working directory.

## What lives here

| File | What it does | Output (in `scripts/tmp/`) |
|---|---|---|
| `Dockerfile` | Builds the `kafko-http:bench` image used by `kafko_docker_bench.ps1`. Multi-stage: Rust builder produces the `kafko-http` binary + installs `oha`, then a slim Debian runtime ships both. | — |
| `kafko_http_bench.ps1` | **Host-side** benchmark of `kafko-http`. Builds the binary locally, starts it, runs `oha` from the host against `127.0.0.1:9091`. | `kafko-http_bench_results_<ts>.txt` (+ `run_<ts>/` while running, then deleted) |
| `kafko_http_samply_bench.ps1` | **Profiling** run of `kafko-http`. Same shape as the host bench, but builds in DEBUG and wraps the binary in `samply record` so a CPU profile is captured while `oha` drives load. Smaller matrix to keep debug-mode runtime tolerable. Throughput is NOT comparable to the release bench. | `kafko-http_samply_results_<ts>.txt`, `kafko-http_samply_<ts>.profile.json` (open with `samply load <file>` or upload to <https://profiler.firefox.com/>) |
| `kafko_lib_samply_bench.ps1` | **Profiling** run of the kafko library directly, via the `kafko-bench` workspace binary. No HTTP, no axum, no oha. The resulting profile shows only kafko's storage hot path + tokio task scheduling, without HTTP machinery dominating the flame graph. Same matrix as the HTTP samply bench. | `kafko-lib_samply_results_<ts>.txt`, `kafko-lib_samply_<ts>.profile.json` |
| `kafko_docker_bench.ps1` | **Docker-side** benchmark of `kafko-http`. Builds the container, starts it, runs `oha` *inside* the container so the request path is container-loopback only (mirrors the Kafka bench shape). | `kafko_docker_bench_results_<ts>.txt` |
| `kafka_bench.ps1` | Baseline Kafka comparison: `apache/kafka:3.7.0` in KRaft mode, default settings. Runs `kafka-producer-perf-test.sh` inside the container. | `kafka_bench_results_<ts>.txt` |
| `kafka_bench_max.ps1` | Kafka tuned for maximum throughput — large socket buffers, 8 io/network threads, `linger.ms=50`, dynamic `batch.size` and `buffer.memory`, 1 GiB heap. Shows Kafka's natural batched throughput. | `kafka_bench_max_results_<ts>.txt` |
| `kafka_bench_unbatched.ps1` | **Apples-to-apples** Kafka bench: 16 concurrent producers with `linger.ms=0`, `batch.size=size+1024`, `max.in.flight.requests.per.connection=1`. Forces Kafka into the same one-record-per-request shape as `kafko-http`. | `kafka_bench_unbatched_results_<ts>.txt` |
| `kafka_bench.sh` | Bash port of `kafka_bench.ps1` for Linux / macOS / WSL. Same matrix, same output schema. | `kafka_bench_results_<ts>.txt` |
| `kafko_hotpath_matrix.ps1` | **In-process hotpath measurement matrix.** Builds `kafko-bench` with `hotpath hotpath-alloc compression-lz4` and runs each scenario (`sequential`, `concurrent`, `batch`, `lz4_sequential`) in its own process so per-function timing and allocation tables stay clean. Used to verify the LZ4 alloc-amortization fix in v0.2.0. | `hotpath_<ts>/<scenario>.txt` + `summary.txt` |
| `kafko_lib_multisize_bench.ps1` | **In-process throughput across record sizes.** Builds `kafko-bench` with `compression-all` and runs the `sequential` / `lz4_sequential` / `zstd_sequential` scenarios at six record sizes (64 B - 1 MiB) per codec, each in its own process with its own data dir. Drives the README's "Library hot path - records/sec (single send per record)" table. | `kafko_lib_multisize_<ts>/cell_<size>_<codec>.txt` + `results.txt` + `results.csv` |

`<ts>` is the script's start time as `YYYYMMDD-HHMMSS`, so re-runs never overwrite earlier results. The entire `scripts/tmp/` directory is gitignored, so nothing produced by a bench run ever lands in commits. Delete the whole folder whenever you want to clean up.

### Ephemeral run folder (host-side bench only)

`kafko_http_bench.ps1` creates a per-run subfolder for everything ephemeral and deletes it at exit (success or failure, via a `finally` cleanup):

```
scripts/tmp/run_<ts>/
  kafko-http_data/   server WAL + segments
  payloads/          oha payload .bin files (one per record size)
  server.log         kafko-http stdout
  server.err         kafko-http stderr
```

Only the results file at `scripts/tmp/kafko-http_bench_results_<ts>.txt` persists. This mirrors how the Docker-based scripts treat the container as the ephemeral run folder — both kinds of bench leave only the results behind.

The Docker scripts don't need a host-side run folder: the container *is* the run folder, and `Invoke-Cleanup` removes it (with all its `/data/kafko`, `/tmp/payload_*.bin`, etc.) at exit.

## Quick choices

- **Comparing kafko vs Kafka fairly** → run `kafko_docker_bench.ps1` + `kafka_bench_unbatched.ps1`. Both put server and client inside their own container; both use one record per network call. Numbers from these two files are directly comparable.
- **Comparing kafko vs production-tuned Kafka** → run `kafko_docker_bench.ps1` + `kafka_bench_max.ps1`. Kafka wins on small-record throughput because client-side batching is its native mode. This is the comparison Kafka's design assumes; kafko narrows the gap via `Producer::send_batch` (shipped in v0.1.1) for clients that can stage records.
- **Local kafko sanity check (no Docker)** → `kafko_http_bench.ps1` builds and runs everything on the host.
- **Find a kafko hot spot** → two flavours of profiling. Pick by what you want to see in the flame graph:
  - `kafko_lib_samply_bench.ps1` — **storage path only.** Runs `kafko-bench` (an in-process workload binary in the workspace), no HTTP / axum / oha. Best for tuning the partition writer, segment append, CRC, compression, fsync paths.
  - `kafko_http_samply_bench.ps1` — **full HTTP request path.** Includes axum routing, hyper, tokio I/O, and kafko storage. Best for finding hot spots that involve the HTTP server itself.

### The 128 KiB cell

Both `kafko_docker_bench.ps1` and `kafka_bench_unbatched.ps1` include `131072` (128 KiB) in their `$Sizes` matrices. The rationale:

Kafka's max-tuned producer accumulates records until it has ~128 KiB of payload, then sends one network call. At that record size, *every* Kafka call carries 128 KiB regardless of individual record size. The 128 KiB cell in the kafko bench gives kafko a 128 KiB payload per HTTP request — so both systems are doing one network call per equivalent payload.

What you learn from this cell:
- If kafko wins, the win comes from raw protocol+storage efficiency (axum HTTP < Kafka wire, kafko WAL < Kafka segment write), not from comparing apples to oranges
- It separates "kafko's per-request overhead is cheaper" (which the small-record cells already show) from "kafko's per-byte cost is competitive at packet sizes Kafka would naturally use"
- It's the cell where Kafka's batching advantage is structurally neutralized — Kafka's max-tuned `batch.size = max(128 KiB, recordSize × 2)` resolves to 256 KiB at 128 KiB records, which holds exactly one record per batch

## Why not use `oha` for the Kafka scripts too?

`oha` is an HTTP load generator. Apache Kafka does not speak HTTP — it has its own binary wire protocol over TCP/9092. You cannot point `oha` at a Kafka broker.

The Kafka scripts use `kafka-producer-perf-test.sh`, which is bundled inside the `apache/kafka:3.7.0` image. It is the *canonical* tool for benchmarking Kafka — written by the Kafka project, speaks the Kafka wire protocol natively, exposes the producer-config knobs we need (`linger.ms`, `batch.size`, `acks`, `max.in.flight.requests.per.connection`).

The comparison is still fair: both clients are the **native, canonical** load generator for their respective transport. What we *can* match — and what we *do* match in `kafka_bench_unbatched.ps1` — is the **workload semantics**:

| | `kafko_docker_bench.ps1` | `kafka_bench_unbatched.ps1` |
|---|---|---|
| One record per network call | yes (`oha` is strictly synchronous per connection) | yes (`linger.ms=0`, `batch.size=size+1024`, `max.in.flight=1`) |
| Concurrency | 16 | 16 (parallel `kafka-producer-perf-test.sh` processes) |
| Client lives in the same container as the server | yes | yes |
| Network | container loopback | container loopback |
| Durability | record in OS file before ack | `acks=1` (leader page cache before ack) |

The thing we *can't* match is the **client implementation**: `oha` is Rust + tokio, the Kafka client is Java. That's an unavoidable side-effect of measuring two different systems against each other. Forcing both to use a third tool (e.g., a REST proxy in front of Kafka) would add a hop to one side and make the comparison less fair, not more.

## Prerequisites

- **Docker Desktop** (steady tray icon = daemon up) for everything except `kafko_http_bench.ps1`
- **Rust toolchain** (`cargo`) for `kafko_http_bench.ps1`; the Docker bench builds Rust inside the image
- **`oha`** on the host only for `kafko_http_bench.ps1` and `kafko_http_samply_bench.ps1` (`cargo install oha`). The Docker bench installs `oha` inside the image.
- **`samply`** for `kafko_http_samply_bench.ps1` only (`cargo install samply`). On Windows it captures via ETW; no admin rights needed for user-space programs.
- **PowerShell 5.1+** (or PowerShell 7). Scripts are ASCII-only on purpose — PS 5.1 mis-decodes UTF-8-without-BOM as CP1252.

## Common knobs (edit at the top of each script)

```
$Sizes        # record sizes in bytes; default 64, 256, 512, 1024, 4096, 1048576
$Codecs       # 'none','lz4','zstd' for the kafko-http scripts
$Concurrency  # parallel connections (oha) or parallel producers (Kafka unbatched)
```

## Expected runtimes

| Script | Wall clock |
|---|---|
| `kafko_http_bench.ps1` | ~3 minutes |
| `kafko_docker_bench.ps1` | ~5-7 minutes (first run includes image build) |
| `kafka_bench.ps1` | ~15-20 minutes |
| `kafka_bench_max.ps1` | ~15-20 minutes |
| `kafka_bench_unbatched.ps1` | ~12-15 minutes |

## Output format

All scripts write a UTF-8 text file (no BOM) with the same general shape: header (date, config, methodology), then per-cell `=== size=… codec=… ===` blocks. They're meant to be diffed and copy-pasted into the root README's benchmark tables; no machine-readable format yet.

## Tearing down

Each Docker-based script cleans up its container on success and inside a `finally` block on failure. If a script is killed mid-run, run:

```powershell
docker rm -f kafka-bench kafka-bench-unbatched kafko-http-bench 2>$null
```

…to remove any orphans.
