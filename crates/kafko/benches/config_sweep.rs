use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kafko::{LogConfig, Partition, Record};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

fn make_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

const VALUE_SIZE: usize = 256;
const KEY_LEN: usize = 16;

fn record_template() -> Record {
    Record::new(
        1_700_000_000_000,
        Some(Bytes::from(vec![0u8; KEY_LEN])),
        Bytes::from(vec![0u8; VALUE_SIZE]),
    )
}

// How much does rotation overhead cost? Smaller threshold = more frequent rotation
// = more frequent active-segment fsync (commit 5ef7dc2 made rotation flush the
// soon-to-be-sealed segment + index before creating the new pair).
//
// The thresholds span four orders of magnitude. The smallest (64 KiB) forces a
// rotation roughly every 200 records at 256-byte values; the largest (256 MiB)
// rotates effectively never within a single bench run.
fn bench_segment_size_threshold(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("segment_size_threshold");

    let thresholds: [u64; 5] = [
        64 * 1024,             // 64 KiB
        1024 * 1024,           // 1 MiB
        16 * 1024 * 1024,      // 16 MiB
        64 * 1024 * 1024,      // 64 MiB
        256 * 1024 * 1024,     // 256 MiB
    ];

    for &threshold in &thresholds {
        let dir = TempDir::new().unwrap();
        let cfg = LogConfig {
            segment_size_threshold: threshold,
            ..Default::default()
        };
        let partition = rt
            .block_on(async { Partition::open(dir.path(), cfg).await })
            .unwrap();
        let template = record_template();

        group.throughput(Throughput::Elements(1));
        // Label the parameter as "64KiB", "1MiB", etc. for readable output.
        group.bench_with_input(
            BenchmarkId::from_parameter(format_bytes(threshold)),
            &template,
            |b, template| {
                b.to_async(&rt).iter_batched(
                    || template.clone(),
                    |r| async { partition.append(r).await.unwrap() },
                    BatchSize::SmallInput,
                );
            },
        );

        rt.block_on(partition.shutdown()).unwrap();
        drop(dir);
    }

    group.finish();
}

// Sparse index density. Smaller interval = more index entries written per record =
// more per-append work; larger interval = sparser index = longer linear scan inside
// the segment on reads. This bench measures the WRITE-side cost; reads are
// unaffected here because we never seek backward during the bench.
fn bench_index_interval(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("index_interval");

    let intervals: [u64; 5] = [
        512,         // very dense — entry every ~2 records at 256 B values
        4 * 1024,    // default
        32 * 1024,   // sparse
        256 * 1024,  // very sparse
        4 * 1024 * 1024, // effectively-no-index for typical workloads
    ];

    for &interval in &intervals {
        let dir = TempDir::new().unwrap();
        let cfg = LogConfig {
            index_interval: interval,
            ..Default::default()
        };
        let partition = rt
            .block_on(async { Partition::open(dir.path(), cfg).await })
            .unwrap();
        let template = record_template();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(format_bytes(interval)),
            &template,
            |b, template| {
                b.to_async(&rt).iter_batched(
                    || template.clone(),
                    |r| async { partition.append(r).await.unwrap() },
                    BatchSize::SmallInput,
                );
            },
        );

        rt.block_on(partition.shutdown()).unwrap();
        drop(dir);
    }

    group.finish();
}

// Same single-append loop benched at three contrasting LogConfig presets so the
// "what should I pick for max throughput" question gets a side-by-side answer in
// one run.
fn bench_preset_configs(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("preset_configs");

    let presets: [(&str, LogConfig); 3] = [
        (
            "default",
            LogConfig::default(),
        ),
        (
            "throughput_oriented",
            LogConfig {
                // Bigger segments = fewer rotation fsyncs; sparser index = less
                // per-append bookkeeping; bigger natural-batch ceiling = more
                // coalescing under concurrent producer load.
                segment_size_threshold: 256 * 1024 * 1024,
                index_interval: 32 * 1024,
                batch_max_bytes: 1024 * 1024,
                batch_max_records: 8192,
                ..Default::default()
            },
        ),
        (
            "small_footprint",
            LogConfig {
                // Tiny segments + dense index = lower latency on cold reads but
                // higher rotation overhead on the write path.
                segment_size_threshold: 1024 * 1024,
                index_interval: 1024,
                ..Default::default()
            },
        ),
    ];

    for (name, cfg) in presets {
        let dir = TempDir::new().unwrap();
        let partition = rt
            .block_on(async { Partition::open(dir.path(), cfg).await })
            .unwrap();
        let template = record_template();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &template,
            |b, template| {
                b.to_async(&rt).iter_batched(
                    || template.clone(),
                    |r| async { partition.append(r).await.unwrap() },
                    BatchSize::SmallInput,
                );
            },
        );

        rt.block_on(partition.shutdown()).unwrap();
        drop(dir);
    }

    group.finish();
}

fn format_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if n >= GIB && n.is_multiple_of(GIB) {
        format!("{}GiB", n / GIB)
    } else if n >= MIB && n.is_multiple_of(MIB) {
        format!("{}MiB", n / MIB)
    } else if n >= KIB && n.is_multiple_of(KIB) {
        format!("{}KiB", n / KIB)
    } else {
        format!("{n}B")
    }
}

criterion_group!(
    benches,
    bench_segment_size_threshold,
    bench_index_interval,
    bench_preset_configs,
);
criterion_main!(benches);
