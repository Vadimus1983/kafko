use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kafko::{Compression, LogConfig, Partition, Record};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

fn make_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

const BATCH_SIZES: [usize; 5] = [1, 8, 32, 128, 1024];
const VALUE_SIZE: usize = 256;
const KEY_LEN: usize = 16;

fn record_template() -> Record {
    Record::new(
        1_700_000_000_000,
        Some(Bytes::from(vec![0u8; KEY_LEN])),
        Bytes::from(vec![0u8; VALUE_SIZE]),
    )
}

fn make_batch(n: usize) -> Vec<Record> {
    (0..n).map(|_| record_template()).collect()
}

fn bench_send_batch_by_size(c: &mut Criterion, group_name: &str, compression: Compression) {
    let rt = make_runtime();
    let mut group = c.benchmark_group(group_name);

    for &batch_n in &BATCH_SIZES {
        let dir = TempDir::new().unwrap();
        let cfg = LogConfig {
            compression,
            ..Default::default()
        };
        let partition = rt
            .block_on(async { Partition::open(dir.path(), cfg).await })
            .unwrap();

        // Throughput in records/sec — easier to compare across batch sizes than
        // a per-iter latency. Each iter calls append_batch once.
        group.throughput(Throughput::Elements(batch_n as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_n),
            &batch_n,
            |b, &batch_n| {
                b.to_async(&rt).iter_batched(
                    || make_batch(batch_n),
                    |records| async { partition.append_batch(records).await.unwrap() },
                    BatchSize::SmallInput,
                );
            },
        );

        rt.block_on(partition.shutdown()).unwrap();
        drop(dir);
    }

    group.finish();
}

fn bench_send_batch_none(c: &mut Criterion) {
    bench_send_batch_by_size(c, "send_batch_none", Compression::None);
}

fn bench_send_batch_lz4(c: &mut Criterion) {
    #[cfg(feature = "compression-lz4")]
    bench_send_batch_by_size(c, "send_batch_lz4", Compression::Lz4);
    #[cfg(not(feature = "compression-lz4"))]
    let _ = c;
}

fn bench_send_batch_zstd(c: &mut Criterion) {
    #[cfg(feature = "compression-zstd")]
    bench_send_batch_by_size(c, "send_batch_zstd", Compression::Zstd);
    #[cfg(not(feature = "compression-zstd"))]
    let _ = c;
}

// For reference: an equivalent loop of N single appends at the same value size,
// so the published numbers carry the head-to-head batched-vs-looped comparison
// without needing a separate run.
fn bench_loop_of_single_appends(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("send_loop_none");

    for &batch_n in &BATCH_SIZES {
        let dir = TempDir::new().unwrap();
        let partition = rt
            .block_on(async { Partition::open(dir.path(), LogConfig::default()).await })
            .unwrap();

        group.throughput(Throughput::Elements(batch_n as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_n),
            &batch_n,
            |b, &batch_n| {
                b.to_async(&rt).iter_batched(
                    || make_batch(batch_n),
                    |records| async {
                        for r in records {
                            partition.append(r).await.unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        rt.block_on(partition.shutdown()).unwrap();
        drop(dir);
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_send_batch_none,
    bench_send_batch_lz4,
    bench_send_batch_zstd,
    bench_loop_of_single_appends,
);
criterion_main!(benches);
