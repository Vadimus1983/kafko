use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(any(feature = "compression-lz4", feature = "compression-zstd"))]
use kafko::Compression;
use kafko::{Consumer, LogConfig, Partition, Producer, Record, Topic};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;

fn make_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

const SIZES: [usize; 6] = [64, 256, 512, 1024, 4096, 1_048_576];
const KEY_LEN: usize = 16;
const CONCURRENT_TASKS: usize = 16;

fn record_with_value_size(value_size: usize) -> Record {
    Record::new(
        1_700_000_000_000,
        Some(Bytes::from(vec![0u8; KEY_LEN])),
        Bytes::from(vec![0u8; value_size]),
    )
}

fn bench_oneshot_overhead(c: &mut Criterion) {
    let rt = make_runtime();
    c.bench_function("oneshot_roundtrip", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, rx) = oneshot::channel::<u64>();
            tx.send(42).unwrap();
            rx.await.unwrap()
        });
    });
}

fn bench_append_single(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("partition_append_single");

    for value_size in SIZES {
        let dir = TempDir::new().unwrap();
        let partition = rt
            .block_on(async { Partition::open(dir.path(), LogConfig::default()).await })
            .unwrap();
        let template = record_with_value_size(value_size);
        let wire_size = template.wire_size();

        group.throughput(Throughput::Bytes(wire_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(value_size),
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

#[cfg(feature = "compression-lz4")]
fn bench_append_single_lz4_inner(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("partition_append_single_lz4");

    for value_size in SIZES {
        let dir = TempDir::new().unwrap();
        let cfg = LogConfig {
            compression: Compression::Lz4,
            ..Default::default()
        };
        let partition = rt
            .block_on(async { Partition::open(dir.path(), cfg).await })
            .unwrap();
        let template = record_with_value_size(value_size);
        let wire_size = template.wire_size();

        group.throughput(Throughput::Bytes(wire_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(value_size),
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

fn bench_append_single_lz4(c: &mut Criterion) {
    #[cfg(feature = "compression-lz4")]
    bench_append_single_lz4_inner(c);
    #[cfg(not(feature = "compression-lz4"))]
    let _ = c;
}

#[cfg(feature = "compression-zstd")]
fn bench_append_single_zstd_inner(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("partition_append_single_zstd");

    for value_size in SIZES {
        let dir = TempDir::new().unwrap();
        let cfg = LogConfig {
            compression: Compression::Zstd,
            ..Default::default()
        };
        let partition = rt
            .block_on(async { Partition::open(dir.path(), cfg).await })
            .unwrap();
        let template = record_with_value_size(value_size);
        let wire_size = template.wire_size();

        group.throughput(Throughput::Bytes(wire_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(value_size),
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

fn bench_append_single_zstd(c: &mut Criterion) {
    #[cfg(feature = "compression-zstd")]
    bench_append_single_zstd_inner(c);
    #[cfg(not(feature = "compression-zstd"))]
    let _ = c;
}

fn bench_append_concurrent(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("partition_append_concurrent");

    let dir = TempDir::new().unwrap();
    let partition = rt.block_on(async {
        Arc::new(
            Partition::open(dir.path(), LogConfig::default())
                .await
                .unwrap(),
        )
    });
    let template = record_with_value_size(256);

    group.throughput(Throughput::Elements(CONCURRENT_TASKS as u64));
    group.bench_function(BenchmarkId::from_parameter(CONCURRENT_TASKS), |b| {
        b.to_async(&rt).iter(|| {
            let partition = partition.clone();
            let template = template.clone();
            async move {
                let mut handles = Vec::with_capacity(CONCURRENT_TASKS);
                for _ in 0..CONCURRENT_TASKS {
                    let p = partition.clone();
                    let r = template.clone();
                    handles.push(tokio::spawn(async move { p.append(r).await.unwrap() }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            }
        });
    });

    group.finish();

    let partition = Arc::try_unwrap(partition)
        .map_err(|_| "Arc still shared after benchmark loop")
        .unwrap();
    rt.block_on(partition.shutdown()).unwrap();
    drop(dir);
}

// End-to-end produce-to-consume latency under a tight write→read alternation loop.
fn bench_produce_to_consume(c: &mut Criterion) {
    let rt = make_runtime();
    let dir = TempDir::new().unwrap();
    let topic = Arc::new(rt.block_on(async {
        Topic::create(dir.path(), "bench", 1, LogConfig::default())
            .await
            .unwrap()
    }));
    let size = SIZES[0];
    let producer = Producer::new(topic.clone());
    let template = record_with_value_size(size);
    let next_offset = Arc::new(std::sync::atomic::AtomicU64::new(0));

    c.bench_function(
        format!("produce_to_consume_latency_{}", size).as_str(),
        |b| {
            b.to_async(&rt).iter(|| {
                let topic = topic.clone();
                let producer = producer.clone();
                let template = template.clone();
                let next_offset = next_offset.clone();
                async move {
                    let offset = next_offset.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    producer.send_record(template).await.unwrap();
                    let mut consumer = Consumer::from_topic_at(topic, offset);
                    consumer.next_record().await.unwrap()
                }
            });
        },
    );

    drop(producer);
    drop(topic);
    drop(dir);
}

criterion_group!(
    benches,
    bench_oneshot_overhead,
    bench_append_single,
    bench_append_single_lz4,
    bench_append_single_zstd,
    bench_append_concurrent,
    bench_produce_to_consume,
);
criterion_main!(benches);
