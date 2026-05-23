use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kafko::Record;

const SIZES: [usize; 3] = [64, 256, 4096];
const KEY_LEN: usize = 16;

fn record_with_value_size(value_size: usize) -> Record {
    Record::new(
        1_700_000_000_000,
        Some(Bytes::from(vec![0u8; KEY_LEN])),
        Bytes::from(vec![0u8; value_size]),
    )
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    for value_size in SIZES {
        let template = record_with_value_size(value_size);
        let wire_size = template.wire_size();

        group.throughput(Throughput::Bytes(wire_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(value_size),
            &template,
            |b, template| {
                b.iter_batched(
                    || (template.clone(), BytesMut::with_capacity(wire_size)),
                    |(record, mut buf)| {
                        record.encode(&mut buf);
                        buf
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    for value_size in SIZES {
        let record = record_with_value_size(value_size);
        let wire_size = record.wire_size();

        let mut buf = BytesMut::with_capacity(wire_size);
        record.encode(&mut buf);
        let encoded = buf.freeze();

        group.throughput(Throughput::Bytes(wire_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(value_size),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let mut slice: &[u8] = encoded;
                    Record::decode(&mut slice).expect("decode")
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
