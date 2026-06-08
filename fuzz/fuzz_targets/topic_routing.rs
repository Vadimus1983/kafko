#![no_main]

use arbitrary::Arbitrary;
use bytes::Bytes;
use kafko::{LogConfig, Producer, Record, Topic};
use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Builder;

// Model-based fuzzer over a multi-partition Topic's routing + storage. Generates
// an Arbitrary sequence of keyed/keyless sends against a Topic with an Arbitrary
// partition count and checks the routing + ordering invariants:
//   - every record routes to a partition < partition_count;
//   - a given key ALWAYS routes to the same partition (deterministic routing);
//   - offsets are sequential and monotonic WITHIN each partition;
//   - every acked (partition, offset) reads back with the same value, in append
//     order (so routing/rotation cannot lose or reorder a partition's records);
//   - no operation panics, regardless of input.

const MAX_OPS: usize = 64;
const MAX_VALUE_LEN: usize = 256;
const MAX_PARTITIONS: u32 = 16;

#[derive(Arbitrary, Debug)]
struct SendOp {
    // None = keyless (round-robin); Some(k) = keyed (must route deterministically).
    // A single u8 keeps the key space small so the fuzzer hits the same key
    // repeatedly and actually exercises routing stability.
    key: Option<u8>,
    len: u16,
}

#[derive(Arbitrary, Debug)]
struct FuzzCase {
    partitions: u8,
    ops: Vec<SendOp>,
}

fuzz_target!(|case: FuzzCase| {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let dir = TempDir::new().unwrap();

    let partition_count = (case.partitions as u32 % MAX_PARTITIONS) + 1;
    let topic = match rt.block_on(Topic::create(
        dir.path(),
        "t",
        partition_count,
        LogConfig::default(),
    )) {
        Ok(t) => Arc::new(t),
        Err(_) => return,
    };
    let producer = Producer::new(topic.clone());

    // key byte -> the partition it first routed to (must stay stable thereafter)
    let mut key_partition: HashMap<u8, u32> = HashMap::new();
    let mut next_offset: Vec<u64> = vec![0; partition_count as usize];
    let mut values: Vec<Vec<Vec<u8>>> = vec![Vec::new(); partition_count as usize];

    for op in case.ops.into_iter().take(MAX_OPS) {
        let len = (op.len as usize).min(MAX_VALUE_LEN);
        let value = vec![(len & 0xFF) as u8; len];
        let key = op.key.map(|k| Bytes::from(vec![k]));
        let record = Record::new(0, key, Bytes::from(value.clone()));

        let pos = match rt.block_on(producer.send_record(record)) {
            Ok(p) => p,
            // IO error is acceptable (the partition stays alive per the contract)
            // and consumes no offset; just skip bookkeeping for this op.
            Err(_) => continue,
        };

        let p = pos.partition();
        assert!(p < partition_count, "routed to out-of-range partition {p}");
        let pi = p as usize;

        if let Some(k) = op.key {
            let mapped = *key_partition.entry(k).or_insert(p);
            assert_eq!(mapped, p, "key {k} routed to two different partitions");
        }

        assert_eq!(
            pos.offset(),
            next_offset[pi],
            "non-sequential offset in partition {p}"
        );
        next_offset[pi] += 1;
        values[pi].push(value);
    }

    // Every acked (partition, offset) must read back with the same value, in order.
    for (pi, partition_values) in values.iter().enumerate() {
        let partition = topic.partition(pi as u32).expect("partition in range");
        for (offset, expected) in partition_values.iter().enumerate() {
            match rt.block_on(partition.read_record_at(offset as u64)) {
                Ok(Some(record)) => assert_eq!(
                    record.value().as_ref(),
                    expected.as_slice(),
                    "value mismatch at partition {pi} offset {offset}"
                ),
                Ok(None) => panic!("acked record missing at partition {pi} offset {offset}"),
                // Closed/IO on read is acceptable; the invariant is "no panic".
                Err(_) => {}
            }
        }
    }

    drop(producer);
    if let Ok(topic) = Arc::try_unwrap(topic) {
        rt.block_on(topic.shutdown());
    }
});
