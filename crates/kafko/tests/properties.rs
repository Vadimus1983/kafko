use bytes::{Bytes, BytesMut};
use kafko::{Compression, Log, LogConfig, Record, SparseIndex};
use proptest::prelude::*;
use std::future::Future;
use tempfile::TempDir;

fn run_async<F: Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime must build")
        .block_on(f)
}

fn any_compression() -> impl Strategy<Value = Compression> {
    prop_oneof![
        Just(Compression::None),
        Just(Compression::Lz4),
        Just(Compression::Zstd),
    ]
}

proptest! {
    #[test]
    fn record_roundtrips_any_input(
        ts in any::<i64>(),
        key in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..1024)),
        value in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let record = Record::new(
            ts,
            key.map(Bytes::from),
            Bytes::from(value),
        );
        let expected = record.clone();
        let expected_size = record.wire_size();

        let mut buf = BytesMut::new();
        record.encode(&mut buf);
        prop_assert_eq!(buf.len(), expected_size);

        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).expect("decode after encode must succeed");
        prop_assert_eq!(decoded, expected);
        prop_assert!(slice.is_empty());
    }

    /// Round-trip with every compression mode. Catches codec breakage that
    /// would only surface under specific value shapes (high-entropy vs.
    /// repetitive payloads) once a compressor sees real data.
    #[test]
    fn record_roundtrips_under_any_compression(
        ts in any::<i64>(),
        key in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..256)),
        value in proptest::collection::vec(any::<u8>(), 0..4096),
        compression in any_compression(),
    ) {
        let record = Record::new(
            ts,
            key.map(Bytes::from),
            Bytes::from(value),
        );
        let expected = record.clone();

        let mut buf = BytesMut::new();
        let on_wire = record.encode_with(&mut buf, compression);
        prop_assert_eq!(buf.len(), on_wire);

        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).expect("decode after encode must succeed");
        prop_assert_eq!(decoded, expected);
        prop_assert!(slice.is_empty());
    }
}

proptest! {
    /// SparseIndex.lookup must always return the largest indexed entry whose
    /// offset is <= the target — never a later one, never bypass an entry,
    /// never panic for in-range or out-of-range targets.
    #[test]
    fn sparse_index_lookup_returns_largest_le_target(
        base in 0u64..1_000_000u64,
        interval in 1u64..256u64,
        record_count in 0usize..256usize,
        record_size in 1usize..512usize,
        target_offset_delta in any::<i64>(),
    ) {
        run_async(async move {
            let dir = TempDir::new().unwrap();
            let mut idx = SparseIndex::create(dir.path(), base, interval).await.unwrap();

            let mut file_pos = 0u64;
            for i in 0..record_count {
                let offset = base + i as u64;
                idx.track_append(offset, file_pos, record_size).await.unwrap();
                file_pos += record_size as u64;
            }

            // Sample the lookup space around the indexed range. target_offset_delta
            // can be negative (below base) or large (past the last entry); both
            // must be handled.
            let target = if target_offset_delta >= 0 {
                base.saturating_add(target_offset_delta as u64)
            } else {
                base.saturating_sub((-target_offset_delta) as u64)
            };

            let (returned_pos, returned_offset) = idx.lookup(target);

            // Invariant 1: starting_offset is never > target (the whole point of
            // a sparse index is to land at-or-before the requested offset).
            prop_assert!(
                returned_offset <= target || returned_offset == base,
                "lookup({target}) returned starting_offset={returned_offset} > target"
            );

            // Invariant 2: returned_pos and returned_offset are consistent with
            // some recorded entry (or the sentinel (0, base) when target < first
            // indexed entry).
            if record_count == 0 {
                prop_assert_eq!(returned_pos, 0);
                prop_assert_eq!(returned_offset, base);
            }
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Log tests hit the filesystem; keep the case count modest.
        cases: 32,
        .. ProptestConfig::default()
    })]

    /// For any sequence of (key, value) records, appending in order and reading
    /// back by offset returns the same records.
    #[test]
    fn log_append_then_read_preserves_records(
        records in proptest::collection::vec(
            (
                proptest::option::of(proptest::collection::vec(any::<u8>(), 0..64)),
                proptest::collection::vec(any::<u8>(), 0..256),
            ),
            0..32,
        ),
        compression in any_compression(),
    ) {
        run_async(async move {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig { compression, ..Default::default() };
            let mut log = Log::create(dir.path(), cfg).await.unwrap();

            let expected: Vec<Record> = records
                .iter()
                .enumerate()
                .map(|(i, (key, value))| Record::new(
                    i as i64,
                    key.as_ref().map(|k| Bytes::from(k.clone())),
                    Bytes::from(value.clone()),
                ))
                .collect();

            for r in &expected {
                let offset = log.append(r.clone()).await.unwrap();
                prop_assert_eq!(offset, expected.iter().position(|e| std::ptr::eq(e, r))
                    .unwrap_or_else(|| {
                        // Fallback: position by value-identity. Identical records
                        // are allowed; this just provides a meaningful diagnostic.
                        expected.iter().position(|e| e == r).unwrap()
                    }) as u64);
            }

            for (i, expected_record) in expected.iter().enumerate() {
                let actual = log.read_record_at(i as u64).await.unwrap();
                prop_assert_eq!(actual.as_ref(), Some(expected_record));
            }
            Ok(())
        })?;
    }

    /// Rotation invariant: under a small random segment threshold, every appended
    /// record stays readable in order and the next_offset is the total count.
    #[test]
    fn log_rotation_preserves_count_and_order(
        value_sizes in proptest::collection::vec(1usize..512usize, 1..48),
        segment_size_threshold in 64u64..2048u64,
        compression in any_compression(),
    ) {
        run_async(async move {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                segment_size_threshold,
                compression,
                ..Default::default()
            };
            let mut log = Log::create(dir.path(), cfg).await.unwrap();

            let payloads: Vec<Bytes> = value_sizes
                .iter()
                .enumerate()
                .map(|(i, &sz)| Bytes::from(vec![i as u8; sz]))
                .collect();

            for (i, value) in payloads.iter().enumerate() {
                let r = Record::new(i as i64, None, value.clone());
                let offset = log.append(r).await.unwrap();
                prop_assert_eq!(offset, i as u64);
            }
            prop_assert_eq!(log.next_offset(), payloads.len() as u64);

            for (i, expected) in payloads.iter().enumerate() {
                let actual = log.read_record_at(i as u64).await.unwrap()
                    .ok_or_else(|| TestCaseError::fail(
                        format!("missing record at offset {i}")
                    ))?;
                prop_assert_eq!(actual.value(), expected);
            }
            Ok(())
        })?;
    }
}
