use bytes::Bytes;
#[cfg(feature = "compression-lz4")]
use kafko::Compression;
use kafko::{Log, LogConfig, Record};
use tempfile::TempDir;

fn record(marker: u64) -> Record {
    Record::new(
        marker as i64,
        Some(Bytes::from(format!("key-{marker}"))),
        Bytes::from(format!("value-{marker}")),
    )
}

fn default_config() -> LogConfig {
    LogConfig::default()
}

#[tokio::test]
async fn create_starts_at_offset_zero() {
    let dir = TempDir::new().unwrap();
    let log = Log::create(dir.path(), default_config()).await.unwrap();
    assert_eq!(log.next_offset(), 0);
    assert_eq!(log.segment_count(), 1);
    assert_eq!(log.total_size(), 0);
    assert_eq!(log.dir(), dir.path());
}

#[tokio::test]
async fn append_assigns_sequential_offsets() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::create(dir.path(), default_config()).await.unwrap();

    assert_eq!(log.append(record(0)).await.unwrap(), 0);
    assert_eq!(log.append(record(1)).await.unwrap(), 1);
    assert_eq!(log.append(record(2)).await.unwrap(), 2);
    assert_eq!(log.next_offset(), 3);
}

#[tokio::test]
async fn read_back_appended_records() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::create(dir.path(), default_config()).await.unwrap();

    let records: Vec<Record> = (0..5).map(record).collect();
    for r in &records {
        log.append(r.clone()).await.unwrap();
    }
    log.sync().await.unwrap();

    for (i, expected) in records.into_iter().enumerate() {
        let actual = log.read_record_at(i as u64).await.unwrap();
        assert_eq!(actual, Some(expected));
    }
}

#[tokio::test]
async fn read_offset_past_end_returns_none() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::create(dir.path(), default_config()).await.unwrap();
    log.append(record(0)).await.unwrap();

    assert_eq!(log.read_record_at(100).await.unwrap(), None);
    assert_eq!(log.read_record_at(1).await.unwrap(), None);
}

#[tokio::test]
async fn rotation_creates_new_segment_when_threshold_crossed() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 200,
        index_interval: 100,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    for i in 0..10 {
        log.append(record(i)).await.unwrap();
    }

    assert!(
        log.segment_count() > 1,
        "expected rotation but only {} segment(s)",
        log.segment_count()
    );
    assert_eq!(log.next_offset(), 10);

    for i in 0..10 {
        let r = log.read_record_at(i).await.unwrap();
        assert!(r.is_some(), "missing record at offset {}", i);
    }
}

#[tokio::test]
async fn random_access_reads_across_many_segments() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 100,
        index_interval: 50,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    let records: Vec<Record> = (0..20).map(record).collect();
    for r in &records {
        log.append(r.clone()).await.unwrap();
    }
    log.sync().await.unwrap();

    assert!(log.segment_count() > 5, "expected many segments");

    for i in [0u64, 5, 10, 15, 19] {
        let r = log.read_record_at(i).await.unwrap();
        assert_eq!(r, Some(records[i as usize].clone()), "offset {i}");
    }
}

#[tokio::test]
async fn total_size_grows_with_appends() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::create(dir.path(), default_config()).await.unwrap();

    assert_eq!(log.total_size(), 0);

    log.append(record(0)).await.unwrap();
    let size_after_one = log.total_size();
    assert!(size_after_one > 0);

    log.append(record(1)).await.unwrap();
    assert!(log.total_size() > size_after_one);
}

#[tokio::test]
async fn config_default_is_one_gib_and_four_kib() {
    let cfg = LogConfig::default();
    assert_eq!(cfg.segment_size_threshold, 1 << 30);
    assert_eq!(cfg.index_interval, 4096);
}

#[tokio::test]
async fn create_fails_on_existing_log_dir() {
    let dir = TempDir::new().unwrap();
    let _log = Log::create(dir.path(), default_config()).await.unwrap();
    let result = Log::create(dir.path(), default_config()).await;
    assert!(
        result.is_err(),
        "expected error when creating Log over existing segments"
    );
}

#[tokio::test]
async fn append_batch_assigns_sequential_offsets() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::create(dir.path(), default_config()).await.unwrap();

    let records: Vec<Record> = (0..10).map(record).collect();
    let offsets = log.append_batch(records.clone()).await.unwrap();
    assert_eq!(offsets, (0..10).collect::<Vec<u64>>());
    assert_eq!(log.next_offset(), 10);

    for (i, expected) in records.into_iter().enumerate() {
        let actual = log.read_record_at(i as u64).await.unwrap();
        assert_eq!(actual, Some(expected));
    }
}

#[tokio::test]
async fn append_batch_empty_returns_empty() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::create(dir.path(), default_config()).await.unwrap();
    let offsets = log.append_batch(Vec::new()).await.unwrap();
    assert!(offsets.is_empty());
    assert_eq!(log.next_offset(), 0);
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn log_with_lz4_compression_roundtrips() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        compression: Compression::Lz4,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    let payload = Bytes::from(vec![0xAAu8; 4096]);
    let r = Record::new(123, None, payload.clone());
    let expected = r.clone();
    log.append(r).await.unwrap();
    log.sync().await.unwrap();

    let read_back = log.read_record_at(0).await.unwrap();
    assert_eq!(read_back, Some(expected));
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn log_with_lz4_uses_fewer_bytes_on_disk() {
    let base = TempDir::new().unwrap();
    let uncompressed_dir = base.path().join("uncompressed");
    let compressed_dir = base.path().join("compressed");
    let payload = Bytes::from(vec![0xCDu8; 4096]);

    let mut log_u = Log::create(&uncompressed_dir, LogConfig::default()).await.unwrap();
    for _ in 0..10 {
        log_u
            .append(Record::new(0, None, payload.clone()))
            .await
            .unwrap();
    }

    let cfg_c = LogConfig {
        compression: Compression::Lz4,
        ..Default::default()
    };
    let mut log_c = Log::create(&compressed_dir, cfg_c).await.unwrap();
    for _ in 0..10 {
        log_c
            .append(Record::new(0, None, payload.clone()))
            .await
            .unwrap();
    }

    assert!(
        log_c.total_size() * 4 < log_u.total_size(),
        "expected compressed (<{}) to be << uncompressed ({})",
        log_c.total_size(),
        log_u.total_size()
    );
}

#[tokio::test]
async fn single_record_larger_than_threshold_on_empty_log_succeeds() {
    // Regression for the v0.1.0 bug where Segment::would_overflow returned true
    // for an empty segment, causing rotation to try to create a duplicate segment
    // at base offset 0 and fail with AlreadyExists. The empty active segment must
    // always accept the first write, even if the record exceeds threshold.
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 128,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    // A single record whose wire form clearly exceeds 128 bytes.
    let r = Record::new(0, None, Bytes::from(vec![0u8; 1024]));
    let offset = log.append(r.clone()).await.unwrap();
    assert_eq!(offset, 0);
    assert_eq!(log.segment_count(), 1, "no premature rotation on empty segment");
    let read_back = log.read_record_at(0).await.unwrap();
    assert_eq!(read_back, Some(r));
}

#[tokio::test]
async fn batch_larger_than_threshold_on_empty_log_succeeds() {
    // Same defect class as the single-record case, exercised through append_batch.
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 128,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    let records: Vec<Record> = (0..32).map(record).collect();
    let offsets = log.append_batch(records.clone()).await.unwrap();
    assert_eq!(offsets, (0..32).collect::<Vec<u64>>());
    assert_eq!(log.segment_count(), 1);

    for (i, expected) in records.into_iter().enumerate() {
        assert_eq!(log.read_record_at(i as u64).await.unwrap(), Some(expected));
    }
}

#[tokio::test]
async fn append_after_oversized_first_write_rotates_normally() {
    // Once a segment has content, the soft cap applies normally: the next write
    // that would push it past threshold triggers rotation.
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 128,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    // First write is oversized → stays in segment 0.
    log.append(Record::new(0, None, Bytes::from(vec![0xAAu8; 1024])))
        .await
        .unwrap();
    assert_eq!(log.segment_count(), 1);

    // Any subsequent append rotates because the segment is now full.
    log.append(Record::new(1, None, Bytes::from_static(b"small")))
        .await
        .unwrap();
    assert!(
        log.segment_count() >= 2,
        "expected rotation after oversized seg-0 + new append, got {} segment(s)",
        log.segment_count()
    );

    // Both records are still readable.
    assert!(log.read_record_at(0).await.unwrap().is_some());
    assert!(log.read_record_at(1).await.unwrap().is_some());
}

#[tokio::test]
async fn mid_life_rotation_mix_preserves_ordering() {
    // Small + small + huge + small + small mix, all readable in order.
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 256,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    let records = vec![
        Record::new(0, None, Bytes::from_static(b"small-0")),
        Record::new(1, None, Bytes::from_static(b"small-1")),
        Record::new(2, None, Bytes::from(vec![0u8; 4096])), // oversized
        Record::new(3, None, Bytes::from_static(b"small-3")),
        Record::new(4, None, Bytes::from_static(b"small-4")),
    ];
    for r in &records {
        log.append(r.clone()).await.unwrap();
    }
    log.sync().await.unwrap();

    for (i, expected) in records.iter().enumerate() {
        let actual = log.read_record_at(i as u64).await.unwrap();
        assert_eq!(actual.as_ref(), Some(expected), "mismatch at offset {i}");
    }
    assert_eq!(log.next_offset(), 5);
}

#[tokio::test]
async fn append_at_exact_threshold_boundary_does_not_double_rotate() {
    // After fill-to-threshold, the next append triggers exactly one rotation —
    // it should not create extra empty segments.
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 200,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    // Fill close to threshold with small records.
    for i in 0..5 {
        log.append(record(i)).await.unwrap();
    }
    let before = log.segment_count();

    // One more append must rotate at most once (segment_count grows by 1 or stays
    // the same depending on where exactly we crossed the threshold).
    log.append(record(99)).await.unwrap();
    let after = log.segment_count();
    assert!(
        after - before <= 1,
        "expected at most one rotation, segment_count went {before} -> {after}"
    );
}

#[tokio::test]
async fn append_batch_continues_offsets_after_individual_appends() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::create(dir.path(), default_config()).await.unwrap();

    log.append(record(0)).await.unwrap();
    log.append(record(1)).await.unwrap();

    let batch: Vec<Record> = (2..5).map(record).collect();
    let offsets = log.append_batch(batch).await.unwrap();
    assert_eq!(offsets, vec![2, 3, 4]);
    assert_eq!(log.next_offset(), 5);

    for i in 0..5 {
        assert!(log.read_record_at(i).await.unwrap().is_some());
    }
}
