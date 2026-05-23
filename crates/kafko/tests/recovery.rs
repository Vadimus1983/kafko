use bytes::Bytes;
use kafko::{Log, LogConfig, Record};
use std::io::SeekFrom;
use std::path::Path;
use tempfile::TempDir;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

fn record(marker: u64) -> Record {
    Record::new(
        marker as i64,
        Some(Bytes::from(format!("key-{marker}"))),
        Bytes::from(format!("value-{marker}")),
    )
}

async fn truncate_segment_file(dir: &Path, base_offset: u64, by_bytes: u64) {
    let path = dir.join(format!("{:020}.log", base_offset));
    let file = OpenOptions::new().write(true).open(&path).await.unwrap();
    let size = file.metadata().await.unwrap().len();
    file.set_len(size - by_bytes).await.unwrap();
}

async fn corrupt_byte_at(dir: &Path, base_offset: u64, file_pos: u64) {
    let path = dir.join(format!("{:020}.log", base_offset));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await
        .unwrap();
    file.seek(SeekFrom::Start(file_pos)).await.unwrap();
    let mut byte = [0u8];
    file.read_exact(&mut byte).await.unwrap();
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(file_pos)).await.unwrap();
    file.write_all(&byte).await.unwrap();
    file.sync_all().await.unwrap();
}

#[tokio::test]
async fn open_empty_dir_creates_fresh_log() {
    let dir = TempDir::new().unwrap();
    let log = Log::open(dir.path(), LogConfig::default()).await.unwrap();
    assert_eq!(log.next_offset(), 0);
    assert_eq!(log.segment_count(), 1);
    assert_eq!(log.total_size(), 0);
}

#[tokio::test]
async fn open_missing_dir_creates_fresh_log() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("nested/log");
    let log = Log::open(&nested, LogConfig::default()).await.unwrap();
    assert_eq!(log.next_offset(), 0);
}

#[tokio::test]
async fn open_clean_log_preserves_all_records() {
    let dir = TempDir::new().unwrap();
    let records: Vec<Record> = (0..5).map(record).collect();
    {
        let mut log = Log::create(dir.path(), LogConfig::default()).await.unwrap();
        for r in &records {
            log.append(r.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
    }

    let mut log = Log::open(dir.path(), LogConfig::default()).await.unwrap();
    assert_eq!(log.next_offset(), 5);
    assert_eq!(log.segment_count(), 1);
    for (i, expected) in records.into_iter().enumerate() {
        assert_eq!(log.read_record_at(i as u64).await.unwrap(), Some(expected));
    }
}

#[tokio::test]
async fn open_with_truncated_tail_recovers_to_last_valid_record() {
    let dir = TempDir::new().unwrap();
    let records: Vec<Record> = (0..5).map(record).collect();
    {
        let mut log = Log::create(dir.path(), LogConfig::default()).await.unwrap();
        for r in &records {
            log.append(r.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
    }

    truncate_segment_file(dir.path(), 0, 3).await;

    let mut log = Log::open(dir.path(), LogConfig::default()).await.unwrap();
    assert_eq!(log.next_offset(), 4);
    for i in 0..4 {
        assert!(log.read_record_at(i).await.unwrap().is_some());
    }
    assert_eq!(log.read_record_at(4).await.unwrap(), None);
}

#[tokio::test]
async fn open_with_crc_corruption_truncates_corrupted_tail() {
    let dir = TempDir::new().unwrap();
    let records: Vec<Record> = (0..5).map(record).collect();
    let log_path = dir.path().join("00000000000000000000.log");
    {
        let mut log = Log::create(dir.path(), LogConfig::default()).await.unwrap();
        for r in &records {
            log.append(r.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
    }

    let file_size = tokio::fs::metadata(&log_path).await.unwrap().len();
    corrupt_byte_at(dir.path(), 0, file_size - 1).await;

    let mut log = Log::open(dir.path(), LogConfig::default()).await.unwrap();
    assert_eq!(log.next_offset(), 4);
    for i in 0..4 {
        assert!(log.read_record_at(i).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn open_after_appends_can_resume_appending() {
    let dir = TempDir::new().unwrap();
    {
        let mut log = Log::create(dir.path(), LogConfig::default()).await.unwrap();
        log.append(record(0)).await.unwrap();
        log.append(record(1)).await.unwrap();
        log.sync().await.unwrap();
    }

    let mut log = Log::open(dir.path(), LogConfig::default()).await.unwrap();
    assert_eq!(log.next_offset(), 2);

    let new_offset = log.append(record(2)).await.unwrap();
    assert_eq!(new_offset, 2);
    assert_eq!(log.next_offset(), 3);

    assert_eq!(log.read_record_at(0).await.unwrap(), Some(record(0)));
    assert_eq!(log.read_record_at(1).await.unwrap(), Some(record(1)));
    assert_eq!(log.read_record_at(2).await.unwrap(), Some(record(2)));
}

#[tokio::test]
async fn open_multi_segment_log_preserves_older_segments() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 100,
        index_interval: 50,
        ..Default::default()
    };
    let records: Vec<Record> = (0..10).map(record).collect();
    {
        let mut log = Log::create(dir.path(), cfg).await.unwrap();
        for r in &records {
            log.append(r.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
    }

    let mut log = Log::open(dir.path(), cfg).await.unwrap();
    assert!(log.segment_count() > 1, "expected multiple segments");
    assert_eq!(log.next_offset(), 10);

    for (i, expected) in records.into_iter().enumerate() {
        assert_eq!(log.read_record_at(i as u64).await.unwrap(), Some(expected));
    }
}

#[tokio::test]
async fn recovery_is_idempotent() {
    let dir = TempDir::new().unwrap();
    {
        let mut log = Log::create(dir.path(), LogConfig::default()).await.unwrap();
        for i in 0..3 {
            log.append(record(i)).await.unwrap();
        }
        log.sync().await.unwrap();
    }

    truncate_segment_file(dir.path(), 0, 2).await;

    let next_offset_first;
    let total_size_first;
    {
        let log = Log::open(dir.path(), LogConfig::default()).await.unwrap();
        next_offset_first = log.next_offset();
        total_size_first = log.total_size();
    }
    {
        let log = Log::open(dir.path(), LogConfig::default()).await.unwrap();
        assert_eq!(log.next_offset(), next_offset_first);
        assert_eq!(log.total_size(), total_size_first);
    }
}

/// Recovery only re-verifies the active (last) segment, so an unflushed tail on
/// a sealed segment would be silently lost on a power-loss between rotation and
/// OS writeback. Every record, including those on segments that were sealed by
/// rotation, must be readable after reopening the log.
#[tokio::test]
async fn rotation_preserves_records_on_sealed_segments() {
    let dir = TempDir::new().unwrap();

    // Tight segment threshold so a handful of records forces several rotations.
    let cfg = LogConfig {
        segment_size_threshold: 256,
        ..LogConfig::default()
    };

    let record_count = 64u64;
    let total_expected: u64;
    {
        let mut log = Log::open(dir.path(), cfg).await.unwrap();
        for i in 0..record_count {
            log.append(record(i)).await.unwrap();
        }
        // We expect rotation to have happened multiple times.
        assert!(
            log.segment_count() > 1,
            "expected multiple segments to exercise rotation; got {}",
            log.segment_count()
        );
        total_expected = log.total_size();
    }

    let mut log = Log::open(dir.path(), cfg).await.unwrap();
    assert_eq!(log.next_offset(), record_count);
    assert_eq!(log.total_size(), total_expected);

    for i in 0..record_count {
        let r = log.read_record_at(i).await.unwrap().unwrap_or_else(|| {
            panic!("record at offset {i} missing after reopen — rotation lost data")
        });
        let expected_value = format!("value-{i}");
        assert_eq!(
            r.value().as_ref(),
            expected_value.as_bytes(),
            "record {i} value differs after reopen — corruption across rotation"
        );
    }
}
