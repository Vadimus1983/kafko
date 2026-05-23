use bytes::Bytes;
use kafko::{Log, LogConfig, Partition, Producer, Record};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn record(marker: u64) -> Record {
    Record::new(
        marker as i64,
        Some(Bytes::from(format!("k-{marker}"))),
        Bytes::from(format!("v-{marker}")),
    )
}

#[tokio::test]
async fn apply_retention_without_config_is_noop() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 100,
        index_interval: 50,
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    for i in 0..10 {
        log.append(record(i)).await.unwrap();
    }
    let initial_count = log.segment_count();
    let initial_size = log.total_size();

    let deleted = log.apply_retention().await.unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(log.segment_count(), initial_count);
    assert_eq!(log.total_size(), initial_size);
}

#[tokio::test]
async fn apply_retention_never_deletes_active_segment() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        max_segment_age: Some(Duration::from_millis(0)),
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();
    log.append(record(0)).await.unwrap();
    log.sync().await.unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;

    let deleted = log.apply_retention().await.unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(log.segment_count(), 1);
}

#[tokio::test]
async fn apply_retention_deletes_old_segments_by_age() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 100,
        index_interval: 50,
        max_segment_age: Some(Duration::from_millis(0)),
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    for i in 0..10 {
        log.append(record(i)).await.unwrap();
    }
    log.sync().await.unwrap();
    let initial_count = log.segment_count();
    assert!(
        initial_count > 1,
        "expected multiple segments after rotation"
    );

    tokio::time::sleep(Duration::from_millis(30)).await;

    let deleted = log.apply_retention().await.unwrap();
    assert!(deleted > 0);
    assert_eq!(log.segment_count(), 1, "only active should remain");
}

#[tokio::test]
async fn apply_retention_by_size_deletes_until_under_limit() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 100,
        index_interval: 50,
        max_partition_bytes: Some(500),
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    for i in 0..20 {
        log.append(record(i)).await.unwrap();
    }
    log.sync().await.unwrap();

    let _ = log.apply_retention().await.unwrap();

    assert!(
        log.total_size() <= 500 || log.segment_count() == 1,
        "expected total_size <= 500 or only active segment, got size={} count={}",
        log.total_size(),
        log.segment_count()
    );
}

#[tokio::test]
async fn apply_retention_deletes_index_files_alongside_log_files() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 100,
        index_interval: 50,
        max_segment_age: Some(Duration::from_millis(0)),
        ..Default::default()
    };
    let mut log = Log::create(dir.path(), cfg).await.unwrap();

    for i in 0..10 {
        log.append(record(i)).await.unwrap();
    }
    log.sync().await.unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;
    log.apply_retention().await.unwrap();

    let log_files: usize = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "log"))
        .count();
    let index_files: usize = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "index"))
        .count();

    assert_eq!(log_files, 1);
    assert_eq!(index_files, 1);
}

#[tokio::test]
async fn partition_runs_retention_periodically() {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size_threshold: 100,
        index_interval: 50,
        max_segment_age: Some(Duration::from_millis(0)),
        retention_check_interval: Duration::from_millis(50),
        ..Default::default()
    };
    let partition = Arc::new(Partition::open(dir.path(), cfg).await.unwrap());
    let producer = Producer::new(partition.clone());

    for i in 0..10 {
        producer.send_record(record(i)).await.unwrap();
    }
    partition.sync().await.unwrap();

    // Wait long enough for the periodic retention tick to fire.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let log_files: usize = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "log"))
        .count();

    assert_eq!(
        log_files, 1,
        "expected only active segment after periodic retention"
    );
}
