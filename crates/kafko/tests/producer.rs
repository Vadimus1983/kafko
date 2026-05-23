use bytes::Bytes;
use kafko::{LogConfig, Partition, Producer, Record};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test]
async fn send_returns_assigned_offset() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    assert_eq!(
        producer
            .send(Some(Bytes::from_static(b"k")), Bytes::from_static(b"v"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        producer
            .send(Some(Bytes::from_static(b"k")), Bytes::from_static(b"v"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(partition.high_water_mark(), 2);
}

#[tokio::test]
async fn send_assigns_current_timestamp() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    let before = now_ms();
    let offset = producer
        .send(None, Bytes::from_static(b"value"))
        .await
        .unwrap();
    let after = now_ms();

    let record = partition.read_record_at(offset).await.unwrap().unwrap();
    assert!(record.timestamp_ms() >= before);
    assert!(record.timestamp_ms() <= after);
}

#[tokio::test]
async fn send_record_preserves_provided_timestamp() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    let r = Record::new(12345, None, Bytes::from_static(b"value"));
    let offset = producer.send_record(r.clone()).await.unwrap();

    let read_back = partition.read_record_at(offset).await.unwrap().unwrap();
    assert_eq!(read_back, r);
}

#[tokio::test]
async fn producer_is_cloneable_for_multi_task_use() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    let mut handles = Vec::new();
    for i in 0..10u64 {
        let p = producer.clone();
        handles.push(tokio::spawn(async move {
            p.send(None, Bytes::from(format!("msg-{i}"))).await.unwrap()
        }));
    }

    let mut offsets = Vec::new();
    for h in handles {
        offsets.push(h.await.unwrap());
    }
    offsets.sort();
    assert_eq!(offsets, (0..10).collect::<Vec<u64>>());
    assert_eq!(partition.high_water_mark(), 10);
}
