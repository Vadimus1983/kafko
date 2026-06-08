use bytes::Bytes;
use kafko::{LogConfig, Producer, Record, Topic};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn single_partition_topic(dir: &TempDir) -> Arc<Topic> {
    Arc::new(
        Topic::create(dir.path(), "t", 1, LogConfig::default())
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn send_returns_assigned_position() {
    let dir = TempDir::new().unwrap();
    let topic = single_partition_topic(&dir).await;
    let producer = Producer::new(topic.clone());

    let pos = producer
        .send(Some(Bytes::from_static(b"k")), Bytes::from_static(b"v"))
        .await
        .unwrap();
    assert_eq!(pos.partition(), 0);
    assert_eq!(pos.offset(), 0);

    let pos = producer
        .send(Some(Bytes::from_static(b"k")), Bytes::from_static(b"v"))
        .await
        .unwrap();
    assert_eq!(pos.offset(), 1);

    assert_eq!(topic.partition(0).unwrap().high_water_mark(), 2);
}

#[tokio::test]
async fn send_assigns_current_timestamp() {
    let dir = TempDir::new().unwrap();
    let topic = single_partition_topic(&dir).await;
    let producer = Producer::new(topic.clone());

    let before = now_ms();
    let pos = producer
        .send(None, Bytes::from_static(b"value"))
        .await
        .unwrap();
    let after = now_ms();

    let record = topic
        .partition(0)
        .unwrap()
        .read_record_at(pos.offset())
        .await
        .unwrap()
        .unwrap();
    assert!(record.timestamp_ms() >= before);
    assert!(record.timestamp_ms() <= after);
}

#[tokio::test]
async fn send_record_preserves_provided_timestamp() {
    let dir = TempDir::new().unwrap();
    let topic = single_partition_topic(&dir).await;
    let producer = Producer::new(topic.clone());

    let r = Record::new(12345, None, Bytes::from_static(b"value"));
    let pos = producer.send_record(r.clone()).await.unwrap();

    let read_back = topic
        .partition(0)
        .unwrap()
        .read_record_at(pos.offset())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_back, r);
}

#[tokio::test]
async fn producer_is_cloneable_for_multi_task_use() {
    let dir = TempDir::new().unwrap();
    let topic = single_partition_topic(&dir).await;
    let producer = Producer::new(topic.clone());

    let mut handles = Vec::new();
    for i in 0..10u64 {
        let p = producer.clone();
        handles.push(tokio::spawn(async move {
            p.send(None, Bytes::from(format!("msg-{i}")))
                .await
                .unwrap()
                .offset()
        }));
    }

    let mut offsets = Vec::new();
    for h in handles {
        offsets.push(h.await.unwrap());
    }
    offsets.sort();
    assert_eq!(offsets, (0..10).collect::<Vec<u64>>());
    assert_eq!(topic.partition(0).unwrap().high_water_mark(), 10);
}
