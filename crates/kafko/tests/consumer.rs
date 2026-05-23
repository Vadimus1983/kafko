use bytes::Bytes;
use kafko::{Consumer, LogConfig, Partition, Producer, Record};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn make_record(marker: u64) -> Record {
    Record::new(
        marker as i64,
        Some(Bytes::from(format!("key-{marker}"))),
        Bytes::from(format!("value-{marker}")),
    )
}

#[tokio::test]
async fn consumer_reads_back_existing_records() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    let records: Vec<Record> = (0..3).map(make_record).collect();
    for r in &records {
        producer.send_record(r.clone()).await.unwrap();
    }

    let mut consumer = Consumer::from_partition(partition.clone());
    for expected in records {
        let actual = consumer.next_record().await.unwrap();
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn consumer_position_advances_after_each_read() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    producer.send_record(make_record(0)).await.unwrap();
    producer.send_record(make_record(1)).await.unwrap();

    let mut consumer = Consumer::from_partition(partition.clone());
    assert_eq!(consumer.position(), 0);
    consumer.next_record().await.unwrap();
    assert_eq!(consumer.position(), 1);
    consumer.next_record().await.unwrap();
    assert_eq!(consumer.position(), 2);
}

#[tokio::test]
async fn consumer_wakes_up_when_record_appended() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    let consumer_partition = partition.clone();
    let waiter = tokio::spawn(async move {
        let mut consumer = Consumer::from_partition(consumer_partition);
        consumer.next_record().await.unwrap()
    });

    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    producer.send_record(make_record(42)).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("waiter should resolve")
        .unwrap();
    assert_eq!(received, make_record(42));
}

#[tokio::test]
async fn consumer_can_seek_to_offset() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    for i in 0..5u64 {
        producer.send_record(make_record(i)).await.unwrap();
    }

    let mut consumer = Consumer::from_partition(partition.clone());
    consumer.seek(3);
    assert_eq!(consumer.next_record().await.unwrap(), make_record(3));
    assert_eq!(consumer.next_record().await.unwrap(), make_record(4));
    assert_eq!(consumer.position(), 5);
}

#[tokio::test]
async fn multiple_consumers_have_independent_cursors() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    for i in 0..5u64 {
        producer.send_record(make_record(i)).await.unwrap();
    }

    let mut c1 = Consumer::from_partition(partition.clone());
    let mut c2 = Consumer::from_partition_at(partition.clone(), 2);

    assert_eq!(c1.next_record().await.unwrap(), make_record(0));
    assert_eq!(c2.next_record().await.unwrap(), make_record(2));
    assert_eq!(c1.next_record().await.unwrap(), make_record(1));
    assert_eq!(c2.next_record().await.unwrap(), make_record(3));

    assert_eq!(c1.position(), 2);
    assert_eq!(c2.position(), 4);
}

#[tokio::test]
async fn consumer_at_start_offset_skips_earlier_records() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    for i in 0..5u64 {
        producer.send_record(make_record(i)).await.unwrap();
    }

    let mut consumer = Consumer::from_partition_at(partition.clone(), 3);
    assert_eq!(consumer.next_record().await.unwrap(), make_record(3));
    assert_eq!(consumer.next_record().await.unwrap(), make_record(4));
}

#[tokio::test]
async fn producer_consumer_handshake_across_tasks() {
    let dir = TempDir::new().unwrap();
    let partition = Arc::new(
        Partition::open(dir.path(), LogConfig::default())
            .await
            .unwrap(),
    );
    let producer = Producer::new(partition.clone());

    let consumer_partition = partition.clone();
    let consumer_task = tokio::spawn(async move {
        let mut consumer = Consumer::from_partition(consumer_partition);
        let mut received = Vec::new();
        for _ in 0..5 {
            received.push(consumer.next_record().await.unwrap());
        }
        received
    });

    for i in 0..5u64 {
        producer.send_record(make_record(i)).await.unwrap();
        tokio::task::yield_now().await;
    }

    let received = tokio::time::timeout(Duration::from_secs(2), consumer_task)
        .await
        .expect("consumer should receive all records")
        .unwrap();

    let expected: Vec<Record> = (0..5).map(make_record).collect();
    assert_eq!(received, expected);
}
