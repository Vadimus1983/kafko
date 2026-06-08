use bytes::Bytes;
use kafko::{Kafko, KafkoError};
use std::collections::{HashMap, HashSet};
use tempfile::TempDir;

fn key(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

fn val(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

#[tokio::test]
async fn same_key_routes_to_same_partition_keys_spread() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic_with_partitions("t", 4).await.unwrap();
    let producer = broker.producer_for("t").await.unwrap();

    // Same key -> same partition, and its offset increments within that partition.
    let a = producer.send(Some(key("user-1")), val("x")).await.unwrap();
    let b = producer.send(Some(key("user-1")), val("y")).await.unwrap();
    assert_eq!(a.partition(), b.partition());
    assert_eq!(b.offset(), a.offset() + 1);

    // Many distinct keys touch more than one partition.
    let mut partitions = HashSet::new();
    for i in 0..200 {
        let pos = producer
            .send(Some(key(&format!("k{i}"))), val("v"))
            .await
            .unwrap();
        partitions.insert(pos.partition());
    }
    assert!(
        partitions.len() > 1,
        "expected keys to spread across partitions, got {partitions:?}"
    );
    for &p in &partitions {
        assert!(p < 4);
    }

    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn per_key_order_is_preserved_through_merged_consume() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic_with_partitions("t", 4).await.unwrap();
    let producer = broker.producer_for("t").await.unwrap();

    // Interleave two keys; each key's records must come back in send order even
    // though they may share a partition with nothing or interleave with the other.
    producer.send(Some(key("A")), val("a0")).await.unwrap();
    producer.send(Some(key("B")), val("b0")).await.unwrap();
    producer.send(Some(key("A")), val("a1")).await.unwrap();
    producer.send(Some(key("B")), val("b1")).await.unwrap();
    producer.send(Some(key("A")), val("a2")).await.unwrap();

    let mut consumer = broker.consumer_for("t").await.unwrap();
    let mut by_key: HashMap<Bytes, Vec<Bytes>> = HashMap::new();
    for _ in 0..5 {
        let record = consumer.next_record().await.unwrap();
        by_key
            .entry(record.key().unwrap().clone())
            .or_default()
            .push(record.value().clone());
    }

    assert_eq!(by_key[&key("A")], vec![val("a0"), val("a1"), val("a2")]);
    assert_eq!(by_key[&key("B")], vec![val("b0"), val("b1")]);

    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn merged_consumer_drains_all_partitions() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic_with_partitions("t", 8).await.unwrap();
    let producer = broker.producer_for("t").await.unwrap();

    let expected: HashSet<Bytes> = (0..100).map(|i| val(&format!("v{i}"))).collect();
    for v in &expected {
        // keyless -> round-robin across all 8 partitions
        producer.send(None, v.clone()).await.unwrap();
    }

    let mut consumer = broker.consumer_for("t").await.unwrap();
    let mut got = HashSet::new();
    for _ in 0..100 {
        got.insert(consumer.next_record().await.unwrap().value().clone());
    }
    assert_eq!(got, expected);

    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_partitions_is_rejected() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();

    match broker.create_topic_with_partitions("t", 0).await {
        Err(KafkoError::InvalidPartitionCount(0)) => {}
        other => panic!("expected InvalidPartitionCount(0), got {other:?}"),
    }
    assert!(!broker.has_topic("t").await);

    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn multi_partition_topic_survives_shutdown_and_reopen() {
    let dir = TempDir::new().unwrap();
    let expected: HashSet<Bytes> = (0..120).map(|i| val(&format!("rec-{i}"))).collect();

    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic_with_partitions("orders", 4).await.unwrap();
        let producer = broker.producer_for("orders").await.unwrap();
        for (i, v) in expected.iter().enumerate() {
            // Mix of keyed and keyless so routing exercises both paths.
            let k = if i % 2 == 0 {
                Some(key(&format!("k{i}")))
            } else {
                None
            };
            producer.send(k, v.clone()).await.unwrap();
        }
        broker.shutdown().await.unwrap();
    }

    let broker = Kafko::open(dir.path()).await.unwrap();
    assert_eq!(broker.partition_count("orders").await, Some(4));
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    let mut got = HashSet::new();
    for _ in 0..expected.len() {
        got.insert(consumer.next_record().await.unwrap().value().clone());
    }
    assert_eq!(got, expected);

    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_flat_topic_layout_is_rejected() {
    let dir = TempDir::new().unwrap();
    // Simulate a kafko <= 0.2 data dir: segments live directly under the topic
    // dir with no numeric partition subdirectories.
    let topic_dir = dir.path().join("orders");
    std::fs::create_dir_all(&topic_dir).unwrap();
    std::fs::write(topic_dir.join("00000000000000000000.log"), b"not real data").unwrap();

    match Kafko::open(dir.path()).await {
        Err(KafkoError::InvalidTopicLayout { topic, .. }) => assert_eq!(topic, "orders"),
        Err(other) => panic!("expected InvalidTopicLayout, got {other:?}"),
        Ok(_) => panic!("expected InvalidTopicLayout, got Ok"),
    }
}
