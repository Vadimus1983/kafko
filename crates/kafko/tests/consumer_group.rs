use bytes::Bytes;
use kafko::{Kafko, KafkoError};
use std::collections::HashSet;
use tempfile::TempDir;

fn val(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

async fn produce(broker: &Kafko, topic: &str, n: u64) {
    let producer = broker.producer_for(topic).await.unwrap();
    for i in 0..n {
        producer.send(None, val(&format!("v{i}"))).await.unwrap();
    }
}

#[tokio::test]
async fn group_resumes_from_committed_offset_after_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic("orders").await.unwrap();
        produce(&broker, "orders", 10).await;

        let mut c = broker.consumer_for_group("orders", "g").await.unwrap();
        for i in 0..4 {
            assert_eq!(c.next_record().await.unwrap().value(), &val(&format!("v{i}")));
        }
        c.commit().await.unwrap();
        assert_eq!(c.committed(0), Some(4));
        broker.shutdown().await.unwrap();
    }

    // A fresh process: the group resumes at 4, not 0.
    let broker = Kafko::open(dir.path()).await.unwrap();
    let mut c = broker.consumer_for_group("orders", "g").await.unwrap();
    assert_eq!(c.committed(0), Some(4));
    assert_eq!(c.position(0), 4);
    for i in 4..10 {
        assert_eq!(c.next_record().await.unwrap().value(), &val(&format!("v{i}")));
    }
    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn distinct_groups_keep_independent_positions() {
    let dir = TempDir::new().unwrap();
    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic("orders").await.unwrap();
        produce(&broker, "orders", 10).await;

        let mut a = broker.consumer_for_group("orders", "a").await.unwrap();
        for _ in 0..3 {
            a.next_record().await.unwrap();
        }
        a.commit().await.unwrap();

        let mut b = broker.consumer_for_group("orders", "b").await.unwrap();
        for _ in 0..7 {
            b.next_record().await.unwrap();
        }
        b.commit().await.unwrap();
        broker.shutdown().await.unwrap();
    }

    let broker = Kafko::open(dir.path()).await.unwrap();
    let a = broker.consumer_for_group("orders", "a").await.unwrap();
    let b = broker.consumer_for_group("orders", "b").await.unwrap();
    assert_eq!(a.committed(0), Some(3));
    assert_eq!(b.committed(0), Some(7));
    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn anonymous_consumer_starts_at_zero_and_commit_is_noop() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    produce(&broker, "orders", 3).await;

    let mut c = broker.consumer_for("orders").await.unwrap();
    assert_eq!(c.group(), None);
    assert_eq!(c.committed(0), None);
    c.commit().await.unwrap(); // no-op, must not error
    assert_eq!(c.next_record().await.unwrap().value(), &val("v0"));

    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_resumes_per_partition_on_multi_partition_topic() {
    let dir = TempDir::new().unwrap();
    let all: HashSet<Bytes> = (0..20).map(|i| val(&format!("v{i}"))).collect();

    let first_half: HashSet<Bytes>;
    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic_with_partitions("t", 4).await.unwrap();
        produce(&broker, "t", 20).await; // keyless -> spread round-robin over 4 partitions

        let mut c = broker.consumer_for_group("t", "g").await.unwrap();
        let mut got = HashSet::new();
        for _ in 0..8 {
            got.insert(c.next_record().await.unwrap().value().clone());
        }
        c.commit().await.unwrap();
        first_half = got;
        broker.shutdown().await.unwrap();
    }

    // Resume: read the remaining 12; together with the first 8 we must see all 20
    // exactly once (no partition re-read its committed prefix, none was skipped).
    let broker = Kafko::open(dir.path()).await.unwrap();
    let mut c = broker.consumer_for_group("t", "g").await.unwrap();
    let mut second_half = HashSet::new();
    for _ in 0..12 {
        second_half.insert(c.next_record().await.unwrap().value().clone());
    }
    broker.shutdown().await.unwrap();

    assert_eq!(first_half.len(), 8);
    assert_eq!(second_half.len(), 12);
    assert!(first_half.is_disjoint(&second_half), "a record was read twice across resume");
    let union: HashSet<Bytes> = first_half.union(&second_half).cloned().collect();
    assert_eq!(union, all);
}

#[tokio::test]
async fn corrupt_offset_file_resumes_from_zero() {
    let dir = TempDir::new().unwrap();
    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic("orders").await.unwrap();
        produce(&broker, "orders", 5).await;
        let mut c = broker.consumer_for_group("orders", "g").await.unwrap();
        for _ in 0..3 {
            c.next_record().await.unwrap();
        }
        c.commit().await.unwrap();
        broker.shutdown().await.unwrap();
    }

    // Corrupt the committed-offset file; recovery must fall back to offset 0.
    let offset_file = dir.path().join("orders").join("offsets").join("g");
    std::fs::write(&offset_file, b"garbage").unwrap();

    let broker = Kafko::open(dir.path()).await.unwrap();
    let mut c = broker.consumer_for_group("orders", "g").await.unwrap();
    assert_eq!(c.committed(0), Some(0));
    assert_eq!(c.next_record().await.unwrap().value(), &val("v0"));
    broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_group_name_is_rejected() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();

    for bad in ["", "a/b", ".."] {
        match broker.consumer_for_group("orders", bad).await {
            Err(KafkoError::InvalidGroupName(_)) => {}
            Err(other) => panic!("group {bad:?} should be InvalidGroupName, got {other:?}"),
            Ok(_) => panic!("group {bad:?} should be rejected, got Ok"),
        }
    }
    broker.shutdown().await.unwrap();
}
