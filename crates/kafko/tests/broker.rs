use bytes::Bytes;
use kafko::{Kafko, KafkoError};
use tempfile::TempDir;

#[tokio::test]
async fn open_empty_kafko_has_no_topics() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    assert!(broker.list_topics().await.is_empty());
}

#[tokio::test]
async fn open_missing_dir_creates_it() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("nested/kafko");
    let broker = Kafko::open(&nested).await.unwrap();
    assert!(broker.list_topics().await.is_empty());
}

#[tokio::test]
async fn create_topic_appears_in_list() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();

    assert_eq!(broker.list_topics().await, vec!["orders".to_string()]);
    assert!(broker.has_topic("orders").await);
}

#[tokio::test]
async fn create_existing_topic_returns_error() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();

    match broker.create_topic("orders").await {
        Err(KafkoError::TopicAlreadyExists(name)) => assert_eq!(name, "orders"),
        other => panic!("expected TopicAlreadyExists, got {:?}", other),
    }
}

#[tokio::test]
async fn multiple_topics_appear_sorted_in_list() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    broker.create_topic("events").await.unwrap();
    broker.create_topic("audit").await.unwrap();

    assert_eq!(
        broker.list_topics().await,
        vec![
            "audit".to_string(),
            "events".to_string(),
            "orders".to_string()
        ]
    );
}

#[tokio::test]
async fn reopen_discovers_existing_topics() {
    let dir = TempDir::new().unwrap();
    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic("orders").await.unwrap();
        broker.create_topic("events").await.unwrap();
    }

    let broker = Kafko::open(dir.path()).await.unwrap();
    assert_eq!(
        broker.list_topics().await,
        vec!["events".to_string(), "orders".to_string()]
    );
}

#[tokio::test]
async fn delete_topic_removes_from_list_and_disk() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    broker.create_topic("events").await.unwrap();

    broker.delete_topic("orders").await.unwrap();

    assert_eq!(broker.list_topics().await, vec!["events".to_string()]);
    assert!(!dir.path().join("orders").exists());
}

#[tokio::test]
async fn delete_nonexistent_topic_returns_error() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();

    match broker.delete_topic("ghost").await {
        Err(KafkoError::TopicNotFound(name)) => assert_eq!(name, "ghost"),
        other => panic!("expected TopicNotFound, got {:?}", other),
    }
}

#[tokio::test]
async fn delete_then_recreate_works() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    broker.delete_topic("orders").await.unwrap();
    broker.create_topic("orders").await.unwrap();

    assert_eq!(broker.list_topics().await, vec!["orders".to_string()]);
}

#[tokio::test]
async fn delete_topic_in_use_returns_error_and_preserves_state() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();

    let _producer = broker.producer_for("orders").await.unwrap();

    match broker.delete_topic("orders").await {
        Err(KafkoError::TopicInUse(name)) => assert_eq!(name, "orders"),
        other => panic!("expected TopicInUse, got {:?}", other),
    }

    // Topic is still in registry after failed delete
    assert!(broker.has_topic("orders").await);
}

#[tokio::test]
async fn producer_and_consumer_for_topic_roundtrip() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();

    let producer = broker.producer_for("orders").await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();

    let offset = producer
        .send(
            Some(Bytes::from_static(b"id-42")),
            Bytes::from_static(b"payload"),
        )
        .await
        .unwrap();
    assert_eq!(offset, 0);

    let record = consumer.next_record().await.unwrap();
    assert_eq!(record.value(), &Bytes::from_static(b"payload"));
    assert_eq!(record.key(), Some(&Bytes::from_static(b"id-42")));
}

#[tokio::test]
async fn producer_for_nonexistent_topic_returns_error() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();

    let result = broker.producer_for("ghost").await;
    match &result {
        Err(KafkoError::TopicNotFound(name)) => assert_eq!(name, "ghost"),
        _ => panic!("expected TopicNotFound"),
    }
}

#[tokio::test]
async fn topics_have_independent_offset_streams() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    broker.create_topic("events").await.unwrap();

    let p_orders = broker.producer_for("orders").await.unwrap();
    let p_events = broker.producer_for("events").await.unwrap();

    assert_eq!(
        p_orders
            .send(None, Bytes::from_static(b"o0"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        p_orders
            .send(None, Bytes::from_static(b"o1"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        p_events
            .send(None, Bytes::from_static(b"e0"))
            .await
            .unwrap(),
        0
    );

    // Both topics start their own offset count at 0
    assert_eq!(
        p_orders
            .send(None, Bytes::from_static(b"o2"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        p_events
            .send(None, Bytes::from_static(b"e1"))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn has_topic_reflects_create_and_delete() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();

    assert!(!broker.has_topic("orders").await);
    broker.create_topic("orders").await.unwrap();
    assert!(broker.has_topic("orders").await);
    broker.delete_topic("orders").await.unwrap();
    assert!(!broker.has_topic("orders").await);
}


#[tokio::test]
async fn second_open_on_same_dir_fails_while_first_is_live() {
    let dir = TempDir::new().unwrap();
    let first = Kafko::open(dir.path()).await.unwrap();

    match Kafko::open(dir.path()).await {
        Err(KafkoError::AlreadyOpen { path }) => assert_eq!(path, dir.path()),
        Err(e) => panic!("expected AlreadyOpen, got error: {:?}", e),
        Ok(_) => panic!("expected AlreadyOpen, got Ok"),
    }

    drop(first);
    // After the first broker is dropped its OS-level lock is released and a
    // fresh open on the same dir must succeed.
    let _second = Kafko::open(dir.path()).await.unwrap();
}

#[tokio::test]
async fn shutdown_releases_lock_so_reopen_succeeds() {
    let dir = TempDir::new().unwrap();
    let first = Kafko::open(dir.path()).await.unwrap();
    first.shutdown().await.unwrap();

    let _second = Kafko::open(dir.path()).await.unwrap();
}

#[tokio::test]
async fn lock_file_persists_across_open_cycles() {
    let dir = TempDir::new().unwrap();
    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.shutdown().await.unwrap();
    }
    // The LOCK file is intentionally left on disk; verify a second open still
    // works and treats the existing file as the lock target rather than failing
    // to create it.
    assert!(dir.path().join("LOCK").exists());
    let _broker = Kafko::open(dir.path()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_without_shutdown_still_fsyncs_on_multi_thread_runtime() {
    // Mirrors `shutdown_is_a_durability_boundary_for_acked_records` but exits
    // the inner scope by simply letting the broker drop, no explicit shutdown.
    // The Drop impl on the multi-thread runtime uses block_in_place + block_on
    // to drive the writer-task shutdown to completion, so this must still see
    // every acked record after reopen.
    let dir = TempDir::new().unwrap();
    let record_count: u64 = 256;

    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic("orders").await.unwrap();
        let producer = broker.producer_for("orders").await.unwrap();
        for i in 0..record_count {
            producer
                .send(None, Bytes::from(format!("rec-{i}")))
                .await
                .unwrap();
        }
        // Producer dropped first (loses its Arc<Partition> ref); broker drops
        // at end of scope, triggering the Drop impl's block_in_place path.
    }

    let broker = Kafko::open(dir.path()).await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);
    for i in 0..record_count {
        let r = consumer
            .next_record()
            .await
            .unwrap_or_else(|e| panic!("missing record at {i}: {e:?}"));
        let expected = format!("rec-{i}");
        assert_eq!(
            r.value().as_ref(),
            expected.as_bytes(),
            "wrong value at offset {i} after drop+reopen"
        );
    }
}

#[tokio::test]
async fn shutdown_is_a_durability_boundary_for_acked_records() {
    let dir = TempDir::new().unwrap();
    let record_count: u64 = 256;

    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic("orders").await.unwrap();
        let producer = broker.producer_for("orders").await.unwrap();
        for i in 0..record_count {
            producer
                .send(None, Bytes::from(format!("rec-{i}")))
                .await
                .unwrap();
        }
        // Explicit shutdown: every previously-acked record must be fsynced to
        // disk before the call returns. The next open on the same dir must see
        // all of them, regardless of what the kernel's writeback decided.
        broker.shutdown().await.unwrap();
    }

    let broker = Kafko::open(dir.path()).await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);
    for i in 0..record_count {
        let r = consumer
            .next_record()
            .await
            .unwrap_or_else(|e| panic!("missing record at {i}: {e:?}"));
        let expected = format!("rec-{i}");
        assert_eq!(
            r.value().as_ref(),
            expected.as_bytes(),
            "wrong value at offset {i} after shutdown+reopen"
        );
    }
}
