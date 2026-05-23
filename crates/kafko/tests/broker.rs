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
