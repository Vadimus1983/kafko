use bytes::Bytes;
#[cfg(any(feature = "compression-lz4", feature = "compression-zstd"))]
use kafko::Compression;
use kafko::{Kafko, LogConfig, Partition, Record};
use tempfile::TempDir;

#[tokio::test]
async fn send_batch_returns_sequential_offsets_starting_from_hwm() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    let producer = broker.producer_for("orders").await.unwrap();

    let offsets = producer
        .send_batch(vec![
            (None, Bytes::from_static(b"a")),
            (None, Bytes::from_static(b"b")),
            (None, Bytes::from_static(b"c")),
        ])
        .await
        .unwrap();
    assert_eq!(offsets, vec![0, 1, 2]);

    // A second batch picks up at the next HWM, not at zero.
    let more = producer
        .send_batch(vec![
            (None, Bytes::from_static(b"d")),
            (None, Bytes::from_static(b"e")),
        ])
        .await
        .unwrap();
    assert_eq!(more, vec![3, 4]);
}

#[tokio::test]
async fn send_batch_empty_input_returns_empty_vec_without_round_trip() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    let producer = broker.producer_for("orders").await.unwrap();

    let offsets = producer.send_batch(Vec::new()).await.unwrap();
    assert!(offsets.is_empty());

    // HWM must not have moved.
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);
    let single = producer.send(None, Bytes::from_static(b"x")).await.unwrap();
    assert_eq!(single, 0);
    let r = consumer.next_record().await.unwrap();
    assert_eq!(r.value().as_ref(), b"x");
}

#[tokio::test]
async fn send_batch_records_round_trip_through_consumer_in_order() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    let producer = broker.producer_for("orders").await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);

    let payloads: Vec<Bytes> = (0..16).map(|i| Bytes::from(format!("rec-{i}"))).collect();
    let items: Vec<(Option<Bytes>, Bytes)> = payloads
        .iter()
        .cloned()
        .map(|v| (None, v))
        .collect();
    let offsets = producer.send_batch(items).await.unwrap();
    assert_eq!(offsets, (0..16).collect::<Vec<_>>());

    for expected in &payloads {
        let r = consumer.next_record().await.unwrap();
        assert_eq!(r.value(), expected);
    }
}

#[tokio::test]
async fn send_batch_records_preserves_caller_supplied_timestamps() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open(dir.path()).await.unwrap();
    broker.create_topic("orders").await.unwrap();
    let producer = broker.producer_for("orders").await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);

    let records = vec![
        Record::new(1_700_000_000_000, None, Bytes::from_static(b"r0")),
        Record::new(1_700_000_000_500, None, Bytes::from_static(b"r1")),
        Record::new(1_700_000_001_000, None, Bytes::from_static(b"r2")),
    ];
    let offsets = producer.send_batch_records(records.clone()).await.unwrap();
    assert_eq!(offsets, vec![0, 1, 2]);

    for expected in &records {
        let r = consumer.next_record().await.unwrap();
        assert_eq!(r.timestamp_ms(), expected.timestamp_ms());
        assert_eq!(r.value(), expected.value());
    }
}

#[tokio::test]
async fn send_batch_is_durable_across_shutdown_and_reopen() {
    let dir = TempDir::new().unwrap();
    let payloads: Vec<Bytes> = (0..200).map(|i| Bytes::from(format!("rec-{i}"))).collect();

    {
        let broker = Kafko::open(dir.path()).await.unwrap();
        broker.create_topic("orders").await.unwrap();
        let producer = broker.producer_for("orders").await.unwrap();

        let items: Vec<(Option<Bytes>, Bytes)> = payloads
            .iter()
            .cloned()
            .map(|v| (None, v))
            .collect();
        let offsets = producer.send_batch(items).await.unwrap();
        assert_eq!(offsets.len(), 200);
        broker.shutdown().await.unwrap();
    }

    let broker = Kafko::open(dir.path()).await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);
    for (i, expected) in payloads.iter().enumerate() {
        let r = consumer
            .next_record()
            .await
            .unwrap_or_else(|e| panic!("missing record at {i}: {e:?}"));
        assert_eq!(r.value(), expected, "wrong value at offset {i}");
    }
}

#[tokio::test]
async fn send_batch_advances_high_water_mark_by_batch_size() {
    // After append_batch returns, the partition's high-water-mark must reflect
    // every record in the batch. (tokio::sync::watch coalesces updates from the
    // writer side, so this test cannot distinguish 1 tick from N ticks; it
    // verifies the user-visible postcondition that HWM has caught up.)
    let dir = TempDir::new().unwrap();
    let p = Partition::open(dir.path(), LogConfig::default())
        .await
        .unwrap();
    assert_eq!(p.high_water_mark(), 0);

    let records: Vec<Record> = (0..8)
        .map(|i| Record::new(i as i64, None, Bytes::from(format!("v{i}"))))
        .collect();
    let offsets = p.append_batch(records).await.unwrap();
    assert_eq!(offsets, (0..8).collect::<Vec<u64>>());
    assert_eq!(p.high_water_mark(), 8);

    p.shutdown().await.unwrap();
}

#[tokio::test]
async fn send_batch_propagates_io_error_to_caller() {
    let dir = TempDir::new().unwrap();
    let p = Partition::open(dir.path(), LogConfig::default())
        .await
        .unwrap();

    p.fail_next_append_for_test(std::io::ErrorKind::StorageFull)
        .await
        .unwrap();

    let records: Vec<Record> = (0..4)
        .map(|i| Record::new(0, None, Bytes::from(format!("v{i}"))))
        .collect();
    match p.append_batch(records).await {
        Err(kafko::KafkoError::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::StorageFull);
        }
        other => panic!("expected Io(StorageFull), got {:?}", other),
    }

    // The partition stays alive after the synthesized failure: a follow-up
    // batch must succeed.
    let r = Record::new(0, None, Bytes::from_static(b"v"));
    assert_eq!(p.append_batch(vec![r]).await.unwrap(), vec![0]);

    p.shutdown().await.unwrap();
}

#[cfg(any(feature = "compression-lz4", feature = "compression-zstd"))]
async fn send_batch_round_trip_with_compression(compression: Compression) {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open_with_config(
        dir.path(),
        LogConfig {
            compression,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    broker.create_topic("orders").await.unwrap();
    let producer = broker.producer_for("orders").await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);

    // Highly-compressible payloads so we exercise the decompress path with
    // sizes that materially differ from the on-wire form.
    let payloads: Vec<Bytes> = (0..32)
        .map(|i| Bytes::from(vec![i as u8; 1024]))
        .collect();
    let items: Vec<(Option<Bytes>, Bytes)> = payloads
        .iter()
        .cloned()
        .map(|v| (None, v))
        .collect();
    let offsets = producer.send_batch(items).await.unwrap();
    assert_eq!(offsets.len(), 32);

    for expected in &payloads {
        let r = consumer.next_record().await.unwrap();
        assert_eq!(r.value(), expected);
    }
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn send_batch_lz4_compressed_round_trip() {
    send_batch_round_trip_with_compression(Compression::Lz4).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn send_batch_zstd_compressed_round_trip() {
    send_batch_round_trip_with_compression(Compression::Zstd).await;
}

#[tokio::test]
async fn send_batch_larger_than_segment_threshold_still_round_trips() {
    let dir = TempDir::new().unwrap();
    let broker = Kafko::open_with_config(dir.path(), LogConfig {
        // ~512 bytes per segment; 64 records at ~30 bytes each comfortably exceeds it.
        segment_size_threshold: 512,
        ..Default::default()
    })
    .await
    .unwrap();
    broker.create_topic("orders").await.unwrap();
    let producer = broker.producer_for("orders").await.unwrap();
    let mut consumer = broker.consumer_for("orders").await.unwrap();
    consumer.seek(0);

    let payloads: Vec<Bytes> = (0..64).map(|i| Bytes::from(format!("rec-{i:03}"))).collect();
    let items: Vec<(Option<Bytes>, Bytes)> = payloads
        .iter()
        .cloned()
        .map(|v| (None, v))
        .collect();
    let offsets = producer.send_batch(items).await.unwrap();
    assert_eq!(offsets.len(), 64);

    for expected in &payloads {
        let r = consumer.next_record().await.unwrap();
        assert_eq!(r.value(), expected);
    }
}
