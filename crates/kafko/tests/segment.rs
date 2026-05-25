use bytes::{Bytes, BytesMut};
use kafko::{Record, Segment};
use tempfile::TempDir;

#[tokio::test]
async fn create_and_append_persists_bytes() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0).await.unwrap();

    let payload = b"hello world";
    let pos = seg.append(payload).await.unwrap();
    assert_eq!(pos, 0);
    assert_eq!(seg.size(), payload.len() as u64);

    let mut buf = vec![0u8; payload.len()];
    let n = seg.read_at(0, &mut buf[..]).await.unwrap();
    assert_eq!(n, payload.len());
    assert_eq!(&buf[..], payload);
}

#[tokio::test]
async fn multiple_appends_track_file_positions() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0).await.unwrap();

    let p1 = seg.append(b"aaaa").await.unwrap();
    let p2 = seg.append(b"bbbb").await.unwrap();
    let p3 = seg.append(b"cccc").await.unwrap();

    assert_eq!(p1, 0);
    assert_eq!(p2, 4);
    assert_eq!(p3, 8);
    assert_eq!(seg.size(), 12);

    let mut buf = [0u8; 4];
    seg.read_at(p1, &mut buf[..]).await.unwrap();
    assert_eq!(&buf[..], b"aaaa");
    seg.read_at(p2, &mut buf[..]).await.unwrap();
    assert_eq!(&buf[..], b"bbbb");
    seg.read_at(p3, &mut buf[..]).await.unwrap();
    assert_eq!(&buf[..], b"cccc");
}

#[tokio::test]
async fn open_existing_segment_loads_current_size() {
    let dir = TempDir::new().unwrap();
    {
        let mut seg = Segment::create(dir.path(), 100).await.unwrap();
        seg.append(b"persisted").await.unwrap();
        seg.sync().await.unwrap();
    }

    let seg = Segment::open(dir.path(), 100).await.unwrap();
    assert_eq!(seg.size(), 9);
    assert_eq!(seg.base_offset(), 100);
}

#[tokio::test]
async fn reopen_and_append_extends_existing_data() {
    let dir = TempDir::new().unwrap();
    {
        let mut seg = Segment::create(dir.path(), 0).await.unwrap();
        seg.append(b"first").await.unwrap();
        seg.sync().await.unwrap();
    }

    let mut seg = Segment::open(dir.path(), 0).await.unwrap();
    let pos = seg.append(b"second").await.unwrap();
    assert_eq!(pos, 5);
    assert_eq!(seg.size(), 11);

    let mut buf = [0u8; 11];
    seg.read_at(0, &mut buf[..]).await.unwrap();
    assert_eq!(&buf[..], b"firstsecond");
}

#[tokio::test]
async fn create_fails_if_file_exists() {
    let dir = TempDir::new().unwrap();
    let _seg = Segment::create(dir.path(), 0).await.unwrap();
    let result = Segment::create(dir.path(), 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn would_overflow_reflects_size_and_threshold() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0).await.unwrap();
    assert!(!seg.would_overflow(100, 1000));

    seg.append(&vec![0u8; 500]).await.unwrap();
    assert!(!seg.would_overflow(400, 1000));
    assert!(!seg.would_overflow(500, 1000));
    assert!(seg.would_overflow(501, 1000));
}

#[tokio::test]
async fn appends_and_reads_record_bytes() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0).await.unwrap();

    let record = Record::new(
        1_700_000_000_000,
        Some(Bytes::from_static(b"order-42")),
        Bytes::from_static(b"{\"qty\":10}"),
    );
    let expected = record.clone();
    let wire_size = record.wire_size();

    let mut encoded = BytesMut::with_capacity(wire_size);
    record.encode(&mut encoded);
    let file_pos = seg.append(&encoded).await.unwrap();
    seg.sync().await.unwrap();

    let mut read_buf = vec![0u8; wire_size];
    let n = seg.read_at(file_pos, &mut read_buf[..]).await.unwrap();
    assert_eq!(n, wire_size);

    let mut slice: &[u8] = &read_buf;
    let decoded = Record::decode(&mut slice).unwrap();
    assert_eq!(decoded, expected);
}

#[tokio::test]
async fn read_past_eof_returns_zero() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0).await.unwrap();
    seg.append(b"hi").await.unwrap();

    let mut buf = [0u8; 10];
    let n = seg.read_at(100, &mut buf[..]).await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn base_offset_preserved_through_create() {
    let dir = TempDir::new().unwrap();
    let seg = Segment::create(dir.path(), 12_345).await.unwrap();
    assert_eq!(seg.base_offset(), 12_345);
    assert_eq!(
        seg.path().file_name().unwrap().to_str().unwrap(),
        "00000000000000012345.log"
    );
}
