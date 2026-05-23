use kafko::SparseIndex;
use tempfile::TempDir;

#[tokio::test]
async fn create_starts_empty() {
    let dir = TempDir::new().unwrap();
    let idx = SparseIndex::create(dir.path(), 0, 4096).await.unwrap();
    assert_eq!(idx.len(), 0);
    assert!(idx.is_empty());
    assert_eq!(idx.lookup(0), (0, 0));
    assert_eq!(idx.lookup(100), (0, 0));
}

#[tokio::test]
async fn first_append_always_creates_entry() {
    let dir = TempDir::new().unwrap();
    let mut idx = SparseIndex::create(dir.path(), 0, 4096).await.unwrap();
    idx.track_append(0, 0, 100).await.unwrap();
    assert_eq!(idx.len(), 1);
    assert_eq!(idx.lookup(0), (0, 0));
}

#[tokio::test]
async fn entries_added_at_interval_boundaries() {
    let dir = TempDir::new().unwrap();
    let mut idx = SparseIndex::create(dir.path(), 0, 4096).await.unwrap();
    let mut file_pos = 0u64;
    for offset in 0..10u64 {
        idx.track_append(offset, file_pos, 1024).await.unwrap();
        file_pos += 1024;
    }
    assert_eq!(idx.len(), 3);
    assert_eq!(idx.lookup(0), (0, 0));
    assert_eq!(idx.lookup(3), (0, 0));
    assert_eq!(idx.lookup(4), (4096, 4));
    assert_eq!(idx.lookup(7), (4096, 4));
    assert_eq!(idx.lookup(8), (8192, 8));
    assert_eq!(idx.lookup(100), (8192, 8));
}

#[tokio::test]
async fn open_reloads_entries_from_disk() {
    let dir = TempDir::new().unwrap();
    {
        let mut idx = SparseIndex::create(dir.path(), 1000, 100).await.unwrap();
        idx.track_append(1000, 0, 50).await.unwrap();
        idx.track_append(1001, 50, 60).await.unwrap();
        idx.track_append(1002, 110, 30).await.unwrap();
        idx.sync().await.unwrap();
    }
    let idx = SparseIndex::open(dir.path(), 1000, 100).await.unwrap();
    assert_eq!(idx.base_offset(), 1000);
    assert_eq!(idx.interval(), 100);
    assert_eq!(idx.len(), 2);
    assert_eq!(idx.lookup(1000), (0, 1000));
    assert_eq!(idx.lookup(1001), (0, 1000));
    assert_eq!(idx.lookup(1002), (110, 1002));
}

#[tokio::test]
async fn lookup_below_base_returns_zero() {
    let dir = TempDir::new().unwrap();
    let mut idx = SparseIndex::create(dir.path(), 100, 4096).await.unwrap();
    idx.track_append(100, 0, 50).await.unwrap();
    assert_eq!(idx.lookup(0), (0, 100));
    assert_eq!(idx.lookup(99), (0, 100));
}

#[tokio::test]
async fn lookup_past_last_entry_returns_last_entry_position() {
    let dir = TempDir::new().unwrap();
    let mut idx = SparseIndex::create(dir.path(), 0, 100).await.unwrap();
    idx.track_append(0, 0, 50).await.unwrap();
    idx.track_append(1, 50, 60).await.unwrap();
    idx.track_append(2, 110, 30).await.unwrap();
    assert_eq!(idx.len(), 2);
    assert_eq!(idx.lookup(1000), (110, 2));
}

#[tokio::test]
async fn create_fails_if_file_exists() {
    let dir = TempDir::new().unwrap();
    let _idx = SparseIndex::create(dir.path(), 0, 4096).await.unwrap();
    let result = SparseIndex::create(dir.path(), 0, 4096).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn entries_continue_to_be_added_after_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut idx = SparseIndex::create(dir.path(), 0, 100).await.unwrap();
        idx.track_append(0, 0, 50).await.unwrap();
        idx.sync().await.unwrap();
    }
    let mut idx = SparseIndex::open(dir.path(), 0, 100).await.unwrap();
    assert_eq!(idx.len(), 1);

    idx.track_append(5, 50, 200).await.unwrap();
    idx.track_append(6, 250, 50).await.unwrap();

    assert_eq!(idx.len(), 2);
    assert_eq!(idx.lookup(0), (0, 0));
    assert_eq!(idx.lookup(5), (0, 0));
    assert_eq!(idx.lookup(6), (250, 6));
}
