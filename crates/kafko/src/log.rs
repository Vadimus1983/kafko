use crate::compression::Compression;
use crate::error::{KafkoError, Result};
use crate::record::Record;
use crate::segment::Segment;
use crate::sparse_index::SparseIndex;
use bytes::BytesMut;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Per-topic configuration for a partition's on-disk log.
///
/// Passed to [`Kafko::create_topic_with_config`] / [`Kafko::open_with_config`].
/// `LogConfig::default()` gives reasonable values for general-purpose use;
/// override individual fields when your workload needs different sizing or
/// retention.
///
/// [`Kafko::create_topic_with_config`]: crate::Kafko::create_topic_with_config
/// [`Kafko::open_with_config`]: crate::Kafko::open_with_config
#[derive(Clone, Copy, Debug)]
pub struct LogConfig {
    /// Maximum size of a single segment file in bytes before rotation.
    /// Default: 1 GiB.
    pub segment_size_threshold: u64,
    /// Bytes between sparse-index entries. Smaller values speed up
    /// random-offset reads; larger values shrink the index file. Default: 4 KiB.
    pub index_interval: u64,
    /// Maximum age of a sealed segment before retention deletes it. `None`
    /// disables age-based retention. Default: `None`.
    pub max_segment_age: Option<Duration>,
    /// Maximum total disk usage per partition before retention starts deleting
    /// the oldest sealed segments. `None` disables byte-based retention. Default: `None`.
    pub max_partition_bytes: Option<u64>,
    /// How often the writer task runs the retention sweep. Default: 60 s.
    pub retention_check_interval: Duration,
    /// Maximum records per natural-batch flush from the writer task's inbox.
    /// Larger values let producers ride more aggressive coalescing. Default: 1024.
    pub batch_max_records: usize,
    /// Maximum bytes per natural-batch flush. Default: 64 KiB.
    pub batch_max_bytes: u64,
    /// Compression codec applied to record values on this topic.
    pub compression: Compression,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_size_threshold: 1 << 30,
            index_interval: 4 * 1024,
            max_segment_age: None,
            max_partition_bytes: None,
            retention_check_interval: Duration::from_secs(60),
            batch_max_records: 1024,
            batch_max_bytes: 64 * 1024,
            compression: Compression::None,
        }
    }
}

/// Append-only segment log backing one [`Partition`].
///
/// Owns the ordered list of [`Segment`] files and their [`SparseIndex`]es plus
/// the next-offset cursor. Not used directly by most callers — [`Partition`]
/// wraps a `Log` and serializes access to it via its writer task.
///
/// [`Partition`]: crate::Partition
/// [`Segment`]: crate::Segment
/// [`SparseIndex`]: crate::SparseIndex
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    segments: Vec<IndexedSegment>,
    next_offset: u64,
    // Scratch buffers reused across append_batch calls. The partition writer task
    // is the single mutator of Log (the actor-style single-writer-per-partition
    // invariant), so there is never a concurrent borrow. Cleared at the start of
    // every batch; their capacity grows to the largest batch's needs and then stays.
    encode_buf: BytesMut,
    actual_sizes: Vec<usize>,
}

struct IndexedSegment {
    segment: Segment,
    index: SparseIndex,
}

impl Log {
    /// Creates a fresh `Log` (no existing segments) at `dir`. Errors if the
    /// directory already contains a segment file.
    pub async fn create(dir: &Path, config: LogConfig) -> Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let segment = Segment::create(dir, 0).await?;
        let index = SparseIndex::create(dir, 0, config.index_interval).await?;
        Ok(Self {
            dir: dir.to_path_buf(),
            config,
            segments: vec![IndexedSegment { segment, index }],
            next_offset: 0,
            encode_buf: BytesMut::new(),
            actual_sizes: Vec::new(),
        })
    }

    /// Opens an existing log or creates a fresh one if the directory is empty.
    ///
    /// Recovers the active (last) segment by CRC-scanning the `.log` file and truncating
    /// torn or corrupted records at the tail. Rebuilds the active segment's sparse index
    /// from scratch. Trusts older segments and their existing indexes.
    pub async fn open(dir: &Path, config: LogConfig) -> Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let base_offsets = discover_segment_offsets(dir).await?;

        if base_offsets.is_empty() {
            return Self::create(dir, config).await;
        }

        let mut segments = Vec::with_capacity(base_offsets.len());
        for &base in &base_offsets[..base_offsets.len() - 1] {
            let segment = Segment::open(dir, base).await?;
            let index = SparseIndex::open(dir, base, config.index_interval).await?;
            segments.push(IndexedSegment { segment, index });
        }

        let active_base = *base_offsets.last().expect("base_offsets non-empty: is_empty branch returns above");
        let (active_segment, active_index, active_record_count) =
            recover_active_segment(dir, active_base, config.index_interval).await?;
        segments.push(IndexedSegment {
            segment: active_segment,
            index: active_index,
        });

        let next_offset = active_base + active_record_count;

        Ok(Self {
            dir: dir.to_path_buf(),
            config,
            segments,
            next_offset,
            encode_buf: BytesMut::new(),
            actual_sizes: Vec::new(),
        })
    }

    /// Returns the directory this log was opened against.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the [`LogConfig`] this log was opened with.
    pub fn config(&self) -> &LogConfig {
        &self.config
    }

    /// Returns the offset that will be assigned to the next appended record.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Returns the number of segments currently in the log (sealed + active).
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Returns the sum of all segment sizes in bytes — i.e., the on-disk
    /// footprint of this log's `.log` files (excluding the `.index` files).
    pub fn total_size(&self) -> u64 {
        self.segments.iter().map(|s| s.segment.size()).sum()
    }

    /// Appends a single record and returns its assigned offset. Rotates the
    /// active segment first if the record would push it past the size threshold.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn append(&mut self, record: Record) -> Result<u64> {
        let wire_size_estimate = record.wire_size();
        let compression = self.config.compression;

        let should_rotate = self
            .segments
            .last()
            .expect("segments invariant: never empty after create")
            .segment
            .would_overflow(wire_size_estimate, self.config.segment_size_threshold);

        if should_rotate {
            self.rotate_active_segment().await?;
        }

        let mut buf = BytesMut::with_capacity(wire_size_estimate);
        let actual_size = record.encode_with(&mut buf, compression)?;

        let active = self
            .segments
            .last_mut()
            .expect("segments invariant: never empty after create");
        let file_pos = active.segment.append(&buf).await?;

        let offset = self.next_offset;
        active
            .index
            .track_append(offset, file_pos, actual_size)
            .await?;

        self.next_offset += 1;
        Ok(offset)
    }

    /// Appends a batch of records in a single disk write. Returns the assigned offsets
    /// in order. Used by the partition actor to coalesce concurrent producer sends.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn append_batch(&mut self, records: Vec<Record>) -> Result<Vec<u64>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let total_wire_size: usize = records.iter().map(|r| r.wire_size()).sum();

        let should_rotate = self
            .segments
            .last()
            .expect("segments invariant: never empty after create")
            .segment
            .would_overflow(total_wire_size, self.config.segment_size_threshold);
        if should_rotate {
            self.rotate_active_segment().await?;
        }

        let compression = self.config.compression;
        self.encode_buf.clear();
        self.encode_buf.reserve(total_wire_size);
        self.actual_sizes.clear();
        let mut offsets = Vec::with_capacity(records.len());
        let mut current_offset = self.next_offset;
        for record in records {
            offsets.push(current_offset);
            current_offset += 1;
            let actual = record.encode_with(&mut self.encode_buf, compression)?;
            self.actual_sizes.push(actual);
        }

        let active = self
            .segments
            .last_mut()
            .expect("segments invariant: never empty after create");
        let mut file_pos = active.segment.append(&self.encode_buf).await?;

        for (i, &size) in self.actual_sizes.iter().enumerate() {
            active
                .index
                .track_append(offsets[i], file_pos, size)
                .await?;
            file_pos += size as u64;
        }

        self.next_offset = current_offset;
        Ok(offsets)
    }

    /// Reads the record at `offset`, returning `Ok(None)` if the offset is past
    /// the high-water-mark. Locates the containing segment via the sparse
    /// index, then decodes forward to the requested offset.
    pub async fn read_record_at(&mut self, offset: u64) -> Result<Option<Record>> {
        if offset >= self.next_offset {
            return Ok(None);
        }

        let seg_idx = self.find_segment_index(offset);
        let segment_pair = &mut self.segments[seg_idx];
        let (file_pos, starting_offset) = segment_pair.index.lookup(offset);

        let remaining = segment_pair.segment.size().saturating_sub(file_pos);
        if remaining == 0 {
            return Ok(None);
        }

        // v0.1: read the entire remainder of the segment. For large segments this is
        // wasteful; a chunked read pattern is a v0.2 optimization.
        let mut buf = vec![0u8; remaining as usize];
        let n = segment_pair.segment.read_at(file_pos, &mut buf).await?;
        let mut slice: &[u8] = &buf[..n];

        let mut current_offset = starting_offset;
        while !slice.is_empty() {
            let record = match Record::decode(&mut slice) {
                Ok(r) => r,
                Err(KafkoError::Truncated { .. }) => return Ok(None),
                Err(e) => return Err(e),
            };
            if current_offset == offset {
                return Ok(Some(record));
            }
            current_offset += 1;
        }

        Ok(None)
    }

    /// Fsyncs the active segment and its sparse index. Returns only after the
    /// kernel reports the writes are on disk.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn sync(&mut self) -> Result<()> {
        let active = self
            .segments
            .last_mut()
            .expect("segments invariant: never empty after create");
        active.segment.sync().await?;
        active.index.sync().await?;
        Ok(())
    }

    /// Deletes old segments based on `LogConfig.max_segment_age` and `max_partition_bytes`.
    /// Never deletes the active (last) segment. Returns the number of segments deleted.
    pub async fn apply_retention(&mut self) -> Result<u64> {
        let mut deleted = 0u64;
        let now_ms = current_timestamp_ms();

        if let Some(max_age) = self.config.max_segment_age {
            let max_age_ms = max_age.as_millis() as i64;
            let cutoff = now_ms.saturating_sub(max_age_ms);
            while self.segments.len() > 1 {
                let mtime = self.segments[0].segment.last_modified_ms().await?;
                if mtime < cutoff {
                    self.delete_oldest_segment().await?;
                    deleted += 1;
                } else {
                    break;
                }
            }
        }

        if let Some(max_bytes) = self.config.max_partition_bytes {
            while self.segments.len() > 1 && self.total_size() > max_bytes {
                self.delete_oldest_segment().await?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// Seals the active segment and creates a fresh one. The previous segment's
    /// `.log` and `.index` files are fsynced BEFORE the new segment exists.
    ///
    /// This closes a data-loss window: recovery on `Log::open` only re-verifies the
    /// last segment via CRC scan; older sealed segments are trusted. If we rotated
    /// without first flushing the previous segment, a power loss between rotation
    /// and the OS's automatic writeback could leave the sealed segment with an
    /// unrecoverable truncated tail — and recovery wouldn't notice. Offset numbering
    /// would silently skip records that were already acked to the producer.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn rotate_active_segment(&mut self) -> Result<()> {
        {
            let active = self.segments.last_mut().expect("segments invariant: never empty after create");
            active.segment.sync().await?;
            active.index.sync().await?;
        }
        let new_segment = Segment::create(&self.dir, self.next_offset).await?;
        let new_index =
            SparseIndex::create(&self.dir, self.next_offset, self.config.index_interval).await?;
        self.segments.push(IndexedSegment {
            segment: new_segment,
            index: new_index,
        });
        Ok(())
    }

    async fn delete_oldest_segment(&mut self) -> Result<()> {
        let oldest = self.segments.remove(0);
        let log_path = oldest.segment.path().to_path_buf();
        let index_path = log_path.with_extension("index");
        drop(oldest);

        tokio::fs::remove_file(&log_path).await?;
        match tokio::fs::remove_file(&index_path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    fn find_segment_index(&self, offset: u64) -> usize {
        match self
            .segments
            .binary_search_by_key(&offset, |s| s.segment.base_offset())
        {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        }
    }
}

fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

async fn discover_segment_offsets(dir: &Path) -> Result<Vec<u64>> {
    let mut offsets = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(s) => s,
            None => continue,
        };
        if let Some(prefix) = name_str.strip_suffix(".log")
            && let Ok(base) = prefix.parse::<u64>()
        {
            offsets.push(base);
        }
    }
    offsets.sort();
    Ok(offsets)
}

async fn recover_active_segment(
    dir: &Path,
    base_offset: u64,
    index_interval: u64,
) -> Result<(Segment, SparseIndex, u64)> {
    let mut segment = Segment::open(dir, base_offset).await?;
    let last_valid_pos = scan_for_last_valid_position(&mut segment).await?;
    if last_valid_pos < segment.size() {
        segment.truncate(last_valid_pos).await?;
    }

    let index_path = dir.join(format!("{:020}.index", base_offset));
    match tokio::fs::remove_file(&index_path).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut index = SparseIndex::create(dir, base_offset, index_interval).await?;

    let record_count = replay_records_to_index(&mut segment, base_offset, &mut index).await?;
    Ok((segment, index, record_count))
}

async fn scan_for_last_valid_position(segment: &mut Segment) -> Result<u64> {
    let size = segment.size() as usize;
    if size == 0 {
        return Ok(0);
    }
    let mut buf = vec![0u8; size];
    segment.read_at(0, &mut buf).await?;

    let mut slice: &[u8] = &buf;
    let mut last_valid_pos = 0u64;
    while !slice.is_empty() {
        let start_len = slice.len();
        match Record::decode(&mut slice) {
            Ok(_) => {
                let consumed = (start_len - slice.len()) as u64;
                last_valid_pos += consumed;
            }
            Err(KafkoError::Truncated { .. })
            | Err(KafkoError::CrcMismatch { .. })
            | Err(KafkoError::InvalidLength(_)) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(last_valid_pos)
}

async fn replay_records_to_index(
    segment: &mut Segment,
    base_offset: u64,
    index: &mut SparseIndex,
) -> Result<u64> {
    let size = segment.size() as usize;
    if size == 0 {
        return Ok(0);
    }
    let mut buf = vec![0u8; size];
    segment.read_at(0, &mut buf).await?;

    let mut slice: &[u8] = &buf;
    let mut file_pos = 0u64;
    let mut count = 0u64;
    while !slice.is_empty() {
        let start_len = slice.len();
        let _record = Record::decode(&mut slice)?;
        let consumed = (start_len - slice.len()) as u64;
        let offset = base_offset + count;
        index
            .track_append(offset, file_pos, consumed as usize)
            .await?;
        file_pos += consumed;
        count += 1;
    }
    Ok(count)
}
