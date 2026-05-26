use crate::error::Result;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const FILENAME_DIGITS: usize = 20;
const FILENAME_EXTENSION: &str = "index";
const ENTRY_SIZE: usize = 8;

/// On-disk sparse offset index mapping `record offset → file position`.
///
/// One entry every `interval` bytes (configured via [`LogConfig::index_interval`]),
/// so lookups locate the closest indexed offset at or before the target and
/// the caller decodes forward from there. Used by [`Log::read_record_at`] to
/// avoid scanning a full segment.
///
/// [`LogConfig::index_interval`]: crate::LogConfig::index_interval
/// [`Log::read_record_at`]: crate::Log::read_record_at
pub struct SparseIndex {
    base_offset: u64,
    path: PathBuf,
    file: File,
    entries: Vec<IndexEntry>,
    bytes_since_last_entry: u64,
    interval: u64,
}

struct IndexEntry {
    relative_offset: u32,
    file_position: u32,
}

impl SparseIndex {
    /// Creates a fresh empty index file at `dir/<base_offset>.index`. Errors
    /// if a file at that path already exists.
    pub async fn create(dir: &Path, base_offset: u64, interval: u64) -> Result<Self> {
        let path = index_path(dir, base_offset);
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            base_offset,
            path,
            file,
            entries: Vec::new(),
            bytes_since_last_entry: 0,
            interval,
        })
    }

    /// Opens an existing index file at `dir/<base_offset>.index` and loads its
    /// entries into memory. Errors if the file is missing.
    pub async fn open(dir: &Path, base_offset: u64, interval: u64) -> Result<Self> {
        let path = index_path(dir, base_offset);
        let bytes = std::fs::read(&path)?;
        let mut entries = Vec::with_capacity(bytes.len() / ENTRY_SIZE);
        for chunk in bytes.chunks_exact(ENTRY_SIZE) {
            let relative_offset = u32::from_be_bytes(
                chunk[0..4]
                    .try_into()
                    .expect("chunks_exact(8) guarantees chunk[0..4] is [u8; 4]"),
            );
            let file_position = u32::from_be_bytes(
                chunk[4..8]
                    .try_into()
                    .expect("chunks_exact(8) guarantees chunk[4..8] is [u8; 4]"),
            );
            entries.push(IndexEntry {
                relative_offset,
                file_position,
            });
        }
        let file = OpenOptions::new().append(true).open(&path)?;
        Ok(Self {
            base_offset,
            path,
            file,
            entries,
            bytes_since_last_entry: 0,
            interval,
        })
    }

    /// Returns the absolute offset of the first record in the segment this
    /// index covers.
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Returns the path of the underlying `.index` file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the configured bytes-between-entries interval.
    pub fn interval(&self) -> u64 {
        self.interval
    }

    /// Returns the number of entries currently in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the index has no entries yet.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Records that a record at `absolute_offset` was just appended at
    /// `file_position`, occupying `record_size` bytes. Adds an index entry if
    /// the running byte count since the last entry exceeds `interval` (or if
    /// this is the first entry).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn track_append(
        &mut self,
        absolute_offset: u64,
        file_position: u64,
        record_size: usize,
    ) -> Result<()> {
        let need_entry = self.entries.is_empty() || self.bytes_since_last_entry >= self.interval;
        if need_entry {
            let relative_offset: u32 = (absolute_offset - self.base_offset)
                .try_into()
                .expect("relative offset exceeds u32 (segment too large?)");
            let file_position_u32: u32 = file_position
                .try_into()
                .expect("file position exceeds u32 (segment too large?)");
            let entry = IndexEntry {
                relative_offset,
                file_position: file_position_u32,
            };
            self.write_entry(&entry)?;
            self.entries.push(entry);
            self.bytes_since_last_entry = 0;
        }
        self.bytes_since_last_entry += record_size as u64;
        Ok(())
    }

    /// Returns `(file_position, starting_offset)` — the file position of the closest
    /// indexed entry at or before `absolute_offset`, and the absolute offset of the
    /// record at that position. Callers seek to `file_position` and decode forward,
    /// counting records from `starting_offset` to find the target.
    pub fn lookup(&self, absolute_offset: u64) -> (u64, u64) {
        if absolute_offset < self.base_offset {
            return (0, self.base_offset);
        }
        let relative: u32 = match (absolute_offset - self.base_offset).try_into() {
            Ok(r) => r,
            Err(_) => u32::MAX,
        };
        match self
            .entries
            .binary_search_by_key(&relative, |e| e.relative_offset)
        {
            Ok(i) => {
                let e = &self.entries[i];
                (
                    e.file_position as u64,
                    self.base_offset + e.relative_offset as u64,
                )
            }
            Err(0) => (0, self.base_offset),
            Err(i) => {
                let e = &self.entries[i - 1];
                (
                    e.file_position as u64,
                    self.base_offset + e.relative_offset as u64,
                )
            }
        }
    }

    /// Fsyncs the index file to disk via `sync_data`.
    pub async fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    fn write_entry(&mut self, entry: &IndexEntry) -> Result<()> {
        let mut buf = [0u8; ENTRY_SIZE];
        buf[0..4].copy_from_slice(&entry.relative_offset.to_be_bytes());
        buf[4..8].copy_from_slice(&entry.file_position.to_be_bytes());
        self.file.write_all(&buf)?;
        Ok(())
    }
}

fn index_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!(
        "{:0width$}.{ext}",
        base_offset,
        width = FILENAME_DIGITS,
        ext = FILENAME_EXTENSION
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_path_is_zero_padded_20_digits() {
        let path = index_path(Path::new("data"), 42);
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "00000000000000000042.index"
        );
    }
}
