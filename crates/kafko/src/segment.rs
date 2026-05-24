use crate::error::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const FILENAME_DIGITS: usize = 20;
const FILENAME_EXTENSION: &str = "log";

pub struct Segment {
    base_offset: u64,
    path: PathBuf,
    file: File,
    size: u64,
    // Tracked cursor position to avoid redundant seeks. None = unknown, force seek.
    //
    // This optimization is sound ONLY because the partition writer task owns this
    // Segment exclusively (the single-writer-per-partition invariant). With one
    // owner, no other task can change `file`'s position between our seek and the
    // following read/write — so we can skip the seek when we already know we're
    // at the requested position. If a second concurrent caller were ever to touch
    // `file` (currently impossible by construction), this field would become a
    // correctness hazard: the cached cursor could disagree with the kernel's view
    // and a "no seek needed" branch would read or write at the wrong offset.
    cursor: Option<u64>,
}

impl Segment {
    pub async fn create(dir: &Path, base_offset: u64) -> Result<Self> {
        let path = segment_path(dir, base_offset);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            base_offset,
            path,
            file,
            size: 0,
            cursor: Some(0),
        })
    }

    pub async fn open(dir: &Path, base_offset: u64) -> Result<Self> {
        let path = segment_path(dir, base_offset);
        let metadata = std::fs::metadata(&path)?;
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        Ok(Self {
            base_offset,
            path,
            file,
            size: metadata.len(),
            cursor: Some(0),
        })
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn would_overflow(&self, additional: usize, threshold: u64) -> bool {
        self.size + additional as u64 > threshold
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn append(&mut self, bytes: &[u8]) -> Result<u64> {
        let file_pos = self.size;
        self.ensure_cursor(file_pos)?;
        self.file.write_all(bytes)?;
        self.cursor = Some(file_pos + bytes.len() as u64);
        self.size = file_pos + bytes.len() as u64;
        Ok(file_pos)
    }

    pub async fn read_at(&mut self, file_pos: u64, into: &mut [u8]) -> Result<usize> {
        self.ensure_cursor(file_pos)?;
        let n = self.file.read(into)?;
        self.cursor = Some(file_pos + n as u64);
        Ok(n)
    }

    pub async fn truncate(&mut self, new_size: u64) -> Result<()> {
        self.file.set_len(new_size)?;
        self.file.sync_data()?;
        self.size = new_size;
        // truncate may leave cursor past EOF on some platforms; force re-seek next op
        self.cursor = None;
        Ok(())
    }

    fn ensure_cursor(&mut self, pos: u64) -> Result<()> {
        if self.cursor != Some(pos) {
            self.file.seek(SeekFrom::Start(pos))?;
            self.cursor = Some(pos);
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub async fn last_modified_ms(&self) -> Result<i64> {
        let metadata = std::fs::metadata(&self.path)?;
        let modified = metadata.modified()?;
        let ms = modified
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Ok(ms)
    }
}

fn segment_path(dir: &Path, base_offset: u64) -> PathBuf {
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
    fn segment_path_is_zero_padded_20_digits() {
        let path = segment_path(Path::new("data"), 42);
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "00000000000000000042.log"
        );

        let path = segment_path(Path::new("data"), 0);
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "00000000000000000000.log"
        );

        let path = segment_path(Path::new("data"), 10_000_000);
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "00000000000010000000.log"
        );
    }
}
