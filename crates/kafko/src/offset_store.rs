use crate::error::{KafkoError, Result};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

const TEMP_SUFFIX: &str = ".tmp";

/// Durable per-`(topic, group)` committed-offset store.
///
/// Holds one offset per partition for a single consumer group, persisted to
/// `<offsets_dir>/<group>`. A consumer loads its group's committed offsets on
/// open (resuming where it left off) and writes them back via [`commit`].
///
/// The on-disk form is CRC-framed, little-endian:
/// `[crc32: u32][count: u32][ (partition: u32, offset: u64) × count ]`, with the
/// CRC covering everything after it. A torn or corrupt file is treated as "no
/// commit" (all offsets 0) rather than an error — a bad commit must never wedge a
/// consumer. Writes are atomic: a temp file is fsynced and then renamed over the
/// target, so a crash mid-commit leaves either the old file or the new one.
///
/// [`commit`]: OffsetStore::commit
pub(crate) struct OffsetStore {
    file_path: PathBuf,
    temp_path: PathBuf,
    group: String,
    committed: Vec<u64>,
}

impl OffsetStore {
    /// Opens (creating the dir if needed) the offset store for `group` with room
    /// for `partition_count` partitions, loading any previously committed offsets.
    /// Errors with [`KafkoError::InvalidGroupName`] for an empty, `.`/`..`, or
    /// non-`[A-Za-z0-9._-]` group name.
    pub(crate) async fn open(
        offsets_dir: &Path,
        group: &str,
        partition_count: u32,
    ) -> Result<Self> {
        validate_group_name(group)?;
        tokio::fs::create_dir_all(offsets_dir).await?;

        let file_path = offsets_dir.join(group);
        let temp_path = offsets_dir.join(format!("{group}{TEMP_SUFFIX}"));
        let n = partition_count as usize;

        let committed = match tokio::fs::read(&file_path).await {
            Ok(bytes) => decode(&bytes, n),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => vec![0; n],
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            file_path,
            temp_path,
            group: group.to_string(),
            committed,
        })
    }

    /// The committed offset per partition, indexed by partition id.
    pub(crate) fn committed(&self) -> &[u64] {
        &self.committed
    }

    /// The group this store belongs to.
    pub(crate) fn group(&self) -> &str {
        &self.group
    }

    /// Durably persists `offsets` (one per partition) as the group's committed
    /// position. Atomic: writes a temp file, fsyncs it, then renames it over the
    /// live file.
    pub(crate) async fn commit(&mut self, offsets: &[u64]) -> Result<()> {
        let encoded = encode(offsets);
        {
            let mut f = tokio::fs::File::create(&self.temp_path).await?;
            f.write_all(&encoded).await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&self.temp_path, &self.file_path).await?;
        self.committed = offsets.to_vec();
        Ok(())
    }
}

fn validate_group_name(group: &str) -> Result<()> {
    let invalid = group.is_empty()
        || group == "."
        || group == ".."
        || !group
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if invalid {
        return Err(KafkoError::InvalidGroupName(group.to_string()));
    }
    Ok(())
}

fn encode(offsets: &[u64]) -> Vec<u8> {
    let mut body = Vec::with_capacity(4 + offsets.len() * 12);
    body.extend_from_slice(&(offsets.len() as u32).to_le_bytes());
    for (partition, &offset) in offsets.iter().enumerate() {
        body.extend_from_slice(&(partition as u32).to_le_bytes());
        body.extend_from_slice(&offset.to_le_bytes());
    }
    let crc = crc32fast::hash(&body);
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Decodes a committed-offset file into a `partition_count`-length vector.
/// Any structural problem (short, bad CRC, truncated entries) yields all-zeros —
/// a corrupt commit degrades to "start from the beginning", never an error.
fn decode(bytes: &[u8], partition_count: usize) -> Vec<u64> {
    let mut result = vec![0u64; partition_count];
    if bytes.len() < 8 {
        return result;
    }
    let crc = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes"));
    let body = &bytes[4..];
    if crc32fast::hash(body) != crc {
        return result;
    }
    let count = u32::from_le_bytes(body[0..4].try_into().expect("4 bytes")) as usize;
    if body.len() < 4 + count * 12 {
        return result;
    }
    for i in 0..count {
        let base = 4 + i * 12;
        let partition =
            u32::from_le_bytes(body[base..base + 4].try_into().expect("4 bytes")) as usize;
        let offset = u64::from_le_bytes(body[base + 4..base + 12].try_into().expect("8 bytes"));
        if partition < partition_count {
            result[partition] = offset;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn encode_decode_roundtrip() {
        let offsets = vec![3u64, 0, 17, 1_000_000];
        let decoded = decode(&encode(&offsets), 4);
        assert_eq!(decoded, offsets);
    }

    #[test]
    fn decode_rejects_corrupt_and_short_inputs() {
        assert_eq!(decode(&[], 3), vec![0, 0, 0]);
        assert_eq!(decode(&[1, 2, 3], 3), vec![0, 0, 0]);
        // Valid frame with a flipped payload byte fails CRC -> zeros.
        let mut bad = encode(&[5, 6, 7]);
        let last = bad.len() - 1;
        bad[last] ^= 0xFF;
        assert_eq!(decode(&bad, 3), vec![0, 0, 0]);
        // Truncated entries (claims 3, only has 1) -> zeros.
        let full = encode(&[5, 6, 7]);
        let truncated = &full[..full.len() - 12];
        assert_eq!(decode(truncated, 3), vec![0, 0, 0]);
    }

    #[test]
    fn decode_ignores_out_of_range_partitions() {
        // Encoded for 4 partitions, decoded expecting only 2 — extra partitions dropped.
        let decoded = decode(&encode(&[1, 2, 3, 4]), 2);
        assert_eq!(decoded, vec![1, 2]);
    }

    #[tokio::test]
    async fn open_commit_reopen_resumes() {
        let dir = TempDir::new().unwrap();
        {
            let mut store = OffsetStore::open(dir.path(), "billing", 3).await.unwrap();
            assert_eq!(store.committed(), &[0, 0, 0]);
            store.commit(&[7, 0, 42]).await.unwrap();
        }
        let store = OffsetStore::open(dir.path(), "billing", 3).await.unwrap();
        assert_eq!(store.committed(), &[7, 0, 42]);
    }

    #[tokio::test]
    async fn groups_are_independent() {
        let dir = TempDir::new().unwrap();
        OffsetStore::open(dir.path(), "a", 2)
            .await
            .unwrap()
            .commit(&[1, 1])
            .await
            .unwrap();
        OffsetStore::open(dir.path(), "b", 2)
            .await
            .unwrap()
            .commit(&[9, 9])
            .await
            .unwrap();
        assert_eq!(OffsetStore::open(dir.path(), "a", 2).await.unwrap().committed(), &[1, 1]);
        assert_eq!(OffsetStore::open(dir.path(), "b", 2).await.unwrap().committed(), &[9, 9]);
    }

    #[tokio::test]
    async fn corrupt_file_resumes_from_zero() {
        let dir = TempDir::new().unwrap();
        tokio::fs::write(dir.path().join("g"), b"garbage").await.unwrap();
        let store = OffsetStore::open(dir.path(), "g", 2).await.unwrap();
        assert_eq!(store.committed(), &[0, 0]);
    }

    #[tokio::test]
    async fn invalid_group_names_rejected() {
        let dir = TempDir::new().unwrap();
        for bad in ["", ".", "..", "a/b", "a b", "grp:1"] {
            assert!(
                matches!(
                    OffsetStore::open(dir.path(), bad, 1).await,
                    Err(KafkoError::InvalidGroupName(_))
                ),
                "group name {bad:?} should be rejected"
            );
        }
    }
}
