use crate::compression::Compression;
use std::path::PathBuf;
use thiserror::Error;

/// Result alias for kafko operations.
pub type Result<T> = std::result::Result<T, KafkoError>;

/// All errors surfaced by kafko's public API.
///
/// Wraps `std::io::Error` for filesystem failures and carries typed variants
/// for the broker's own error conditions. Callers can match on
/// [`KafkoError::Io`]'s inner `ErrorKind` (e.g. `StorageFull`) to decide
/// whether to retry; other variants are terminal for the operation that
/// produced them but do not necessarily take the broker offline.
#[derive(Debug, Error)]
pub enum KafkoError {
    /// Wraps an underlying I/O error from segment or index access.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Record decoder ran out of bytes before the record was complete.
    #[error("record decode: input truncated (need {needed} more bytes)")]
    Truncated {
        /// Number of additional bytes the decoder still needed.
        needed: usize,
    },

    /// On-disk record's CRC32 didn't match the recomputed value — corruption
    /// or torn-write at the segment tail.
    #[error("record decode: CRC mismatch (expected {expected:#x}, got {actual:#x})")]
    CrcMismatch {
        /// CRC32 stored in the record header.
        expected: u32,
        /// CRC32 recomputed from the payload bytes.
        actual: u32,
    },

    /// Record's length field is impossible (e.g. zero or shorter than the
    /// minimum header).
    #[error("record decode: invalid length field ({0})")]
    InvalidLength(u32),

    /// Partition's writer task has exited (clean shutdown). New operations
    /// against the partition will keep returning this until the broker is
    /// re-opened.
    #[error("partition closed")]
    Closed,

    /// A topic with the given name already exists on this broker.
    #[error("topic '{0}' already exists")]
    TopicAlreadyExists(String),

    /// No topic with the given name exists on this broker.
    #[error("topic '{0}' not found")]
    TopicNotFound(String),

    /// `delete_topic` was called while outstanding `Producer` or `Consumer`
    /// handles for the topic still exist. Drop those handles and retry.
    #[error("topic '{0}' in use; drop all producers and consumers before deleting")]
    TopicInUse(String),

    /// On-disk record carries a compression-flag byte kafko doesn't recognize.
    /// Indicates either corruption or data written by a newer kafko version.
    #[error("unknown compression flag: {0}")]
    UnknownCompression(u8),

    /// LZ4 or zstd refused to decompress the record's payload.
    #[error("decompression failed")]
    DecompressionFailed,

    /// A record was produced or read with a [`Compression`] variant whose
    /// Cargo feature is not enabled in this build of kafko. Rebuild with
    /// `--features compression-lz4` / `compression-zstd` / `compression-all`
    /// to handle this codec.
    #[error("compression codec {0:?} is not enabled in this build; rebuild kafko with the matching `compression-*` feature")]
    CompressionUnavailable(Compression),

    /// Another `Kafko::open` already holds the advisory lock on this data dir.
    /// Either close the existing broker, or point at a different directory.
    #[error("data directory {} is already opened by another Kafko instance (file lock held)", .path.display())]
    AlreadyOpen {
        /// Path of the data directory whose lock could not be acquired.
        path: PathBuf,
    },

    /// Partition writer task panicked. The payload string is the panic
    /// message; the partition will not accept further operations.
    #[error("partition writer task panicked: {payload}")]
    PartitionPanicked {
        /// Panic payload as a string (recovered from `Any` downcasts).
        payload: String,
    },

    /// A topic was requested with zero partitions. Topics need at least one.
    #[error("invalid partition count {0}: a topic needs at least 1 partition")]
    InvalidPartitionCount(u32),

    /// A consumer-group name was empty, `.`/`..`, or contained characters outside
    /// `[A-Za-z0-9._-]` (group names are used as on-disk filenames).
    #[error("invalid consumer group name {0:?}: use non-empty [A-Za-z0-9._-]")]
    InvalidGroupName(String),

    /// A topic directory on disk does not have the expected
    /// `<topic>/<partition-index>/` layout — no numeric partition subdirectories
    /// were found, or the indices are not contiguous from 0. Data directories
    /// written by kafko <= 0.2 (which stored segments directly under the topic
    /// dir) trip this; they are not compatible with 0.3's partitioned layout.
    #[error("topic '{topic}' has an invalid on-disk layout: {detail}")]
    InvalidTopicLayout {
        /// The topic whose directory could not be interpreted.
        topic: String,
        /// Human-readable description of what was wrong.
        detail: String,
    },
}

// `std::io::Error` is not `Clone` in stable Rust, so we synthesize an equivalent
// fresh `io::Error` carrying the same `ErrorKind` and the same display string.
// Every other variant is trivially cloneable. Used by the partition writer to
// fan one underlying batch failure out to every waiter that was coalesced into
// that batch — without losing the error kind, which callers may want to match on
// to decide whether the failure is transient (e.g. `StorageFull`) and worth a retry.
impl Clone for KafkoError {
    fn clone(&self) -> Self {
        match self {
            KafkoError::Io(e) => KafkoError::Io(std::io::Error::new(e.kind(), e.to_string())),
            KafkoError::Truncated { needed } => KafkoError::Truncated { needed: *needed },
            KafkoError::CrcMismatch { expected, actual } => KafkoError::CrcMismatch {
                expected: *expected,
                actual: *actual,
            },
            KafkoError::InvalidLength(n) => KafkoError::InvalidLength(*n),
            KafkoError::Closed => KafkoError::Closed,
            KafkoError::TopicAlreadyExists(s) => KafkoError::TopicAlreadyExists(s.clone()),
            KafkoError::TopicNotFound(s) => KafkoError::TopicNotFound(s.clone()),
            KafkoError::TopicInUse(s) => KafkoError::TopicInUse(s.clone()),
            KafkoError::UnknownCompression(b) => KafkoError::UnknownCompression(*b),
            KafkoError::DecompressionFailed => KafkoError::DecompressionFailed,
            KafkoError::CompressionUnavailable(c) => KafkoError::CompressionUnavailable(*c),
            KafkoError::AlreadyOpen { path } => KafkoError::AlreadyOpen {
                path: path.clone(),
            },
            KafkoError::PartitionPanicked { payload } => KafkoError::PartitionPanicked {
                payload: payload.clone(),
            },
            KafkoError::InvalidPartitionCount(n) => KafkoError::InvalidPartitionCount(*n),
            KafkoError::InvalidGroupName(s) => KafkoError::InvalidGroupName(s.clone()),
            KafkoError::InvalidTopicLayout { topic, detail } => KafkoError::InvalidTopicLayout {
                topic: topic.clone(),
                detail: detail.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    // The `Io` arm is the only non-trivial clone: `std::io::Error` is not `Clone`,
    // so the impl synthesizes a fresh error. Callers match on the inner `ErrorKind`
    // to decide whether a failure is retryable, so the clone MUST preserve the kind
    // (and the display string). That contract is the whole reason `Clone` is
    // hand-written, so it gets its own test.
    #[test]
    fn clone_io_preserves_kind_and_message() {
        let original = KafkoError::Io(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
        match original.clone() {
            KafkoError::Io(e) => {
                assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
                assert_eq!(e.to_string(), "denied");
            }
            other => panic!("expected Io after clone, got {other:?}"),
        }
    }

    // Every other variant is a field-by-field clone. Round-trip a representative of
    // each shape (tuple, struct, unit) and confirm the clone is indistinguishable
    // from the original by Debug + Display — this also guards against a new variant
    // being added without a matching clone arm.
    #[test]
    fn clone_round_trips_every_variant() {
        let cases = [
            KafkoError::Io(io::Error::new(io::ErrorKind::Other, "io")),
            KafkoError::Truncated { needed: 7 },
            KafkoError::CrcMismatch {
                expected: 0xdead_beef,
                actual: 0x0bad_f00d,
            },
            KafkoError::InvalidLength(3),
            KafkoError::Closed,
            KafkoError::TopicAlreadyExists("orders".into()),
            KafkoError::TopicNotFound("orders".into()),
            KafkoError::TopicInUse("orders".into()),
            KafkoError::UnknownCompression(9),
            KafkoError::DecompressionFailed,
            KafkoError::CompressionUnavailable(Compression::None),
            KafkoError::AlreadyOpen {
                path: PathBuf::from("data"),
            },
            KafkoError::PartitionPanicked {
                payload: "boom".into(),
            },
            KafkoError::InvalidPartitionCount(0),
            KafkoError::InvalidGroupName("bad/name".into()),
            KafkoError::InvalidTopicLayout {
                topic: "orders".into(),
                detail: "no partition subdirectories".into(),
            },
        ];

        for original in &cases {
            let cloned = original.clone();
            assert_eq!(format!("{original:?}"), format!("{cloned:?}"));
            assert_eq!(original.to_string(), cloned.to_string());
        }
    }
}
