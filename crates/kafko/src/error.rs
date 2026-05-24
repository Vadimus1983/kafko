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
            KafkoError::AlreadyOpen { path } => KafkoError::AlreadyOpen {
                path: path.clone(),
            },
            KafkoError::PartitionPanicked { payload } => KafkoError::PartitionPanicked {
                payload: payload.clone(),
            },
        }
    }
}
