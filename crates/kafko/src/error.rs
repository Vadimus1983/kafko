use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, KafkoError>;

#[derive(Debug, Error)]
pub enum KafkoError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("record decode: input truncated (need {needed} more bytes)")]
    Truncated { needed: usize },

    #[error("record decode: CRC mismatch (expected {expected:#x}, got {actual:#x})")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("record decode: invalid length field ({0})")]
    InvalidLength(u32),

    #[error("partition closed")]
    Closed,

    #[error("topic '{0}' already exists")]
    TopicAlreadyExists(String),

    #[error("topic '{0}' not found")]
    TopicNotFound(String),

    #[error("topic '{0}' in use; drop all producers and consumers before deleting")]
    TopicInUse(String),

    #[error("unknown compression flag: {0}")]
    UnknownCompression(u8),

    #[error("decompression failed")]
    DecompressionFailed,

    #[error("data directory {} is already opened by another Kafko instance (file lock held)", .path.display())]
    AlreadyOpen { path: PathBuf },

    #[error("partition writer task panicked: {payload}")]
    PartitionPanicked { payload: String },
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
