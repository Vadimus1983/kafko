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
}
