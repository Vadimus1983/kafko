//! In-process Kafka-semantics log: topics, partitions, offset-based reads, replay, retention.

pub mod broker;
pub mod compression;
pub mod consumer;
pub mod error;
pub mod log;
pub mod partition;
pub mod producer;
pub mod record;
pub mod segment;
pub mod sparse_index;

pub use broker::Kafko;
pub use compression::Compression;
pub use consumer::Consumer;
pub use error::{KafkoError, Result};
pub use log::{Log, LogConfig};
pub use partition::Partition;
pub use producer::Producer;
pub use record::Record;
pub use segment::Segment;
pub use sparse_index::SparseIndex;
