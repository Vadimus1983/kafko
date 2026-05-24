#![deny(missing_docs)]
#![deny(clippy::await_holding_lock)]
//! In-process log with Kafka-like semantics for Rust.
//!
//! kafko exists for use cases where your data never needs to leave the
//! process: embedded event sourcing, edge buffers, durable in-process
//! pub/sub, deterministic integration tests without Docker or a broker,
//! single-binary services that want a real log instead of a `VecDeque<T>`
//! under a mutex.
//!
//! # Quickstart
//!
//! ```no_run
//! use bytes::Bytes;
//! use kafko::Kafko;
//!
//! #[tokio::main]
//! async fn main() -> kafko::Result<()> {
//!     let broker = Kafko::open("./data").await?;
//!     broker.create_topic("orders").await?;
//!
//!     // Produce
//!     let producer = broker.producer_for("orders").await?;
//!     let offset = producer.send(None, Bytes::from("order-1")).await?;
//!     println!("appended at offset {offset}");
//!
//!     // Consume from the beginning
//!     let mut consumer = broker.consumer_for("orders").await?;
//!     consumer.seek(0);
//!     let record = consumer.next_record().await?;
//!     println!("read: {:?}", record.value());
//!
//!     broker.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Durability
//!
//! `Producer::send().await` resolves once the record is in the OS file
//! (page cache) — the same contract as Kafka `acks=1`. Records survive
//! process crashes (panic, SIGKILL, OOM) but may be lost on OS panic or
//! power loss until the kernel writes back. Call [`Kafko::shutdown`] for
//! a hard durability boundary, or [`Partition::sync`] for mid-life fsync.
//!
//! The recovery path on next `Kafko::open` CRC-scans the active segment
//! and truncates any torn writes at the tail.
//!
//! See the [project README](https://github.com/Vadimus1983/kafko) for
//! benchmarks, the full architecture diagram, and the v0.2 roadmap.

mod broker;
mod compression;
mod consumer;
mod error;
mod log;
mod partition;
mod producer;
mod record;
mod segment;
mod sparse_index;

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
