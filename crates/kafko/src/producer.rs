use crate::error::Result;
use crate::partition::Partition;
use crate::record::Record;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Cheap handle that appends records to a single topic partition.
///
/// `Producer` is `Clone` (wraps `Arc<Partition>`); cloning is free and every
/// clone writes to the same underlying partition. Records are timestamped at
/// `send`-time and routed through the partition's writer task, which
/// serializes all writes — there is no producer-side ordering.
#[derive(Clone)]
pub struct Producer {
    partition: Arc<Partition>,
}

impl Producer {
    /// Wraps a [`Partition`] in a `Producer`. Prefer [`Kafko::producer_for`]
    /// which looks the partition up by topic name.
    ///
    /// [`Kafko::producer_for`]: crate::Kafko::producer_for
    pub fn new(partition: Arc<Partition>) -> Self {
        Self { partition }
    }

    /// Appends a record to the topic and returns its assigned offset.
    ///
    /// Resolves once the bytes are in the OS file (page cache) — the same
    /// durability contract as Kafka `acks=1`. See the crate-level docs for
    /// the full durability story.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bytes::Bytes;
    /// use kafko::Kafko;
    ///
    /// # async fn run() -> kafko::Result<()> {
    /// let broker = Kafko::open("./data").await?;
    /// broker.create_topic("orders").await?;
    /// let producer = broker.producer_for("orders").await?;
    ///
    /// // No key, raw value
    /// let offset = producer.send(None, Bytes::from("order-1")).await?;
    /// assert_eq!(offset, 0);
    ///
    /// // With a key
    /// let _ = producer
    ///     .send(Some(Bytes::from("k")), Bytes::from("order-2"))
    ///     .await?;
    /// # broker.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send(&self, key: Option<Bytes>, value: Bytes) -> Result<u64> {
        let timestamp_ms = current_timestamp_ms();
        let record = Record::new(timestamp_ms, key, value);
        self.partition.append(record).await
    }

    /// Appends an already-constructed [`Record`] (preserving its timestamp)
    /// and returns its assigned offset. Use [`send`] for the common case of
    /// "use the current wall-clock time."
    ///
    /// [`send`]: Producer::send
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send_record(&self, record: Record) -> Result<u64> {
        self.partition.append(record).await
    }
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
