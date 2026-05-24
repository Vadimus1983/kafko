use crate::error::{KafkoError, Result};
use crate::partition::Partition;
use crate::record::Record;
use std::sync::Arc;
use tokio::sync::watch;

/// Cursor over the records of a single topic partition.
///
/// Holds a per-consumer position (`next_offset`) and follows the partition's
/// high-water-mark via a `tokio::sync::watch` channel, so [`next_record`]
/// suspends — rather than spins — when the consumer catches up to the tail.
///
/// Each consumer's position is independent; multiple consumers on the same
/// topic can read at different offsets. Position is in-memory only and is
/// reset to 0 every time a new consumer is constructed (no committed-offset
/// store yet — that's v0.2).
///
/// [`next_record`]: Consumer::next_record
pub struct Consumer {
    partition: Arc<Partition>,
    hwm_watch: watch::Receiver<u64>,
    next_offset: u64,
}

impl Consumer {
    /// Builds a [`Consumer`] for `partition`, starting at offset 0. Prefer
    /// [`Kafko::consumer_for`] which looks the partition up by topic name.
    ///
    /// [`Kafko::consumer_for`]: crate::Kafko::consumer_for
    pub fn from_partition(partition: Arc<Partition>) -> Self {
        let hwm_watch = partition.watch_high_water_mark();
        Self {
            partition,
            hwm_watch,
            next_offset: 0,
        }
    }

    /// Like [`from_partition`] but starts reading at `start_offset`.
    ///
    /// [`from_partition`]: Consumer::from_partition
    pub fn from_partition_at(partition: Arc<Partition>, start_offset: u64) -> Self {
        let hwm_watch = partition.watch_high_water_mark();
        Self {
            partition,
            hwm_watch,
            next_offset: start_offset,
        }
    }

    /// Returns the offset this consumer will read next.
    pub fn position(&self) -> u64 {
        self.next_offset
    }

    /// Moves the consumer's read cursor to `offset`. The next call to
    /// [`next_record`] returns the record at this offset (or waits, if it
    /// hasn't been written yet).
    ///
    /// [`next_record`]: Consumer::next_record
    pub fn seek(&mut self, offset: u64) {
        self.next_offset = offset;
    }

    /// Returns the next record, suspending the task until one is available at
    /// the consumer's current offset. Advances the cursor on success. Returns
    /// [`KafkoError::Closed`] if the partition has been shut down.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bytes::Bytes;
    /// use kafko::Kafko;
    ///
    /// # async fn run() -> kafko::Result<()> {
    /// let broker = Kafko::open("./data").await?;
    /// broker.create_topic("events").await?;
    ///
    /// let producer = broker.producer_for("events").await?;
    /// producer.send(None, Bytes::from("hello")).await?;
    ///
    /// let mut consumer = broker.consumer_for("events").await?;
    /// let record = consumer.next_record().await?;
    /// assert_eq!(record.value(), &Bytes::from("hello"));
    /// # broker.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_record(&mut self) -> Result<Record> {
        loop {
            if let Some(record) = self.partition.read_record_at(self.next_offset).await? {
                self.next_offset += 1;
                return Ok(record);
            }
            self.hwm_watch
                .wait_for(|&hwm| hwm > self.next_offset)
                .await
                .map_err(|_| KafkoError::Closed)?;
        }
    }
}
