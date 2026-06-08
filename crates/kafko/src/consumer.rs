use crate::error::Result;
use crate::offset_store::OffsetStore;
use crate::position::RecordPosition;
use crate::record::Record;
use crate::topic::{Topic, Wakeup};
use std::sync::Arc;

/// Cursor over the records of a topic, merging all its partitions into one stream.
///
/// Holds an independent read position per partition. [`next_record`] returns
/// records in offset order *within* each partition; the interleaving *across*
/// partitions is by availability (round-robin over what's ready) and carries no
/// ordering guarantee — same as Kafka. Use [`next_with_position`] to learn which
/// partition a record came from.
///
/// When caught up to the tail of every partition, [`next_record`] suspends —
/// rather than spins — on the topic's shared wakeup signal until any partition's
/// writer makes progress.
///
/// Positions are in-memory only and reset to 0 when a new consumer is
/// constructed (no committed-offset store yet — that's a later feature).
///
/// [`next_record`]: Consumer::next_record
/// [`next_with_position`]: Consumer::next_with_position
pub struct Consumer {
    topic: Arc<Topic>,
    // One next-offset cursor per partition, indexed by partition id.
    cursors: Vec<u64>,
    // Rotating start index for the round-robin scan, for fairness across partitions.
    scan_start: usize,
    wake: Arc<Wakeup>,
    // Set when this consumer belongs to a group: durable committed-offset storage
    // that `commit` writes to. `None` for an anonymous consumer (no persistence).
    store: Option<OffsetStore>,
}

impl Consumer {
    /// Builds a [`Consumer`] for `topic`, every partition starting at offset 0.
    /// Prefer [`Kafko::consumer_for`] which looks the topic up by name.
    ///
    /// [`Kafko::consumer_for`]: crate::Kafko::consumer_for
    pub fn from_topic(topic: Arc<Topic>) -> Self {
        Self::from_topic_at(topic, 0)
    }

    /// Like [`from_topic`] but starts every partition at `start_offset`.
    ///
    /// [`from_topic`]: Consumer::from_topic
    pub fn from_topic_at(topic: Arc<Topic>, start_offset: u64) -> Self {
        let n = topic.partition_count() as usize;
        let wake = topic.wake_handle();
        Self {
            topic,
            cursors: vec![start_offset; n],
            scan_start: 0,
            wake,
            store: None,
        }
    }

    /// Builds a group consumer that resumes from `store`'s committed offsets and
    /// persists progress back to it via [`commit`]. Each partition's cursor starts
    /// at its committed offset (clamped to the partition's high-water-mark).
    /// Prefer [`Kafko::consumer_for_group`] which builds the store by topic + group.
    ///
    /// [`commit`]: Consumer::commit
    /// [`Kafko::consumer_for_group`]: crate::Kafko::consumer_for_group
    pub(crate) fn from_topic_with_group(topic: Arc<Topic>, store: OffsetStore) -> Self {
        let wake = topic.wake_handle();
        let cursors: Vec<u64> = topic
            .partitions()
            .iter()
            .enumerate()
            .map(|(p, partition)| {
                let committed = store.committed().get(p).copied().unwrap_or(0);
                committed.min(partition.high_water_mark())
            })
            .collect();
        Self {
            topic,
            cursors,
            scan_start: 0,
            wake,
            store: Some(store),
        }
    }

    /// The number of partitions this consumer reads from.
    pub fn partition_count(&self) -> u32 {
        self.cursors.len() as u32
    }

    /// The offset this consumer will read next from `partition` (0 if `partition`
    /// is out of range).
    pub fn position(&self, partition: u32) -> u64 {
        self.cursors.get(partition as usize).copied().unwrap_or(0)
    }

    /// The consumer group this consumer belongs to, or `None` if it's anonymous
    /// (built via [`Kafko::consumer_for`] rather than [`Kafko::consumer_for_group`]).
    ///
    /// [`Kafko::consumer_for`]: crate::Kafko::consumer_for
    /// [`Kafko::consumer_for_group`]: crate::Kafko::consumer_for_group
    pub fn group(&self) -> Option<&str> {
        self.store.as_ref().map(|s| s.group())
    }

    /// The group's last committed offset for `partition`, or `None` if this is an
    /// anonymous consumer or `partition` is out of range. Reflects the last
    /// successful [`commit`], not the live read position (see [`position`]).
    ///
    /// [`commit`]: Consumer::commit
    /// [`position`]: Consumer::position
    pub fn committed(&self, partition: u32) -> Option<u64> {
        self.store
            .as_ref()
            .and_then(|s| s.committed().get(partition as usize).copied())
    }

    /// Durably commits the current read position of every partition as this
    /// group's committed offset, so a future consumer in the same group resumes
    /// here. Resolves only after the write is fsynced. A no-op returning `Ok(())`
    /// for an anonymous consumer.
    ///
    /// Commit *after* you've processed the records you read (at-least-once): a
    /// crash between processing and commit replays from the last commit.
    pub async fn commit(&mut self) -> Result<()> {
        if self.store.is_some() {
            let offsets = self.cursors.clone();
            self.store
                .as_mut()
                .expect("store is Some")
                .commit(&offsets)
                .await?;
        }
        Ok(())
    }

    /// Moves every partition's cursor to `offset`.
    pub fn seek_all(&mut self, offset: u64) {
        for cursor in &mut self.cursors {
            *cursor = offset;
        }
    }

    /// Moves one partition's cursor to `offset`. No-op if `partition` is out of
    /// range.
    pub fn seek(&mut self, partition: u32, offset: u64) {
        if let Some(cursor) = self.cursors.get_mut(partition as usize) {
            *cursor = offset;
        }
    }

    /// Returns the next record from any partition, suspending until one is
    /// available. See [`next_with_position`] for the variant that also reports
    /// the source partition and offset.
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
    ///
    /// [`next_with_position`]: Consumer::next_with_position
    pub async fn next_record(&mut self) -> Result<Record> {
        self.next_with_position().await.map(|(_, record)| record)
    }

    /// Like [`next_record`] but also returns the [`RecordPosition`] (partition +
    /// offset) the record was read from.
    ///
    /// [`next_record`]: Consumer::next_record
    pub async fn next_with_position(&mut self) -> Result<(RecordPosition, Record)> {
        loop {
            if let Some(item) = self.try_take_next().await? {
                return Ok(item);
            }
            // Caught up on every partition. Mark ourselves parked and register
            // interest on the shared wakeup BEFORE the tail re-scan: marking first
            // is what lets the writer's relaxed `parked` check be lost-wakeup-safe
            // (see Wakeup's docs), and enabling first means an append that races
            // the re-scan still wakes us. Clone the Arc into a local so the
            // Notified future doesn't borrow `self` across the `&mut self` calls.
            let wake = self.wake.clone();
            wake.mark_parked();
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_take_next().await {
                Ok(Some(item)) => {
                    wake.unmark_parked();
                    return Ok(item);
                }
                Ok(None) => {}
                Err(e) => {
                    wake.unmark_parked();
                    return Err(e);
                }
            }
            notified.await;
            wake.unmark_parked();
        }
    }

    /// One round-robin pass over the partitions. On the first partition with a
    /// record at its cursor, advances that cursor + the rotation and returns it.
    /// `Ok(None)` means every partition is currently caught up.
    async fn try_take_next(&mut self) -> Result<Option<(RecordPosition, Record)>> {
        let n = self.cursors.len();
        for i in 0..n {
            let p = (self.scan_start + i) % n;
            let partition = self
                .topic
                .partition(p as u32)
                .expect("scan index is always < partition_count");
            if let Some(record) = partition.read_record_at(self.cursors[p]).await? {
                let position = RecordPosition::new(p as u32, self.cursors[p]);
                self.cursors[p] += 1;
                self.scan_start = (p + 1) % n;
                return Ok(Some((position, record)));
            }
        }
        Ok(None)
    }
}
