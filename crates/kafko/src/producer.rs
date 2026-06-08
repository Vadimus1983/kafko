use crate::error::{KafkoError, Result};
use crate::position::RecordPosition;
use crate::record::Record;
use crate::topic::Topic;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Cheap handle that appends records to a topic, routing each to a partition.
///
/// `Producer` is `Clone` (wraps `Arc<Topic>`); cloning is free and every clone
/// writes to the same topic. Records are timestamped at `send`-time. Routing:
/// a keyed record goes to `hash(key) % partition_count` (so same-key records
/// keep their order); a keyless record is spread round-robin across partitions.
#[derive(Clone)]
pub struct Producer {
    topic: Arc<Topic>,
}

impl Producer {
    /// Wraps a [`Topic`] in a `Producer`. Prefer [`Kafko::producer_for`] which
    /// looks the topic up by name.
    ///
    /// [`Kafko::producer_for`]: crate::Kafko::producer_for
    pub fn new(topic: Arc<Topic>) -> Self {
        Self { topic }
    }

    /// Number of partitions on the topic this producer writes to.
    pub fn partition_count(&self) -> u32 {
        self.topic.partition_count()
    }

    /// Appends a record, routing by `key`, and returns its [`RecordPosition`]
    /// (the partition it landed on plus its offset within that partition).
    ///
    /// Resolves once the bytes are in the OS file (page cache) — the same
    /// durability contract as Kafka `acks=1`.
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
    /// let pos = producer.send(Some(Bytes::from("cust-1")), Bytes::from("order-1")).await?;
    /// println!("partition {} offset {}", pos.partition(), pos.offset());
    /// # broker.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send(&self, key: Option<Bytes>, value: Bytes) -> Result<RecordPosition> {
        let partition = self.route(key.as_deref());
        let record = Record::new(current_timestamp_ms(), key, value);
        self.append_to(partition, record).await
    }

    /// Appends a record to an explicit `partition`, ignoring key-based routing.
    /// Errors with [`KafkoError::InvalidPartitionCount`] if `partition` is out
    /// of range for the topic.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send_to(
        &self,
        partition: u32,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<RecordPosition> {
        let record = Record::new(current_timestamp_ms(), key, value);
        self.append_to(partition, record).await
    }

    /// Appends an already-constructed [`Record`] (preserving its timestamp),
    /// routing by the record's own key, and returns its [`RecordPosition`].
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send_record(&self, record: Record) -> Result<RecordPosition> {
        let partition = self.route(record.key().map(|k| k.as_ref()));
        self.append_to(partition, record).await
    }

    /// Appends a batch of records, timestamping each at the moment of the call,
    /// and returns their [`RecordPosition`]s in input order.
    ///
    /// Records are grouped by their routed partition and each group is written
    /// as one atomic append. **Atomicity is per partition**: a batch whose
    /// records span partitions is atomic within each partition, not across them.
    /// For a single-partition topic this is one fully-atomic append, identical
    /// to a single `Log::append_batch`.
    ///
    /// An empty input is a no-op returning `Ok(Vec::new())`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send_batch(
        &self,
        items: Vec<(Option<Bytes>, Bytes)>,
    ) -> Result<Vec<RecordPosition>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let timestamp_ms = current_timestamp_ms();
        let records = items
            .into_iter()
            .map(|(key, value)| Record::new(timestamp_ms, key, value));
        self.append_batch_routed(records).await
    }

    /// Like [`send_batch`], but takes already-constructed records so callers can
    /// preserve per-record timestamps. Each record routes by its own key; the
    /// per-partition atomicity contract is identical.
    ///
    /// [`send_batch`]: Producer::send_batch
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send_batch_records(&self, records: Vec<Record>) -> Result<Vec<RecordPosition>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        self.append_batch_routed(records.into_iter()).await
    }

    fn route(&self, key: Option<&[u8]>) -> u32 {
        match key {
            Some(k) => self.topic.partition_for_key(k),
            None => self.topic.next_round_robin(),
        }
    }

    async fn append_to(&self, partition: u32, record: Record) -> Result<RecordPosition> {
        let target = self
            .topic
            .partition(partition)
            .ok_or(KafkoError::InvalidPartitionCount(partition))?;
        let offset = target.append(record).await?;
        Ok(RecordPosition::new(partition, offset))
    }

    async fn append_batch_routed(
        &self,
        records: impl Iterator<Item = Record>,
    ) -> Result<Vec<RecordPosition>> {
        let n = self.topic.partition_count() as usize;
        // Bucket records by target partition, remembering each record's original
        // input index so we can scatter the assigned offsets back in order.
        let mut buckets: Vec<Vec<(usize, Record)>> = vec![Vec::new(); n];
        let mut total = 0usize;
        for (idx, record) in records.enumerate() {
            let p = self.route(record.key().map(|k| k.as_ref())) as usize;
            buckets[p].push((idx, record));
            total += 1;
        }

        let mut positions = vec![RecordPosition::new(0, 0); total];
        for (p, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let (indices, recs): (Vec<usize>, Vec<Record>) = bucket.into_iter().unzip();
            let partition = self
                .topic
                .partition(p as u32)
                .expect("bucket index is always a valid partition");
            let offsets = partition.append_batch(recs).await?;
            for (input_idx, offset) in indices.into_iter().zip(offsets) {
                positions[input_idx] = RecordPosition::new(p as u32, offset);
            }
        }
        Ok(positions)
    }
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
