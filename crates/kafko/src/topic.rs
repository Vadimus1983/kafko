use crate::error::{KafkoError, Result};
use crate::log::LogConfig;
use crate::partition::Partition;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

/// Cross-partition wakeup shared by a topic's partitions and its merged consumers.
///
/// A consumer that catches up to the tail parks on `notify`; a partition writer
/// pulses it after making progress. The `parked` counter lets the writer skip the
/// `notify_waiters()` call (which takes an internal mutex) when no consumer is
/// actually parked — the common case for a pure producer, and the difference
/// between a relaxed atomic load and a mutex acquire on every writer wake-up.
///
/// The gate is lost-wakeup-safe because a consumer's tail re-scan goes *through*
/// the partition writer task (a `ReadAt` command on its inbox). A consumer marks
/// itself parked before that re-scan, so the channel send→recv edge guarantees the
/// writer sees the increment before any later append it might pulse for. If the
/// writer instead processes the re-scan after the append, the re-scan returns the
/// record directly. Either way the consumer makes progress.
pub(crate) struct Wakeup {
    notify: Notify,
    parked: AtomicUsize,
}

impl Wakeup {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            parked: AtomicUsize::new(0),
        }
    }

    /// Writer side: wake parked consumers, paying the `notify_waiters()` mutex
    /// only when at least one is parked.
    pub(crate) fn signal(&self) {
        if self.parked.load(Ordering::Relaxed) > 0 {
            self.notify.notify_waiters();
        }
    }

    /// Consumer side: register as parked. Must be called before the tail re-scan
    /// (see the type-level note on why that ordering is what makes the gate safe).
    pub(crate) fn mark_parked(&self) {
        self.parked.fetch_add(1, Ordering::Relaxed);
    }

    /// Consumer side: deregister after waking or finding a record.
    pub(crate) fn unmark_parked(&self) {
        self.parked.fetch_sub(1, Ordering::Relaxed);
    }

    /// Consumer side: a future that resolves on the next `signal()` from any
    /// partition. Enable it (`as_mut().enable()`) before the tail re-scan.
    pub(crate) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

/// A named stream split into one or more [`Partition`]s.
///
/// A topic owns its partitions and routes records to them: by key
/// (`hash(key) % partition_count`, so same-key records keep their relative
/// order) or round-robin when a record has no key. Ordering is guaranteed
/// *within* a partition; there is no order across partitions — that's the
/// trade-off that lets the partitions' writer tasks run in parallel.
///
/// All partitions share one [`Notify`] so a merged [`Consumer`] can park on a
/// single signal and wake on activity from any partition.
///
/// Usually obtained via [`Kafko::topic`] rather than constructed directly.
///
/// [`Partition`]: crate::Partition
/// [`Consumer`]: crate::Consumer
/// [`Kafko::topic`]: crate::Kafko::topic
pub struct Topic {
    name: String,
    partitions: Vec<Arc<Partition>>,
    config: LogConfig,
    // Round-robin cursor for keyless sends. Relaxed is fine: we only need each
    // send to pick *a* partition and for the distribution to spread out; exact
    // interleaving across racing producers carries no ordering meaning.
    round_robin: AtomicU64,
    wake: Arc<Wakeup>,
}

impl Topic {
    /// Creates a fresh topic with `partition_count` partitions under `topic_dir`
    /// (`<topic_dir>/0`, `<topic_dir>/1`, …). Errors with
    /// [`KafkoError::InvalidPartitionCount`] if `partition_count` is 0.
    pub async fn create(
        topic_dir: &Path,
        name: &str,
        partition_count: u32,
        config: LogConfig,
    ) -> Result<Self> {
        if partition_count == 0 {
            return Err(KafkoError::InvalidPartitionCount(0));
        }
        let wake = Arc::new(Wakeup::new());
        let mut partitions = Vec::with_capacity(partition_count as usize);
        for i in 0..partition_count {
            let dir = topic_dir.join(i.to_string());
            let partition = Partition::open_with_wake(&dir, config, Some(wake.clone())).await?;
            partitions.push(Arc::new(partition));
        }
        Ok(Self::from_parts(name, partitions, config, wake))
    }

    /// Recovers a topic from `topic_dir` by discovering its numeric partition
    /// subdirectories. Errors with [`KafkoError::InvalidTopicLayout`] if there
    /// are none (e.g. a kafko <= 0.2 directory that stored segments directly
    /// under the topic dir) or if the indices are not contiguous from 0.
    pub async fn open(topic_dir: &Path, name: &str, config: LogConfig) -> Result<Self> {
        let mut indices = discover_partition_indices(topic_dir).await?;
        indices.sort_unstable();

        if indices.is_empty() {
            return Err(KafkoError::InvalidTopicLayout {
                topic: name.to_string(),
                detail: "no partition subdirectories found (a data directory \
                         written by kafko <= 0.2 is not compatible with the \
                         partitioned 0.3 layout)"
                    .to_string(),
            });
        }
        for (expected, &actual) in indices.iter().enumerate() {
            if expected as u32 != actual {
                return Err(KafkoError::InvalidTopicLayout {
                    topic: name.to_string(),
                    detail: format!(
                        "partition indices are not contiguous from 0 (expected {expected}, found {actual})"
                    ),
                });
            }
        }

        let wake = Arc::new(Wakeup::new());
        let mut partitions = Vec::with_capacity(indices.len());
        for i in indices {
            let dir = topic_dir.join(i.to_string());
            let partition = Partition::open_with_wake(&dir, config, Some(wake.clone())).await?;
            partitions.push(Arc::new(partition));
        }
        Ok(Self::from_parts(name, partitions, config, wake))
    }

    fn from_parts(
        name: &str,
        partitions: Vec<Arc<Partition>>,
        config: LogConfig,
        wake: Arc<Wakeup>,
    ) -> Self {
        Self {
            name: name.to_string(),
            partitions,
            config,
            round_robin: AtomicU64::new(0),
            wake,
        }
    }

    /// The topic's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The [`LogConfig`] every partition of this topic was opened with.
    pub fn config(&self) -> &LogConfig {
        &self.config
    }

    /// Number of partitions in this topic (always >= 1).
    pub fn partition_count(&self) -> u32 {
        self.partitions.len() as u32
    }

    /// All partitions in index order.
    pub fn partitions(&self) -> &[Arc<Partition>] {
        &self.partitions
    }

    /// The partition at `index`, or `None` if out of range.
    pub fn partition(&self, index: u32) -> Option<&Arc<Partition>> {
        self.partitions.get(index as usize)
    }

    /// The partition a keyed record routes to: `hash(key) % partition_count`.
    pub fn partition_for_key(&self, key: &[u8]) -> u32 {
        partition_index(key, self.partition_count())
    }

    /// The next partition for a keyless record, advancing the round-robin cursor.
    pub fn next_round_robin(&self) -> u32 {
        let n = self.round_robin.fetch_add(1, Ordering::Relaxed);
        (n % self.partition_count() as u64) as u32
    }

    /// A clonable handle to the shared wakeup signal. A merged [`Consumer`] parks
    /// on this and is woken whenever any partition's writer makes progress.
    ///
    /// [`Consumer`]: crate::Consumer
    pub(crate) fn wake_handle(&self) -> Arc<Wakeup> {
        self.wake.clone()
    }

    /// Gracefully shuts down every partition's writer task (drain inbox, fsync,
    /// exit). Partitions still referenced elsewhere are left running.
    pub async fn shutdown(self) {
        for partition in self.partitions {
            if let Ok(owned) = Arc::try_unwrap(partition) {
                let _ = owned.shutdown().await;
            }
        }
    }
}

/// FNV-1a (64-bit). Used for key routing instead of `std::hash::DefaultHasher`
/// because routing must be deterministic across runs, platforms, and Rust
/// versions — `DefaultHasher`'s algorithm carries no such stability guarantee.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn partition_index(key: &[u8], partition_count: u32) -> u32 {
    (fnv1a_64(key) % partition_count as u64) as u32
}

async fn discover_partition_indices(topic_dir: &Path) -> Result<Vec<u32>> {
    let mut indices = Vec::new();
    let mut entries = tokio::fs::read_dir(topic_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str()
            && let Ok(index) = name.parse::<u32>()
        {
            indices.push(index);
        }
    }
    Ok(indices)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_index_is_in_range_and_deterministic() {
        for count in [1u32, 2, 3, 7, 16] {
            for key in [b"".as_slice(), b"a", b"customer-42", b"\x00\xff\x10"] {
                let p = partition_index(key, count);
                assert!(p < count, "index {p} out of range for count {count}");
                // Deterministic: same key + count always lands on the same partition.
                assert_eq!(p, partition_index(key, count));
            }
        }
    }

    #[test]
    fn partition_index_spreads_keys_across_partitions() {
        let count = 8u32;
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            seen.insert(partition_index(key.as_bytes(), count));
        }
        // 1000 distinct keys over 8 partitions should touch every partition.
        assert_eq!(seen.len(), count as usize);
    }
}
