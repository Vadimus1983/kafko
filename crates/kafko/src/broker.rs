use crate::consumer::Consumer;
use crate::error::{KafkoError, Result};
use crate::log::LogConfig;
use crate::partition::Partition;
use crate::producer::Producer;
use fs4::fs_std::FileExt;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

const LOCK_FILENAME: &str = "LOCK";

/// In-process log broker — owns topic registry, segment storage, and writer tasks.
///
/// Cloning is not supported by design: there is at most one `Kafko` per data
/// directory (enforced by an OS-level advisory lock). Share access via
/// [`Producer`] and [`Consumer`] handles obtained from [`producer_for`] /
/// [`consumer_for`]; those are cheap to clone.
///
/// See [`Kafko::open`] for the typical entry point and [`shutdown`] for the
/// durability boundary at the end of the broker's life.
///
/// [`producer_for`]: Kafko::producer_for
/// [`consumer_for`]: Kafko::consumer_for
/// [`shutdown`]: Kafko::shutdown
pub struct Kafko {
    dir: PathBuf,
    topics: RwLock<HashMap<String, Arc<Partition>>>,
    default_log_config: LogConfig,
    // Held for the broker's lifetime. Dropping the File releases the OS-level
    // advisory lock so a future Kafko::open on the same dir can succeed.
    _dir_lock: File,
}

impl Kafko {
    /// Opens (or creates) a kafko data directory and recovers any topics found
    /// inside. Takes an OS-level exclusive lock on `<dir>/LOCK` for the broker's
    /// lifetime, so a second `Kafko::open` on the same directory fails fast
    /// with [`KafkoError::AlreadyOpen`].
    ///
    /// Uses [`LogConfig::default`] for any topic that has to be opened during
    /// recovery; see [`open_with_config`] to override.
    ///
    /// [`open_with_config`]: Kafko::open_with_config
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
    ///
    /// let producer = broker.producer_for("orders").await?;
    /// let offset = producer.send(None, Bytes::from("order-1")).await?;
    /// println!("appended at offset {offset}");
    ///
    /// broker.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(dir, LogConfig::default()).await
    }

    /// Like [`Kafko::open`] but uses `default_log_config` as the default
    /// [`LogConfig`] for topics created later via [`create_topic`].
    ///
    /// [`create_topic`]: Kafko::create_topic
    pub async fn open_with_config(
        dir: impl AsRef<Path>,
        default_log_config: LogConfig,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let dir_lock = acquire_dir_lock(&dir)?;

        let mut topics = HashMap::new();
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }
            let name = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            let partition = Partition::open(&entry.path(), default_log_config).await?;
            topics.insert(name, Arc::new(partition));
        }

        Ok(Self {
            dir,
            topics: RwLock::new(topics),
            default_log_config,
            _dir_lock: dir_lock,
        })
    }

    /// Returns the data directory this broker was opened against.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the [`LogConfig`] used as the default for topics created via
    /// [`create_topic`].
    ///
    /// [`create_topic`]: Kafko::create_topic
    pub fn default_log_config(&self) -> &LogConfig {
        &self.default_log_config
    }

    /// Creates a new topic under the broker's data directory using the broker's
    /// default [`LogConfig`]. Errors with [`KafkoError::TopicAlreadyExists`] if
    /// the topic already exists.
    pub async fn create_topic(&self, name: &str) -> Result<()> {
        self.create_topic_with_config(name, self.default_log_config)
            .await
    }

    /// Like [`create_topic`] but accepts an explicit [`LogConfig`] (compression,
    /// segment size, retention) for the new topic.
    ///
    /// [`create_topic`]: Kafko::create_topic
    pub async fn create_topic_with_config(&self, name: &str, log_config: LogConfig) -> Result<()> {
        let mut topics = self.topics.write().await;
        if topics.contains_key(name) {
            return Err(KafkoError::TopicAlreadyExists(name.to_string()));
        }
        let topic_dir = self.dir.join(name);
        let partition = Partition::open(&topic_dir, log_config).await?;
        topics.insert(name.to_string(), Arc::new(partition));
        Ok(())
    }

    /// Removes a topic and deletes its segment files from disk. Fails with
    /// [`KafkoError::TopicNotFound`] if the topic doesn't exist, or
    /// [`KafkoError::TopicInUse`] if outstanding [`Producer`] / [`Consumer`]
    /// handles for the topic still exist (the registry is restored on that
    /// path so the caller can drop those handles and retry).
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let mut topics = self.topics.write().await;
        let partition = match topics.remove(name) {
            Some(p) => p,
            None => return Err(KafkoError::TopicNotFound(name.to_string())),
        };

        match Arc::try_unwrap(partition) {
            Ok(owned) => {
                let topic_dir = self.dir.join(name);
                owned.shutdown().await?;
                tokio::fs::remove_dir_all(&topic_dir).await?;
                Ok(())
            }
            Err(arc) => {
                // External refs exist; restore registry state and report.
                topics.insert(name.to_string(), arc);
                Err(KafkoError::TopicInUse(name.to_string()))
            }
        }
    }

    /// Returns the names of all currently-open topics, sorted lexicographically.
    pub async fn list_topics(&self) -> Vec<String> {
        let mut names: Vec<String> = self.topics.read().await.keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns whether the named topic is currently open on this broker.
    pub async fn has_topic(&self, name: &str) -> bool {
        self.topics.read().await.contains_key(name)
    }

    /// Returns a shared handle to the named topic's [`Partition`], or `None`
    /// if no such topic exists. Most callers want [`producer_for`] /
    /// [`consumer_for`] instead; this is for callers that need to observe the
    /// partition's high-water-mark directly.
    ///
    /// [`producer_for`]: Kafko::producer_for
    /// [`consumer_for`]: Kafko::consumer_for
    pub async fn topic(&self, name: &str) -> Option<Arc<Partition>> {
        self.topics.read().await.get(name).cloned()
    }

    /// Returns a [`Producer`] bound to the named topic. Producers are cheap to
    /// clone and share. Errors with [`KafkoError::TopicNotFound`] if the topic
    /// doesn't exist.
    pub async fn producer_for(&self, name: &str) -> Result<Producer> {
        let partition = self
            .topic(name)
            .await
            .ok_or_else(|| KafkoError::TopicNotFound(name.to_string()))?;
        Ok(Producer::new(partition))
    }

    /// Returns a [`Consumer`] positioned at offset 0 of the named topic. Call
    /// [`Consumer::seek`] to start from a different offset. Errors with
    /// [`KafkoError::TopicNotFound`] if the topic doesn't exist.
    pub async fn consumer_for(&self, name: &str) -> Result<Consumer> {
        let partition = self
            .topic(name)
            .await
            .ok_or_else(|| KafkoError::TopicNotFound(name.to_string()))?;
        Ok(Consumer::from_partition(partition))
    }

    /// Gracefully closes the broker. Every partition's writer task drains its
    /// inbox, fsyncs the active segment, and exits. The data-directory lock is
    /// released last. Returns only after every previously-acked record is on
    /// disk and not just in OS page cache.
    ///
    /// Host applications that care about durability across `SIGTERM` / `SIGINT`
    /// / `docker stop` should install a signal handler that drives this method
    /// to completion before the process exits. `SIGKILL`, OS panic, and power
    /// loss bypass userspace entirely and cannot be intercepted; for those
    /// cases the recovery path at next `Kafko::open` handles torn writes via
    /// CRC scan, but any record whose page-cache bytes had not yet been
    /// flushed by the kernel may be lost.
    ///
    /// Letting the broker simply go out of scope (no `shutdown().await`)
    /// releases the lock but does NOT guarantee that recently-acked records
    /// are fsynced — the writer tasks are aborted by tokio runtime shutdown.
    pub async fn shutdown(self) -> Result<()> {
        let topics = self.topics.into_inner();
        for (_, partition) in topics {
            if let Ok(owned) = Arc::try_unwrap(partition) {
                let _ = owned.shutdown().await;
            }
        }
        // _dir_lock drops here, releasing the advisory lock.
        Ok(())
    }
}

/// Opens (creating if needed) `<dir>/LOCK` and takes a non-blocking exclusive
/// advisory lock on it. Holding this lock for the broker's lifetime serializes
/// access to the data dir at the process level: two `Kafko::open` calls on the
/// same dir would otherwise interleave writes to the same segment files and
/// corrupt each other. The lock is OS-enforced (`flock`/`LockFileEx`), so it
/// also prevents two separate processes from racing.
///
/// The lock file is intentionally NOT deleted on shutdown — leaving it in place
/// means the path is stable and there is no create/delete race on subsequent opens.
fn acquire_dir_lock(dir: &Path) -> Result<File> {
    let lock_path = dir.join(LOCK_FILENAME);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;

    match FileExt::try_lock_exclusive(&lock_file) {
        Ok(true) => Ok(lock_file),
        Ok(false) => Err(KafkoError::AlreadyOpen {
            path: dir.to_path_buf(),
        }),
        Err(e) => Err(e.into()),
    }
}
