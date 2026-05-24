use crate::consumer::Consumer;
use crate::error::{KafkoError, Result};
use crate::log::LogConfig;
use crate::partition::Partition;
use crate::producer::Producer;
use fs4::fs_std::FileExt;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

const LOCK_FILENAME: &str = "LOCK";

/// In-process log broker — owns topic registry, segment storage, and writer tasks.
///
/// Cloning is not supported by design: there is at most one `Kafko` per data
/// directory (enforced by an OS-level advisory lock). Share access via
/// [`Producer`] and [`Consumer`] handles obtained from [`producer_for`] /
/// [`consumer_for`]; those are cheap to clone.
///
/// Dropping a `Kafko` without first calling [`shutdown`] still attempts a
/// graceful shutdown — see the [`Drop`](#impl-Drop-for-Kafko) impl for the
/// exact contract — but explicit [`shutdown`] gives you error visibility and
/// works on any tokio runtime flavor.
///
/// [`producer_for`]: Kafko::producer_for
/// [`consumer_for`]: Kafko::consumer_for
/// [`shutdown`]: Kafko::shutdown
pub struct Kafko {
    dir: PathBuf,
    // std::sync::RwLock (not tokio::sync) so Drop can `get_mut()` the registry
    // synchronously. Touched only on admin / handle-binding paths, never on the
    // record hot path; there is no perf cost to blocking-locks here.
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
        // Check existence + reserve under the write lock; release before the async
        // Partition::open so we don't hold the lock across an .await.
        {
            let topics = self.topics.read().expect("topics RwLock poisoned");
            if topics.contains_key(name) {
                return Err(KafkoError::TopicAlreadyExists(name.to_string()));
            }
        }
        let topic_dir = self.dir.join(name);
        let partition = Partition::open(&topic_dir, log_config).await?;

        let mut topics = self.topics.write().expect("topics RwLock poisoned");
        if topics.contains_key(name) {
            // Lost a race; another caller created the same topic in between.
            return Err(KafkoError::TopicAlreadyExists(name.to_string()));
        }
        topics.insert(name.to_string(), Arc::new(partition));
        Ok(())
    }

    /// Removes a topic and deletes its segment files from disk. Fails with
    /// [`KafkoError::TopicNotFound`] if the topic doesn't exist, or
    /// [`KafkoError::TopicInUse`] if outstanding [`Producer`] / [`Consumer`]
    /// handles for the topic still exist (the registry is restored on that
    /// path so the caller can drop those handles and retry).
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let partition = {
            let mut topics = self.topics.write().expect("topics RwLock poisoned");
            match topics.remove(name) {
                Some(p) => p,
                None => return Err(KafkoError::TopicNotFound(name.to_string())),
            }
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
                self.topics
                    .write()
                    .expect("topics RwLock poisoned")
                    .insert(name.to_string(), arc);
                Err(KafkoError::TopicInUse(name.to_string()))
            }
        }
    }

    /// Returns the names of all currently-open topics, sorted lexicographically.
    pub async fn list_topics(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .topics
            .read()
            .expect("topics RwLock poisoned")
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    /// Returns whether the named topic is currently open on this broker.
    pub async fn has_topic(&self, name: &str) -> bool {
        self.topics
            .read()
            .expect("topics RwLock poisoned")
            .contains_key(name)
    }

    /// Returns a shared handle to the named topic's [`Partition`], or `None`
    /// if no such topic exists. Most callers want [`producer_for`] /
    /// [`consumer_for`] instead; this is for callers that need to observe the
    /// partition's high-water-mark directly.
    ///
    /// [`producer_for`]: Kafko::producer_for
    /// [`consumer_for`]: Kafko::consumer_for
    pub async fn topic(&self, name: &str) -> Option<Arc<Partition>> {
        self.topics
            .read()
            .expect("topics RwLock poisoned")
            .get(name)
            .cloned()
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
    /// Prefer this over relying on [`Drop`](#impl-Drop-for-Kafko) when:
    /// - your runtime is the `current_thread` flavor (Drop can't block there
    ///   and falls back to a detached task that may be aborted by runtime
    ///   shutdown),
    /// - you want to observe shutdown errors (Drop swallows them), or
    /// - you need a deterministic shutdown point in your program flow.
    ///
    /// Host applications that care about durability across `SIGTERM` / `SIGINT`
    /// / `docker stop` should install a signal handler that drives this method
    /// to completion before the process exits. `SIGKILL`, OS panic, and power
    /// loss bypass userspace entirely and cannot be intercepted; for those
    /// cases the recovery path at next `Kafko::open` handles torn writes via
    /// CRC scan, but any record whose page-cache bytes had not yet been
    /// flushed by the kernel may be lost.
    pub async fn shutdown(mut self) -> Result<()> {
        let topics = std::mem::take(self.topics.get_mut().expect("topics RwLock poisoned"));
        shutdown_partitions(topics).await;
        // `Drop::drop` will run next on self, but `topics` is now empty so the
        // Drop impl is a no-op. `_dir_lock` drops at the end of Drop, releasing
        // the advisory lock.
        Ok(())
    }
}

/// Best-effort graceful shutdown when the broker goes out of scope without an
/// explicit [`Kafko::shutdown`].
///
/// Behavior depends on whether a tokio runtime is reachable from the dropping
/// thread:
///
/// - **Multi-thread runtime (default `#[tokio::main]`):** drives every
///   partition's writer task to completion before `Drop` returns. The data
///   directory lock is released only after the final fsync. Effectively the
///   same durability as explicit [`shutdown`](Kafko::shutdown), minus error
///   visibility.
/// - **Current-thread runtime:** spawns the cleanup task detached. It may or
///   may not complete before the runtime tears down. Use explicit
///   [`shutdown`](Kafko::shutdown) instead if you need guarantees.
/// - **No runtime reachable:** drops the directory lock and lets the writer
///   tasks be aborted by whatever owns the runtime they were spawned on.
///
/// Errors during shutdown are silently swallowed because `Drop` cannot return
/// a `Result`. Call [`shutdown`](Kafko::shutdown) explicitly if you need to
/// observe them.
impl Drop for Kafko {
    fn drop(&mut self) {
        let topics = std::mem::take(self.topics.get_mut().expect("topics RwLock poisoned"));
        if topics.is_empty() {
            // Already shut down explicitly, or never had any topics.
            return;
        }

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // No reachable runtime — the spawned writer tasks belong to some
            // runtime that's either dead or unreachable. Nothing useful to do
            // here; the dir lock drops with self._dir_lock below.
            return;
        };

        let cleanup = shutdown_partitions(topics);

        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => {
                // block_in_place panics on the current-thread runtime, so we
                // fall back to a detached cleanup task. Best-effort: if the
                // runtime is itself shutting down (typical Drop-at-end-of-main),
                // the task may not finish before its host runtime aborts it.
                handle.spawn(cleanup);
            }
            _ => {
                // Multi-thread runtime: tell tokio to let the other workers
                // continue with our pending work while we drive the cleanup
                // future to completion on this thread.
                tokio::task::block_in_place(|| handle.block_on(cleanup));
            }
        }
        // self._dir_lock drops here, releasing the advisory lock.
    }
}

/// Shared cleanup helper: takes ownership of the topics map and drives each
/// partition's `shutdown()` to completion. Partitions whose `Arc` still has
/// external producer/consumer references are skipped (their writer tasks keep
/// running until those handles also drop).
async fn shutdown_partitions(topics: HashMap<String, Arc<Partition>>) {
    for (_, partition) in topics {
        if let Ok(owned) = Arc::try_unwrap(partition) {
            let _ = owned.shutdown().await;
        }
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
