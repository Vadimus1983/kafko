use crate::error::{KafkoError, Result};
use crate::log::{Log, LogConfig};
use crate::record::Record;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

const INBOX_CAPACITY: usize = 1024;

pub struct Partition {
    inbox: mpsc::Sender<PartitionCommand>,
    hwm_rx: watch::Receiver<u64>,
    // Set once by the supervisor if the writer task exits via panic. None for clean shutdowns.
    panic_info: Arc<OnceLock<String>>,
    // Flips to true exactly once when the supervisor has observed the writer's termination
    // (clean or panic). Lets every method that fails to communicate with the writer wait
    // for the post-mortem result without busy-spinning.
    writer_done_rx: watch::Receiver<bool>,
    supervisor: JoinHandle<()>,
}

enum PartitionCommand {
    Append {
        record: Record,
        reply: oneshot::Sender<Result<u64>>,
    },
    ReadAt {
        offset: u64,
        reply: oneshot::Sender<Result<Option<Record>>>,
    },
    Sync {
        reply: oneshot::Sender<Result<()>>,
    },
    // Forces the writer task to panic. Reachable only via Partition::poison_for_test
    // (doc-hidden test seam); not exposed in any public producer or consumer API.
    Poison,
    // Stashes an io::ErrorKind that the writer will use to synthesize a failure on
    // its next Append batch instead of touching the disk. Reachable only via
    // Partition::fail_next_append_for_test; doc-hidden test seam.
    FailNextAppend { kind: std::io::ErrorKind },
}

impl Partition {
    pub async fn open(dir: &Path, config: LogConfig) -> Result<Self> {
        let log = Log::open(dir, config).await?;
        let initial_hwm = log.next_offset();
        let (inbox_tx, inbox_rx) = mpsc::channel(INBOX_CAPACITY);
        let (hwm_tx, hwm_rx) = watch::channel(initial_hwm);

        let writer_handle = tokio::spawn(partition_writer_loop(log, inbox_rx, hwm_tx));

        let panic_info = Arc::new(OnceLock::new());
        let (done_tx, writer_done_rx) = watch::channel(false);

        let panic_info_clone = panic_info.clone();
        let supervisor = tokio::spawn(async move {
            match writer_handle.await {
                Ok(()) => {}
                Err(join_err) if join_err.is_panic() => {
                    let s = panic_payload_to_string(join_err.into_panic());
                    let _ = panic_info_clone.set(s);
                }
                Err(_) => {
                    // Cancelled by runtime shutdown; treated as clean for the caller.
                }
            }
            let _ = done_tx.send(true);
        });

        Ok(Self {
            inbox: inbox_tx,
            hwm_rx,
            panic_info,
            writer_done_rx,
            supervisor,
        })
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn append(&self, record: Record) -> Result<u64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .inbox
            .send(PartitionCommand::Append {
                record,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return Err(self.writer_death_error().await);
        }
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err(self.writer_death_error().await),
        }
    }

    pub async fn read_record_at(&self, offset: u64) -> Result<Option<Record>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .inbox
            .send(PartitionCommand::ReadAt {
                offset,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return Err(self.writer_death_error().await);
        }
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err(self.writer_death_error().await),
        }
    }

    pub async fn sync(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .inbox
            .send(PartitionCommand::Sync { reply: reply_tx })
            .await
            .is_err()
        {
            return Err(self.writer_death_error().await);
        }
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err(self.writer_death_error().await),
        }
    }

    pub fn high_water_mark(&self) -> u64 {
        *self.hwm_rx.borrow()
    }

    pub fn watch_high_water_mark(&self) -> watch::Receiver<u64> {
        self.hwm_rx.clone()
    }

    /// Resolves once the writer task has actually terminated. Returns
    /// `PartitionPanicked { payload }` if the termination was a panic, otherwise
    /// `Closed`. Waits on a watch channel populated by the supervisor task, so
    /// there is no busy-spin and no risk of returning `Closed` while the
    /// post-mortem is still in flight.
    async fn writer_death_error(&self) -> KafkoError {
        let mut rx = self.writer_done_rx.clone();
        loop {
            if *rx.borrow() {
                break;
            }
            if rx.changed().await.is_err() {
                break;
            }
        }
        match self.panic_info.get() {
            Some(payload) => KafkoError::PartitionPanicked {
                payload: payload.clone(),
            },
            None => KafkoError::Closed,
        }
    }

    /// Test-only escape hatch that forces the writer task to panic. Used by
    /// integration tests to verify that subsequent calls surface
    /// `KafkoError::PartitionPanicked` rather than the generic `Closed`. Not
    /// intended for production code.
    #[doc(hidden)]
    pub async fn poison_for_test(&self) -> Result<()> {
        self.inbox
            .send(PartitionCommand::Poison)
            .await
            .map_err(|_| KafkoError::Closed)?;
        Ok(())
    }

    /// Test-only escape hatch that makes the writer's next Append batch fail
    /// with `io::Error::from(kind)` without touching the disk. After the
    /// failure, the stash clears and subsequent appends behave normally —
    /// exercising the contract that an IO error from `append` does NOT take
    /// the partition offline. Not intended for production code.
    #[doc(hidden)]
    pub async fn fail_next_append_for_test(&self, kind: std::io::ErrorKind) -> Result<()> {
        self.inbox
            .send(PartitionCommand::FailNextAppend { kind })
            .await
            .map_err(|_| KafkoError::Closed)?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<()> {
        drop(self.inbox);
        // Awaiting the supervisor instead of the writer task directly: the
        // supervisor finishes only after observing the writer's termination, so
        // by the time this returns the final-fsync done inside the writer loop
        // is complete and the panic-capture (if any) has been recorded.
        self.supervisor.await.map_err(|_| KafkoError::Closed)?;
        Ok(())
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "panic with non-string payload".to_string()
}

async fn partition_writer_loop(
    mut log: Log,
    mut inbox: mpsc::Receiver<PartitionCommand>,
    hwm_tx: watch::Sender<u64>,
) {
    let mut retention_tick = tokio::time::interval(log.config().retention_check_interval);
    retention_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    retention_tick.tick().await;

    let batch_max_records = log.config().batch_max_records;
    let batch_max_bytes = log.config().batch_max_bytes;
    // Stash for the test-only fault injection. Set by FailNextAppend, consumed
    // by the next Append (batched or single). Never read by production callers.
    let mut fail_next_kind: Option<std::io::ErrorKind> = None;

    loop {
        tokio::select! {
            cmd = inbox.recv() => {
                let Some(cmd) = cmd else { break; };
                process_with_batching(
                    &mut log,
                    &hwm_tx,
                    &mut inbox,
                    cmd,
                    batch_max_records,
                    batch_max_bytes,
                    &mut fail_next_kind,
                ).await;
            }
            _ = retention_tick.tick() => {
                let _ = log.apply_retention().await;
            }
        }
    }

    // The inbox is closed (Partition::shutdown was called or Partition was dropped).
    // Flush every previously-acked record from OS page cache to disk before exiting.
    // This is what makes Kafko::shutdown().await a true durability boundary: after
    // it returns, every record this writer ever acked is fsynced. Errors here are
    // silently ignored — the task is unwinding and there is no caller to report to.
    let _ = log.sync().await;
}

// When the first command of a wake-up is an Append, drain any other ready Appends
// from the inbox and write them all as a single batch. This is "natural batching" —
// records that piled up while the actor was busy get coalesced into one disk write
// without any producer-visible latency change.
async fn process_with_batching(
    log: &mut Log,
    hwm_tx: &watch::Sender<u64>,
    inbox: &mut mpsc::Receiver<PartitionCommand>,
    first: PartitionCommand,
    batch_max_records: usize,
    batch_max_bytes: u64,
    fail_next_kind: &mut Option<std::io::ErrorKind>,
) {
    let (record, reply) = match first {
        PartitionCommand::Append { record, reply } => (record, reply),
        other => {
            handle_single_command(log, hwm_tx, other, fail_next_kind).await;
            return;
        }
    };

    let mut records: Vec<Record> = Vec::with_capacity(8);
    let mut replies: Vec<oneshot::Sender<Result<u64>>> = Vec::with_capacity(8);
    let mut batch_bytes = record.wire_size() as u64;
    records.push(record);
    replies.push(reply);

    while records.len() < batch_max_records && batch_bytes < batch_max_bytes {
        match inbox.try_recv() {
            Ok(PartitionCommand::Append { record, reply }) => {
                batch_bytes += record.wire_size() as u64;
                records.push(record);
                replies.push(reply);
            }
            Ok(other) => {
                flush_append_batch(log, hwm_tx, records, replies, fail_next_kind).await;
                handle_single_command(log, hwm_tx, other, fail_next_kind).await;
                return;
            }
            Err(_) => break,
        }
    }

    flush_append_batch(log, hwm_tx, records, replies, fail_next_kind).await;
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn flush_append_batch(
    log: &mut Log,
    hwm_tx: &watch::Sender<u64>,
    records: Vec<Record>,
    replies: Vec<oneshot::Sender<Result<u64>>>,
    fail_next_kind: &mut Option<std::io::ErrorKind>,
) {
    if records.is_empty() {
        return;
    }
    if let Some(kind) = fail_next_kind.take() {
        let synth = KafkoError::Io(std::io::Error::from(kind));
        for reply in replies {
            let _ = reply.send(Err(synth.clone()));
        }
        return;
    }
    match log.append_batch(records).await {
        Ok(offsets) => {
            if let Some(&last) = offsets.last() {
                let _ = hwm_tx.send(last + 1);
            }
            for (offset, reply) in offsets.into_iter().zip(replies) {
                let _ = reply.send(Ok(offset));
            }
        }
        Err(e) => {
            // Surface the real cause to every waiter so callers can match on
            // io::ErrorKind (e.g. StorageFull) and decide whether to retry.
            // The partition stays alive: the writer task continues serving
            // subsequent commands. A successful future append clears the
            // condition from the caller's perspective.
            for reply in replies {
                let _ = reply.send(Err(e.clone()));
            }
        }
    }
}

async fn handle_single_command(
    log: &mut Log,
    hwm_tx: &watch::Sender<u64>,
    cmd: PartitionCommand,
    fail_next_kind: &mut Option<std::io::ErrorKind>,
) {
    match cmd {
        PartitionCommand::Append { record, reply } => {
            if let Some(kind) = fail_next_kind.take() {
                let _ = reply.send(Err(KafkoError::Io(std::io::Error::from(kind))));
                return;
            }
            let result = log.append(record).await;
            if let Ok(offset) = &result {
                let _ = hwm_tx.send(*offset + 1);
            }
            let _ = reply.send(result);
        }
        PartitionCommand::ReadAt { offset, reply } => {
            let result = log.read_record_at(offset).await;
            let _ = reply.send(result);
        }
        PartitionCommand::Sync { reply } => {
            let result = log.sync().await;
            let _ = reply.send(result);
        }
        PartitionCommand::Poison => {
            panic!("intentional panic from poison command (test-only)");
        }
        PartitionCommand::FailNextAppend { kind } => {
            *fail_next_kind = Some(kind);
        }
    }
}
