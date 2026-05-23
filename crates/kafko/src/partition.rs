use crate::error::{KafkoError, Result};
use crate::log::{Log, LogConfig};
use crate::record::Record;
use std::path::Path;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

const INBOX_CAPACITY: usize = 1024;

pub struct Partition {
    inbox: mpsc::Sender<PartitionCommand>,
    hwm_rx: watch::Receiver<u64>,
    task: JoinHandle<()>,
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
}

impl Partition {
    pub async fn open(dir: &Path, config: LogConfig) -> Result<Self> {
        let log = Log::open(dir, config).await?;
        let initial_hwm = log.next_offset();
        let (inbox_tx, inbox_rx) = mpsc::channel(INBOX_CAPACITY);
        let (hwm_tx, hwm_rx) = watch::channel(initial_hwm);
        let task = tokio::spawn(partition_writer_loop(log, inbox_rx, hwm_tx));
        Ok(Self {
            inbox: inbox_tx,
            hwm_rx,
            task,
        })
    }

    pub async fn append(&self, record: Record) -> Result<u64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inbox
            .send(PartitionCommand::Append {
                record,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KafkoError::Closed)?;
        reply_rx.await.map_err(|_| KafkoError::Closed)?
    }

    pub async fn read_record_at(&self, offset: u64) -> Result<Option<Record>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inbox
            .send(PartitionCommand::ReadAt {
                offset,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KafkoError::Closed)?;
        reply_rx.await.map_err(|_| KafkoError::Closed)?
    }

    pub async fn sync(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inbox
            .send(PartitionCommand::Sync { reply: reply_tx })
            .await
            .map_err(|_| KafkoError::Closed)?;
        reply_rx.await.map_err(|_| KafkoError::Closed)?
    }

    pub fn high_water_mark(&self) -> u64 {
        *self.hwm_rx.borrow()
    }

    pub fn watch_high_water_mark(&self) -> watch::Receiver<u64> {
        self.hwm_rx.clone()
    }

    pub async fn shutdown(self) -> Result<()> {
        drop(self.inbox);
        self.task.await.map_err(|_| KafkoError::Closed)?;
        Ok(())
    }
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
) {
    let (record, reply) = match first {
        PartitionCommand::Append { record, reply } => (record, reply),
        other => {
            handle_single_command(log, hwm_tx, other).await;
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
                flush_append_batch(log, hwm_tx, records, replies).await;
                handle_single_command(log, hwm_tx, other).await;
                return;
            }
            Err(_) => break,
        }
    }

    flush_append_batch(log, hwm_tx, records, replies).await;
}

async fn flush_append_batch(
    log: &mut Log,
    hwm_tx: &watch::Sender<u64>,
    records: Vec<Record>,
    replies: Vec<oneshot::Sender<Result<u64>>>,
) {
    if records.is_empty() {
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
        Err(_) => {
            for reply in replies {
                let _ = reply.send(Err(KafkoError::Closed));
            }
        }
    }
}

async fn handle_single_command(log: &mut Log, hwm_tx: &watch::Sender<u64>, cmd: PartitionCommand) {
    match cmd {
        PartitionCommand::Append { record, reply } => {
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
    }
}
