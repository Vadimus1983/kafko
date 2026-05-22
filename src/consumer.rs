use crate::error::{KafkoError, Result};
use crate::partition::Partition;
use crate::record::Record;
use std::sync::Arc;
use tokio::sync::watch;

pub struct Consumer {
    partition: Arc<Partition>,
    hwm_watch: watch::Receiver<u64>,
    next_offset: u64,
}

impl Consumer {
    pub fn from_partition(partition: Arc<Partition>) -> Self {
        let hwm_watch = partition.watch_high_water_mark();
        Self {
            partition,
            hwm_watch,
            next_offset: 0,
        }
    }

    pub fn from_partition_at(partition: Arc<Partition>, start_offset: u64) -> Self {
        let hwm_watch = partition.watch_high_water_mark();
        Self {
            partition,
            hwm_watch,
            next_offset: start_offset,
        }
    }

    pub fn position(&self) -> u64 {
        self.next_offset
    }

    pub fn seek(&mut self, offset: u64) {
        self.next_offset = offset;
    }

    /// Returns the next record, blocking until one is available at the current position.
    /// Returns `Err(Closed)` if the partition has been shut down.
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
