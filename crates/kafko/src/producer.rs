use crate::error::Result;
use crate::partition::Partition;
use crate::record::Record;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct Producer {
    partition: Arc<Partition>,
}

impl Producer {
    pub fn new(partition: Arc<Partition>) -> Self {
        Self { partition }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn send(&self, key: Option<Bytes>, value: Bytes) -> Result<u64> {
        let timestamp_ms = current_timestamp_ms();
        let record = Record::new(timestamp_ms, key, value);
        self.partition.append(record).await
    }

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
