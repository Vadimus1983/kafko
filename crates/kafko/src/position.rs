/// Where a record landed: the partition it was routed to and its offset within
/// that partition.
///
/// Returned by the [`Producer`] send methods. An offset is only meaningful in
/// the context of its partition (each partition has its own independent 0,1,2,…
/// sequence), so kafko reports the pair rather than a bare offset.
///
/// [`Producer`]: crate::Producer
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordPosition {
    partition: u32,
    offset: u64,
}

impl RecordPosition {
    /// Constructs a position from a partition index and an in-partition offset.
    pub fn new(partition: u32, offset: u64) -> Self {
        Self { partition, offset }
    }

    /// The partition the record was written to.
    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// The record's offset within its partition.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}
