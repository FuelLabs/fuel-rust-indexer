use crate::storage::Column;
use fuel_core_services::stream::BoxStream;
use fuel_core_storage::{
    iter::IterableStore,
    kv_store::KeyValueInspect,
};
use fuel_core_types::fuel_types::BlockHeight;
use fuel_indexer_types::events::{
    SuccessfulTransactionReceipts,
    UnstableReceipts,
};
use fuel_storage_utils::CommitLazyChanges;
use std::borrow::Borrow;

pub trait Storage:
    CommitLazyChanges
    + KeyValueInspect<Column = Column>
    + IterableStore
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<T> Storage for T where
    T: CommitLazyChanges
        + KeyValueInspect<Column = Column>
        + IterableStore
        + Clone
        + Send
        + Sync
        + 'static
{
}

pub trait StreamsSource: Send + Sync + 'static {
    fn events_starting_from(
        &mut self,
        start_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<UnstableReceipts>>>;
}

/// Source of block header timestamps for heights already committed to the
/// receipts storage. The events manager persists only events — not block
/// headers — so a checkpoint it replays from its own storage has no timestamp
/// of its own; it stamps such checkpoints through this port. Live checkpoints
/// already carry the timestamp from the header received over the stream and
/// never consult this port.
pub trait BlockTimestamps: Send + Sync + 'static {
    /// The block header's timestamp (seconds since the Unix epoch) at
    /// `block_height`. Errors if the height's header is not available.
    fn timestamp_at(&self, block_height: &BlockHeight) -> anyhow::Result<u128>;
}

pub trait ReceiptsProcessor: Send + Sync + 'static {
    type Event: StorableEvent;

    fn process_transaction_receipts<'a>(
        &'a self,
        receipts: &'a SuccessfulTransactionReceipts,
    ) -> impl Iterator<Item = Self::Event> + 'a;
}

pub trait StorableEvent
where
    Self: ToOwned + From<<Self as ToOwned>::Owned> + Borrow<Self> + Clone,
    Self: Send + Sync + 'static,
    Self: serde::Serialize + serde::de::DeserializeOwned,
{
}
