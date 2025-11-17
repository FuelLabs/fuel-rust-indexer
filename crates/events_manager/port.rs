use crate::storage::Column;
use fuel_core_services::stream::BoxStream;
use fuel_core_storage::{
    iter::IterableStore,
    kv_store::KeyValueInspect,
};
use fuel_core_types::{
    fuel_tx::{
        Receipt,
        TxId,
        TxPointer,
    },
    fuel_types::BlockHeight,
};
use fuel_indexer_types::events::UnstableReceipts;
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

pub trait ReceiptsProcessor: Send + Sync + 'static {
    type Event: StorableEvent;

    fn process_transaction_receipts<'a>(
        &'a self,
        tx_pointer: TxPointer,
        tx_id: TxId,
        receipts: impl Iterator<Item = &'a Receipt> + 'a,
    ) -> impl Iterator<Item = Self::Event> + 'a;
}

pub trait StorableEvent
where
    Self: ToOwned + From<<Self as ToOwned>::Owned> + Borrow<Self> + Clone,
    Self: Send + Sync + 'static,
    Self: serde::Serialize + serde::de::DeserializeOwned,
{
}
