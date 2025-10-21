use crate::storage::Column;
use fuel_core_services::stream::{
    BoxStream,
    RefBoxStream,
};
use fuel_core_storage::{
    iter::IterableStore,
    kv_store::KeyValueInspect,
};
use fuel_core_types::fuel_types::BlockHeight;
use fuel_indexer_types::events::TransactionEvent;
use fuel_storage_utils::CommitLazyChanges;

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

pub trait Fetcher: Send + Sync + 'static {
    /// Returns a realtime stream of predicted events.
    /// Doesn't guarantee that events are final.
    fn predicted_events_stream(
        &self,
    ) -> impl Future<Output = anyhow::Result<BoxStream<TransactionEvent>>> + Send;

    /// Returns a realtime stream of confirmed(finalized unchangeable) events.
    /// Events are grouped together based how they were confirmed.
    fn confirmed_events_stream(
        &self,
    ) -> impl Future<
        Output = anyhow::Result<BoxStream<(BlockHeight, Vec<TransactionEvent>)>>,
    > + Send;

    /// Returns stream of confirmed(finalized unchangeable) events for provided range.
    /// Events are grouped together based how they were confirmed.
    ///
    /// Returns an error if the range is higher than `last_height`.
    #[allow(clippy::type_complexity)]
    fn confirmed_events_for_range(
        &self,
        range: std::ops::RangeInclusive<u32>,
    ) -> RefBoxStream<'static, anyhow::Result<(BlockHeight, Vec<TransactionEvent>)>>;

    /// Returns the last available height of the events.
    fn last_height(&self) -> impl Future<Output = anyhow::Result<BlockHeight>> + Send;
}
