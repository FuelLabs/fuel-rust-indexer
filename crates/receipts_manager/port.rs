use crate::storage::Column;
use fuel_core_services::stream::BoxStream;
use fuel_core_storage::{
    iter::IterableStore,
    kv_store::KeyValueInspect,
};
use fuel_core_types::{
    blockchain::{
        consensus::Consensus,
        header::BlockHeader,
    },
    fuel_types::BlockHeight,
    services::executor::TransactionExecutionStatus,
};
use fuel_indexer_types::events::TransactionReceipts;
use fuel_storage_utils::CommitLazyChanges;

#[cfg(feature = "blocks-subscription")]
use fuel_core_types::fuel_tx::Transaction;
use futures_core::Stream;
#[cfg(feature = "blocks-subscription")]
use std::sync::Arc;

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

#[derive(Clone)]
pub struct FinalizedBlock {
    pub header: BlockHeader,
    pub consensus: Consensus,
    #[cfg(feature = "blocks-subscription")]
    pub transactions: Vec<Arc<Transaction>>,
    pub statuses: Vec<TransactionExecutionStatus>,
}

pub trait Fetcher: Send + Sync + 'static {
    /// Returns a realtime stream of predicted receipts.
    /// Doesn't guarantee that receipts are final.
    fn predicted_receipts_stream(&self)
    -> anyhow::Result<BoxStream<TransactionReceipts>>;

    /// Returns a realtime stream of finalized blocks.
    fn finalized_blocks_stream(&self) -> anyhow::Result<BoxStream<FinalizedBlock>>;

    /// Returns stream of finalized blocks for provided range.
    ///
    /// Returns an error if the range is higher than `last_height`.
    #[allow(clippy::type_complexity)]
    fn finalized_blocks_for_range(
        &self,
        range: std::ops::RangeInclusive<u32>,
    ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static;

    /// Returns the last available height of the events.
    fn last_height(&self) -> impl Future<Output = anyhow::Result<BlockHeight>> + Send;
}
