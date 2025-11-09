use crate::storage::Column;
use fuel_core_services::stream::{
    BoxStream,
    RefBoxStream,
};
use fuel_core_storage::{
    iter::IterableStore,
    kv_store::KeyValueInspect,
};
use fuel_core_types::{
    blockchain::header::BlockHeader,
    fuel_types::BlockHeight,
};
use fuel_indexer_types::events::TransactionReceipts;
use fuel_storage_utils::CommitLazyChanges;

use fuel_core_types::fuel_tx::Address;
#[cfg(feature = "blocks-subscription")]
use fuel_core_types::fuel_tx::Transaction;
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
    pub producer: Option<Address>,
    #[cfg(feature = "blocks-subscription")]
    pub transactions: Vec<Arc<Transaction>>,
    pub receipts: Vec<TransactionReceipts>,
}

pub trait Fetcher: Send + Sync + 'static {
    /// Returns a realtime stream of predicted receipts.
    /// Doesn't guarantee that receipts are final.
    fn predicted_receipts_stream(
        &self,
    ) -> impl Future<Output = anyhow::Result<BoxStream<TransactionReceipts>>> + Send;

    /// Returns a realtime stream of finalized blocks.
    fn finalized_blocks_stream(
        &self,
    ) -> impl Future<Output = anyhow::Result<BoxStream<FinalizedBlock>>> + Send;

    /// Returns stream of finalized blocks for provided range.
    ///
    /// Returns an error if the range is higher than `last_height`.
    #[allow(clippy::type_complexity)]
    fn finalized_blocks_for_range(
        &self,
        range: std::ops::RangeInclusive<u32>,
    ) -> RefBoxStream<'static, anyhow::Result<FinalizedBlock>>;

    /// Returns the last available height of the events.
    fn last_height(&self) -> impl Future<Output = anyhow::Result<BlockHeight>> + Send;
}
