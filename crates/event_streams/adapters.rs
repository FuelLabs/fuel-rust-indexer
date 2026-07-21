use crate::processors::{
    ReceiptParser,
    simple_processor::ReceiptProcessor,
};
use fuel_core_services::stream::BoxStream;
use fuel_core_types::fuel_types::BlockHeight;
use fuel_indexer_types::events::{
    SuccessfulTransactionReceipts,
    UnstableReceipts,
};

pub struct StreamsAdapter<S> {
    receipts: fuel_receipts_manager::service::SharedState<S>,
}

impl<S> StreamsAdapter<S> {
    pub fn new(receipts: fuel_receipts_manager::service::SharedState<S>) -> Self {
        Self { receipts }
    }
}

impl<S> fuel_events_manager::port::StreamsSource for StreamsAdapter<S>
where
    S: fuel_receipts_manager::port::Storage,
{
    fn events_starting_from(
        &mut self,
        start_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<UnstableReceipts>>> {
        self.receipts.unstable_receipts_starting_from(start_height)
    }
}

/// Exposes the receipts manager's block-header timestamps to the events
/// manager, which stores only events and so cannot stamp the checkpoints it
/// replays from its own storage without this.
pub struct ReceiptsTimestamps<S> {
    receipts: fuel_receipts_manager::service::SharedState<S>,
}

impl<S> ReceiptsTimestamps<S> {
    pub fn new(receipts: fuel_receipts_manager::service::SharedState<S>) -> Self {
        Self { receipts }
    }
}

impl<S> fuel_events_manager::port::BlockTimestamps for ReceiptsTimestamps<S>
where
    S: fuel_receipts_manager::port::Storage,
{
    fn timestamp_at(&self, block_height: &BlockHeight) -> anyhow::Result<u128> {
        self.receipts.timestamp_at(block_height)
    }
}

pub struct SimplerProcessorAdapter<R> {
    processor: ReceiptProcessor<R>,
}

impl<R> SimplerProcessorAdapter<R> {
    pub fn new(parser: R) -> Self {
        Self {
            processor: ReceiptProcessor::new(parser),
        }
    }
}

impl<R> fuel_events_manager::port::ReceiptsProcessor for SimplerProcessorAdapter<R>
where
    R: ReceiptParser,
    R::Event: fuel_events_manager::port::StorableEvent,
{
    type Event = R::Event;

    fn process_transaction_receipts<'a>(
        &'a self,
        receipts: &'a SuccessfulTransactionReceipts,
    ) -> impl Iterator<Item = Self::Event> + 'a {
        self.processor.process_iter(
            receipts.tx_pointer,
            receipts.tx_id,
            receipts.receipts.iter(),
        )
    }
}
