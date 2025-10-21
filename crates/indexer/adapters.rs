use crate::processors::{
    ReceiptParser,
    simple_processor::ReceiptProcessor,
};
use fuel_core_services::stream::BoxStream;
use fuel_core_types::{
    fuel_tx::{
        Receipt,
        TxId,
        TxPointer,
    },
    fuel_types::BlockHeight,
};
use fuel_indexer_types::events::ServiceEvent;

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
    async fn events_starting_from(
        &mut self,
        start_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<ServiceEvent>>> {
        self.receipts.events_starting_from(start_height).await
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
        tx_pointer: TxPointer,
        tx_id: TxId,
        receipts: impl Iterator<Item = &'a Receipt> + 'a,
    ) -> impl Iterator<Item = Self::Event> + 'a {
        self.processor.process_iter(tx_pointer, tx_id, receipts)
    }
}
