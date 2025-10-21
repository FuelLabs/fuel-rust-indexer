use fuel_core_types::fuel_tx::{
    Receipt,
    TxId,
    TxPointer,
};

pub mod receipt_parser;
pub mod simple_processor;

pub trait ReceiptParser: Send + Sync + 'static {
    type Event;

    fn parse(
        &self,
        tx_pointer: TxPointer,
        receipt_index: u16,
        tx_id: TxId,
        receipt: &Receipt,
    ) -> Option<Vec<Self::Event>>;
}
