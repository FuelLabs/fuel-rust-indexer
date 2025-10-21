use fuel_core_types::{
    fuel_tx::{
        ContractId,
        Receipt,
        TxId,
        TxPointer,
    },
    fuel_types::BlockHeight,
};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionEvent {
    pub tx_pointer: TxPointer,
    pub tx_id: TxId,
    pub receipts: Arc<Vec<Receipt>>,
}

#[derive(
    Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Clone, Copy, Hash,
)]
pub struct CheckpointEvent {
    pub block_height: BlockHeight,
    pub events_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceEvent {
    TransactionEvent(TransactionEvent),
    Checkpoint(CheckpointEvent),
    Rollback(BlockHeight),
}

impl ServiceEvent {
    pub fn block_height(&self) -> BlockHeight {
        match self {
            ServiceEvent::TransactionEvent(event) => event.tx_pointer.block_height(),
            ServiceEvent::Checkpoint(event) => event.block_height,
            ServiceEvent::Rollback(at) => *at,
        }
    }
}

impl From<TransactionEvent> for ServiceEvent {
    fn from(event: TransactionEvent) -> Self {
        ServiceEvent::TransactionEvent(event)
    }
}

#[derive(
    Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Clone, Copy, Hash,
)]
pub struct BlockChainEvent<Event> {
    pub tx_pointer: TxPointer,
    pub receipt_index: u16,
    /// The id of the contract for which the event was emitted.
    pub contract_id: ContractId,
    pub tx_id: TxId,
    pub event: Event,
}

impl<Event> BlockChainEvent<Event> {
    pub fn require(&self, height: BlockHeight) -> anyhow::Result<()> {
        if self.tx_pointer.block_height() != height {
            return Err(anyhow::anyhow!(
                "Event tx_pointer {} does not match required height {}",
                self.tx_pointer,
                height
            ))
        }

        Ok(())
    }
}
