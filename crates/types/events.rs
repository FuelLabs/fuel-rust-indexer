use fuel_core_types::{
    blockchain::header::BlockHeader,
    fuel_tx::{
        ContractId,
        Receipt,
        Transaction,
        TxId,
        TxPointer,
    },
    fuel_types::{
        Address,
        BlockHeight,
    },
};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionReceipts {
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
pub enum UnstableReceipts {
    Receipts(TransactionReceipts),
    Checkpoint(CheckpointEvent),
    Rollback(BlockHeight),
}

impl UnstableReceipts {
    pub fn block_height(&self) -> BlockHeight {
        match self {
            UnstableReceipts::Receipts(event) => event.tx_pointer.block_height(),
            UnstableReceipts::Checkpoint(event) => event.block_height,
            UnstableReceipts::Rollback(at) => *at,
        }
    }
}

impl From<TransactionReceipts> for UnstableReceipts {
    fn from(event: TransactionReceipts) -> Self {
        UnstableReceipts::Receipts(event)
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

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Clone)]
pub struct BlockEvent {
    pub header: BlockHeader,
    pub producer: Option<Address>,
    pub transactions: Vec<Arc<Transaction>>,
    pub receipts: Vec<TransactionReceipts>,
}
