use crate::{
    adapters::{
        client_ext::{
            ClientExt,
            FullBlock,
            TransactionWithReceipts,
        },
        concurrent_stream::ConcurrentStream,
    },
    port::FinalizedBlock,
};
use fuel_core_client::client::{
    FuelClient,
    pagination::{
        PageDirection,
        PaginationRequest,
    },
    schema::{
        block::{
            Consensus as ClientConsensus,
            HeaderVersion,
        },
        tx::TransactionStatus,
    },
};
use fuel_core_services::stream::{
    BoxStream,
    IntoBoxStream,
    RefBoxStream,
};
use fuel_core_types::{
    blockchain::header::{
        ApplicationHeader,
        BlockHeader,
        BlockHeaderV1,
        ConsensusHeader,
        GeneratedConsensusFields,
        v1::GeneratedApplicationFieldsV1,
    },
    fuel_tx::TxPointer,
    fuel_types::BlockHeight,
    services::executor::{
        TransactionExecutionResult,
        TransactionExecutionStatus,
    },
};
use fuel_indexer_types::events::TransactionReceipts;
use fuels::tx::Receipt;
use futures::{
    Stream,
    StreamExt,
};
use iter_tools::Itertools;
use std::{
    iter,
    num::NonZeroUsize,
    ops::{
        Range,
        RangeInclusive,
    },
    sync::Arc,
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use fuel_core_types::blockchain::consensus::{
    Consensus,
    Genesis,
    poa::PoAConsensus,
};
#[cfg(feature = "blocks-subscription")]
use fuel_core_types::{
    fuel_tx::Transaction,
    fuel_types::canonical::Deserialize,
};

#[cfg(test)]
pub mod tests;

#[cfg(test)]
pub mod fuel_core_mock;

pub struct GraphqlFetcher {
    client: Arc<FuelClient>,
    event_capacity: NonZeroUsize,
    heartbeat_capacity: NonZeroUsize,
    blocks_request_batch_size: usize,
    blocks_request_concurrency: usize,
}

pub struct GraphqlEventAdapterConfig {
    pub client: Arc<FuelClient>,
    pub heartbeat_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub blocks_request_batch_size: usize,
    pub blocks_request_concurrency: usize,
}

trait TransactionExecutionExt {
    fn receipts(self) -> anyhow::Result<Option<Arc<Vec<Receipt>>>>;
}

impl TransactionExecutionExt for TransactionExecutionStatus {
    fn receipts(self) -> anyhow::Result<Option<Arc<Vec<Receipt>>>> {
        let receipts = match self.result {
            TransactionExecutionResult::Success { receipts, .. } => Some(receipts),
            _ => None,
        };

        Ok(receipts)
    }
}

impl TransactionExecutionExt for TransactionWithReceipts {
    fn receipts(self) -> anyhow::Result<Option<Arc<Vec<Receipt>>>> {
        let Some(status) = self.status else {
            return Err(anyhow::anyhow!("Transaction has no status"));
        };
        let receipts = match status {
            TransactionStatus::SuccessStatus(s) => s.receipts,
            _ => return Ok(None),
        };

        let receipts: Vec<Receipt> =
            receipts.into_iter().map(Receipt::try_from).try_collect()?;

        Ok(Some(receipts.into()))
    }
}

impl super::super::port::Fetcher for GraphqlFetcher {
    async fn predicted_receipts_stream(
        &self,
    ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
        let (tx, rx) = broadcast::channel(self.event_capacity.into());
        let client_clone = self.client.clone();

        tracing::info!("Subscribing to preconfirmation events");

        tokio::spawn(async move {
            let Ok(mut subscription) = client_clone.preconfirmations_subscription().await
            else {
                return
            };

            tracing::info!("Subscribed to preconfirmation events");

            loop {
                match subscription.next().await {
                    Some(Ok(preconf_result)) => {
                        match preconf_result {
                            fuel_core_client::client::types::TransactionStatus::PreconfirmationFailure {
                                transaction_id, reason, ..
                            } => {
                                tracing::error!(
                                    "Preconfirmation failure for transaction {}: {}",
                                    transaction_id,
                                    reason
                                );
                                continue;
                            }
                            fuel_core_client::client::types::TransactionStatus::PreconfirmationSuccess {
                                tx_pointer, receipts, transaction_id, ..
                            } => {
                                let Some(receipts) = receipts.filter(|r| !r.is_empty()) else {
                                    continue;
                                };

                                // `fuel-core` has a bug where pre confirmations use incorrect tx pointer
                                let corrected_tx_pointer = TxPointer::new(tx_pointer.block_height(), tx_pointer.tx_index().saturating_sub(1));
                                let event = TransactionReceipts {
                                    tx_pointer: corrected_tx_pointer,
                                    tx_id: transaction_id,
                                    receipts: Arc::new(receipts),
                                };

                                if tx.send(event).is_err() {
                                    return;
                                }
                            }
                            _ => {
                                continue;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        tracing::error!("Preconfirmation subscription error: {}", err);
                        return;
                    }
                    None => {
                        tracing::info!("Preconfirmation subscription ended");
                        return;
                    }
                }
            }
        });

        let stream = BroadcastStream::new(rx)
            .take_while(|result| {
                let good = result.is_ok();
                async move { good }
            })
            .map(|result| result.expect("We only take successful results; qed"));

        Ok(stream.into_boxed())
    }

    async fn finalized_blocks_stream(&self) -> anyhow::Result<BoxStream<FinalizedBlock>> {
        let (tx, rx) = broadcast::channel(self.heartbeat_capacity.into());
        let client_clone = self.client.clone();

        tokio::spawn(async move {
            tracing::info!("Subscribing to heartbeat events");

            let Ok(mut subscription) = client_clone.new_blocks_subscription().await
            else {
                return;
            };

            tracing::info!("Subscribed to heartbeat events");

            loop {
                match subscription.next().await {
                    Some(Ok(import_result)) => {
                        let block_height =
                            *import_result.sealed_block.entity.header().height();

                        let mut events =
                            Vec::with_capacity(import_result.tx_status.len());

                        for (i, transaction) in
                            import_result.tx_status.into_iter().enumerate()
                        {
                            let tx_id = transaction.id;
                            if let Ok(Some(receipts)) = transaction.receipts() {
                                let is_mint =
                                    import_result.sealed_block.entity.transactions()[i]
                                        .is_mint();

                                if !is_mint {
                                    let event = TransactionReceipts {
                                        tx_pointer: TxPointer::new(
                                            block_height,
                                            i as u16,
                                        ),
                                        tx_id,
                                        receipts,
                                    };

                                    events.push(event);
                                }
                            }
                        }

                        let consensus = import_result.sealed_block.consensus.clone();

                        let (header, _transactions) =
                            import_result.sealed_block.entity.into_inner();
                        #[cfg(feature = "blocks-subscription")]
                        let transactions =
                            _transactions.into_iter().map(Arc::new).collect::<Vec<_>>();

                        let block = FinalizedBlock {
                            header,
                            consensus,
                            #[cfg(feature = "blocks-subscription")]
                            transactions,
                            receipts: events,
                        };

                        if tx.send(block).is_err() {
                            tracing::info!("Heartbeat event channel closed");
                            return;
                        }
                    }
                    Some(Err(err)) => {
                        tracing::error!("Heartbeat subscription error: {}", err);
                        return;
                    }
                    None => {
                        tracing::info!("Heartbeat subscription ended");
                        return;
                    }
                }
            }
        });

        use futures::stream::StreamExt;
        let stream = BroadcastStream::new(rx)
            .take_while(|result| {
                let good = result.is_ok();
                async move { good }
            })
            .map(|result| result.expect("We only take successful results; qed"));

        Ok(stream.into_boxed())
    }

    fn finalized_blocks_for_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> RefBoxStream<'static, anyhow::Result<FinalizedBlock>> {
        let blocks_stream = blocks_for_batched(
            self.client.clone(),
            *range.start()..range.end().saturating_add(1),
            self.blocks_request_batch_size,
            self.blocks_request_concurrency,
        );

        let stream = blocks_stream.filter_map(|block| async move {
            let mut header = match block.header.version {
                HeaderVersion::V1 => {
                    let mut default = BlockHeaderV1::default();

                    default.set_application_header(ApplicationHeader {
                        da_height: block.header.da_height.0.into(),
                        consensus_parameters_version: block
                            .header
                            .consensus_parameters_version
                            .into(),
                        state_transition_bytecode_version: block
                            .header
                            .state_transition_bytecode_version
                            .into(),
                        generated: GeneratedApplicationFieldsV1 {
                            transactions_count: block.header.transactions_count.into(),
                            message_receipt_count: block
                                .header
                                .message_receipt_count
                                .into(),
                            transactions_root: block.header.transactions_root.into(),
                            message_outbox_root: block.header.message_outbox_root.into(),
                            event_inbox_root: block.header.event_inbox_root.into(),
                        },
                    });

                    BlockHeader::V1(default)
                }
                HeaderVersion::V2 => {
                    return Some(Err(anyhow::anyhow!(
                        "Unsupported block header version 2"
                    )));
                }
                HeaderVersion::Unknown => {
                    return Some(Err(anyhow::anyhow!(
                        "Unsupported block header version: {:?}",
                        block.header.version
                    )));
                }
            };

            header.set_consensus_header(ConsensusHeader {
                prev_root: block.header.prev_root.into(),
                height: block.header.height.0.into(),
                time: block.header.time.0,
                generated: GeneratedConsensusFields {
                    application_hash: Default::default(),
                },
            });
            header.recalculate_metadata();

            let consensus = match block.consensus {
                ClientConsensus::Genesis(c) => Consensus::Genesis(Genesis {
                    chain_config_hash: c.chain_config_hash.into(),
                    coins_root: c.coins_root.into(),
                    contracts_root: c.contracts_root.into(),
                    messages_root: c.messages_root.into(),
                    transactions_root: c.transactions_root.into(),
                }),
                ClientConsensus::PoAConsensus(c) => Consensus::PoA(PoAConsensus {
                    signature: c.signature.into_signature(),
                }),
                ClientConsensus::Unknown => {
                    return Some(Err(anyhow::anyhow!(
                        "Unsupported consensus variant: Unknown"
                    )));
                }
            };

            let block_height = *header.height();

            #[cfg(feature = "blocks-subscription")]
            let transactions = block
                .transactions
                .iter()
                .map(|tx| Transaction::from_bytes(&tx.raw_payload.0.0).map(Arc::new))
                .try_collect::<_, Vec<_>, _>();

            #[cfg(feature = "blocks-subscription")]
            let transactions = match transactions {
                Ok(txs) => txs,
                Err(e) => {
                    return Some(Err(anyhow::anyhow!(
                        "Failed to deserialize transaction: {}",
                        e
                    )))
                }
            };

            let iter = block.transactions.into_iter().enumerate().filter_map(
                move |(i, tx_with_receipts)| {
                    let tx_pointer = TxPointer::new(block_height, i as u16);

                    parse_receipts(tx_with_receipts.is_mint, tx_pointer, tx_with_receipts)
                        .transpose()
                },
            );
            let result = iter.try_collect::<_, Vec<_>, _>();

            let events = match result {
                Ok(events) => events,
                Err(e) => {
                    return Some(Err(anyhow::anyhow!("Failed to parse receipts: {}", e)))
                }
            };

            let block = FinalizedBlock {
                header,
                consensus,
                #[cfg(feature = "blocks-subscription")]
                transactions,
                receipts: events,
            };

            Some(Ok(block))
        });

        stream.into_boxed_ref()
    }

    async fn last_height(&self) -> anyhow::Result<BlockHeight> {
        let chain_info = self.client.chain_info().await?;
        let height = chain_info.latest_block.header.height.into();
        Ok(height)
    }
}

pub fn create_graphql_event_adapter(config: GraphqlEventAdapterConfig) -> GraphqlFetcher {
    GraphqlFetcher {
        client: config.client,
        event_capacity: config.event_capacity,
        heartbeat_capacity: config.heartbeat_capacity,
        blocks_request_batch_size: config.blocks_request_batch_size,
        blocks_request_concurrency: config.blocks_request_concurrency,
    }
}

pub fn blocks_for_batched(
    client: Arc<FuelClient>,
    range: Range<u32>,
    window_size: usize,
    concurrency: usize,
) -> impl Stream<Item = FullBlock> + Send + 'static {
    let chunks = range.chunks(window_size);

    let mut ranges = vec![];
    for chunk in chunks.into_iter() {
        let chunk = chunk.collect::<Vec<_>>();
        let start = chunk[0];
        let last =
            start.saturating_add(u32::try_from(chunk.len()).expect("It is 40 above"));

        ranges.push(start..last);
    }

    let iter = ranges
        .into_iter()
        .zip(iter::repeat_with(move || client.clone()));

    let stream = futures::stream::iter(iter)
        .map(|(r, c)| async move {
            let start = r.start;
            let last = r.end;

            loop {
                let result = blocks_for(c.as_ref(), start..last).await;

                match result {
                    Ok(blocks) => return futures::stream::iter(blocks),
                    Err(e) => {
                        // If there was an error, we can log it and try the next chunk.
                        tracing::error!(
                            "Error fetching blocks for range {start}..{last}: {e}"
                        );
                    }
                }
            }
        })
        .concurrent(concurrency);

    stream.flatten()
}

pub async fn blocks_for(
    client: &FuelClient,
    range: Range<u32>,
) -> anyhow::Result<Vec<FullBlock>> {
    if range.is_empty() {
        return Ok(vec![]);
    }

    let start = range.start.saturating_sub(1);
    let size = i32::try_from(range.len()).expect("Should be a valid i32");

    let request = PaginationRequest {
        cursor: Some(start.to_string()),
        results: size,
        direction: PageDirection::Forward,
    };
    let response = client.full_blocks(request).await?;
    Ok(response.results)
}

fn parse_receipts(
    is_mint: bool,
    tx_pointer: TxPointer,
    tx_with_receipts: TransactionWithReceipts,
) -> anyhow::Result<Option<TransactionReceipts>> {
    if is_mint {
        return Ok(None);
    }

    let tx_id = tx_with_receipts.id.0.0;
    let receipts = tx_with_receipts.receipts()?.unwrap_or_default();

    let event = TransactionReceipts {
        tx_pointer,
        tx_id,
        receipts,
    };

    Ok(Some(event))
}
