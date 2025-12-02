use crate::{
    adapters::{
        client_ext::{
            ClientExt,
            FullBlock,
        },
        concurrent_unordered_stream::ConcurrentUnorderedStream,
    },
    port::{
        Fetcher,
        FinalizedBlock,
    },
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
};
use fuel_core_types::{
    blockchain::{
        consensus::{
            Consensus,
            Genesis,
            poa::PoAConsensus,
        },
        header::{
            ApplicationHeader,
            BlockHeader,
            BlockHeaderV1,
            ConsensusHeader,
            GeneratedConsensusFields,
            v1::GeneratedApplicationFieldsV1,
        },
    },
    fuel_tx::TxPointer,
    fuel_types::BlockHeight,
    services::executor::{
        TransactionExecutionResult,
        TransactionExecutionStatus,
    },
};
use fuel_indexer_types::events::{
    ExecutionStatus,
    TransactionReceipts,
};
use fuels::tx::Receipt;
use futures::{
    Stream,
    pin_mut,
    stream::StreamExt,
};
use futures_util::TryStreamExt;
use iter_tools::Itertools;
use std::{
    collections::BTreeMap,
    iter,
    num::NonZeroUsize,
    ops::{
        Range,
        RangeInclusive,
    },
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        broadcast,
        mpsc,
    },
    time::{
        Instant,
        interval_at,
    },
};
use tokio_stream::wrappers::BroadcastStream;

#[cfg(feature = "blocks-subscription")]
use fuel_core_types::{
    fuel_tx::Transaction,
    fuel_types::canonical::Deserialize,
};

#[cfg(test)]
pub mod tests;

#[cfg(test)]
pub mod fuel_core_mock;

#[derive(Clone)]
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

impl Fetcher for GraphqlFetcher {
    fn predicted_receipts_stream(
        &self,
    ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
        let (tx, rx) = broadcast::channel(self.event_capacity.into());
        let client_clone = self.client.clone();

        tokio::spawn(async move {
            tracing::info!("Subscribing to preconfirmation events");

            let result = client_clone.preconfirmations_subscription().await;
            let mut subscription = match result {
                Ok(subscription) => subscription,
                Err(err) => {
                    if err.to_string().contains("--expensive-subscriptions") {
                        tracing::warn!(
                            "Preconfirmation subscriptions are disabled on the Fuel node."
                        );
                        futures::future::pending().await
                    } else {
                        tracing::error!(
                            "Failed to subscribe to preconfirmation events: {err:?}"
                        );
                    }
                    return
                }
            };

            loop {
                match subscription.next().await {
                    Some(Ok(preconf_result)) => {
                        match preconf_result {
                            fuel_core_client::client::types::TransactionStatus::PreconfirmationFailure {
                                transaction_id, tx_pointer, reason, receipts, ..
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
                                    execution_status: ExecutionStatus::Failure { reason }
                                };

                                if tx.send(event).is_err() {
                                    return;
                                }
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
                                    execution_status: ExecutionStatus::Success,
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
                        if err.to_string().contains("--expensive-subscriptions") {
                            tracing::warn!(
                                "Preconfirmation subscriptions are disabled on the Fuel node."
                            );
                            futures::future::pending().await
                        } else {
                            tracing::error!(
                                "Preconfirmation subscription error: {err:?}"
                            );
                        }
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

    fn finalized_blocks_stream(&self) -> anyhow::Result<BoxStream<FinalizedBlock>> {
        let (tx, rx) = broadcast::channel(self.heartbeat_capacity.into());
        let client_clone = self.client.clone();
        let fetcher = self.clone();

        tokio::spawn(async move {
            tracing::info!("Subscribing to heartbeat events");

            let result = client_clone.new_blocks_subscription().await;

            let pull_mode = {
                let tx = tx.clone();
                async move {
                    tracing::warn!("Block subscriptions are disabled on the Fuel node.");
                    let stream =
                        fetcher.pull_block_stream(Duration::from_millis(200)).await;
                    pin_mut!(stream);

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(block) => {
                                if tx.send(block).is_err() {
                                    return;
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    "Error fetching block via polling: {err}"
                                );
                            }
                        }
                    }
                }
            };

            let mut subscription = match result {
                Ok(subscription) => subscription,
                Err(err) => {
                    if err.to_string().contains("--expensive-subscriptions") {
                        pull_mode.await
                    } else {
                        tracing::error!(
                            "Failed to subscribe to preconfirmation events: {err:?}"
                        );
                    }
                    return
                }
            };

            loop {
                match subscription.next().await {
                    Some(Ok(import_result)) => {
                        let statuses = import_result.tx_status;

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
                            statuses,
                        };

                        if tx.send(block).is_err() {
                            tracing::info!("Heartbeat event channel closed");
                            return;
                        }
                    }
                    Some(Err(err)) => {
                        if err.to_string().contains("--expensive-subscriptions") {
                            pull_mode.await
                        } else {
                            tracing::error!("Heartbeat subscription error: {err:?}");
                        }
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
    ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static {
        let blocks_unordered_stream = blocks_for_batched(
            self.client.clone(),
            *range.start()..range.end().saturating_add(1),
            self.blocks_request_batch_size,
            self.blocks_request_concurrency,
        );

        let unordered_stream = blocks_unordered_stream.filter_map(|block| async move {
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

            let statuses = block
                .transactions
                .into_iter()
                .map(|tx| match tx.status {
                    None => {
                        Err(anyhow::anyhow!("Transaction status is missing for {tx:?}"))
                    }
                    Some(status) => match status {
                        TransactionStatus::SuccessStatus(s) => {
                            let receipts: Vec<_> = s
                                .receipts
                                .into_iter()
                                .map(Receipt::try_from)
                                .try_collect()?;

                            let status = TransactionExecutionStatus {
                                result: TransactionExecutionResult::Success {
                                    result: s
                                        .program_state
                                        .map(TryInto::try_into)
                                        .transpose()?,
                                    receipts: Arc::new(receipts),
                                    total_gas: s.total_gas.0,
                                    total_fee: s.total_fee.0,
                                },
                                id: tx.id.0.0,
                            };

                            Ok(status)
                        }
                        TransactionStatus::FailureStatus(s) => {
                            let receipts: Vec<_> = s
                                .receipts
                                .into_iter()
                                .map(Receipt::try_from)
                                .try_collect()?;

                            let status = TransactionExecutionStatus {
                                result: TransactionExecutionResult::Failed {
                                    result: s
                                        .program_state
                                        .map(TryInto::try_into)
                                        .transpose()?,
                                    receipts: Arc::new(receipts),
                                    total_gas: s.total_gas.0,
                                    total_fee: s.total_fee.0,
                                },
                                id: tx.id.0.0,
                            };

                            Ok(status)
                        }
                        _ => Err(anyhow::anyhow!(
                            "Unsupported transaction status variant: {status:?}"
                        )),
                    },
                })
                .try_collect::<_, Vec<_>, _>();

            let statuses = match statuses {
                Ok(statuses) => statuses,
                Err(e) => {
                    return Some(Err(anyhow::anyhow!(
                        "Failed to parse transaction statuses: {}",
                        e
                    )))
                }
            };

            let block = FinalizedBlock {
                header,
                consensus,
                #[cfg(feature = "blocks-subscription")]
                transactions,
                statuses,
            };

            Some(Ok(block))
        });

        let (sender, receiver) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let unordered_stream = unordered_stream;
            pin_mut!(unordered_stream);
            let mut remaining_range = range;

            let mut ready_blocks: BTreeMap<BlockHeight, FinalizedBlock> = BTreeMap::new();

            while let Some(result) = unordered_stream.next().await {
                match result {
                    Ok(block) => {
                        ready_blocks.insert(*block.header.height(), block);
                    }
                    Err(err) => {
                        let _ = sender.send(Err(err));
                        break;
                    }
                }

                while let Some(entry) = ready_blocks.first_entry() {
                    if **entry.key() == *remaining_range.start() {
                        let block = entry.remove();
                        if sender.send(Ok(block)).is_err() {
                            return;
                        }
                        remaining_range = (remaining_range.start().saturating_add(1))
                            ..=*remaining_range.end();
                    } else {
                        break;
                    }
                }
            }
        });

        tokio_stream::wrappers::UnboundedReceiverStream::new(receiver)
    }

    async fn last_height(&self) -> anyhow::Result<BlockHeight> {
        let chain_info = self.client.chain_info().await?;
        let height = chain_info.latest_block.header.height.into();
        Ok(height)
    }
}

impl GraphqlFetcher {
    /// Returns a stream of finalized blocks starting from the given height.
    pub async fn blocks_stream_starting_from(
        &self,
        start: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<FinalizedBlock>>> {
        let real_time = self.finalized_blocks_stream()?;
        let last_height = self.last_height().await?;

        let real_time = real_time
            .skip_while(move |block| {
                let skip = *block.header.height() <= last_height
                    || *block.header.height() < start;

                async move { skip }
            })
            .map(Result::<_, anyhow::Error>::Ok);

        let old_blocks_stream = self.finalized_blocks_for_range(*start..=*last_height);
        Ok(old_blocks_stream.chain(real_time).into_boxed())
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
                    Err(err) => {
                        // If there was an error, we can log it and try the next chunk.
                        tracing::error!(
                            "Error fetching blocks for range {start}..{last}: {err:?}"
                        );
                    }
                }
            }
        })
        .concurrent_unordered(concurrency);

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

impl GraphqlFetcher {
    async fn pull_block_stream(
        &self,
        interval_duration: Duration,
    ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> {
        let last_known_height = match self.last_height().await {
            Ok(height) => *height,
            Err(err) => {
                return futures::stream::once(async { Err(err) }).left_stream();
            }
        };
        let last_known_height = Arc::new(tokio::sync::Mutex::new(last_known_height));

        // Create an interval stream that ticks every X seconds
        let interval = interval_at(Instant::now() + interval_duration, interval_duration);

        // Convert the Interval stream into a stream of results
        tokio_stream::wrappers::IntervalStream::new(interval)
            .filter_map(move |_| {
                // We must spawn a new task to run the async logic for each tick.
                // The spawned task runs the fetch_data future.
                let last_known_height = last_known_height.clone();
                async move {
                    let current_known_height = match self.last_height().await {
                        Ok(height) => *height,
                        Err(err) => {
                            return Some(Err(err));
                        }
                    };

                    let mut last_known_height = last_known_height.lock().await;

                    if current_known_height > *last_known_height {
                        let stream = self.finalized_blocks_for_range(
                            (*last_known_height + 1)..=current_known_height,
                        );
                        *last_known_height = current_known_height;
                        Some(Ok(stream))
                    } else {
                        None
                    }
                }
            })
            .try_flatten()
            .right_stream()
    }
}
