//! gRPC/protobuf-based fetcher, mirroring [`GraphqlFetcher`] but using the
//! `BlockAggregator` RPC exposed by `fuel-core` 0.48 (via
//! `fuel-core-client`'s `rpc` feature).
//!
//! The protobuf RPC covers block-range queries, the new-block subscription,
//! and the aggregator's synced height. Preconfirmations are not in scope of
//! the RPC, so [`RpcFetcher::predicted_receipts_stream`] still relies on the
//! GraphQL subscription provided by the same [`FuelClient`].
//!
//! Compared to the GraphQL path, the RPC stream is naturally ordered and
//! already returns native `fuel_core_types` values, so we drop the cynic
//! query and the V1 header reconstruction. We keep the chunked
//! unordered-to-ordered fan-out and the `PendingBlocksController` because a
//! single `get_block_range` is internally sequential: each S3 GET for block
//! N blocks block N+1. Splitting the range into smaller windows and running
//! them concurrently is what gives us throughput.

use crate::{
    adapters::{
        concurrent_unordered_stream::ConcurrentUnorderedStream,
        graphql_event_adapter::PendingBlocksController,
    },
    port::{
        Fetcher,
        FinalizedBlock,
    },
};
use fuel_core_client::client::FuelClient;
use fuel_core_services::stream::{
    BoxStream,
    IntoBoxStream,
};
use fuel_core_types::{
    blockchain::consensus::{
        Consensus,
        poa::PoAConsensus,
    },
    fuel_tx::{
        Receipt,
        ScriptExecutionResult,
        Transaction,
        UniqueIdentifier,
    },
    fuel_types::{
        BlockHeight,
        ChainId,
    },
    services::executor::{
        TransactionExecutionResult,
        TransactionExecutionStatus,
    },
};
use fuel_indexer_types::events::{
    ExecutionStatus,
    TransactionReceipts,
};
use futures::{
    Stream,
    StreamExt,
    TryStreamExt,
    pin_mut,
};
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

#[derive(Clone)]
pub struct RpcFetcher {
    client: Arc<FuelClient>,
    chain_id: ChainId,
    event_capacity: NonZeroUsize,
    heartbeat_capacity: NonZeroUsize,
    blocks_request_batch_size: usize,
    blocks_request_concurrency: usize,
    pending_blocks_limit: usize,
    pull_block_interval: Duration,
}

pub struct RpcEventAdapterConfig {
    pub client: Arc<FuelClient>,
    pub chain_id: ChainId,
    pub heartbeat_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub blocks_request_batch_size: usize,
    pub blocks_request_concurrency: usize,
    pub pending_blocks_limit: usize,
    /// Polling interval of the new-block pull fallback (used when the block
    /// RPC subscription is unavailable).
    pub pull_block_interval: Duration,
}

pub fn create_rpc_event_adapter(config: RpcEventAdapterConfig) -> RpcFetcher {
    RpcFetcher {
        client: config.client,
        chain_id: config.chain_id,
        event_capacity: config.event_capacity,
        heartbeat_capacity: config.heartbeat_capacity,
        blocks_request_batch_size: config.blocks_request_batch_size,
        blocks_request_concurrency: config.blocks_request_concurrency,
        pending_blocks_limit: config.pending_blocks_limit,
        pull_block_interval: config.pull_block_interval,
    }
}

impl Fetcher for RpcFetcher {
    /// Preconfirmations are not exposed over the protobuf RPC. We reuse the
    /// GraphQL preconfirmation subscription on the same `FuelClient`.
    fn predicted_receipts_stream(
        &self,
    ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
        use fuel_core_types::fuel_tx::TxPointer;
        use semver::Version;

        let (tx, rx) = broadcast::channel(self.event_capacity.into());
        let client_clone: Arc<FuelClient> = self.client.clone();

        tokio::spawn(async move {
            tracing::info!("Subscribing to preconfirmation events");

            let version = match client_clone.node_info().await {
                Ok(info) => Version::parse(&info.node_version).expect("Invalid semver"),
                Err(err) => {
                    tracing::error!("Failed to fetch node info: {err:?}");
                    return;
                }
            };
            let handle_index_bug = version.minor <= 47;

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
                    return;
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

                                let index = if handle_index_bug {
                                    tx_pointer.tx_index().saturating_sub(1)
                                } else {
                                    tx_pointer.tx_index()
                                };

                                let corrected_tx_pointer = TxPointer::new(tx_pointer.block_height(), index);
                                let event = TransactionReceipts {
                                    tx_pointer: corrected_tx_pointer,
                                    tx_id: transaction_id,
                                    receipts: Arc::new(receipts),
                                    execution_status: ExecutionStatus::Failure { reason },
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

                                let index = if handle_index_bug {
                                    tx_pointer.tx_index().saturating_sub(1)
                                } else {
                                    tx_pointer.tx_index()
                                };

                                let corrected_tx_pointer = TxPointer::new(tx_pointer.block_height(), index);
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
                            _ => continue,
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
            tracing::info!("Subscribing to new-block RPC stream");

            let result = client_clone.new_block_subscription().await;

            let pull_mode = {
                let tx = tx.clone();
                let fetcher = fetcher.clone();
                async move {
                    tracing::warn!(
                        "Block RPC subscription unavailable; falling back to polling."
                    );
                    let stream =
                        fetcher.pull_block_stream(fetcher.pull_block_interval).await;
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
                Ok(subscription) => Box::pin(subscription),
                Err(err) => {
                    tracing::warn!(
                        "Failed to subscribe to new-block RPC stream: {err:?}; \
                         falling back to polling"
                    );
                    pull_mode.await;
                    return;
                }
            };

            loop {
                match subscription.next().await {
                    Some(Ok((block, receipts))) => {
                        let finalized = match fetcher.to_finalized_block(block, receipts)
                        {
                            Ok(block) => block,
                            Err(err) => {
                                tracing::error!(
                                    "Failed to convert RPC block to FinalizedBlock: {err}"
                                );
                                continue;
                            }
                        };

                        if tx.send(finalized).is_err() {
                            tracing::info!("New-block RPC channel closed");
                            return;
                        }
                    }
                    Some(Err(err)) => {
                        tracing::error!("New-block RPC subscription error: {err:?}");
                        return;
                    }
                    None => {
                        tracing::info!("New-block RPC subscription ended");
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

    fn finalized_blocks_for_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static {
        let controller = PendingBlocksController::new(
            (*range.start()).into(),
            self.pending_blocks_limit,
        );
        let unordered = blocks_for_batched_rpc(
            self.client.clone(),
            self.clone(),
            *range.start()..range.end().saturating_add(1),
            self.blocks_request_batch_size,
            self.blocks_request_concurrency,
            controller.clone(),
        );

        // Bounded so a slow downstream consumer applies backpressure all the
        // way to the fetch chunks: when this channel fills, the spawn awaits
        // on `send`, `remaining_range.start()` stops advancing, the
        // `PendingBlocksController` stops releasing new chunks, and S3 GETs
        // stop being issued. Without the bound the channel grows unboundedly
        // under any downstream stall (rocksdb compaction, lagging broadcast
        // subscriber, retry storms upstream, …) and we'd OOM in production.
        let (sender, receiver) =
            mpsc::channel::<anyhow::Result<FinalizedBlock>>(self.pending_blocks_limit);

        tokio::spawn(async move {
            pin_mut!(unordered);
            let mut remaining_range = range;
            let mut ready: BTreeMap<BlockHeight, FinalizedBlock> = BTreeMap::new();

            while let Some(result) = unordered.next().await {
                match result {
                    Ok(block) => {
                        ready.insert(*block.header.height(), block);
                    }
                    Err(err) => {
                        let _ = sender.send(Err(err)).await;
                        break;
                    }
                }

                while let Some(entry) = ready.first_entry() {
                    if **entry.key() == *remaining_range.start() {
                        let block = entry.remove();
                        if sender.send(Ok(block)).await.is_err() {
                            return;
                        }
                        remaining_range = (remaining_range.start().saturating_add(1))
                            ..=*remaining_range.end();
                    } else {
                        break;
                    }
                }

                controller.set_latest_height((*remaining_range.start()).into());
            }
        });

        tokio_stream::wrappers::ReceiverStream::new(receiver)
    }

    async fn last_height(&self) -> anyhow::Result<BlockHeight> {
        let height = self.client.get_aggregated_height().await?;
        Ok(height)
    }
}

impl RpcFetcher {
    /// Returns a stream of finalized blocks starting from the given height,
    /// chaining historical backfill with the realtime subscription.
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

    async fn pull_block_stream(
        &self,
        interval_duration: Duration,
    ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> {
        use std::sync::atomic::{
            AtomicU32,
            Ordering,
        };

        let last_known_height = match self.last_height().await {
            Ok(height) => *height,
            Err(err) => {
                return futures::stream::once(async { Err(err) }).left_stream();
            }
        };
        // Advanced per *delivered* block (not when a range fetch is merely
        // started), so a range that errors or ends early is re-requested
        // from the last delivered height on the next tick instead of being
        // skipped forever.
        let last_known_height = Arc::new(AtomicU32::new(last_known_height));

        let interval = interval_at(Instant::now() + interval_duration, interval_duration);

        tokio_stream::wrappers::IntervalStream::new(interval)
            .filter_map(move |_| {
                let last_known_height = last_known_height.clone();
                async move {
                    let current_known_height = match self.last_height().await {
                        Ok(height) => *height,
                        Err(err) => return Some(Err(err)),
                    };

                    let last = last_known_height.load(Ordering::Acquire);

                    if current_known_height > last {
                        let stream = self
                            .finalized_blocks_for_range(
                                last.saturating_add(1)..=current_known_height,
                            )
                            .map(move |result| {
                                if let Ok(block) = &result {
                                    last_known_height.fetch_max(
                                        (*block.header.height()).into(),
                                        Ordering::AcqRel,
                                    );
                                }
                                result
                            });
                        Some(Ok(stream))
                    } else {
                        None
                    }
                }
            })
            .try_flatten()
            .right_stream()
    }

    fn to_finalized_block(
        &self,
        block: fuel_core_types::blockchain::block::Block,
        receipts_per_tx: Vec<Vec<Receipt>>,
    ) -> anyhow::Result<FinalizedBlock> {
        let (header, transactions) = block.into_inner();

        if receipts_per_tx.len() != transactions.len() {
            return Err(anyhow::anyhow!(
                "RPC block had {} transactions but {} receipt vectors",
                transactions.len(),
                receipts_per_tx.len()
            ));
        }

        let statuses = transactions
            .iter()
            .zip(receipts_per_tx.into_iter())
            .map(|(tx, receipts)| build_status(tx, receipts, &self.chain_id))
            .collect::<Vec<_>>();

        // The protobuf BlockAggregator payload does not carry the consensus
        // signature. We synthesize a default PoA consensus for downstream
        // consumers; the indexer never verifies the signature itself.
        let consensus = Consensus::PoA(PoAConsensus {
            signature: Default::default(),
        });

        #[cfg(feature = "blocks-subscription")]
        let transactions = transactions.into_iter().map(Arc::new).collect::<Vec<_>>();
        #[cfg(not(feature = "blocks-subscription"))]
        let _ = transactions;

        Ok(FinalizedBlock {
            header,
            consensus,
            #[cfg(feature = "blocks-subscription")]
            transactions,
            statuses,
        })
    }
}

fn build_status(
    tx: &fuel_core_types::fuel_tx::Transaction,
    receipts: Vec<Receipt>,
    chain_id: &ChainId,
) -> TransactionExecutionStatus {
    // Recover success/failure from receipts: the canonical signal is a
    // trailing `ScriptResult` receipt for script transactions. For non-script
    // transactions (Mint, Create, Upgrade, …) there is no ScriptResult and
    // the transaction is considered successful — failed non-script txs are
    // not included in a block by the executor.
    let is_failure = receipts.iter().rev().find_map(|r| match r {
        Receipt::ScriptResult { result, .. } => {
            Some(!matches!(result, ScriptExecutionResult::Success))
        }
        _ => None,
    });

    let id = <Transaction as UniqueIdentifier>::id(tx, chain_id);
    let receipts = Arc::new(receipts);

    let result = if is_failure == Some(true) {
        TransactionExecutionResult::Failed {
            result: None,
            receipts,
            total_gas: 0,
            total_fee: 0,
        }
    } else {
        TransactionExecutionResult::Success {
            result: None,
            receipts,
            total_gas: 0,
            total_fee: 0,
        }
    };

    TransactionExecutionStatus { id, result }
}

/// Fan-out wrapper: splits `range` into `window_size`-wide chunks, runs up to
/// `concurrency` chunked `get_block_range` calls in parallel, and emits the
/// resulting blocks unordered. The caller is responsible for reordering by
/// `block.header.height()`. Transport errors retry indefinitely (matching the
/// GraphQL path); conversion errors propagate.
pub fn blocks_for_batched_rpc(
    client: Arc<FuelClient>,
    fetcher: RpcFetcher,
    range: Range<u32>,
    window_size: usize,
    concurrency: usize,
    controller: PendingBlocksController,
) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static {
    let chunks = range.chunks(window_size);

    let mut ranges = vec![];
    for chunk in chunks.into_iter() {
        let chunk = chunk.collect::<Vec<_>>();
        let start = chunk[0];
        let last =
            start.saturating_add(u32::try_from(chunk.len()).expect("chunk fits in u32"));
        ranges.push(start..last);
    }

    let iter = ranges.into_iter().zip(iter::repeat_with(move || {
        (client.clone(), fetcher.clone(), controller.clone())
    }));

    let stream = futures::stream::iter(iter)
        .map(|(r, (client, fetcher, controller))| async move {
            controller.wait_until_height(r.start.into()).await;

            // Retry transport errors. Conversion happens after the chunk
            // is fully fetched, and conversion errors are propagated
            // per-block via the returned stream.
            let raw = loop {
                match fetch_chunk(&client, r.clone()).await {
                    Ok(raw) => break raw,
                    Err(err) => {
                        tracing::error!(
                            "Error fetching RPC blocks for range {}..{}: {err:?}",
                            r.start,
                            r.end
                        );
                    }
                }
            };

            let results: Vec<anyhow::Result<FinalizedBlock>> = raw
                .into_iter()
                .map(|(block, receipts)| fetcher.to_finalized_block(block, receipts))
                .collect();
            futures::stream::iter(results)
        })
        .concurrent_unordered(concurrency);

    stream.flatten()
}

async fn fetch_chunk(
    client: &FuelClient,
    range: Range<u32>,
) -> anyhow::Result<Vec<(fuel_core_types::blockchain::block::Block, Vec<Vec<Receipt>>)>> {
    if range.is_empty() {
        return Ok(vec![]);
    }
    let start = BlockHeight::from(range.start);
    let end = BlockHeight::from(range.end.saturating_sub(1));
    let stream = client.get_block_range(start, end).await?;
    pin_mut!(stream);
    let mut out = Vec::with_capacity((range.end - range.start) as usize);
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}
