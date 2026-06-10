//! Hybrid GraphQL+RPC fetcher.
//!
//! The protobuf `BlockAggregator` RPC is built for throughput: it serves
//! historical block ranges out of object storage and is the right transport
//! while the indexer is far behind the chain. But the aggregator lags the
//! chain tip (blocks are uploaded asynchronously) and does not expose
//! preconfirmations, so it is the wrong transport once the indexer has
//! caught up.
//!
//! [`HybridFetcher`] therefore routes by distance to the tip:
//!
//! * **Bulk synchronization** ([`Fetcher::finalized_blocks_for_range`]):
//!   heights up to `graphql_tip - sync_tail_blocks` (additionally capped by
//!   the aggregator's own synced height) are fetched over RPC; the remaining
//!   tail of the range falls through to GraphQL. If the RPC endpoint cannot
//!   be reached when the split is computed, the probe is retried
//!   [`RPC_PROBE_ATTEMPTS`] times before the whole range falls back to
//!   GraphQL with an error logged.
//! * **Steady state** (realtime [`Fetcher::finalized_blocks_stream`],
//!   [`Fetcher::predicted_receipts_stream`], [`Fetcher::last_height`]):
//!   always GraphQL. Once the indexer is within `sync_tail_blocks` of the
//!   tip no RPC request is made at all.

use crate::{
    adapters::{
        graphql_event_adapter::GraphqlFetcher,
        rpc_event_adapter::RpcFetcher,
    },
    port::{
        Fetcher,
        FinalizedBlock,
    },
};
use fuel_core_services::stream::BoxStream;
use fuel_core_types::fuel_types::BlockHeight;
use fuel_indexer_types::events::TransactionReceipts;
use futures::{
    Stream,
    StreamExt,
};
use std::ops::RangeInclusive;

/// Default number of blocks below the GraphQL chain tip that are always
/// fetched via GraphQL instead of RPC. Within this distance the indexer is
/// considered synchronized.
pub const DEFAULT_SYNC_TAIL_BLOCKS: u32 = 5;

/// How many times the RPC endpoint is probed before a bulk synchronization
/// range gives up on RPC and falls back to GraphQL.
pub const RPC_PROBE_ATTEMPTS: u32 = 3;

/// Delay between consecutive RPC probe attempts.
const RPC_PROBE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// Runs `attempt` up to `attempts` times, sleeping `delay` between failures.
/// Returns the first success, or the last error once attempts are exhausted.
async fn with_retries<T, F, Fut>(
    attempts: u32,
    delay: std::time::Duration,
    mut attempt: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut last_err = None;
    for i in 1..=attempts {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                tracing::warn!("RPC probe attempt {i}/{attempts} failed: {err:?}");
                last_err = Some(err);
                if i < attempts {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no attempts were made")))
}

#[derive(Clone)]
pub struct HybridFetcher {
    graphql: GraphqlFetcher,
    rpc: RpcFetcher,
    sync_tail_blocks: u32,
}

impl HybridFetcher {
    pub fn new(graphql: GraphqlFetcher, rpc: RpcFetcher, sync_tail_blocks: u32) -> Self {
        Self {
            graphql,
            rpc,
            sync_tail_blocks,
        }
    }
}

/// Splits a sync range into the RPC-served prefix and the GraphQL-served
/// tail. The RPC prefix ends at the lowest of: the requested end, the
/// aggregator's synced height (`rpc_tip`), and `graphql_tip - tail` (the
/// synchronized threshold). Returns `(rpc_range, graphql_range)`; either
/// side is `None` when empty.
fn split_sync_range(
    range: RangeInclusive<u32>,
    graphql_tip: u32,
    rpc_tip: u32,
    tail: u32,
) -> (Option<RangeInclusive<u32>>, Option<RangeInclusive<u32>>) {
    if range.is_empty() {
        return (None, None);
    }

    let rpc_end = (*range.end())
        .min(graphql_tip.saturating_sub(tail))
        .min(rpc_tip);

    if rpc_end < *range.start() {
        return (None, Some(range));
    }

    let graphql_range =
        (rpc_end < *range.end()).then(|| rpc_end.saturating_add(1)..=*range.end());

    (Some(*range.start()..=rpc_end), graphql_range)
}

impl Fetcher for HybridFetcher {
    /// Preconfirmations are only available over GraphQL.
    fn predicted_receipts_stream(
        &self,
    ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
        self.graphql.predicted_receipts_stream()
    }

    /// Realtime blocks come from GraphQL: when the indexer is synchronized
    /// it must not depend on the lagging aggregator.
    fn finalized_blocks_stream(&self) -> anyhow::Result<BoxStream<FinalizedBlock>> {
        self.graphql.finalized_blocks_stream()
    }

    fn finalized_blocks_for_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static {
        let graphql = self.graphql.clone();
        let rpc = self.rpc.clone();
        let tail = self.sync_tail_blocks;

        futures::stream::once(async move {
            let graphql_tip = match graphql.last_height().await {
                Ok(tip) => *tip,
                Err(err) => {
                    tracing::warn!(
                        "Hybrid fetcher: failed to query the GraphQL tip; \
                         fetching {range:?} via GraphQL only: {err:?}"
                    );
                    return graphql.finalized_blocks_for_range(range).boxed();
                }
            };

            // The whole range is within the synchronized tail — serve it
            // from GraphQL without touching the RPC endpoint.
            if *range.start() > graphql_tip.saturating_sub(tail) {
                return graphql.finalized_blocks_for_range(range).boxed();
            }

            let rpc_probe =
                with_retries(RPC_PROBE_ATTEMPTS, RPC_PROBE_RETRY_DELAY, || {
                    let rpc = rpc.clone();
                    async move { rpc.last_height().await }
                })
                .await;
            let rpc_tip = match rpc_probe {
                Ok(tip) => *tip,
                Err(err) => {
                    tracing::error!(
                        "Hybrid fetcher: RPC endpoint unavailable after \
                         {RPC_PROBE_ATTEMPTS} attempts; falling back to \
                         GraphQL for {range:?}: {err:?}"
                    );
                    return graphql.finalized_blocks_for_range(range).boxed();
                }
            };

            let (rpc_range, graphql_range) =
                split_sync_range(range, graphql_tip, rpc_tip, tail);

            tracing::debug!(
                "Hybrid fetcher: syncing {rpc_range:?} via RPC, \
                 {graphql_range:?} via GraphQL (graphql_tip {graphql_tip}, \
                 rpc_tip {rpc_tip})"
            );

            let rpc_part = rpc_range.map(|r| rpc.finalized_blocks_for_range(r).boxed());
            let graphql_part =
                graphql_range.map(|r| graphql.finalized_blocks_for_range(r).boxed());

            match (rpc_part, graphql_part) {
                (Some(rpc), Some(graphql)) => rpc.chain(graphql).boxed(),
                (Some(rpc), None) => rpc,
                (None, Some(graphql)) => graphql,
                (None, None) => futures::stream::empty().boxed(),
            }
        })
        .flatten()
    }

    /// The chain tip as GraphQL sees it — the aggregator's height lags and
    /// must not be mistaken for the latest height.
    async fn last_height(&self) -> anyhow::Result<BlockHeight> {
        self.graphql.last_height().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_far_behind_goes_fully_to_rpc() {
        // Tip 1000, syncing 0..=900: everything is below tip - tail.
        let (rpc, graphql) = split_sync_range(0..=900, 1000, 950, 5);
        assert_eq!(rpc, Some(0..=900));
        assert_eq!(graphql, None);
    }

    #[test]
    fn split_tail_of_range_goes_to_graphql() {
        // Syncing right up to the tip: the last `tail` blocks fall through.
        let (rpc, graphql) = split_sync_range(0..=999, 1000, 1000, 5);
        assert_eq!(rpc, Some(0..=995));
        assert_eq!(graphql, Some(996..=999));
    }

    #[test]
    fn split_synchronized_range_goes_fully_to_graphql() {
        // Fewer than `tail` blocks remaining: GraphQL only.
        let (rpc, graphql) = split_sync_range(997..=999, 1000, 1000, 5);
        assert_eq!(rpc, None);
        assert_eq!(graphql, Some(997..=999));
    }

    #[test]
    fn split_rpc_tip_caps_the_rpc_prefix() {
        // The aggregator lags at 500; everything above it must come from
        // GraphQL even though it is below tip - tail.
        let (rpc, graphql) = split_sync_range(0..=900, 1000, 500, 5);
        assert_eq!(rpc, Some(0..=500));
        assert_eq!(graphql, Some(501..=900));
    }

    #[test]
    fn split_rpc_tip_at_zero_goes_fully_to_graphql() {
        let (rpc, graphql) = split_sync_range(1..=900, 1000, 0, 5);
        assert_eq!(rpc, None);
        assert_eq!(graphql, Some(1..=900));
    }

    #[test]
    fn split_short_chain_goes_fully_to_graphql() {
        // Tip below `tail`: tip - tail saturates to 0.
        let (rpc, graphql) = split_sync_range(1..=3, 4, 4, 5);
        assert_eq!(rpc, None);
        assert_eq!(graphql, Some(1..=3));
    }

    #[test]
    fn split_empty_range_yields_nothing() {
        #[allow(clippy::reversed_empty_ranges)]
        let (rpc, graphql) = split_sync_range(10..=9, 1000, 1000, 5);
        assert_eq!(rpc, None);
        assert_eq!(graphql, None);
    }

    #[test]
    fn split_zero_tail_uses_rpc_to_the_tip() {
        let (rpc, graphql) = split_sync_range(0..=1000, 1000, 1000, 0);
        assert_eq!(rpc, Some(0..=1000));
        assert_eq!(graphql, None);
    }

    #[tokio::test]
    async fn with_retries_stops_at_first_success() {
        use std::sync::atomic::{
            AtomicU32,
            Ordering,
        };

        let calls = AtomicU32::new(0);
        let result = with_retries(3, std::time::Duration::ZERO, || {
            let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if attempt < 2 {
                    Err(anyhow::anyhow!("transient"))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn with_retries_exhausts_attempts_then_returns_last_error() {
        use std::sync::atomic::{
            AtomicU32,
            Ordering,
        };

        let calls = AtomicU32::new(0);
        let result: anyhow::Result<u32> =
            with_retries(3, std::time::Duration::ZERO, || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err(anyhow::anyhow!("still down")) }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
