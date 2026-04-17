//! Fan-in fetcher that aggregates preconfirmation and finalized-block streams
//! from multiple GraphQL subscription endpoints and routes them through a
//! [`RouterState`] so each height is served from the first source that reports
//! it.

use crate::{
    adapters::{
        graphql_event_adapter::{
            GraphqlEventAdapterConfig,
            GraphqlFetcher,
            create_graphql_event_adapter,
        },
        subscription_router::{
            BlockDecision,
            PreconfDecision,
            RouterState,
            SourceId,
        },
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
use fuel_core_types::fuel_types::BlockHeight;
use fuel_indexer_types::events::TransactionReceipts;
use futures::{
    Stream,
    stream::{
        self,
        StreamExt,
        select_all,
    },
};
use std::{
    num::NonZeroUsize,
    ops::RangeInclusive,
    sync::{
        Arc,
        Mutex,
    },
};
use url::Url;

/// Number of recent sealed heights whose block id is retained to detect
/// cross-source mismatches.
#[derive(Clone)]
pub struct MultiSourceFetcher {
    main: GraphqlFetcher,
    sources: Vec<GraphqlFetcher>,
    router: Arc<Mutex<RouterState>>,
}

pub struct MultiSourceFetcherConfig {
    pub main_urls: Vec<Url>,
    pub subscription_sources: Vec<Url>,
    pub heartbeat_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub blocks_request_batch_size: usize,
    pub blocks_request_concurrency: usize,
    pub pending_blocks_limit: usize,
}

impl MultiSourceFetcher {
    pub fn new(config: MultiSourceFetcherConfig) -> anyhow::Result<Self> {
        let MultiSourceFetcherConfig {
            main_urls,
            subscription_sources,
            heartbeat_capacity,
            event_capacity,
            blocks_request_batch_size,
            blocks_request_concurrency,
            pending_blocks_limit,
        } = config;

        let build = |client: Arc<FuelClient>| -> GraphqlFetcher {
            create_graphql_event_adapter(GraphqlEventAdapterConfig {
                client,
                heartbeat_capacity,
                event_capacity,
                blocks_request_batch_size,
                blocks_request_concurrency,
                pending_blocks_limit,
            })
        };

        let main_client = Arc::new(FuelClient::with_urls(&main_urls)?);
        let main = build(main_client);

        let sources = subscription_sources
            .into_iter()
            .map(|url| {
                let client = Arc::new(FuelClient::new(url.clone())?);
                Ok::<_, anyhow::Error>(build(client))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            main,
            sources,
            router: Arc::new(Mutex::new(RouterState::new())),
        })
    }
}

/// Tagged item from a per-source stream. `Ended` is emitted when a source's
/// underlying subscription terminates (the GraphQL stream yielded `None`, which
/// happens on connection/transport error); it lets the merged stream short-
/// circuit so the service rebuilds every source instead of silently running
/// with one dead endpoint.
enum SourceEvent<T> {
    Item(SourceId, T),
    Ended(SourceId),
}

fn tagged_preconfs(
    fetcher: &GraphqlFetcher,
    source: SourceId,
) -> anyhow::Result<BoxStream<SourceEvent<TransactionReceipts>>> {
    let stream = fetcher
        .predicted_receipts_stream()?
        .map(move |r| SourceEvent::Item(source, r))
        .chain(stream::once(async move { SourceEvent::Ended(source) }));
    Ok(stream.into_boxed())
}

fn tagged_blocks(
    fetcher: &GraphqlFetcher,
    source: SourceId,
) -> anyhow::Result<BoxStream<SourceEvent<FinalizedBlock>>> {
    let stream = fetcher
        .finalized_blocks_stream()?
        .map(move |b| SourceEvent::Item(source, b))
        .chain(stream::once(async move { SourceEvent::Ended(source) }));
    Ok(stream.into_boxed())
}

/// Stops the merged stream on the first `Ended` sentinel, logging which source
/// died. `select_all` only ends when every child ends, so without this one
/// broken source would be silently masked by the others.
fn stop_on_first_end<T: Send + 'static>(
    merged: impl Stream<Item = SourceEvent<T>> + Send + 'static,
    kind: &'static str,
) -> impl Stream<Item = (SourceId, T)> + Send + 'static {
    merged
        .take_while(move |e| {
            let cont = match e {
                SourceEvent::Ended(source) => {
                    tracing::warn!(
                        "{kind} subscription from {source} ended; tearing down \
                         merged stream to trigger reconnect of all sources"
                    );
                    false
                }
                SourceEvent::Item(..) => true,
            };
            async move { cont }
        })
        .filter_map(|e| async move {
            match e {
                SourceEvent::Item(source, item) => Some((source, item)),
                SourceEvent::Ended(_) => None,
            }
        })
}

impl Fetcher for MultiSourceFetcher {
    fn predicted_receipts_stream(
        &self,
    ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
        if self.sources.is_empty() {
            return self.main.predicted_receipts_stream();
        }

        let tagged = self
            .sources
            .iter()
            .enumerate()
            .map(|(i, f)| tagged_preconfs(f, SourceId(i)))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let merged = stop_on_first_end(select_all(tagged), "preconfirmation");
        let router = self.router.clone();

        let filtered = merged.filter_map(move |(source, event)| {
            let router = router.clone();
            async move {
                let height = event.tx_pointer.block_height();
                let decision = {
                    let mut state = router.lock().expect("router poisoned");
                    state.admit_preconf(source, height)
                };

                match decision {
                    PreconfDecision::Forward => Some(event),
                    PreconfDecision::DropStale => None,
                    PreconfDecision::DropAlreadyOwned { owner } => {
                        tracing::trace!(
                            "Discarding preconfirmation for block {height} from \
                             {source}: already owned by {owner}"
                        );
                        None
                    }
                }
            }
        });

        Ok(filtered.into_boxed())
    }

    fn finalized_blocks_stream(&self) -> anyhow::Result<BoxStream<FinalizedBlock>> {
        if self.sources.is_empty() {
            return self.main.finalized_blocks_stream();
        }

        let tagged = self
            .sources
            .iter()
            .enumerate()
            .map(|(i, f)| tagged_blocks(f, SourceId(i)))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let merged = stop_on_first_end(select_all(tagged), "finalized block");
        let router = self.router.clone();

        let filtered = merged.filter_map(move |(source, block)| {
            let router = router.clone();
            async move {
                let height = *block.header.height();
                let id = block.header.id();
                let decision = {
                    let mut state = router.lock().expect("router poisoned");
                    state.admit_block(source, height, id)
                };

                match decision {
                    BlockDecision::Forward => Some(block),
                    BlockDecision::DropStale | BlockDecision::DropDuplicate => {
                        tracing::trace!(
                            "Discarding duplicate finalized block {height} from {source}"
                        );
                        None
                    }
                    BlockDecision::DropMismatch {
                        first_source,
                        first_id,
                    } => {
                        tracing::warn!(
                            "Finalized block mismatch at height {height}: \
                             {first_source} reported {first_id:?} first, \
                             {source} now reports {:?}; discarding the latter",
                            id
                        );
                        None
                    }
                }
            }
        });

        Ok(filtered.into_boxed())
    }

    fn finalized_blocks_for_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static {
        self.main.finalized_blocks_for_range(range)
    }

    async fn last_height(&self) -> anyhow::Result<BlockHeight> {
        self.main.last_height().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    fn tagged<T: Send + Sync + 'static>(
        source: SourceId,
        items: Vec<T>,
    ) -> BoxStream<SourceEvent<T>> {
        stream::iter(items)
            .map(move |v| SourceEvent::Item(source, v))
            .chain(stream::once(async move { SourceEvent::Ended(source) }))
            .into_boxed()
    }

    #[tokio::test]
    async fn merged_stream_ends_when_any_source_ends() {
        // s0 ends after 2 items; s1 has 100 items still to deliver. The merged
        // stream must terminate as soon as s0's Ended sentinel arrives, even
        // though s1 is still live — this forces the service to rebuild both.
        let s0 = tagged(SourceId(0), vec![10u32, 11]);
        let s1 = tagged(SourceId(1), (0..100).collect::<Vec<u32>>());

        let merged = stop_on_first_end(select_all(vec![s0, s1]), "test");
        let collected: Vec<_> = merged.collect().await;

        // s0 contributes exactly 2 items before its sentinel terminates the
        // merge; s1 may contribute some items that were interleaved before
        // the sentinel, but far fewer than its full 100.
        let from_s0 = collected.iter().filter(|(s, _)| *s == SourceId(0)).count();
        assert_eq!(from_s0, 2);
        assert!(
            collected.len() < 100,
            "merged stream should terminate before s1 drains; got {} items",
            collected.len()
        );
    }

    #[tokio::test]
    async fn service_rebuild_loop_recovers_after_source_dies() {
        // Simulates the service's rebuild path (service.rs: on stream `None`,
        // call `fetcher.predicted_receipts_stream()` again). Factory call 0
        // returns a merge where s0 dies after 1 item; factory call 1 returns
        // a healthy merge. The loop must:
        //   1. exhaust the first stream (stop_on_first_end kicks in),
        //   2. invoke the factory again,
        //   3. drain the fresh stream successfully.
        use std::sync::{
            Arc,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_factory = calls.clone();
        let factory = move || {
            let n = calls_for_factory.fetch_add(1, Ordering::SeqCst);
            let streams = if n == 0 {
                // First build: source 0 dies after 1 item; source 1 would
                // happily deliver 1000 more but must be torn down.
                vec![
                    tagged(SourceId(0), vec![1u32]),
                    tagged(SourceId(1), (100..1100).collect::<Vec<u32>>()),
                ]
            } else {
                // Rebuild: fresh source delivers its full batch before ending.
                vec![tagged(SourceId(0), vec![10u32, 11, 12])]
            };
            stop_on_first_end(select_all(streams), "test").into_boxed()
        };

        // Iteration 1: drain until the merged stream ends (s0 sentinel fires).
        let mut stream = factory();
        let mut drained_first: Vec<(SourceId, u32)> = Vec::new();
        while let Some(item) = stream.next().await {
            drained_first.push(item);
        }
        assert!(
            drained_first.len() < 1001,
            "first stream must terminate before s1 drains; got {} items",
            drained_first.len()
        );

        // Iteration 2: rebuild. Verify the factory was called again and the
        // new stream delivers items.
        let stream = factory();
        let drained_second: Vec<_> = stream.collect().await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "factory must be re-invoked"
        );
        assert_eq!(
            drained_second,
            vec![(SourceId(0), 10), (SourceId(0), 11), (SourceId(0), 12),]
        );
    }

    #[tokio::test]
    async fn merged_stream_delivers_items_until_end() {
        // Single source, finite: every item is delivered, then the stream ends.
        let s0 = tagged(SourceId(0), vec![1u32, 2, 3]);
        let merged = stop_on_first_end(select_all(vec![s0]), "test");
        let collected: Vec<_> = merged.collect().await;
        assert_eq!(
            collected,
            vec![(SourceId(0), 1), (SourceId(0), 2), (SourceId(0), 3),]
        );
    }
}
