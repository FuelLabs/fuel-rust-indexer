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
use rand::Rng;
use std::{
    num::NonZeroUsize,
    ops::RangeInclusive,
    sync::{
        Arc,
        Mutex,
    },
    time::{
        Duration,
        Instant,
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

/// Policy controlling how aggressively a per-source subscription retries
/// after it ends or fails to establish. The goal is to keep failed sources
/// reconnecting in the background without hammering them: each consecutive
/// failure doubles the delay up to `max`, and the delay resets to `base`
/// only after a connection has been stably delivering items for at least
/// `stability_threshold`. ±20% jitter is applied so multiple indexer
/// instances don't synchronize their reconnects.
#[derive(Clone, Copy, Debug)]
pub struct BackoffPolicy {
    pub base: Duration,
    pub max: Duration,
    pub stability_threshold: Duration,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(60),
            stability_threshold: Duration::from_secs(30),
        }
    }
}

/// Computes the next backoff given the previous value and what happened on
/// the just-ended connection attempt. Resets to `base` only when the prior
/// connection was stable (delivered items for at least `stability_threshold`).
fn next_backoff(
    prev: Duration,
    items_delivered: u64,
    connection_lifetime: Duration,
    policy: BackoffPolicy,
) -> Duration {
    if items_delivered > 0 && connection_lifetime >= policy.stability_threshold {
        policy.base
    } else {
        let doubled = prev.saturating_mul(2);
        doubled.min(policy.max)
    }
}

fn jitter(delay: Duration) -> Duration {
    // Multiplicative jitter in [0.8, 1.2].
    let factor = rand::thread_rng().gen_range(0.8f64..1.2f64);
    let nanos = (delay.as_nanos() as f64 * factor) as u64;
    Duration::from_nanos(nanos)
}

/// Builds a self-reconnecting per-source stream: when the inner subscription
/// ends (connection drop, transport error), the source sleeps with
/// exponential backoff + jitter and tries again, transparently emitting items
/// from the new subscription. The merged fan-in stream stays alive on
/// whichever sources are currently up; sources that are down keep retrying
/// politely in the background and rejoin the fan-in once they reconnect.
fn reconnecting_source_stream<Fetcher, T, F>(
    fetcher: Fetcher,
    source: SourceId,
    kind: &'static str,
    subscribe: F,
) -> BoxStream<(SourceId, T)>
where
    Fetcher: Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: Fn(&Fetcher) -> anyhow::Result<BoxStream<T>> + Send + Sync + 'static,
{
    reconnecting_source_stream_with_policy(
        fetcher,
        source,
        kind,
        subscribe,
        BackoffPolicy::default(),
    )
}

fn reconnecting_source_stream_with_policy<Fetcher, T, F>(
    fetcher: Fetcher,
    source: SourceId,
    kind: &'static str,
    subscribe: F,
    policy: BackoffPolicy,
) -> BoxStream<(SourceId, T)>
where
    Fetcher: Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: Fn(&Fetcher) -> anyhow::Result<BoxStream<T>> + Send + Sync + 'static,
{
    struct State<Fetcher, T, F> {
        fetcher: Fetcher,
        subscribe: Arc<F>,
        source: SourceId,
        kind: &'static str,
        is_first: bool,
        backoff: Duration,
        inner: Option<BoxStream<T>>,
        connection_start: Option<Instant>,
        items_on_current: u64,
        policy: BackoffPolicy,
    }

    let state = State::<Fetcher, T, F> {
        fetcher,
        subscribe: Arc::new(subscribe),
        source,
        kind,
        is_first: true,
        backoff: policy.base,
        inner: None,
        connection_start: None,
        items_on_current: 0,
        policy,
    };

    stream::unfold(state, |mut s| async move {
        loop {
            if s.inner.is_none() {
                if !s.is_first {
                    let delay = jitter(s.backoff);
                    tracing::info!(
                        "{} subscription from {} reconnecting in {:?}",
                        s.kind,
                        s.source,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
                s.is_first = false;
                match (s.subscribe)(&s.fetcher) {
                    Ok(stream) => {
                        s.inner = Some(stream);
                        s.connection_start = Some(Instant::now());
                        s.items_on_current = 0;
                    }
                    Err(err) => {
                        tracing::warn!(
                            "{} subscription from {} failed to establish: \
                             {err:?}; will retry",
                            s.kind,
                            s.source
                        );
                        s.backoff = next_backoff(s.backoff, 0, Duration::ZERO, s.policy);
                        continue;
                    }
                }
            }

            let inner = s.inner.as_mut().expect("inner set above");
            match inner.next().await {
                Some(item) => {
                    s.items_on_current += 1;
                    return Some(((s.source, item), s));
                }
                None => {
                    let lifetime = s
                        .connection_start
                        .take()
                        .map(|t| t.elapsed())
                        .unwrap_or_default();
                    let items = s.items_on_current;
                    s.inner = None;
                    s.backoff = next_backoff(s.backoff, items, lifetime, s.policy);
                    tracing::info!(
                        "{} subscription from {} ended after {items} items \
                         in {lifetime:?}; next backoff {:?}",
                        s.kind,
                        s.source,
                        s.backoff,
                    );
                }
            }
        }
    })
    .into_boxed()
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
            .map(|(i, f)| {
                reconnecting_source_stream(
                    f.clone(),
                    SourceId(i),
                    "preconfirmation",
                    |f: &GraphqlFetcher| f.predicted_receipts_stream(),
                )
            })
            .collect::<Vec<_>>();

        let merged = select_all(tagged);
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
            .map(|(i, f)| {
                reconnecting_source_stream(
                    f.clone(),
                    SourceId(i),
                    "finalized block",
                    |f: &GraphqlFetcher| f.finalized_blocks_stream(),
                )
            })
            .collect::<Vec<_>>();

        let merged = select_all(tagged);
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
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };

    #[derive(Clone)]
    struct MockFetcher {
        calls: Arc<AtomicUsize>,
        #[allow(clippy::type_complexity)]
        factory: Arc<dyn Fn(usize) -> anyhow::Result<BoxStream<u32>> + Send + Sync>,
    }

    impl MockFetcher {
        fn new<F>(factory: F) -> Self
        where
            F: Fn(usize) -> anyhow::Result<BoxStream<u32>> + Send + Sync + 'static,
        {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                factory: Arc::new(factory),
            }
        }

        fn subscribe(&self) -> anyhow::Result<BoxStream<u32>> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            (self.factory)(n)
        }
    }

    fn fast_policy() -> BackoffPolicy {
        // Short delays so reconnect-path tests don't sleep for real seconds.
        BackoffPolicy {
            base: Duration::from_millis(10),
            max: Duration::from_millis(80),
            stability_threshold: Duration::from_millis(500),
        }
    }

    #[test]
    fn next_backoff_doubles_on_failure_until_cap() {
        let policy = BackoffPolicy {
            base: Duration::from_millis(10),
            max: Duration::from_millis(40),
            stability_threshold: Duration::from_secs(30),
        };
        // Failure: 0 items, near-zero lifetime → double, capped at max.
        let b0 = policy.base;
        let b1 = next_backoff(b0, 0, Duration::ZERO, policy);
        let b2 = next_backoff(b1, 0, Duration::ZERO, policy);
        let b3 = next_backoff(b2, 0, Duration::ZERO, policy);
        let b4 = next_backoff(b3, 0, Duration::ZERO, policy);
        assert_eq!(b1, Duration::from_millis(20));
        assert_eq!(b2, Duration::from_millis(40));
        assert_eq!(b3, Duration::from_millis(40)); // capped
        assert_eq!(b4, Duration::from_millis(40)); // stays at cap
    }

    #[test]
    fn next_backoff_resets_only_after_stable_connection() {
        let policy = BackoffPolicy {
            base: Duration::from_millis(10),
            max: Duration::from_secs(1),
            stability_threshold: Duration::from_millis(500),
        };
        let elevated = Duration::from_millis(160);

        // Short-lived connection with items: does NOT reset (server likely
        // accepting then kicking us — treat as failure).
        let after_short = next_backoff(elevated, 5, Duration::from_millis(100), policy);
        assert_eq!(after_short, Duration::from_millis(320));

        // Stable connection (≥ threshold) with items: resets to base.
        let after_stable = next_backoff(elevated, 5, Duration::from_millis(500), policy);
        assert_eq!(after_stable, policy.base);

        // Stable-duration connection with zero items: not stable (no data),
        // treat as failure.
        let after_empty = next_backoff(elevated, 0, Duration::from_secs(10), policy);
        assert_eq!(after_empty, Duration::from_millis(320));
    }

    #[tokio::test]
    async fn reconnecting_source_retries_after_stream_end() {
        // First subscription delivers 1 item then ends; second delivers more.
        let fetcher = MockFetcher::new(|n| {
            let items: Vec<u32> = if n == 0 { vec![1] } else { vec![2, 3, 4] };
            Ok(stream::iter(items).into_boxed())
        });
        let calls_handle = fetcher.calls.clone();

        let stream = reconnecting_source_stream_with_policy(
            fetcher,
            SourceId(0),
            "test",
            |f: &MockFetcher| f.subscribe(),
            fast_policy(),
        );

        let collected: Vec<_> = stream.take(4).collect().await;
        assert_eq!(
            collected,
            vec![
                (SourceId(0), 1),
                (SourceId(0), 2),
                (SourceId(0), 3),
                (SourceId(0), 4),
            ]
        );
        assert!(calls_handle.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn reconnecting_source_retries_after_subscribe_error() {
        // First subscribe fails; second succeeds. The helper must not give up.
        let fetcher = MockFetcher::new(|n| {
            if n == 0 {
                Err(anyhow::anyhow!("transient"))
            } else {
                Ok(stream::iter(vec![42u32]).into_boxed())
            }
        });

        let stream = reconnecting_source_stream_with_policy(
            fetcher,
            SourceId(7),
            "test",
            |f: &MockFetcher| f.subscribe(),
            fast_policy(),
        );

        let collected: Vec<_> = stream.take(1).collect().await;
        assert_eq!(collected, vec![(SourceId(7), 42)]);
    }

    #[tokio::test]
    async fn healthy_source_keeps_flowing_while_other_reconnects() {
        // Source 0 is always-failing; source 1 is healthy. select_all over
        // both must keep delivering from source 1 regardless of source 0.
        let broken = MockFetcher::new(|_| Err(anyhow::anyhow!("always down")));
        let healthy = MockFetcher::new(|n| {
            let base = (n as u32) * 100;
            Ok(stream::iter(vec![base, base + 1, base + 2]).into_boxed())
        });

        let s0 = reconnecting_source_stream_with_policy(
            broken,
            SourceId(0),
            "test",
            |f: &MockFetcher| f.subscribe(),
            fast_policy(),
        );
        let s1 = reconnecting_source_stream_with_policy(
            healthy,
            SourceId(1),
            "test",
            |f: &MockFetcher| f.subscribe(),
            fast_policy(),
        );

        let merged = select_all(vec![s0, s1]);
        let collected: Vec<_> = merged.take(6).collect().await;

        assert!(collected.iter().all(|(s, _)| *s == SourceId(1)));
        assert_eq!(collected.len(), 6);
    }

    #[tokio::test]
    async fn recovered_source_rejoins_merge() {
        // Source 0 is broken on the first subscribe attempt; subsequent attempts
        // succeed. Source 1 is always healthy. After the reconnect delay fires,
        // source 0's items must appear alongside source 1's in the merged
        // output — proving a recovered source rejoins the fan-in.
        let flaky = MockFetcher::new(|n| {
            if n == 0 {
                Err(anyhow::anyhow!("down on first attempt"))
            } else {
                // On the second+ attempt, emit an item then hold the stream
                // open so select_all keeps polling it.
                Ok(stream::iter(vec![999u32])
                    .chain(stream::pending())
                    .into_boxed())
            }
        });
        let healthy = MockFetcher::new(|_| Ok(stream::iter(0u32..10_000).into_boxed()));

        let s0 = reconnecting_source_stream_with_policy(
            flaky,
            SourceId(0),
            "test",
            |f: &MockFetcher| f.subscribe(),
            fast_policy(),
        );
        let s1 = reconnecting_source_stream_with_policy(
            healthy,
            SourceId(1),
            "test",
            |f: &MockFetcher| f.subscribe(),
            fast_policy(),
        );

        let mut merged = select_all(vec![s0, s1]);
        let mut collected: Vec<(SourceId, u32)> = Vec::new();
        let collect = async {
            while let Some(item) = merged.next().await {
                let is_s0 = item.0 == SourceId(0);
                collected.push(item);
                if is_s0 {
                    break;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), collect)
            .await
            .expect("recovered source must rejoin within timeout");

        let from_s1 = collected.iter().filter(|(s, _)| *s == SourceId(1)).count();
        let from_s0 = collected.iter().filter(|(s, _)| *s == SourceId(0)).count();
        assert!(
            from_s1 > 0,
            "healthy source 1 must keep flowing while source 0 reconnects"
        );
        assert!(
            from_s0 >= 1,
            "recovered source 0 must rejoin the merge; got {collected:?}"
        );
    }

    #[tokio::test]
    async fn repeated_failures_space_out_retries() {
        // Source always fails. With a 10ms base that doubles, five failed
        // attempts should take at least base + 2*base + 4*base + 8*base
        // (ignoring the first, which doesn't sleep) = 150ms minus jitter.
        // We measure wall time between attempt-start events via call count.
        let fetcher = MockFetcher::new(|_| Err(anyhow::anyhow!("down")));
        let calls = fetcher.calls.clone();

        let stream = reconnecting_source_stream_with_policy(
            fetcher,
            SourceId(0),
            "test",
            |f: &MockFetcher| f.subscribe(),
            fast_policy(),
        );

        // Drive the stream for a bounded time; we never get an item, so
        // rely on the timeout to stop polling.
        let start = Instant::now();
        let _ = tokio::time::timeout(
            Duration::from_millis(200),
            stream.take(1).collect::<Vec<_>>(),
        )
        .await;
        let elapsed = start.elapsed();
        let attempts = calls.load(Ordering::SeqCst);

        // Without backoff (fixed 10ms), 200ms would produce ~20 attempts.
        // With exponential growth to cap=80ms and ±20% jitter, a healthy
        // upper bound is ~15. Assert we're well below the no-backoff rate.
        assert!(
            attempts <= 15,
            "expected exponential backoff to limit attempts; got {attempts} \
             in {elapsed:?}"
        );
        // And we do retry more than once (backoff kicks in only after
        // the first sleep).
        assert!(attempts >= 2, "expected at least one retry; got {attempts}");
    }
}
