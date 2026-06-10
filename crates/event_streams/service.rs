use crate::adapters::StreamsAdapter;
use fuel_core_services::{
    RunnableService,
    RunnableTask,
    Service,
    ServiceRunner,
    StateWatcher,
    TaskNextAction,
    stream::BoxStream,
};
use fuel_core_types::fuel_types::BlockHeight;
use fuel_events_manager::service::{
    EventManager,
    TransactionEvents,
    UnstableEvent,
};
#[cfg(feature = "rpc")]
use fuel_receipts_manager::adapters::{
    hybrid_fetcher::HybridFetcher,
    multi_source_fetcher::{
        MultiSourceRpcConfig,
        RpcSource,
    },
};
use fuel_receipts_manager::{
    adapters::{
        graphql_event_adapter::GraphqlFetcher,
        multi_source_fetcher::{
            MultiSourceFetcher,
            MultiSourceFetcherConfig,
        },
    },
    service::ReceiptsManager,
};
use std::num::NonZeroUsize;
use url::Url;

pub use fuel_receipts_manager::adapters::graphql_event_adapter::DEFAULT_PULL_BLOCK_INTERVAL;
/// Re-exported defaults so service consumers can fill config struct
/// literals without reaching into `fuel_receipts_manager` internals.
#[cfg(feature = "rpc")]
pub use fuel_receipts_manager::adapters::hybrid_fetcher::DEFAULT_SYNC_TAIL_BLOCKS;

#[cfg(feature = "rocksdb")]
pub use rocksdb::*;

#[cfg(feature = "blocks-subscription")]
use fuel_indexer_types::events::BlockEvent;

pub struct Config {
    pub starting_block_height: BlockHeight,
    pub use_preconfirmations: bool,
    /// List of Fuel GraphQL URLs for failover support.
    /// The FuelClient will automatically switch to the next URL if one fails.
    pub fuel_graphql_urls: Vec<Url>,
    /// Dedicated GraphQL endpoints used exclusively for preconfirmation and
    /// finalized-block subscriptions. Each entry is an independent source; the
    /// first source to deliver events for a given block height becomes
    /// authoritative for that height. When empty, subscriptions fall back to
    /// `fuel_graphql_urls`.
    pub subscription_sources: Vec<Url>,
    pub heartbeat_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub blocks_request_batch_size: usize,
    pub blocks_request_concurrency: usize,
    pub pending_blocks_limit: usize,
    /// Polling interval of the new-block pull fallback (used when block
    /// subscriptions are unavailable on the node).
    pub pull_block_interval: std::time::Duration,
}

impl Config {
    pub fn new(
        starting_block_height: BlockHeight,
        use_preconfirmations: bool,
        urls: Vec<Url>,
    ) -> Self {
        Self {
            starting_block_height,
            use_preconfirmations,
            fuel_graphql_urls: urls,
            subscription_sources: Vec::new(),
            heartbeat_capacity: NonZeroUsize::new(1000).expect("Is not zero; qed"),
            event_capacity: NonZeroUsize::new(10000).expect("Is not zero; qed"),
            blocks_request_batch_size: 1,
            blocks_request_concurrency: 100,
            pending_blocks_limit: 10_000,
            pull_block_interval: DEFAULT_PULL_BLOCK_INTERVAL,
        }
    }

    /// Configures dedicated GraphQL endpoints used exclusively for
    /// preconfirmation and finalized-block subscriptions. First-arrival
    /// semantics: the first source to deliver events for a given block
    /// height becomes authoritative for that height. When empty,
    /// subscriptions fall back to `fuel_graphql_urls`.
    pub fn with_subscription_sources(mut self, subscription_sources: Vec<Url>) -> Self {
        self.subscription_sources = subscription_sources;
        self
    }
}

#[derive(Clone)]
pub struct SharedState<Event, ES, RS> {
    events: fuel_events_manager::service::SharedState<Event, ES>,
    receipts: fuel_receipts_manager::service::SharedState<RS>,
}

pub struct Task<Processor, ES, RS, F = MultiSourceFetcher<GraphqlFetcher>>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
    F: fuel_receipts_manager::port::Fetcher,
{
    receipts_manager: ReceiptsManager<RS, F>,
    events_manager: EventManager<Processor, ES, StreamsAdapter<RS>>,
}

#[cfg(feature = "rpc")]
pub type RpcTask<Processor, ES, RS> =
    Task<Processor, ES, RS, MultiSourceFetcher<HybridFetcher>>;

#[async_trait::async_trait]
impl<Processor, RS, ES, F> RunnableService for Task<Processor, ES, RS, F>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
    F: fuel_receipts_manager::port::Fetcher,
{
    const NAME: &'static str = "StreamsService";
    type SharedData = SharedState<Processor::Event, ES, RS>;
    type Task = Self;
    type TaskParams = ();

    fn shared_data(&self) -> Self::SharedData {
        SharedState {
            events: self.events_manager.shared.clone(),
            receipts: self.receipts_manager.shared.clone(),
        }
    }

    async fn into_task(
        self,
        _: &StateWatcher,
        _: Self::TaskParams,
    ) -> anyhow::Result<Self::Task> {
        self.receipts_manager.start()?;
        self.events_manager.start()?;

        self.receipts_manager.await_start_or_stop().await?;
        self.events_manager.await_start_or_stop().await?;

        Ok(self)
    }
}

impl<Processor, RS, ES, F> RunnableTask for Task<Processor, ES, RS, F>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
    F: fuel_receipts_manager::port::Fetcher,
{
    async fn run(&mut self, watcher: &mut StateWatcher) -> TaskNextAction {
        tokio::select! {
            _ = watcher.while_started() => {
                TaskNextAction::Stop
            }
            _ = self.receipts_manager.await_stop() => {
                TaskNextAction::Stop
            }
            _ = self.events_manager.await_stop() => {
                TaskNextAction::Stop
            }
        }
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        self.receipts_manager.stop();
        self.events_manager.stop();

        self.receipts_manager.stop_and_await().await?;
        self.events_manager.stop_and_await().await?;

        Ok(())
    }
}

impl<Event, ES, RS> SharedState<Event, ES, RS>
where
    Event: fuel_events_manager::port::StorableEvent,
    ES: fuel_events_manager::port::Storage,
    RS: fuel_receipts_manager::port::Storage,
{
    pub fn events(&self) -> &fuel_events_manager::service::SharedState<Event, ES> {
        &self.events
    }

    pub fn receipts(&self) -> &fuel_receipts_manager::service::SharedState<RS> {
        &self.receipts
    }

    pub async fn stable_events_starting_from(
        &self,
        starting_block_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<TransactionEvents<Event>>>> {
        self.events
            .stable_events_starting_from(starting_block_height)
            .await
    }

    pub async fn unstable_events_starting_from(
        &self,
        starting_block_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<UnstableEvent<Event>>>> {
        self.events
            .unstable_events_starting_from(starting_block_height)
            .await
    }

    #[cfg(feature = "blocks-subscription")]
    pub async fn blocks_starting_from(
        &self,
        starting_block_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<BlockEvent>>> {
        self.receipts
            .blocks_starting_from(starting_block_height)
            .await
    }
}

pub fn new_service<Processor, ES, RS>(
    config: Config,
    processor: Processor,
    receipts_storage: RS,
    events_storage: ES,
) -> anyhow::Result<ServiceRunner<Task<Processor, ES, RS>>>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
{
    let Config {
        starting_block_height,
        use_preconfirmations,
        fuel_graphql_urls,
        subscription_sources,
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
        pull_block_interval,
    } = config;

    let fetcher = MultiSourceFetcher::new(MultiSourceFetcherConfig {
        main_urls: fuel_graphql_urls,
        subscription_sources,
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
        pull_block_interval,
    })?;

    let receipts_manager = fuel_receipts_manager::service::new_service(
        starting_block_height,
        use_preconfirmations,
        receipts_storage,
        fetcher,
    )?;

    let events_manager = fuel_events_manager::service::new_service(
        processor,
        starting_block_height,
        events_storage,
        StreamsAdapter::new(receipts_manager.shared.clone()),
    )?;

    let task = Task {
        receipts_manager,
        events_manager,
    };

    Ok(ServiceRunner::new(task))
}

#[cfg(feature = "rpc")]
pub struct RpcConfig {
    pub starting_block_height: BlockHeight,
    pub use_preconfirmations: bool,
    /// GraphQL URLs that back the main client. Preconfirmation
    /// subscriptions, the realtime block stream, chain-info lookups, and the
    /// synchronized tail of block synchronization go through GraphQL.
    pub fuel_graphql_urls: Vec<Url>,
    /// Protobuf RPC URL paired with `fuel_graphql_urls`. Used for bulk block
    /// synchronization only, while the indexer is more than
    /// `sync_tail_blocks` behind the chain tip.
    pub fuel_rpc_url: Url,
    /// Each subscription source pairs a GraphQL URL (preconfs) with an RPC
    /// URL (block stream / range).
    pub subscription_sources: Vec<RpcSource>,
    pub heartbeat_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub blocks_request_batch_size: usize,
    pub blocks_request_concurrency: usize,
    pub pending_blocks_limit: usize,
    /// Number of blocks below the GraphQL tip that are always synced via
    /// GraphQL instead of RPC.
    pub sync_tail_blocks: u32,
    /// Polling interval of the new-block pull fallback (used when block
    /// subscriptions are unavailable on the node).
    pub pull_block_interval: std::time::Duration,
}

#[cfg(feature = "rpc")]
impl RpcConfig {
    pub fn new(
        starting_block_height: BlockHeight,
        use_preconfirmations: bool,
        fuel_graphql_urls: Vec<Url>,
        fuel_rpc_url: Url,
    ) -> Self {
        Self {
            starting_block_height,
            use_preconfirmations,
            fuel_graphql_urls,
            fuel_rpc_url,
            subscription_sources: Vec::new(),
            heartbeat_capacity: NonZeroUsize::new(1000).expect("Is not zero; qed"),
            event_capacity: NonZeroUsize::new(10000).expect("Is not zero; qed"),
            blocks_request_batch_size: 1,
            blocks_request_concurrency: 100,
            pending_blocks_limit: 10_000,
            sync_tail_blocks: DEFAULT_SYNC_TAIL_BLOCKS,
            pull_block_interval: DEFAULT_PULL_BLOCK_INTERVAL,
        }
    }

    pub fn with_subscription_sources(
        mut self,
        subscription_sources: Vec<RpcSource>,
    ) -> Self {
        self.subscription_sources = subscription_sources;
        self
    }
}

#[cfg(feature = "rpc")]
pub async fn new_rpc_service<Processor, ES, RS>(
    config: RpcConfig,
    processor: Processor,
    receipts_storage: RS,
    events_storage: ES,
) -> anyhow::Result<ServiceRunner<RpcTask<Processor, ES, RS>>>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
{
    let RpcConfig {
        starting_block_height,
        use_preconfirmations,
        fuel_graphql_urls,
        fuel_rpc_url,
        subscription_sources,
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
        sync_tail_blocks,
        pull_block_interval,
    } = config;

    let fetcher = MultiSourceFetcher::new_hybrid(MultiSourceRpcConfig {
        main_graphql_urls: fuel_graphql_urls,
        main_rpc_url: fuel_rpc_url,
        subscription_sources,
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
        sync_tail_blocks,
        pull_block_interval,
    })
    .await?;

    let receipts_manager = fuel_receipts_manager::service::new_service(
        starting_block_height,
        use_preconfirmations,
        receipts_storage,
        fetcher,
    )?;

    let events_manager = fuel_events_manager::service::new_service(
        processor,
        starting_block_height,
        events_storage,
        StreamsAdapter::new(receipts_manager.shared.clone()),
    )?;

    let task: RpcTask<Processor, ES, RS> = Task {
        receipts_manager,
        events_manager,
    };

    Ok(ServiceRunner::new(task))
}

#[cfg(feature = "rocksdb")]
mod rocksdb {
    use super::*;
    use crate::{
        adapters::SimplerProcessorAdapter,
        processors::simple_processor::FnReceiptParser,
    };
    use fuel_core::state::{
        historical_rocksdb::StateRewindPolicy,
        rocks_db::DatabaseConfig,
    };
    use fuel_core_types::fuel_tx::Receipt;
    use fuels::core::codec::DecoderConfig;
    use std::path::PathBuf;

    pub type LogsStreamsService<Fn> = ServiceRunner<
        Task<
            SimplerProcessorAdapter<FnReceiptParser<Fn>>,
            fuel_events_manager::rocksdb::Storage,
            fuel_receipts_manager::rocksdb::Storage,
        >,
    >;

    pub fn new_logs_streams<Fn, Event>(
        parser: Fn,
        path: PathBuf,
        state_rewind_policy: StateRewindPolicy,
        database_config: DatabaseConfig,
        config: Config,
    ) -> anyhow::Result<LogsStreamsService<Fn>>
    where
        Event: fuel_events_manager::port::StorableEvent,
        Fn: FnOnce(DecoderConfig, &Receipt) -> Option<Event>
            + Copy
            + Send
            + Sync
            + 'static,
    {
        let parser = FnReceiptParser::new(parser, DecoderConfig::default());

        let receipts_storage = fuel_receipts_manager::rocksdb::open_database(
            path.as_path(),
            state_rewind_policy,
            database_config,
        )?;
        let events_storage = fuel_events_manager::rocksdb::open_database(
            path.as_path(),
            state_rewind_policy,
            database_config,
        )?;
        new_service(
            config,
            SimplerProcessorAdapter::new(parser),
            receipts_storage,
            events_storage,
        )
    }

    #[cfg(feature = "rpc")]
    pub type RpcLogsStreamsService<Fn> = ServiceRunner<
        RpcTask<
            SimplerProcessorAdapter<FnReceiptParser<Fn>>,
            fuel_events_manager::rocksdb::Storage,
            fuel_receipts_manager::rocksdb::Storage,
        >,
    >;

    #[cfg(feature = "rpc")]
    pub async fn new_rpc_logs_streams<Fn, Event>(
        parser: Fn,
        path: PathBuf,
        state_rewind_policy: StateRewindPolicy,
        database_config: DatabaseConfig,
        config: RpcConfig,
    ) -> anyhow::Result<RpcLogsStreamsService<Fn>>
    where
        Event: fuel_events_manager::port::StorableEvent,
        Fn: FnOnce(DecoderConfig, &Receipt) -> Option<Event>
            + Copy
            + Send
            + Sync
            + 'static,
    {
        let parser = FnReceiptParser::new(parser, DecoderConfig::default());

        let receipts_storage = fuel_receipts_manager::rocksdb::open_database(
            path.as_path(),
            state_rewind_policy,
            database_config,
        )?;
        let events_storage = fuel_events_manager::rocksdb::open_database(
            path.as_path(),
            state_rewind_policy,
            database_config,
        )?;
        new_rpc_service(
            config,
            SimplerProcessorAdapter::new(parser),
            receipts_storage,
            events_storage,
        )
        .await
    }
}
