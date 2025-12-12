use crate::adapters::StreamsAdapter;
use fuel_core_client::client::FuelClient;
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
use fuel_receipts_manager::adapters::{
    ReceiptGraphqlManager,
    graphql_event_adapter,
};
use std::{
    num::NonZeroUsize,
    sync::Arc,
};
use url::Url;

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
    pub heartbeat_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub blocks_request_batch_size: usize,
    pub blocks_request_concurrency: usize,
    pub pending_blocks_limit: usize,
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
            heartbeat_capacity: NonZeroUsize::new(1000).expect("Is not zero; qed"),
            event_capacity: NonZeroUsize::new(10000).expect("Is not zero; qed"),
            blocks_request_batch_size: 10,
            blocks_request_concurrency: 100,
            pending_blocks_limit: 10_000,
        }
    }
}

#[derive(Clone)]
pub struct SharedState<Event, ES, RS> {
    events: fuel_events_manager::service::SharedState<Event, ES>,
    receipts: fuel_receipts_manager::service::SharedState<RS>,
}

pub struct Task<Processor, ES, RS>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
{
    receipts_manager: ReceiptGraphqlManager<RS>,
    events_manager: EventManager<Processor, ES, StreamsAdapter<RS>>,
}

#[async_trait::async_trait]
impl<Processor, RS, ES> RunnableService for Task<Processor, ES, RS>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
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

impl<Processor, RS, ES> RunnableTask for Task<Processor, ES, RS>
where
    Processor: fuel_events_manager::port::ReceiptsProcessor,
    RS: fuel_receipts_manager::port::Storage,
    ES: fuel_events_manager::port::Storage,
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
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
    } = config;

    let client = Arc::new(FuelClient::with_urls(&fuel_graphql_urls)?);

    let graphql_event_adapter_config = graphql_event_adapter::GraphqlEventAdapterConfig {
        client,
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
    };
    let fetcher =
        graphql_event_adapter::create_graphql_event_adapter(graphql_event_adapter_config);

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
}
