use super::service::ReceiptsManager;
use fuel_core_client::client::FuelClient;
use fuel_core_types::fuel_types::BlockHeight;
use std::{
    num::NonZeroUsize,
    sync::Arc,
};
use url::Url;

pub mod client_ext;
pub mod concurrent_stream;
pub mod concurrent_unordered_stream;
pub mod graphql_event_adapter;
pub mod resizable_buffered;
pub mod resizable_buffered_unordered;

pub fn new_service<S, F>(
    starting_block_height: BlockHeight,
    use_preconfirmations: bool,
    storage: S,
    fetcher: F,
) -> anyhow::Result<ReceiptsManager<S, F>>
where
    S: super::port::Storage,
    F: super::port::Fetcher,
{
    crate::service::new_service(
        starting_block_height,
        use_preconfirmations,
        storage,
        fetcher,
    )
}

pub struct ManagerConfig {
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

pub type ReceiptGraphqlManager<Database> =
    ReceiptsManager<Database, graphql_event_adapter::GraphqlFetcher>;

pub fn new_graphql_service<S>(
    config: ManagerConfig,
    storage: S,
) -> anyhow::Result<ReceiptGraphqlManager<S>>
where
    S: super::port::Storage,
{
    let ManagerConfig {
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
        client: client.clone(),
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
    };
    let fetcher =
        graphql_event_adapter::create_graphql_event_adapter(graphql_event_adapter_config);

    let event_manager = crate::service::new_service(
        starting_block_height,
        use_preconfirmations,
        storage,
        fetcher,
    )?;

    Ok(event_manager)
}
