use super::service::ReceiptsManager;
use crate::adapters::multi_source_fetcher::{
    MultiSourceFetcher,
    MultiSourceFetcherConfig,
};
use fuel_core_types::fuel_types::BlockHeight;
use std::num::NonZeroUsize;
use url::Url;

pub mod client_ext;
pub mod concurrent_stream;
pub mod concurrent_unordered_stream;
pub mod graphql_event_adapter;
#[cfg(feature = "rpc")]
pub mod hybrid_fetcher;
pub mod multi_source_fetcher;
pub mod resizable_buffered;
pub mod resizable_buffered_unordered;
#[cfg(feature = "rpc")]
pub mod rpc_event_adapter;
pub mod subscription_router;

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
}

pub type ReceiptGraphqlManager<Database> =
    ReceiptsManager<Database, graphql_event_adapter::GraphqlFetcher>;

pub type ReceiptMultiSourceManager<Database> =
    ReceiptsManager<Database, MultiSourceFetcher<graphql_event_adapter::GraphqlFetcher>>;

#[cfg(feature = "rpc")]
pub type ReceiptRpcMultiSourceManager<Database> =
    ReceiptsManager<Database, MultiSourceFetcher<hybrid_fetcher::HybridFetcher>>;

pub fn new_graphql_service<S>(
    config: ManagerConfig,
    storage: S,
) -> anyhow::Result<ReceiptMultiSourceManager<S>>
where
    S: super::port::Storage,
{
    let ManagerConfig {
        starting_block_height,
        use_preconfirmations,
        fuel_graphql_urls,
        subscription_sources,
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
    } = config;

    let fetcher = MultiSourceFetcher::new(MultiSourceFetcherConfig {
        main_urls: fuel_graphql_urls,
        subscription_sources,
        heartbeat_capacity,
        event_capacity,
        blocks_request_batch_size,
        blocks_request_concurrency,
        pending_blocks_limit,
    })?;

    let event_manager = crate::service::new_service(
        starting_block_height,
        use_preconfirmations,
        storage,
        fetcher,
    )?;

    Ok(event_manager)
}

#[cfg(feature = "rpc")]
pub struct RpcManagerConfig {
    pub starting_block_height: BlockHeight,
    pub use_preconfirmations: bool,
    /// GraphQL URLs backing the main client (preconfirmation subscriptions,
    /// realtime blocks, chain-info lookups, and the synchronized tail of
    /// block synchronization).
    pub fuel_graphql_urls: Vec<Url>,
    /// RPC (protobuf) URL paired with `fuel_graphql_urls`, used for bulk
    /// block synchronization only.
    pub fuel_rpc_url: Url,
    /// Additional subscription sources, each pairing a GraphQL URL (preconfs)
    /// with an RPC URL (block stream / range).
    pub subscription_sources: Vec<multi_source_fetcher::RpcSource>,
    pub heartbeat_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub blocks_request_batch_size: usize,
    pub blocks_request_concurrency: usize,
    pub pending_blocks_limit: usize,
    /// Number of blocks below the GraphQL tip that are always synced via
    /// GraphQL instead of RPC. See [`hybrid_fetcher::HybridFetcher`].
    pub sync_tail_blocks: u32,
}

#[cfg(feature = "rpc")]
pub async fn new_rpc_service<S>(
    config: RpcManagerConfig,
    storage: S,
) -> anyhow::Result<ReceiptRpcMultiSourceManager<S>>
where
    S: super::port::Storage,
{
    let RpcManagerConfig {
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
    } = config;

    let fetcher =
        MultiSourceFetcher::new_hybrid(multi_source_fetcher::MultiSourceRpcConfig {
            main_graphql_urls: fuel_graphql_urls,
            main_rpc_url: fuel_rpc_url,
            subscription_sources,
            heartbeat_capacity,
            event_capacity,
            blocks_request_batch_size,
            blocks_request_concurrency,
            pending_blocks_limit,
            sync_tail_blocks,
        })
        .await?;

    let event_manager = crate::service::new_service(
        starting_block_height,
        use_preconfirmations,
        storage,
        fetcher,
    )?;

    Ok(event_manager)
}
