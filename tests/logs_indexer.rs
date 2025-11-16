use fuel_core::{
    service::{
        Config,
        FuelService,
        ServiceTrait,
    },
    state::rocks_db::{
        ColumnsPolicy,
        DatabaseConfig,
    },
};
use fuel_event_streams::{
    fuel_events_manager::port::StorableEvent,
    service::Config as StreamsConfig,
    try_parse_events,
};
use fuels::{
    core::codec::DecoderConfig,
    tx::Receipt,
};
use url::Url;

fuels::prelude::abigen!(Contract(
    name = "OrderBook",
    abi = "artifacts/order-book-abi.json"
));

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
enum Event {
    Created { timestamp: u64 },
    Matched { timestamp: u64 },
}

impl StorableEvent for Event {}

fn parse_o2_logs(decoder: DecoderConfig, receipt: &Receipt) -> Option<Event> {
    try_parse_events!(
        [decoder, receipt]
        OrderCreatedEvent => |event| {
            Some(Event::Created {
                timestamp: event.timestamp.unix,
            })
        },
        OrderMatchedEvent => |event| {
            Some(Event::Matched{
                timestamp: event.timestamp.unix,
            })
        }
    )
}

#[tokio::test]
async fn defining_logs_indexer__stable_logs() {
    let node = FuelService::new_node(Config::local_node()).await.unwrap();
    let url = Url::parse(format!("http://{}", node.bound_address).as_str()).unwrap();
    let temp_dir = tempdir::TempDir::new("database").unwrap();
    let database_config = DatabaseConfig {
        cache_capacity: None,
        max_fds: 512,
        columns_policy: ColumnsPolicy::Lazy,
    };

    // Given
    let indexer = fuel_event_streams::service::new_logs_streams(
        parse_o2_logs,
        temp_dir.path().to_path_buf(),
        database_config,
        StreamsConfig::new(0u32.into(), true, url),
    )
    .unwrap();
    indexer.start_and_await().await.unwrap();

    // When
    let result = indexer
        .shared
        .stable_events_starting_from(0u32.into())
        .await;

    // Then
    assert!(result.is_ok());
}

#[tokio::test]
async fn defining_logs_indexer__unstable_logs() {
    let node = FuelService::new_node(Config::local_node()).await.unwrap();
    let url = Url::parse(format!("http://{}", node.bound_address).as_str()).unwrap();
    let temp_dir = tempdir::TempDir::new("database").unwrap();
    let database_config = DatabaseConfig {
        cache_capacity: None,
        max_fds: 512,
        columns_policy: ColumnsPolicy::Lazy,
    };

    // Given
    let indexer = fuel_event_streams::service::new_logs_streams(
        parse_o2_logs,
        temp_dir.path().to_path_buf(),
        database_config,
        StreamsConfig::new(0u32.into(), true, url),
    )
    .unwrap();
    indexer.start_and_await().await.unwrap();

    // When
    let result = indexer
        .shared
        .unstable_events_starting_from(0u32.into())
        .await;

    // Then
    assert!(result.is_ok());
}

#[tokio::test]
async fn defining_logs_indexer__blocks() {
    let node = FuelService::new_node(Config::local_node()).await.unwrap();
    let url = Url::parse(format!("http://{}", node.bound_address).as_str()).unwrap();
    let temp_dir = tempdir::TempDir::new("database").unwrap();
    let database_config = DatabaseConfig {
        cache_capacity: None,
        max_fds: 512,
        columns_policy: ColumnsPolicy::Lazy,
    };

    // Given
    let indexer = fuel_event_streams::service::new_logs_streams(
        parse_o2_logs,
        temp_dir.path().to_path_buf(),
        database_config,
        StreamsConfig::new(0u32.into(), true, url),
    )
    .unwrap();
    indexer.start_and_await().await.unwrap();

    // When
    let result = indexer.shared.blocks_starting_from(0u32.into()).await;

    // Then
    assert!(result.is_ok());
}
