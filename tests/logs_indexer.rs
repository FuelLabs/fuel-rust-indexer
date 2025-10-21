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
use fuel_indexer::{
    fuel_events_manager::port::StorableEvent,
    indexer::IndexerConfig,
    try_parse_events,
};
use fuels::{
    core::codec::DecoderConfig,
    tx::Receipt,
};
use url::Url;

#[tokio::test]
async fn defining_logs_indexer() {
    fuels::prelude::abigen!(Contract(
        name = "OrderBook",
        abi = "crates/indexer/processors/receipt_parser/order-book-abi.json"
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

    let node = FuelService::new_node(Config::local_node()).await.unwrap();
    let url = Url::parse(format!("http://{}", node.bound_address).as_str()).unwrap();
    let temp_dir = tempdir::TempDir::new("database").unwrap();
    let database_config = DatabaseConfig {
        cache_capacity: None,
        max_fds: 512,
        columns_policy: ColumnsPolicy::Lazy,
    };

    // Given
    let indexer = fuel_indexer::indexer::new_logs_indexer(
        parse_o2_logs,
        temp_dir.path().to_path_buf(),
        database_config,
        IndexerConfig::new(0u32.into(), url),
    )
    .unwrap();
    indexer.start_and_await().await.unwrap();

    // When
    let result = indexer.shared.events_starting_from(0u32.into()).await;

    // Then
    assert!(result.is_ok());
}
