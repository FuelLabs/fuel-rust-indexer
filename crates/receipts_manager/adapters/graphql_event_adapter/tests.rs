#![allow(non_snake_case)]

use super::{
    fuel_core_mock::MockFuelCore,
    *,
};
use crate::port::Fetcher;

pub trait ConfigExt {
    fn generate_fake() -> Self;
    fn generate_from_mock_fuel_core(mock_fuel_core: &MockFuelCore) -> Self;
}

impl ConfigExt for GraphqlEventAdapterConfig {
    fn generate_fake() -> Self {
        GraphqlEventAdapterConfig {
            client: Arc::new(FuelClient::new("http://localhost:4000/graphql").unwrap()),
            heartbeat_capacity: NonZeroUsize::new(10).unwrap(),
            event_capacity: NonZeroUsize::new(10).unwrap(),
            blocks_request_batch_size: 10,
            blocks_request_concurrency: 200,
        }
    }

    fn generate_from_mock_fuel_core(mock_fuel_core: &MockFuelCore) -> Self {
        GraphqlEventAdapterConfig {
            client: mock_fuel_core.client(),
            heartbeat_capacity: NonZeroUsize::new(10).unwrap(),
            event_capacity: NonZeroUsize::new(10).unwrap(),
            blocks_request_batch_size: 10,
            blocks_request_concurrency: 200,
        }
    }
}

#[tokio::test]
async fn create_graphql_event_adapter__works() {
    let fuel_core = MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");

    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let _ = create_graphql_event_adapter(config);
}

#[tokio::test]
async fn event_source_next__waits_until_next_event_available_with_valid_subscription() {
    // given
    let fuel_core = MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");

    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);

    // when
    let mut event_source = fetcher
        .predicted_receipts_stream()
        .await
        .expect("Failed to subscribe to event source");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // then
    let res =
        tokio::time::timeout(std::time::Duration::from_millis(100), event_source.next())
            .await;

    // should timeout
    assert!(
        res.is_err(),
        "Expected timeout when waiting for next event with valid subscription"
    );
}

#[tokio::test]
async fn heartbeat_next__waits_until_next_heartbeat_available_with_valid_subscription() {
    // given
    let fuel_core = MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");

    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);

    // when
    let mut heartbeat = fetcher
        .finalized_blocks_stream()
        .await
        .expect("Failed to subscribe to heartbeat");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // then
    let res =
        tokio::time::timeout(std::time::Duration::from_millis(100), heartbeat.next())
            .await;

    // should timeout
    assert!(
        res.is_err(),
        "Expected timeout when waiting for next heartbeat with valid subscription"
    );
}

#[tokio::test]
async fn heartbeat_next__returns_next_block_height_with_valid_subscription() {
    // given
    let fuel_core = MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");

    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);

    // when
    let mut heartbeat = fetcher
        .finalized_blocks_stream()
        .await
        .expect("Failed to subscribe to heartbeat");

    // produce a block to trigger a heartbeat
    fuel_core
        .produce_block()
        .await
        .expect("Failed to produce block");

    // then
    let res =
        tokio::time::timeout(std::time::Duration::from_millis(100), heartbeat.next())
            .await
            .expect("Failed to get next heartbeat");

    assert!(
        res.is_some(),
        "Expected successful retrieval of next heartbeat"
    );
}

#[tokio::test]
async fn event_source_next__returns_next_event_with_valid_subscription() {
    // given
    let mut fuel_core = MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");

    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);

    // when
    let mut event_source = fetcher
        .predicted_receipts_stream()
        .await
        .expect("Failed to subscribe to event source");

    // produce a transaction to trigger an event
    fuel_core
        .produce_transaction()
        .await
        .expect("Failed to produce transaction");

    // then
    let res =
        tokio::time::timeout(std::time::Duration::from_millis(2000), event_source.next())
            .await
            .expect("Failed to get next event");

    assert!(
        res.is_some(),
        "Expected successful retrieval of next event, got {res:?}"
    );
}
#[tokio::test]
async fn event_source_next__returns_returns_only_four_events_per_transfer() {
    let mut fuel_core = MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");

    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);

    // Given
    let mut event_source = fetcher
        .predicted_receipts_stream()
        .await
        .expect("Failed to subscribe to event source");

    // When
    // produce a transaction to trigger an event
    fuel_core
        .produce_transaction()
        .await
        .expect("Failed to produce transaction");

    // Then
    let first = event_source.next().await;
    let second =
        tokio::time::timeout(std::time::Duration::from_millis(5000), event_source.next())
            .await;

    assert!(first.is_some());
    assert!(
        second.is_err(),
        "Expected no more than one event per transaction, got {second:?}"
    );
}

#[tokio::test]
async fn event_source_next__returns_channel_lagged_error_when_receiver_falls_behind() {
    const TRANSACTION_COUNT: usize = 5;

    // given
    let mut fuel_core = MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");

    // create config with very small capacity to trigger lag
    let mut config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    config.event_capacity = NonZeroUsize::new(1).unwrap();

    let fetcher = create_graphql_event_adapter(config);

    // when
    let mut event_source = fetcher
        .predicted_receipts_stream()
        .await
        .expect("Failed to subscribe to event source");

    // produce multiple transactions rapidly to overwhelm the small channel
    for _ in 0..TRANSACTION_COUNT {
        fuel_core
            .produce_transaction()
            .await
            .expect("Failed to produce transaction");
    }

    // allow some time for events to be processed
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // then - should get channel lagged error
    let result = event_source.next().await;
    assert_eq!(result, None, "Expected stream to finish due to channel lag");
}
