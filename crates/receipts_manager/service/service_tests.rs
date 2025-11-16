#![allow(non_snake_case)]

use super::*;
use crate::{
    adapters::graphql_event_adapter::{
        self,
        GraphqlEventAdapterConfig,
        create_graphql_event_adapter,
        tests::ConfigExt,
    },
    storage::Column,
};
use fuel_core_services::{
    StateWatcher,
    TaskNextAction,
};
use fuel_core_types::fuel_tx::{
    Receipt,
    TxPointer,
};
use fuel_storage_utils::in_memory_storage::InMemoryStorage;
use futures::TryStreamExt;
use std::sync::Arc;

#[tokio::test]
async fn uninitialized_service__can_be_created() {
    let storage = InMemoryStorage::<Column>::default();

    let fuel_core = graphql_event_adapter::fuel_core_mock::MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");
    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);

    // Given
    let service = UninitializedService::new(0u32.into(), true, storage, fetcher).unwrap();

    let mut task = service
        .into_task(&StateWatcher::started(), ())
        .await
        .expect("Failed to create task");

    // When
    let _ = fuel_core.produce_block().await;
    let action = task.run(&mut StateWatcher::started()).await;

    // Then
    assert!(
        matches!(action, fuel_core_services::TaskNextAction::Continue),
        "Task should continue running"
    );
}

fn default_tx_id() -> fuel_core_types::fuel_tx::TxId {
    "088610950b6ff24c97591b98de792e372d71fcfd8788df08fd517635325f8d04"
        .parse()
        .unwrap()
}

#[tokio::test]
async fn uninitialized_service__events_starting_from__returns_events_after_subscription()
{
    let storage = InMemoryStorage::<Column>::default();

    let fuel_core = graphql_event_adapter::fuel_core_mock::MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");
    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);
    let starting_height = 0u32.into();

    let service =
        UninitializedService::new(starting_height, true, storage, fetcher).unwrap();
    let shared_state = service.shared_data();
    let mut task = service
        .into_task(&StateWatcher::started(), ())
        .await
        .expect("Failed to create task");

    // Given
    let stream = shared_state
        .unstable_receipts_starting_from(starting_height)
        .await
        .unwrap();

    // When
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;

    // Then
    let result = stream.take(2).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        result,
        vec![
            UnstableReceipts::Receipts(SuccessfulTransactionReceipts {
                tx_pointer: TxPointer::new(1u32.into(), 0),
                tx_id: default_tx_id(),
                receipts: Arc::new(vec![]),
            }),
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: 1u32.into(),
                events_count: 1,
            })
        ],
        "Should return an event from the subscription"
    );
}

#[tokio::test]
async fn uninitialized_service__events_starting_from__returns_events_before_subscription()
{
    let storage = InMemoryStorage::<Column>::default();

    let fuel_core = graphql_event_adapter::fuel_core_mock::MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");
    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);
    let starting_height = 0u32.into();

    let service =
        UninitializedService::new(starting_height, true, storage, fetcher).unwrap();
    let shared_state = service.shared_data();
    let mut task = service
        .into_task(&StateWatcher::started(), ())
        .await
        .expect("Failed to create task");

    // Given
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;

    // When
    let stream = shared_state
        .unstable_receipts_starting_from(starting_height)
        .await
        .unwrap();

    // Then
    let result = stream.take(4).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        result,
        vec![
            UnstableReceipts::Receipts(SuccessfulTransactionReceipts {
                tx_pointer: TxPointer::new(1u32.into(), 0),
                tx_id: default_tx_id(),
                receipts: Arc::new(vec![]),
            }),
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: 1u32.into(),
                events_count: 1
            }),
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: 2u32.into(),
                events_count: 0
            }),
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: 3u32.into(),
                events_count: 0
            }),
        ],
        "Should return an event from the subscription"
    );
}

#[tokio::test]
async fn uninitialized_service__events_starting_from__returns_events_middle_subscription()
{
    let storage = InMemoryStorage::<Column>::default();

    let fuel_core = graphql_event_adapter::fuel_core_mock::MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");
    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);
    let starting_height = 0u32.into();

    let service =
        UninitializedService::new(starting_height, true, storage, fetcher).unwrap();
    let shared_state = service.shared_data();
    let mut task = service
        .into_task(&StateWatcher::started(), ())
        .await
        .expect("Failed to create task");

    // Given
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;

    // When
    let stream = shared_state
        .unstable_receipts_starting_from(starting_height)
        .await
        .unwrap();
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;

    // Then
    let result = stream.take(4).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        result,
        vec![
            UnstableReceipts::Receipts(SuccessfulTransactionReceipts {
                tx_pointer: TxPointer::new(1u32.into(), 0),
                tx_id: default_tx_id(),
                receipts: Arc::new(vec![]),
            }),
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: 1u32.into(),
                events_count: 1
            }),
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: 2u32.into(),
                events_count: 0
            }),
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: 3u32.into(),
                events_count: 0
            }),
        ],
        "Should return an event from the subscription"
    );
}

#[tokio::test]
async fn uninitialized_service__events_starting_from__returns_events_after_subscription_preconfirmations()
 {
    let storage = InMemoryStorage::<Column>::default();

    let mut fuel_core = graphql_event_adapter::fuel_core_mock::MockFuelCore::new()
        .await
        .expect("Failed to create mock fuel core");
    let config = GraphqlEventAdapterConfig::generate_from_mock_fuel_core(&fuel_core);
    let fetcher = create_graphql_event_adapter(config);
    let starting_height = 0u32.into();

    let service =
        UninitializedService::new(starting_height, true, storage, fetcher).unwrap();
    let shared_state = service.shared_data();
    let mut task = service
        .into_task(&StateWatcher::started(), ())
        .await
        .expect("Failed to create task");

    // Given
    let stream = shared_state
        .unstable_receipts_starting_from(2u32.into())
        .await
        .unwrap();

    // When
    let _ = fuel_core.produce_transaction().await;
    tokio::spawn(async move {
        loop {
            let result = task.run(&mut StateWatcher::started()).await;

            if matches!(result, TaskNextAction::Stop) {
                break
            }
        }
    });

    // Then
    let result = stream.take(1).try_collect::<Vec<_>>().await.unwrap();
    let UnstableReceipts::Receipts(event) = &result[0] else {
        panic!("Expected a TransactionEvent");
    };
    assert_eq!(event.receipts.len(), 5);
    assert!(matches!(event.receipts[0], Receipt::Call { .. }));
    assert!(matches!(event.receipts[1], Receipt::Transfer { .. }));
    assert!(matches!(event.receipts[2], Receipt::Return { .. }));
    assert!(matches!(event.receipts[3], Receipt::Return { .. }));
    assert!(matches!(event.receipts[4], Receipt::ScriptResult { .. }));
}
