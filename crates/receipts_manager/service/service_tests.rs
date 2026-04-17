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
        .unwrap();

    // When
    let _ = fuel_core.produce_block().await;
    let _ = task.run(&mut StateWatcher::started()).await;

    // Then
    let result = stream.take(2).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        result,
        vec![
            UnstableReceipts::Receipts(TransactionReceipts {
                tx_pointer: TxPointer::new(1u32.into(), 0),
                tx_id: default_tx_id(),
                receipts: Arc::new(vec![]),
                execution_status: ExecutionStatus::Success,
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
        .unwrap();

    // Then
    let result = stream.take(4).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        result,
        vec![
            UnstableReceipts::Receipts(TransactionReceipts {
                tx_pointer: TxPointer::new(1u32.into(), 0),
                tx_id: default_tx_id(),
                receipts: Arc::new(vec![]),
                execution_status: ExecutionStatus::Success,
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
            UnstableReceipts::Receipts(TransactionReceipts {
                tx_pointer: TxPointer::new(1u32.into(), 0),
                tx_id: default_tx_id(),
                receipts: Arc::new(vec![]),
                execution_status: ExecutionStatus::Success,
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

mod rebuild_on_stream_end {
    use super::*;
    use crate::port::{
        Fetcher,
        FinalizedBlock,
    };
    use fuel_core_services::stream::{
        BoxStream,
        IntoBoxStream,
    };
    use fuel_core_types::fuel_types::BlockHeight;
    use fuel_indexer_types::events::TransactionReceipts;
    use futures::{
        Stream,
        stream,
    };
    use std::sync::{
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    /// Mock fetcher whose preconf stream ends immediately on the first call
    /// (simulating one source dying and our `stop_on_first_end` tearing down
    /// the merged stream). Subsequent calls return a pending stream so the
    /// service observes a successful rebuild.
    #[derive(Default)]
    struct CountingFetcher {
        preconf_calls: AtomicUsize,
        block_calls: AtomicUsize,
        notify_rebuilt: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl Fetcher for CountingFetcher {
        fn predicted_receipts_stream(
            &self,
        ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
            let n = self.preconf_calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First call: empty stream — yields None immediately,
                // forcing the service down its rebuild branch.
                Ok(stream::empty().into_boxed())
            } else {
                // Rebuild: signal the test and then park forever so
                // `task.run()` returns Continue without further churn.
                if let Some(tx) =
                    self.notify_rebuilt.lock().expect("notify poisoned").take()
                {
                    let _ = tx.send(());
                }
                Ok(stream::pending().into_boxed())
            }
        }

        fn finalized_blocks_stream(&self) -> anyhow::Result<BoxStream<FinalizedBlock>> {
            self.block_calls.fetch_add(1, Ordering::SeqCst);
            Ok(stream::pending().into_boxed())
        }

        fn finalized_blocks_for_range(
            &self,
            _range: std::ops::RangeInclusive<u32>,
        ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static {
            stream::empty()
        }

        async fn last_height(&self) -> anyhow::Result<BlockHeight> {
            Ok(0u32.into())
        }
    }

    #[tokio::test]
    async fn service_rebuilds_preconf_stream_after_it_ends() {
        let storage = InMemoryStorage::<Column>::default();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let fetcher = Arc::new(CountingFetcher::default());
        *fetcher.notify_rebuilt.lock().unwrap() = Some(tx);

        // The Fetcher trait is implemented on CountingFetcher; the service
        // takes F by value, so wrap it in a trivial newtype that forwards.
        struct ArcFetcher(Arc<CountingFetcher>);
        impl Fetcher for ArcFetcher {
            fn predicted_receipts_stream(
                &self,
            ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
                self.0.predicted_receipts_stream()
            }
            fn finalized_blocks_stream(
                &self,
            ) -> anyhow::Result<BoxStream<FinalizedBlock>> {
                self.0.finalized_blocks_stream()
            }
            fn finalized_blocks_for_range(
                &self,
                range: std::ops::RangeInclusive<u32>,
            ) -> impl Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static
            {
                self.0.finalized_blocks_for_range(range)
            }
            async fn last_height(&self) -> anyhow::Result<BlockHeight> {
                self.0.last_height().await
            }
        }

        let service = UninitializedService::new(
            0u32.into(),
            true,
            storage,
            ArcFetcher(fetcher.clone()),
        )
        .unwrap();
        let mut task = service
            .into_task(&StateWatcher::started(), ())
            .await
            .expect("into_task");

        // into_task builds the initial (dying) preconf stream → call #1.
        assert_eq!(fetcher.preconf_calls.load(Ordering::SeqCst), 1);

        // One run tick: the empty stream yields None, the service rebuilds.
        let action = task.run(&mut StateWatcher::started()).await;
        assert!(matches!(action, TaskNextAction::Continue));

        // Rebuild happened: second call is observed, and the notify signal fires.
        tokio::time::timeout(std::time::Duration::from_secs(1), rx)
            .await
            .expect("rebuild never happened")
            .expect("rebuild channel dropped");
        assert_eq!(fetcher.preconf_calls.load(Ordering::SeqCst), 2);
    }
}
