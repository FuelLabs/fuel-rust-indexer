use super::*;
use fuel_core_services::stream::IntoBoxStream;
use fuel_core_types::{
    blockchain::{
        consensus::Consensus,
        header::BlockHeader,
    },
    services::executor::{
        TransactionExecutionResult,
        TransactionExecutionStatus,
    },
};
use fuel_indexer_types::events::{
    SuccessfulTransactionReceipts,
    TransactionReceipts,
};
use fuel_receipts_manager::port::{
    Fetcher,
    FinalizedBlock,
};
use fuel_storage_utils::in_memory_storage::InMemoryStorage;
use futures::{
    StreamExt,
    TryStreamExt,
};
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Event(u16);
impl fuel_events_manager::port::StorableEvent for Event {}

struct Processor;
impl fuel_events_manager::port::ReceiptsProcessor for Processor {
    type Event = Event;

    fn process_transaction_receipts<'a>(
        &'a self,
        receipts: &'a SuccessfulTransactionReceipts,
    ) -> impl Iterator<Item = Event> + 'a {
        std::iter::once(Event(receipts.tx_pointer.tx_index()))
    }
}

/// A lazy finalized-block source. Advancing the receipts manager pulls another
/// block; stopping it must stop this counter as well as decoded checkpoints.
#[derive(Clone)]
struct Fetch {
    blocks: Arc<Vec<FinalizedBlock>>,
    pulled: Arc<AtomicUsize>,
}

impl Fetcher for Fetch {
    fn predicted_receipts_stream(
        &self,
    ) -> anyhow::Result<BoxStream<TransactionReceipts>> {
        Ok(futures::stream::pending().into_boxed())
    }

    fn finalized_blocks_stream(&self) -> anyhow::Result<BoxStream<FinalizedBlock>> {
        Ok(futures::stream::iter([self.blocks.last().unwrap().clone()])
            .chain(futures::stream::pending())
            .into_boxed())
    }

    fn finalized_blocks_for_range(
        &self,
        range: std::ops::RangeInclusive<u32>,
    ) -> impl futures::Stream<Item = anyhow::Result<FinalizedBlock>> + Send + 'static
    {
        let fetch = self.clone();
        futures::stream::iter(range).map(move |height| {
            fetch.pulled.fetch_add(1, Ordering::SeqCst);
            Ok(fetch.blocks[(height - 1) as usize].clone())
        })
    }

    async fn last_height(&self) -> anyhow::Result<BlockHeight> {
        Ok((self.blocks.len() as u32).into())
    }
}

fn blocks(count: u32) -> Vec<FinalizedBlock> {
    (1..=count)
        .map(|height| {
            let mut header = BlockHeader::default();
            header.set_block_height(height.into());
            // Alternate large and empty blocks. Even the handoff block is larger
            // than the two-message queue, so enqueueing replay data deadlocks.
            let transactions = if height % 2 == 0 { 0 } else { 32 };
            let statuses = (0..=transactions)
                .map(|_| TransactionExecutionStatus {
                    id: Default::default(),
                    result: TransactionExecutionResult::Success {
                        result: None,
                        receipts: Arc::new(vec![]),
                        total_gas: 0,
                        total_fee: 0,
                    },
                })
                .collect();
            FinalizedBlock {
                header,
                consensus: Consensus::default(),
                #[cfg(feature = "blocks-subscription")]
                transactions: vec![],
                statuses,
            }
        })
        .collect()
}

type EventStorage = InMemoryStorage<fuel_events_manager::storage::Column>;
type ReceiptStorage = InMemoryStorage<fuel_receipts_manager::storage::Column>;
type Pipeline = ServiceRunner<Task<Processor, EventStorage, ReceiptStorage, Fetch>>;

fn pipeline(count: u32) -> (Pipeline, Arc<AtomicUsize>) {
    pipeline_with_storage(count, ReceiptStorage::default(), EventStorage::default())
}

fn pipeline_with_storage(
    count: u32,
    receipts: ReceiptStorage,
    events: EventStorage,
) -> (Pipeline, Arc<AtomicUsize>) {
    let pulled = Arc::new(AtomicUsize::new(0));
    let receipts_manager = fuel_receipts_manager::service::new_service(
        0.into(),
        false,
        receipts,
        Fetch {
            blocks: Arc::new(blocks(count)),
            pulled: pulled.clone(),
        },
    )
    .unwrap();
    let events_manager = fuel_events_manager::service::new_service(
        Processor,
        0.into(),
        events,
        StreamsAdapter::new(receipts_manager.shared.clone())
            .with_backpressure(NonZeroUsize::new(2)),
        Arc::new(ReceiptsTimestamps::new(receipts_manager.shared.clone())),
    )
    .unwrap();
    (
        ServiceRunner::new(Task {
            receipts_manager,
            events_manager,
        }),
        pulled,
    )
}

async fn drain(
    stream: &mut BoxStream<anyhow::Result<UnstableEvent<Event>>>,
    through: u32,
) -> Vec<UnstableEvent<Event>> {
    let mut result = vec![];
    loop {
        let event = stream.try_next().await.unwrap().expect("stream stays open");
        let done =
            matches!(&event, UnstableEvent::Checkpoint(c) if *c.block_height == through);
        result.push(event);
        if done {
            return result;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_fold_stops_both_producers_and_resumes_without_gaps() {
    let (service, pulled) = pipeline(20);
    let mut fold = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    // An unread observer is deliberately kept alive throughout this run.
    let _observer = service
        .shared
        .unstable_events_starting_from(1.into())
        .await
        .unwrap();
    service.start_and_await().await.unwrap();

    // Stop polling the fold. With a bounded queue, height 3 cannot commit and
    // the receipts source stops pulling after the finite intermediate buffers.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            service.shared.events().await_height(3.into())
        )
        .await
        .is_err()
    );
    let stopped_at = pulled.load(Ordering::SeqCst);
    assert!(
        stopped_at > 0 && stopped_at <= 4,
        "pulled {stopped_at} blocks while the fold was stopped"
    );
    // Ten minutes of simulated stall must not turn waiting into producer
    // overrun or require a process restart once the consumer resumes.
    assert!(
        tokio::time::timeout(
            Duration::from_secs(600),
            service.shared.events().await_height(3.into())
        )
        .await
        .is_err()
    );
    assert_eq!(pulled.load(Ordering::SeqCst), stopped_at);

    let actual = tokio::time::timeout(Duration::from_secs(1), drain(&mut fold, 20))
        .await
        .unwrap();
    service
        .shared
        .events()
        .await_height(20.into())
        .await
        .unwrap();
    // Compare every live/replayed event against an independent replay of the
    // committed data, including transaction order and checkpoint metadata.
    let mut replay = service
        .shared
        .unstable_events_starting_from(1.into())
        .await
        .unwrap();
    assert_eq!(actual, drain(&mut replay, 20).await);
    assert_eq!(
        actual
            .iter()
            .filter(|e| matches!(e, UnstableEvent::Checkpoint(_)))
            .count(),
        20
    );
    service.stop_and_await().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn stalled_fold_does_not_prevent_shutdown() {
    let (service, _) = pipeline(20);
    let mut fold = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    service.start_and_await().await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            service.shared.events().await_height(3.into())
        )
        .await
        .is_err()
    );
    tokio::time::timeout(Duration::from_secs(1), service.stop_and_await())
        .await
        .unwrap()
        .unwrap();
    // The task owns queue senders: retaining SharedState cannot keep a required
    // subscription open forever after shutdown. Drain any buffered messages.
    tokio::time::timeout(Duration::from_secs(1), async {
        while fold.next().await.is_some() {}
    })
    .await
    .unwrap();
    assert!(
        service
            .shared
            .unstable_events_starting_from_with_backpressure(
                1.into(),
                NonZeroUsize::new(2).unwrap(),
            )
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn unread_observer_does_not_gate_indexing() {
    let (service, _) = pipeline(20);
    let _observer = service
        .shared
        .unstable_events_starting_from(1.into())
        .await
        .unwrap();
    service.start_and_await().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        service.shared.events().await_height(20.into()),
    )
    .await
    .unwrap()
    .unwrap();
    service.stop_and_await().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dropping_required_subscription_releases_producer() {
    let (service, _) = pipeline(20);
    let fold = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    service.start_and_await().await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            service.shared.events().await_height(3.into())
        )
        .await
        .is_err()
    );
    drop(fold);
    tokio::time::timeout(
        Duration::from_secs(1),
        service.shared.events().await_height(20.into()),
    )
    .await
    .unwrap()
    .unwrap();
    service.stop_and_await().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn new_consumer_can_join_mid_block_without_deadlocking_existing_consumer() {
    let (service, _) = pipeline(20);
    let mut registry = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    service.start_and_await().await.unwrap();
    service
        .shared
        .events()
        .await_height(2.into())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            service.shared.events().await_height(3.into())
        )
        .await
        .is_err()
    );
    // Block 3 is partially published and cannot fit in either live queue.
    // The new book must get ALL of block 3 via the existing replay handoff.
    let mut book = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    let (registry, book) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(drain(&mut registry, 20), drain(&mut book, 20))
    })
    .await
    .unwrap();
    assert_eq!(registry, book);
    service.stop_and_await().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn restart_with_persisted_backlog_preserves_replay_and_backpressure() {
    restart_backlog(false).await;
}

#[tokio::test(start_paused = true)]
async fn receipts_ahead_of_decoder_cannot_flood_replaying_projection() {
    restart_backlog(true).await;
}

async fn restart_backlog(replay_receipts: bool) {
    let receipts = ReceiptStorage::default();
    let events = EventStorage::default();
    let (original, _) = pipeline_with_storage(20, receipts.clone(), events.clone());
    original.start_and_await().await.unwrap();
    original
        .shared
        .events()
        .await_height(20.into())
        .await
        .unwrap();
    original.stop_and_await().await.unwrap();

    let events = if replay_receipts {
        EventStorage::default()
    } else {
        events
    };
    let blocked_height = if replay_receipts { 4 } else { 24 };
    let (restarted, pulled) = pipeline_with_storage(40, receipts, events);
    // The projection is behind both persisted indexer checkpoints.
    let mut fold = restarted
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    restarted.start_and_await().await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            restarted
                .shared
                .events()
                .await_height(blocked_height.into())
        )
        .await
        .is_err()
    );
    assert!(pulled.load(Ordering::SeqCst) <= 4);
    let actual = tokio::time::timeout(Duration::from_secs(1), drain(&mut fold, 40))
        .await
        .unwrap();
    restarted
        .shared
        .events()
        .await_height(40.into())
        .await
        .unwrap();
    let mut replay = restarted
        .shared
        .unstable_events_starting_from(1.into())
        .await
        .unwrap();
    assert_eq!(actual, drain(&mut replay, 40).await);
    restarted.stop_and_await().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn legacy_observer_can_lag_without_stopping_required_internal_pipeline() {
    // More than Tokio's rounded-up 131072-message broadcast retention. This
    // reproduces the overrun on the legacy subscription while the internal
    // receipts -> events subscription waits instead of losing messages.
    let (service, _) = pipeline(8200);
    let mut observer = service
        .shared
        .unstable_events_starting_from(1.into())
        .await
        .unwrap();
    service.start_and_await().await.unwrap();
    service
        .shared
        .events()
        .await_height(8200.into())
        .await
        .unwrap();
    let error = loop {
        if let Err(error) = observer.try_next().await {
            break error;
        }
    };
    assert!(error.to_string().contains("lagged"), "{error}");
    service.stop_and_await().await.unwrap();
}

struct Source(
    Option<
        tokio::sync::mpsc::Receiver<
            anyhow::Result<fuel_indexer_types::events::UnstableReceipts>,
        >,
    >,
);
impl fuel_events_manager::port::StreamsSource for Source {
    fn events_starting_from(
        &mut self,
        _: BlockHeight,
    ) -> anyhow::Result<
        BoxStream<anyhow::Result<fuel_indexer_types::events::UnstableReceipts>>,
    > {
        Ok(tokio_stream_receiver(
            self.0.take().expect("one subscription"),
        ))
    }
}

fn tokio_stream_receiver<T: Send + 'static>(
    receiver: tokio::sync::mpsc::Receiver<T>,
) -> BoxStream<T> {
    futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|event| (event, receiver))
    })
    .into_boxed()
}

struct Clock;
impl fuel_events_manager::port::BlockTimestamps for Clock {
    fn timestamp_at(&self, height: &BlockHeight) -> anyhow::Result<u128> {
        Ok(**height as u128)
    }
}

#[tokio::test(start_paused = true)]
async fn bounded_live_delivery_preserves_rollback_and_failed_transaction_counting() {
    use fuel_core_types::fuel_tx::TxPointer;
    use fuel_indexer_types::events::{
        CheckpointEvent,
        ExecutionStatus,
        UnstableReceipts,
    };

    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let service = fuel_events_manager::service::new_service(
        Processor,
        0.into(),
        EventStorage::default(),
        Source(Some(receiver)),
        Arc::new(Clock),
    )
    .unwrap();
    let mut fold = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap();
    service.start_and_await().await.unwrap();
    let checkpoint = |height: u32, count| {
        UnstableReceipts::Checkpoint(CheckpointEvent {
            block_height: height.into(),
            events_count: count,
            timestamp: height as u128,
        })
    };
    let transaction = |id, index, execution_status| {
        UnstableReceipts::Receipts(TransactionReceipts {
            tx_pointer: TxPointer::new(2.into(), index),
            tx_id: [id; 32].into(),
            receipts: Arc::new(vec![]),
            execution_status,
        })
    };
    let receipts = vec![
        checkpoint(1u32, 0),
        transaction(1, 0, ExecutionStatus::Success),
        UnstableReceipts::Rollback(2.into()),
        transaction(2, 0, ExecutionStatus::Success),
        transaction(
            3,
            1,
            ExecutionStatus::Failure {
                reason: "test failure".into(),
            },
        ),
        checkpoint(2, 2),
        checkpoint(3, 0),
    ];
    let (_, actual) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            async {
                for event in receipts {
                    sender.send(Ok(event)).await.unwrap();
                }
            },
            drain(&mut fold, 3)
        )
    })
    .await
    .unwrap();
    service.shared.await_height(3.into()).await.unwrap();
    assert_eq!(actual.len(), 6);
    assert!(
        matches!(&actual[1], UnstableEvent::Transaction(tx) if tx.tx_id == [1;32].into())
    );
    assert_eq!(actual[2], UnstableEvent::Rollback(2.into()));
    assert!(
        matches!(&actual[3], UnstableEvent::Transaction(tx) if tx.tx_id == [2;32].into())
    );
    assert!(matches!(&actual[4], UnstableEvent::Checkpoint(c) if c.events_count == 1));
    let committed = service.shared.events_at(&2.into()).unwrap().unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].tx_id, [2; 32].into());
    assert_eq!(committed[0].events, vec![Event(0)]);
    service.stop_and_await().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn slow_book_gates_producers_even_while_registry_keeps_polling() {
    let (service, pulled) = pipeline(20);
    let mut registry = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    let mut book = service
        .shared
        .unstable_events_starting_from_with_backpressure(
            1.into(),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    service.start_and_await().await.unwrap();
    let mut registry_task = tokio::spawn(async move { drain(&mut registry, 20).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut registry_task)
            .await
            .is_err()
    );
    assert!(pulled.load(Ordering::SeqCst) <= 4);
    let book_events = tokio::time::timeout(Duration::from_secs(1), drain(&mut book, 20))
        .await
        .unwrap();
    assert_eq!(registry_task.await.unwrap(), book_events);
    service.stop_and_await().await.unwrap();
}
