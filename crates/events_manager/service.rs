use crate::storage::{
    Events,
    LastCheckpoint,
};
use fuel_core_services::{
    RunnableService,
    RunnableTask,
    ServiceRunner,
    TaskNextAction,
    stream::{
        BoxStream,
        IntoBoxStream,
    },
};
use fuel_core_storage::{
    StorageAsMut,
    StorageAsRef,
    iter::{
        IterDirection,
        IteratorOverTable,
    },
    not_found,
    transactional::{
        IntoTransaction,
        ReadTransaction,
        WriteTransaction,
    },
};
use fuel_core_types::{
    fuel_tx::{
        TxId,
        TxPointer,
    },
    fuel_types::BlockHeight,
};
use fuel_indexer_types::events::{
    CheckpointEvent,
    UnstableReceipts,
};
use fuel_storage_utils::StorageIterator;
use futures::StreamExt;
use std::{
    iter,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{
    broadcast,
    watch,
};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Clone, Hash)]
pub struct TransactionEvents<Event> {
    pub tx_id: TxId,
    pub tx_pointer: TxPointer,
    pub events: Vec<Event>,
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    strum_macros::AsRefStr,
    Hash,
)]
pub enum UnstableEvent<Event> {
    Transaction(TransactionEvents<Event>),
    Checkpoint(CheckpointEvent),
    Rollback(BlockHeight),
}

impl<Event> UnstableEvent<Event> {
    pub fn block_height(&self) -> BlockHeight {
        match self {
            UnstableEvent::Transaction(tx) => tx.tx_pointer.block_height(),
            UnstableEvent::Checkpoint(checkpoint) => checkpoint.block_height,
            UnstableEvent::Rollback(height) => *height,
        }
    }
}

pub struct UninitializedService<Processor, S, StreamsSource>
where
    Processor: super::port::ReceiptsProcessor,
{
    processor: Processor,
    storage: S,
    streams: StreamsSource,
    checkpoint_height: watch::Sender<BlockHeight>,
    shared_state: SharedState<Processor::Event, S>,
}

impl<Processor, S, StreamsSource> UninitializedService<Processor, S, StreamsSource>
where
    Processor: super::port::ReceiptsProcessor,
    S: super::port::Storage,
    StreamsSource: super::port::StreamsSource,
{
    pub fn new(
        processor: Processor,
        starting_height: BlockHeight,
        storage: S,
        streams: StreamsSource,
    ) -> anyhow::Result<Self> {
        let checkpoint_height = storage
            .read_transaction()
            .storage_as_ref::<LastCheckpoint>()
            .get(&())?
            .map(|c| c.into_owned())
            .unwrap_or(starting_height);

        let (checkpoint_height, checkpoint_height_receiver) =
            watch::channel(checkpoint_height);

        let (unstable_broadcast, _) = broadcast::channel(100_000);
        let (stable_broadcast, _) = broadcast::channel(100_000);
        let shared_state = SharedState {
            starting_height,
            storage: storage.clone(),
            unstable_broadcast: Arc::new(unstable_broadcast),
            stable_broadcast: Arc::new(stable_broadcast),
            checkpoint_height: checkpoint_height_receiver,
        };

        let _self = Self {
            processor,
            storage,
            streams,
            checkpoint_height,
            shared_state,
        };

        Ok(_self)
    }
}

pub struct Task<Processor, S, StreamsSource>
where
    Processor: super::port::ReceiptsProcessor,
{
    storage: S,
    events: Vec<TransactionEvents<Processor::Event>>,
    processor: Processor,
    event_source: BoxStream<anyhow::Result<UnstableReceipts>>,
    streams: StreamsSource,
    checkpoint_height: watch::Sender<BlockHeight>,
    unstable_broadcast: Arc<broadcast::Sender<UnstableEvent<Processor::Event>>>,
    stable_broadcast: Arc<broadcast::Sender<TransactionEvents<Processor::Event>>>,
}

#[async_trait::async_trait]
impl<Processor, S, StreamsSource> RunnableService
    for UninitializedService<Processor, S, StreamsSource>
where
    Processor: super::port::ReceiptsProcessor,
    S: super::port::Storage,
    StreamsSource: super::port::StreamsSource,
{
    const NAME: &'static str = "EventManager";

    type SharedData = SharedState<Processor::Event, S>;

    type Task = Task<Processor, S, StreamsSource>;

    type TaskParams = ();

    fn shared_data(&self) -> Self::SharedData {
        self.shared_state.clone()
    }

    async fn into_task(
        mut self,
        _: &fuel_core_services::StateWatcher,
        _: Self::TaskParams,
    ) -> anyhow::Result<Self::Task> {
        let UninitializedService {
            processor,
            storage,
            streams,
            checkpoint_height,
            shared_state,
        } = self;

        let mut task = Task {
            storage,
            events: Default::default(),
            processor,
            // Set it via `reconnect_service_events_stream` later
            event_source: futures::stream::empty().into_boxed(),
            streams,
            checkpoint_height,
            unstable_broadcast: shared_state.unstable_broadcast,
            stable_broadcast: shared_state.stable_broadcast,
        };

        task.reconnect_service_events_stream().await?;

        Ok(task)
    }
}

impl<Processor, S, StreamsSource> Task<Processor, S, StreamsSource>
where
    Processor: super::port::ReceiptsProcessor,
    S: super::port::Storage,
    StreamsSource: super::port::StreamsSource,
{
    fn broadcast_unstable_event(&self, event: UnstableEvent<Processor::Event>) {
        // Broadcast to internal channel
        let result = self.unstable_broadcast.send(event);

        if let Err(err) = result {
            tracing::warn!("Failed to broadcast unstable event: {}", err);
        }
    }

    async fn reconnect_service_events_stream(&mut self) -> anyhow::Result<()> {
        tracing::info!("Reconnecting to receipts provider event stream");

        let starting_height =
            self.checkpoint_height.borrow().succ().ok_or_else(|| {
                anyhow::anyhow!("Reached the maximum height for event indexer")
            })?;

        let event_source = self.streams.events_starting_from(starting_height).await?;

        self.event_source = event_source;
        self.broadcast_unstable_event(UnstableEvent::Rollback(starting_height));
        self.events.clear();

        Ok(())
    }

    async fn handle_service_event(
        &mut self,
        event: Option<anyhow::Result<UnstableReceipts>>,
    ) -> anyhow::Result<()> {
        match event {
            None => {
                self.reconnect_service_events_stream().await?;
            }
            Some(event) => {
                let event = event?;

                let next_block_height =
                    self.checkpoint_height.borrow().succ().ok_or_else(|| {
                        anyhow::anyhow!(
                            "Reached the maximum height for event indexer during handling"
                        )
                    })?;

                match event {
                    UnstableReceipts::Receipts(receipts) => {
                        let events: Vec<_> = self
                            .processor
                            .process_transaction_receipts(
                                receipts.tx_pointer,
                                receipts.tx_id,
                                receipts.receipts.iter(),
                            )
                            .collect();

                        let tx = TransactionEvents {
                            tx_id: receipts.tx_id,
                            tx_pointer: receipts.tx_pointer,
                            events,
                        };
                        self.events.push(tx.clone());

                        assert_eq!(next_block_height, receipts.tx_pointer.block_height());

                        self.broadcast_unstable_event(UnstableEvent::Transaction(tx));
                    }
                    UnstableReceipts::Checkpoint(checkpoint) => {
                        // This may happen when the service was start at the middle of the block
                        // production.
                        if checkpoint.events_count != self.events.len()
                            || next_block_height != checkpoint.block_height
                        {
                            return self.reconnect_service_events_stream().await;
                        }

                        let total_events_count =
                            self.events.iter().map(|e| e.events.len()).sum::<usize>();

                        self.broadcast_unstable_event(UnstableEvent::Checkpoint(
                            CheckpointEvent {
                                block_height: checkpoint.block_height,
                                events_count: total_events_count,
                            },
                        ));

                        let mut tx = self.storage.write_transaction();

                        tx.storage_as_mut::<Events<Processor::Event>>()
                            .insert(&next_block_height, &self.events)?;
                        tx.storage_as_mut::<LastCheckpoint>()
                            .insert(&(), &next_block_height)?;

                        let changes = tx.into_changes();
                        self.storage.commit_changes(changes)?;

                        self.checkpoint_height.send_replace(next_block_height);

                        for events in self.events.drain(..) {
                            let result = self.stable_broadcast.send(events);

                            if let Err(err) = result {
                                tracing::warn!(
                                    "Failed to broadcast stable event: {}",
                                    err
                                );
                            }
                        }

                        tracing::info!(
                            "Checkpoint at height {} with {} events",
                            checkpoint.block_height,
                            total_events_count
                        );
                        tokio::task::yield_now().await;
                    }
                    UnstableReceipts::Rollback(at) => {
                        self.broadcast_unstable_event(UnstableEvent::Rollback(at));
                        self.events.clear();
                    }
                }
            }
        }

        Ok(())
    }
}

impl<Processor, S, StreamsSource> RunnableTask for Task<Processor, S, StreamsSource>
where
    Processor: super::port::ReceiptsProcessor,
    S: super::port::Storage,
    StreamsSource: super::port::StreamsSource,
{
    async fn run(
        &mut self,
        watcher: &mut fuel_core_services::StateWatcher,
    ) -> TaskNextAction {
        tokio::select! {
            biased;
            _ = watcher.while_started() => TaskNextAction::Stop,

            event = self.event_source.next() => {
                match self.handle_service_event(event).await {
                    Ok(()) => TaskNextAction::Continue,
                    Err(err) =>  {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        TaskNextAction::ErrorContinue(err)
                    },
                }
            }
        }
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct SharedState<Event, S> {
    starting_height: BlockHeight,
    storage: S,
    unstable_broadcast: Arc<broadcast::Sender<UnstableEvent<Event>>>,
    stable_broadcast: Arc<broadcast::Sender<TransactionEvents<Event>>>,
    checkpoint_height: watch::Receiver<BlockHeight>,
}

impl<Event, S> SharedState<Event, S>
where
    Event: super::port::StorableEvent,
    S: super::port::Storage,
{
    pub fn starting_height(&self) -> BlockHeight {
        self.starting_height
    }

    pub async fn await_height(&self, height: BlockHeight) -> anyhow::Result<()> {
        let mut receiver = self.checkpoint_height.clone();
        loop {
            let current_height = *receiver.borrow_and_update();
            if current_height >= height {
                return Ok(());
            }
            receiver.changed().await.map_err(|e| {
                anyhow::anyhow!("Failed to wait for checkpoint height change: {}", e)
            })?;
        }
    }

    pub async fn unstable_events_starting_from(
        &self,
        start_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<UnstableEvent<Event>>>> {
        use futures::TryStreamExt;

        if start_height < self.starting_height {
            return Err(anyhow::anyhow!(
                "Start height {} is less than the initial starting height {}",
                start_height,
                self.starting_height
            ));
        }

        let stream_life_events =
            BroadcastStream::new(self.unstable_broadcast.subscribe());
        let available_height = *self.checkpoint_height.borrow();
        // We want to wait for next available height, because `stream_life_events` could be created
        // at the mid of the block, and it will not contain all events for the `available_height`.
        let next_available_height = available_height.succ().ok_or_else(|| {
            anyhow::anyhow!(
                "Failed to increment available height from {}",
                available_height
            )
        })?;

        let next_next_available_height_life_stream = stream_life_events
            .map(|result| {
                result.map_err(|e| anyhow::anyhow!("Broadcast stream error: {}", e))
            })
            .skip_while(move |event| {
                let skip = match event {
                    Ok(event) => event.block_height() <= next_available_height,
                    Err(_) => {
                        // In the case of error we want to propagate it, so no skipping
                        false
                    }
                };
                async move { skip }
            });

        let shared_state = self.clone();
        let storage = self.storage.clone().into_transaction();
        let next_available_height_events = futures::stream::once(async move {
            shared_state.await_height(next_available_height).await?;
            let events = storage
                .storage_as_ref::<Events<Event>>()
                .get(&next_available_height)?
                .ok_or_else(|| not_found!(Events<Event>))?
                .into_owned();

            let events_count = events
                .iter()
                .map(|events| events.events.len())
                .sum::<usize>();

            let checkpoint_event = UnstableEvent::Checkpoint(CheckpointEvent {
                block_height: next_available_height,
                events_count,
            });

            let iter = events
                .into_iter()
                .map(|events| UnstableEvent::Transaction(events))
                .chain(iter::once(checkpoint_event));

            Ok::<_, anyhow::Error>(futures::stream::iter(
                iter.map(Ok::<_, anyhow::Error>),
            ))
        })
        .try_flatten();

        let storage_iter = StorageIterator::from(self.storage.clone(), |storage| {
            storage.iter_all_by_start::<Events<Event>>(
                Some(&start_height),
                Some(IterDirection::Forward),
            )
        });

        let storage_iter_until_available_height = storage_iter
            .take_while(move |result| match result {
                Ok((block_height, _)) => *block_height <= available_height,
                Err(_) => true,
            })
            .map(|result| {
                result
                    .map(|(block_height, events)| {
                        let events_count = events
                            .iter()
                            .map(|events| events.events.len())
                            .sum::<usize>();

                        let checkpoint_event =
                            UnstableEvent::Checkpoint(CheckpointEvent {
                                block_height,
                                events_count,
                            });

                        let iter = events
                            .into_iter()
                            .map(|events| UnstableEvent::Transaction(events))
                            .chain(iter::once(checkpoint_event));

                        futures::stream::iter(iter.map(Ok::<_, anyhow::Error>))
                    })
                    .map_err(|e| anyhow::anyhow!("Storage iterator error: {}", e))
            });

        let storage_iter_until_available_height =
            futures::stream::iter(storage_iter_until_available_height.into_iter())
                .try_flatten();

        Ok(storage_iter_until_available_height
            .chain(next_available_height_events)
            .chain(next_next_available_height_life_stream)
            .skip_while(move |event| {
                let skip = match event {
                    Ok(event) => event.block_height() < start_height,
                    Err(_) => {
                        // In the case of error we want to propagate it, so no skipping
                        false
                    }
                };
                async move { skip }
            })
            .into_boxed())
    }

    pub async fn stable_events_starting_from(
        &self,
        start_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<TransactionEvents<Event>>>> {
        use futures::TryStreamExt;

        if start_height < self.starting_height {
            return Err(anyhow::anyhow!(
                "Start height {} is less than the initial starting height {}",
                start_height,
                self.starting_height
            ));
        }

        let stream_life_events = BroadcastStream::new(self.stable_broadcast.subscribe());
        let available_height = *self.checkpoint_height.borrow();
        // We want to wait for next available height, because `stream_life_events` could be created
        // at the mid of the block, and it will not contain all events for the `available_height`.
        let next_available_height = available_height.succ().ok_or_else(|| {
            anyhow::anyhow!(
                "Failed to increment available height from {}",
                available_height
            )
        })?;

        let next_next_available_height_life_stream = stream_life_events
            .map(|result| {
                result.map_err(|e| anyhow::anyhow!("Broadcast stream error: {}", e))
            })
            .skip_while(move |event| {
                let skip = match event {
                    Ok(event) => event.tx_pointer.block_height() <= next_available_height,
                    Err(_) => {
                        // In the case of error we want to propagate it, so no skipping
                        false
                    }
                };
                async move { skip }
            });

        let shared_state = self.clone();
        let storage = self.storage.clone().into_transaction();
        let next_available_height_events = futures::stream::once(async move {
            shared_state.await_height(next_available_height).await?;
            let events = storage
                .storage_as_ref::<Events<Event>>()
                .get(&next_available_height)?
                .ok_or_else(|| not_found!(Events<Event>))?
                .into_owned();

            Ok::<_, anyhow::Error>(futures::stream::iter(
                events.into_iter().map(Ok::<_, anyhow::Error>),
            ))
        })
        .try_flatten();

        let storage_iter = StorageIterator::from(self.storage.clone(), |storage| {
            storage.iter_all_by_start::<Events<Event>>(
                Some(&start_height),
                Some(IterDirection::Forward),
            )
        });

        let storage_iter_until_available_height = storage_iter
            .take_while(move |result| match result {
                Ok((block_height, _)) => *block_height <= available_height,
                Err(_) => true,
            })
            .map(|result| {
                result
                    .map(|(_, events)| {
                        futures::stream::iter(
                            events.into_iter().map(Ok::<_, anyhow::Error>),
                        )
                    })
                    .map_err(|e| anyhow::anyhow!("Storage iterator error: {}", e))
            });

        let storage_iter_until_available_height =
            futures::stream::iter(storage_iter_until_available_height.into_iter())
                .try_flatten();

        Ok(storage_iter_until_available_height
            .chain(next_available_height_events)
            .chain(next_next_available_height_life_stream)
            .skip_while(move |event| {
                let skip = match event {
                    Ok(event) => event.tx_pointer.block_height() < start_height,
                    Err(_) => {
                        // In the case of error we want to propagate it, so no skipping
                        false
                    }
                };
                async move { skip }
            })
            .into_boxed())
    }

    pub fn events_at(
        &self,
        block_height: &BlockHeight,
    ) -> anyhow::Result<Option<Vec<TransactionEvents<Event>>>> {
        let events = self
            .storage
            .read_transaction()
            .storage_as_ref::<Events<Event>>()
            .get(block_height)?
            .map(|v| v.into_owned());

        Ok(events)
    }
}

pub type EventManager<Processor, S, StreamsSource> =
    ServiceRunner<UninitializedService<Processor, S, StreamsSource>>;

pub fn new_service<Processor, S, StreamsSource>(
    processor: Processor,
    starting_height: BlockHeight,
    storage: S,
    streams: StreamsSource,
) -> anyhow::Result<EventManager<Processor, S, StreamsSource>>
where
    Processor: super::port::ReceiptsProcessor,
    S: super::port::Storage,
    StreamsSource: super::port::StreamsSource,
{
    let uninit = UninitializedService::new(processor, starting_height, storage, streams)?;
    Ok(ServiceRunner::new(uninit))
}
