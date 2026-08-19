use crate::{
    port::FinalizedBlock,
    storage::{
        LastCheckpoint,
        Receipts,
    },
};
use fuel_core_services::{
    RunnableService,
    RunnableTask,
    ServiceRunner,
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
    fuel_tx::TxPointer,
    fuel_types::BlockHeight,
    services::executor::TransactionExecutionResult,
};
use fuel_indexer_types::events::{
    CheckpointEvent,
    ExecutionStatus,
    TransactionReceipts,
    UnstableReceipts,
};
use fuel_storage_utils::StorageIterator;
use futures::{
    StreamExt,
    pin_mut,
};
use std::{
    collections::BTreeMap,
    iter,
    time::Duration,
};
use tokio::{
    sync::{
        broadcast,
        watch,
    },
    time::Instant,
};
use tokio_stream::wrappers::BroadcastStream;

#[cfg(feature = "blocks-subscription")]
use crate::storage::Blocks;
#[cfg(not(feature = "blocks-subscription"))]
use crate::storage::Headers;
use fuel_core_types::blockchain::header::BlockHeader;
#[cfg(feature = "blocks-subscription")]
use fuel_indexer_types::events::BlockEvent;

/// Returns the block's timestamp in seconds since the Unix epoch.
fn header_timestamp(header: &BlockHeader) -> anyhow::Result<u128> {
    u128::try_from(header.time().to_unix()).map_err(|_| {
        anyhow::anyhow!(
            "Block {} has a timestamp before the Unix epoch",
            header.height()
        )
    })
}

#[cfg(test)]
mod service_tests;

/// How long to wait before picking the next heartbeat back up after one could
/// not be processed. Roughly a block time: the usual cause is the node we read
/// from lagging, which clears as soon as it produces the block we waited for.
pub const DEFAULT_HEARTBEAT_RETRY_DELAY: Duration = Duration::from_secs(1);

pub struct UninitializedService<S, F> {
    use_preconfirmations: bool,
    heartbeat_retry_delay: Duration,
    storage: S,
    event_fetcher: F,
    checkpoint_height: watch::Sender<BlockHeight>,
    shared_state: SharedState<S>,
}

impl<S, F> UninitializedService<S, F>
where
    S: super::port::Storage,
    F: super::port::Fetcher,
{
    pub fn new(
        starting_height: BlockHeight,
        use_preconfirmations: bool,
        storage: S,
        event_fetcher: F,
    ) -> anyhow::Result<Self> {
        let checkpoint_height = storage
            .read_transaction()
            .storage_as_ref::<LastCheckpoint>()
            .get(&())?
            .map(|c| c.into_owned())
            .unwrap_or(starting_height);

        let (checkpoint_height, checkpoint_height_receiver) =
            watch::channel(checkpoint_height);

        // TODO: Decrease channel size and start handle backpressure properly to avoid
        //  huge memory consumption.
        let (broadcast_receipts, _) = broadcast::channel(100_000);
        #[cfg(feature = "blocks-subscription")]
        let (broadcast_blocks, _) = broadcast::channel(100_000);
        let shared_state = SharedState {
            starting_height,
            storage: storage.clone(),
            broadcast_receipts,
            #[cfg(feature = "blocks-subscription")]
            broadcast_blocks,
            checkpoint_height: checkpoint_height_receiver,
        };

        let _self = Self {
            use_preconfirmations,
            heartbeat_retry_delay: DEFAULT_HEARTBEAT_RETRY_DELAY,
            storage,
            event_fetcher,
            checkpoint_height,
            shared_state,
        };

        Ok(_self)
    }

    /// Overrides how long the task waits before retrying a heartbeat it could
    /// not process. Defaults to [`DEFAULT_HEARTBEAT_RETRY_DELAY`].
    pub fn with_heartbeat_retry_delay(mut self, delay: Duration) -> Self {
        self.heartbeat_retry_delay = delay;
        self
    }
}

pub struct Task<S, F> {
    storage: S,
    event_source: BoxStream<TransactionReceipts>,
    fetcher: F,
    heartbeat: BoxStream<FinalizedBlock>,
    heartbeat_liveness: tokio::time::Interval,
    /// See [`DEFAULT_HEARTBEAT_RETRY_DELAY`].
    heartbeat_retry_delay: Duration,
    emitted_events: Vec<TransactionReceipts>,
    pending_events: BTreeMap<BlockHeight, BTreeMap<u16, TransactionReceipts>>,
    checkpoint_height: watch::Sender<BlockHeight>,
    broadcast_receipts: broadcast::Sender<UnstableReceipts>,
    #[cfg(feature = "blocks-subscription")]
    broadcast_blocks: broadcast::Sender<BlockEvent>,
}

#[async_trait::async_trait]
impl<S, F> RunnableService for UninitializedService<S, F>
where
    S: super::port::Storage,
    F: super::port::Fetcher,
{
    const NAME: &'static str = "ReceiptsManager";

    type SharedData = SharedState<S>;

    type Task = Task<S, F>;

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
            use_preconfirmations,
            heartbeat_retry_delay,
            storage,
            event_fetcher,
            checkpoint_height,
            shared_state,
        } = self;

        let event_source = if use_preconfirmations {
            event_fetcher.predicted_receipts_stream()?
        } else {
            tracing::info!("Pre-confirmations are disabled, using empty event source.");
            futures::stream::pending().into_boxed()
        };

        let heartbeat = event_fetcher.finalized_blocks_stream()?;

        let liveness_duration = Duration::from_secs(5);
        let heartbeat_liveness = tokio::time::interval_at(
            Instant::now() + liveness_duration,
            liveness_duration,
        );

        let task = Task {
            storage,
            event_source,
            fetcher: event_fetcher,
            heartbeat,
            pending_events: Default::default(),
            emitted_events: Default::default(),
            checkpoint_height,
            broadcast_receipts: shared_state.broadcast_receipts,
            #[cfg(feature = "blocks-subscription")]
            broadcast_blocks: shared_state.broadcast_blocks,
            heartbeat_liveness,
            heartbeat_retry_delay,
        };

        Ok(task)
    }
}

impl<S, F> Task<S, F>
where
    S: super::port::Storage,
    F: super::port::Fetcher,
{
    fn broadcast_event(
        broadcast: &broadcast::Sender<UnstableReceipts>,
        event: UnstableReceipts,
    ) -> anyhow::Result<()> {
        // Broadcast to internal channel
        let _ = broadcast.send(event);

        Ok(())
    }

    async fn sync_up_to(
        &mut self,
        final_block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let start = (**self.checkpoint_height.borrow()).saturating_add(1);
        let end = *final_block_height;

        tracing::info!("Syncing up to from {start} to {end}");

        let stream = self.fetcher.finalized_blocks_for_range(start..=end);
        pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let block = result?;
            tracing::info!("Handle block height {}", block.header.height());
            self.handle_heartbeat(block, false).await?;
        }

        let checkpoint_height_after_sync = *self.checkpoint_height.borrow();
        if checkpoint_height_after_sync != final_block_height {
            return Err(anyhow::anyhow!(
                "Failed to sync up to the final block height: {}. Current checkpoint height: {}",
                final_block_height,
                checkpoint_height_after_sync
            ));
        }

        Ok(())
    }

    async fn handle_heartbeat(
        &mut self,
        block: FinalizedBlock,
        broadcast_pending_events: bool,
    ) -> anyhow::Result<()> {
        let block_height = *block.header.height();
        let block_timestamp = header_timestamp(&block.header)?;
        let checkpoint_height = *self.checkpoint_height.borrow();

        // If the block height is less than or equal to the checkpoint height,
        // we should not process it, as it is a duplicate.
        if block_height <= checkpoint_height {
            tracing::warn!(
                "Received heartbeat for block height {} which is <= checkpoint height {}",
                block_height,
                checkpoint_height
            );
            return Ok(());
        }

        let next_checkpoint_height = checkpoint_height.succ().ok_or_else(|| {
            anyhow::anyhow!(
                "Failed to increment checkpoint height from {}",
                checkpoint_height
            )
        })?;

        // If the hearbeat event comes from the future, we need to sync up until
        // the previous height, in order to process this heartbeat correctly.
        if next_checkpoint_height != block_height {
            let previous_height = block_height.pred().ok_or_else(|| {
                anyhow::anyhow!("Failed to decrement block height from {}", block_height)
            })?;

            let future = self.sync_up_to(previous_height);
            let future = Box::pin(future);
            future.await?;
        }

        let emitted_events = core::mem::take(&mut self.emitted_events);
        self.pending_events.remove(&block_height);

        let mut tx = self.storage.write_transaction();

        let statuses = block.statuses;
        let mut receipts = statuses
            .iter()
            .enumerate()
            .map(|(tx_index, status)| {
                let (receipts, execution_status) = match &status.result {
                    TransactionExecutionResult::Success { receipts, .. } => {
                        (receipts.clone(), ExecutionStatus::Success)
                    }
                    TransactionExecutionResult::Failed {
                        receipts, result, ..
                    } => {
                        let reason = TransactionExecutionResult::reason(receipts, result);
                        (receipts.clone(), ExecutionStatus::Failure { reason })
                    }
                };

                TransactionReceipts {
                    tx_pointer: TxPointer::new(block_height, tx_index as u16),
                    tx_id: status.id,
                    receipts,
                    execution_status,
                }
            })
            .collect::<Vec<_>>();

        // Remove receipts from `Mint` transactions, as they are not relevant for the indexer.
        receipts.pop();

        tx.storage_as_mut::<Receipts>()
            .insert(&block_height, &receipts)?;

        #[cfg(feature = "blocks-subscription")]
        let block_event = BlockEvent {
            header: block.header,
            consensus: block.consensus,
            transactions: block.transactions,
            statuses,
        };
        #[cfg(feature = "blocks-subscription")]
        tx.storage_as_mut::<Blocks>()
            .insert(&block_height, &block_event)?;
        #[cfg(not(feature = "blocks-subscription"))]
        tx.storage_as_mut::<Headers>()
            .insert(&block_height, &block.header)?;

        tx.storage_as_mut::<LastCheckpoint>()
            .insert(&(), &block_height)?;

        let changes = tx.into_changes();
        self.storage.commit_changes(changes)?;

        self.checkpoint_height.send_replace(block_height);

        let events_count = receipts.len();

        if receipts != emitted_events {
            // It is possible that we received a heartbeat before we got all pre confirmations.
            // In this case, we can just emit the rest of events.
            if receipts.len() > emitted_events.len()
                && receipts[..emitted_events.len()] == emitted_events[..]
            {
                for event in receipts.into_iter().skip(emitted_events.len()) {
                    Self::broadcast_event(&self.broadcast_receipts, event.into())?;
                }
            } else {
                if !emitted_events.is_empty() {
                    tracing::warn!(
                        "Received heartbeat for block height {} with receipts \
                            that do not match pending receipts. \
                            Emitted events: {:?}, Received events count: {:?}",
                        block_height,
                        emitted_events,
                        receipts
                    );

                    Self::broadcast_event(
                        &self.broadcast_receipts,
                        UnstableReceipts::Rollback(block_height),
                    )?;
                }

                for event in receipts {
                    Self::broadcast_event(&self.broadcast_receipts, event.into())?;
                }
            }
        }

        Self::broadcast_event(
            &self.broadcast_receipts,
            UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height,
                events_count,
                timestamp: block_timestamp,
            }),
        )?;
        #[cfg(feature = "blocks-subscription")]
        let _ = self.broadcast_blocks.send(block_event);

        if broadcast_pending_events {
            let next_block_height = block_height.succ().ok_or_else(|| {
                anyhow::anyhow!(
                    "Failed to increment checkpoint height from {}",
                    block_height
                )
            })?;

            // If we have any next events that are pending for the next checkpoint height,
            // emit them now.
            let next_pending_events = self
                .pending_events
                .remove(&next_block_height)
                .unwrap_or_default();

            for (_, next_event) in next_pending_events {
                self.handle_next_event(next_event)?;
            }
        }

        tokio::task::yield_now().await;

        Ok(())
    }

    fn handle_next_event(
        &mut self,
        next_event: TransactionReceipts,
    ) -> anyhow::Result<()> {
        let checkpoint_height = *self.checkpoint_height.borrow();
        let next_checkpoint_height = checkpoint_height.succ().ok_or_else(|| {
            anyhow::anyhow!(
                "Failed to increment checkpoint height from {}",
                checkpoint_height
            )
        })?;

        if next_event.tx_pointer.block_height() < next_checkpoint_height {
            tracing::warn!(
                "Received pre-confirmation event for block height {} which is <= checkpoint height {}",
                next_event.tx_pointer.block_height(),
                checkpoint_height
            );

            return Ok(());
        }

        self.pending_events
            .entry(next_event.tx_pointer.block_height())
            .or_default()
            .insert(next_event.tx_pointer.tx_index(), next_event);

        while let Some(mut entry) = self.pending_events.first_entry() {
            if entry.key() < &next_checkpoint_height {
                tracing::warn!(
                    "Service in incorrect state {} which is <= checkpoint height {}",
                    entry.key(),
                    next_checkpoint_height
                );

                entry.remove();
                continue;
            }

            // Pending events are from the future
            if entry.key() != &next_checkpoint_height {
                break;
            }

            while let Some(event_entry) = entry.get_mut().first_entry() {
                let tx_index = self.emitted_events.len();
                let next_tx_pointer =
                    TxPointer::new(next_checkpoint_height, tx_index as u16);

                if event_entry.get().tx_pointer == next_tx_pointer {
                    let event = event_entry.remove();
                    self.emitted_events.push(event.clone());
                    Self::broadcast_event(&self.broadcast_receipts, event.into())?;
                } else {
                    break;
                }
            }

            if entry.get().is_empty() {
                entry.remove();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Keep the service alive when a heartbeat cannot be processed.
    ///
    /// The common cause is lag, not corruption: `sync_up_to` gives up when the
    /// range it asked for did not reach the final height, which happens
    /// whenever the node being read from is a block or two behind. Stopping on
    /// that took the whole indexer down, and with it the checkpoint channel
    /// every consumer depends on — on 2026-08-19 a one-block shortfall
    /// (`03b12a75` against a final `03b12a76`) ended the service and left the
    /// consuming API without a data source.
    ///
    /// So keep trying until it works. The condition clears by itself once the
    /// node catches up, and a stream that has genuinely died is already
    /// replaced by the liveness arm.
    ///
    /// Retries are paced by block arrival, since the loop goes back to awaiting
    /// the next heartbeat; `heartbeat_retry_delay` adds a floor on top of that
    /// so a fast-arriving backlog cannot hammer a failure. `ErrorContinue`
    /// reports each attempt rather than swallowing it, so a failure that never
    /// clears stays visible.
    async fn retry_after_heartbeat_failure(
        &self,
        err: anyhow::Error,
    ) -> fuel_core_services::TaskNextAction {
        tracing::warn!("Failed to handle heartbeat, retrying: {err:?}");
        tokio::time::sleep(self.heartbeat_retry_delay).await;
        fuel_core_services::TaskNextAction::ErrorContinue(err)
    }
}

impl<S, F> RunnableTask for Task<S, F>
where
    S: super::port::Storage,
    F: super::port::Fetcher,
{
    async fn run(
        &mut self,
        watcher: &mut fuel_core_services::StateWatcher,
    ) -> fuel_core_services::TaskNextAction {
        tokio::select! {
            biased;

            _ = watcher.while_started() => fuel_core_services::TaskNextAction::Stop,

            // First we should process pre confirmation events,
            // and only after that finalize it with the heartbeat.
            event = self.event_source.next() => {
                match event {
                    None => {
                        match self.fetcher.predicted_receipts_stream() {
                            Ok(stream) => {
                                self.event_source = stream;
                                fuel_core_services::TaskNextAction::Continue
                            }
                            Err(e) => {
                                tracing::error!("Failed to fetch predicted events stream: {}", e);
                                fuel_core_services::TaskNextAction::ErrorContinue(e)
                            }
                        }
                    }
                    Some(event) => {
                        if let Err(err) = self.handle_next_event(event) {
                            tracing::error!("Failed to handle next event: {err:?}");
                            fuel_core_services::TaskNextAction::Stop
                        } else {
                            fuel_core_services::TaskNextAction::Continue
                        }
                    }
                }
            }

            _ = self.heartbeat_liveness.tick() => {
                tracing::error!("Heartbeat liveness timeout reached, \
                    attempting to reconnect to confirmed events stream.");
                match self.fetcher.finalized_blocks_stream() {
                    Ok(stream) => {
                        self.heartbeat = stream;
                        self.heartbeat_liveness.reset();
                        fuel_core_services::TaskNextAction::Continue
                    }
                    Err(e) => {
                        tracing::error!("Failed to fetch confirmed events stream: {}", e);
                        fuel_core_services::TaskNextAction::ErrorContinue(e)
                    }
                }
            }

            hearbeat = self.heartbeat.next() => {
                self.heartbeat_liveness.reset();
                match hearbeat {
                    None => {
                        match self.fetcher.finalized_blocks_stream() {
                            Ok(stream) => {
                                self.heartbeat = stream;
                                self.heartbeat_liveness.reset();
                                fuel_core_services::TaskNextAction::Continue
                            }
                            Err(e) => {
                                tracing::error!("Failed to fetch confirmed events stream: {}", e);
                                fuel_core_services::TaskNextAction::ErrorContinue(e)
                            }
                        }
                    }
                    Some(block) => {
                        tokio::select! {
                            result = self.handle_heartbeat(block, true) => {
                                match result {
                                    Ok(()) => {
                                        fuel_core_services::TaskNextAction::Continue
                                    }
                                    Err(err) => {
                                        self.retry_after_heartbeat_failure(err).await
                                    }
                                }
                            }

                            _ = watcher.while_started() => fuel_core_services::TaskNextAction::Stop,
                        }
                    }
                }
            }
        }
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        // Do nothing
        Ok(())
    }
}

#[derive(Clone)]
pub struct SharedState<S> {
    starting_height: BlockHeight,
    storage: S,
    broadcast_receipts: broadcast::Sender<UnstableReceipts>,
    #[cfg(feature = "blocks-subscription")]
    broadcast_blocks: broadcast::Sender<BlockEvent>,
    checkpoint_height: watch::Receiver<BlockHeight>,
}

impl<S> SharedState<S>
where
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

    pub fn unstable_receipts_starting_from(
        &self,
        start_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<UnstableReceipts>>> {
        use futures::TryStreamExt;

        if start_height < self.starting_height {
            return Err(anyhow::anyhow!(
                "Start height {} is less than the initial starting height {}",
                start_height,
                self.starting_height
            ));
        }

        let stream_life_events =
            BroadcastStream::new(self.broadcast_receipts.subscribe());
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
                .storage_as_ref::<Receipts>()
                .get(&next_available_height)?
                .ok_or_else(|| not_found!(Receipts))?
                .into_owned();

            let checkpoint_event = UnstableReceipts::Checkpoint(CheckpointEvent {
                block_height: next_available_height,
                events_count: events.len(),
                timestamp: shared_state.timestamp_at(&next_available_height)?,
            });

            let iter = events
                .into_iter()
                .map(UnstableReceipts::from)
                .chain(iter::once(checkpoint_event));

            Ok::<_, anyhow::Error>(futures::stream::iter(iter.map(Ok)))
        })
        .try_flatten();

        let storage_iter = StorageIterator::from(self.storage.clone(), |storage| {
            storage.iter_all_by_start::<Receipts>(
                Some(&start_height),
                Some(IterDirection::Forward),
            )
        });

        let shared_state = self.clone();
        let storage_iter_until_available_height = storage_iter
            .take_while(move |result| match result {
                Ok((block_height, _)) => *block_height <= available_height,
                Err(_) => true,
            })
            .map(move |result| {
                let (block_height, events) = result
                    .map_err(|e| anyhow::anyhow!("Storage iterator error: {}", e))?;

                let checkpoint_event = UnstableReceipts::Checkpoint(CheckpointEvent {
                    block_height,
                    events_count: events.len(),
                    timestamp: shared_state.timestamp_at(&block_height)?,
                });

                let iter = events
                    .into_iter()
                    .map(UnstableReceipts::from)
                    .chain(iter::once(checkpoint_event));

                Ok::<_, anyhow::Error>(futures::stream::iter(iter.map(Ok)))
            });

        let storage_iter_until_available_height =
            futures::stream::iter(storage_iter_until_available_height).try_flatten();

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

    #[cfg(feature = "blocks-subscription")]
    pub async fn blocks_starting_from(
        &self,
        start_height: BlockHeight,
    ) -> anyhow::Result<BoxStream<anyhow::Result<BlockEvent>>> {
        if start_height < self.starting_height {
            return Err(anyhow::anyhow!(
                "Start height {} is less than the initial starting height {}",
                start_height,
                self.starting_height
            ));
        }

        let stream_life_events = BroadcastStream::new(self.broadcast_blocks.subscribe());
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
                    Ok(event) => event.header.height() <= &next_available_height,
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
            let event = storage
                .storage_as_ref::<Blocks>()
                .get(&next_available_height)?
                .ok_or_else(|| not_found!(Receipts))?
                .into_owned();

            Ok::<_, anyhow::Error>(event)
        });

        let storage_iter = StorageIterator::from(self.storage.clone(), |storage| {
            storage.iter_all_by_start::<Blocks>(
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
                    .map(|(_, event)| event)
                    .map_err(|e| anyhow::anyhow!("Storage iterator error: {}", e))
            });

        let storage_iter_until_available_height =
            futures::stream::iter(storage_iter_until_available_height);

        Ok(storage_iter_until_available_height
            .chain(next_available_height_events)
            .chain(next_next_available_height_life_stream)
            .skip_while(move |event| {
                let skip = match event {
                    Ok(event) => event.header.height() < &start_height,
                    Err(_) => {
                        // In the case of error we want to propagate it, so no skipping
                        false
                    }
                };
                async move { skip }
            })
            .into_boxed())
    }

    pub fn receipts_at(
        &self,
        block_height: &BlockHeight,
    ) -> anyhow::Result<Option<Vec<TransactionReceipts>>> {
        let events = self
            .storage
            .read_transaction()
            .storage_as_ref::<Receipts>()
            .get(block_height)?
            .map(|v| v.into_owned());

        Ok(events)
    }

    #[cfg(feature = "blocks-subscription")]
    pub fn blocks_at(
        &self,
        block_height: &BlockHeight,
    ) -> anyhow::Result<Option<BlockEvent>> {
        let events = self
            .storage
            .read_transaction()
            .storage_as_ref::<Blocks>()
            .get(block_height)?
            .map(|v| v.into_owned());

        Ok(events)
    }

    #[cfg(not(feature = "blocks-subscription"))]
    pub fn header_at(
        &self,
        block_height: &BlockHeight,
    ) -> anyhow::Result<Option<BlockHeader>> {
        let header = self
            .storage
            .read_transaction()
            .storage_as_ref::<Headers>()
            .get(block_height)?
            .map(|v| v.into_owned());

        Ok(header)
    }

    /// Returns the committed block's timestamp in seconds since the Unix epoch.
    pub fn timestamp_at(&self, block_height: &BlockHeight) -> anyhow::Result<u128> {
        #[cfg(not(feature = "blocks-subscription"))]
        let header = self.header_at(block_height)?.ok_or_else(|| {
            anyhow::anyhow!("No block header for height {block_height}")
        })?;
        #[cfg(feature = "blocks-subscription")]
        let header = self
            .blocks_at(block_height)?
            .ok_or_else(|| anyhow::anyhow!("No block for height {block_height}"))?
            .header;

        header_timestamp(&header)
    }
}

pub type ReceiptsManager<S, F> = ServiceRunner<UninitializedService<S, F>>;

pub fn new_service<S, F>(
    starting_height: BlockHeight,
    use_preconfirmations: bool,
    storage: S,
    fetcher: F,
    heartbeat_retry_delay: Duration,
) -> anyhow::Result<ReceiptsManager<S, F>>
where
    S: super::port::Storage,
    F: super::port::Fetcher,
{
    let uninit = UninitializedService::new(
        starting_height,
        use_preconfirmations,
        storage,
        fetcher,
    )?
    .with_heartbeat_retry_delay(heartbeat_retry_delay);
    Ok(ServiceRunner::new(uninit))
}
