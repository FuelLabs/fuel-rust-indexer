//! Bounded live delivery for consumers whose progress must constrain a producer.
//!
//! Observer broadcasts remain independent. Only the producing task owns the
//! publisher; dropping that task closes its required consumers' live queues.

use fuel_core_types::fuel_types::BlockHeight;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        Mutex,
        Weak,
    },
};
use tokio::sync::{
    mpsc,
    watch,
};

/// Live messages retained per required consumer (transactions, checkpoints and
/// rollbacks). This bounds queue length, not the size of an individual message.
pub const DEFAULT_CAPACITY: NonZeroUsize = NonZeroUsize::new(256).unwrap();

struct Consumer<T> {
    replay_through: BlockHeight,
    sender: mpsc::Sender<Arc<T>>,
}

type Consumers<T> = Mutex<Vec<Consumer<T>>>;

pub struct Publisher<T> {
    consumers: Arc<Consumers<T>>,
}

#[derive(Clone)]
pub struct Subscriptions<T> {
    consumers: Weak<Consumers<T>>,
}

impl<T> Publisher<T> {
    pub fn new() -> (Self, Subscriptions<T>) {
        let consumers = Arc::new(Mutex::new(Vec::new()));
        let subscriptions = Subscriptions {
            consumers: Arc::downgrade(&consumers),
        };
        (Self { consumers }, subscriptions)
    }

    /// Call in publication order from the single producing task. No lock is
    /// held while waiting for queue space. A dropped subscription releases its
    /// wait, allowing normal service shutdown or replay reconnection.
    pub async fn send(&self, height: BlockHeight, event: &T)
    where
        T: Clone,
    {
        let senders: Vec<_> = {
            let mut consumers = self.consumers.lock().expect("consumer lock poisoned");
            consumers.retain(|consumer| !consumer.sender.is_closed());
            consumers
                .iter()
                .filter(|consumer| height > consumer.replay_through)
                .map(|consumer| consumer.sender.clone())
                .collect()
        };
        if senders.is_empty() {
            return;
        }
        let event = Arc::new(event.clone());
        for sender in senders {
            let _ = sender.send(event.clone()).await;
        }
    }
}

impl<T> Subscriptions<T> {
    /// Register before replay begins, capturing the same checkpoint used for
    /// historical replay. Existing streams replay through H+1 from storage
    /// because a subscription can join halfway through a block. Those messages
    /// MUST bypass this queue: waiting for space in H+1 could prevent its commit
    /// while the consumer waits for that very commit to finish replay.
    pub fn subscribe(
        &self,
        capacity: NonZeroUsize,
        checkpoint: &watch::Receiver<BlockHeight>,
    ) -> anyhow::Result<(BlockHeight, mpsc::Receiver<Arc<T>>)> {
        let consumers = self
            .consumers
            .upgrade()
            .ok_or(crate::shutdown::ServiceShutDown)?;
        let mut consumers = consumers.lock().expect("consumer lock poisoned");
        let available_height = *checkpoint.borrow();
        let replay_through = available_height
            .succ()
            .ok_or_else(|| anyhow::anyhow!("Cannot subscribe beyond maximum height"))?;
        let (sender, receiver) = mpsc::channel(capacity.get());
        consumers.push(Consumer {
            replay_through,
            sender,
        });
        Ok((available_height, receiver))
    }
}
