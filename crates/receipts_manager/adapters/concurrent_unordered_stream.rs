use crate::adapters::resizable_buffered_unordered::ResizableBufferedUnordered;
use futures_util::{
    Stream,
    StreamExt,
};
use std::{
    collections::VecDeque,
    future::Future,
};

pub trait ConcurrentUnorderedStream
where
    Self: Stream + Send + 'static,
    Self::Item: Future + Send + 'static,
    <Self::Item as Future>::Output: Send + 'static,
{
    fn concurrent_unordered(
        self,
        size: usize,
    ) -> impl Stream<Item = <Self::Item as Future>::Output>;
}

impl<S> ConcurrentUnorderedStream for S
where
    Self: Stream + Send + 'static,
    Self::Item: Future + Send + 'static,
    <Self::Item as Future>::Output: Send + 'static,
{
    fn concurrent_unordered(
        self,
        size: usize,
    ) -> impl Stream<Item = <Self::Item as Future>::Output> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _self = ResizableBufferedUnordered::new(self, size);
            futures_util::pin_mut!(_self);

            let mut queue = VecDeque::with_capacity(size);
            loop {
                if queue.is_empty() {
                    if let Some(item) = _self.next().await {
                        _self.as_mut().shrink_by_one();
                        queue.push_back(item);
                    } else {
                        break;
                    }
                } else if queue.len() == size {
                    if let Ok(permit) = sender.reserve().await {
                        _self.as_mut().grow_by_one();
                        let item = queue.pop_front().expect("buffer is non-empty");
                        permit.send(item);
                    } else {
                        break;
                    }
                } else {
                    tokio::select! {
                        biased;
                        permit = sender.reserve() => {
                            if let Ok(permit) = permit {
                                _self.as_mut().grow_by_one();
                                let item = queue.pop_front().expect("buffer is non-empty");
                                permit.send(item);
                            } else {
                                break;
                            }
                        }
                        item = _self.next() => {
                            if let Some(item) = item {
                                _self.as_mut().shrink_by_one();
                                queue.push_back(item);
                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            while let Some(item) = queue.pop_front() {
                let _ = sender.send(item).await;
            }
        });

        tokio_stream::wrappers::ReceiverStream::new(receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::ConcurrentUnorderedStream;
    use futures_util::StreamExt;
    use std::{
        future::pending,
        time::Duration,
    };

    #[tokio::test]
    async fn regular_buffered_unordered() {
        let mut senders = vec![];
        let mut receivers = vec![];
        let durations = [5, 0, 0, 0, 0];

        for duration in durations {
            let (s, r) = tokio::sync::watch::channel(false);
            senders.push((s, duration));
            receivers.push(r);
        }

        let stream = futures_util::stream::iter(senders)
            .map(|(sender, duration)| async move {
                tokio::time::sleep(Duration::from_secs(duration)).await;
                sender.send_replace(true);
            })
            // Given
            .buffer_unordered(2)
            // Infinite future to block the stream
            .filter_map(|()| async move { pending::<Option<()>>().await });

        // When
        tokio::spawn(async move {
            stream.collect::<Vec<_>>().await;
        });

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Then
        let values = receivers
            .into_iter()
            .map(|mut r| *r.borrow_and_update())
            .collect::<Vec<_>>();
        // The buffered unordered stream adds 5 tasks into its queue, and checks them periodically.
        // When one of them finishes, it is propagated to `filter_map`, which has infinite loop,
        // returning `Pending`. Because this future is not resolved, it blocks
        // the buffered unordered stream from pulling more tasks.
        assert_eq!(values, vec![false, true, false, false, false]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_unordered() {
        let mut senders = vec![];
        let mut receivers = vec![];
        let durations = [5, 0, 0, 0, 0];

        for duration in durations {
            let (s, r) = tokio::sync::watch::channel(false);
            senders.push((s, duration));
            receivers.push(r);
        }

        // Given
        let stream = futures_util::stream::iter(senders)
            .map(|(sender, duration)| async move {
                tokio::time::sleep(Duration::from_secs(duration)).await;
                sender.send_replace(true);
            })
            .concurrent_unordered(2)
            // Infinite future to block the stream
            .filter_map(|()| async move { pending::<Option<()>>().await });

        // When
        tokio::spawn(async move {
            stream.collect::<Vec<_>>().await;
        });

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Then
        let values = receivers
            .into_iter()
            .map(|mut r| *r.borrow_and_update())
            .collect::<Vec<_>>();
        // The concurrent unordered stream adds 2 tasks into its queue, and checks them periodically.
        // First task takes 5 seconds, so it will not finish in time.
        // All other tasks take 0 seconds, so they will finish immediately when they are pulled.
        // When one of them finishes, it is added to the buffer, and immediately propagated to the
        // `mpsc` channel. After it is propagated to `mpsc`, we add another task to the queue.
        // At this point we have 3 started tasks.
        // 1 in the queue, 1 finished in the `mpsc` channel, and 1 finished in `filter_map`.
        assert_eq!(values, vec![false, true, true, true, false]);
    }
}
