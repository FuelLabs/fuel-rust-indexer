use crate::adapters::resizable_buffered::ResizableBuffered;
use futures_util::{
    Stream,
    StreamExt,
};
use std::{
    collections::VecDeque,
    future::Future,
};

pub trait ConcurrentStream
where
    Self: Stream + Send + 'static,
    Self::Item: Future + Send + 'static,
    <Self::Item as Future>::Output: Send + 'static,
{
    fn concurrent(
        self,
        size: usize,
    ) -> impl Stream<Item = <Self::Item as Future>::Output>;
}

impl<S> ConcurrentStream for S
where
    Self: Stream + Send + 'static,
    Self::Item: Future + Send + 'static,
    <Self::Item as Future>::Output: Send + 'static,
{
    fn concurrent(
        self,
        size: usize,
    ) -> impl Stream<Item = <Self::Item as Future>::Output> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _self = ResizableBuffered::new(self, size);
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
    use crate::adapters::concurrent_stream::ConcurrentStream;
    use futures_util::StreamExt;
    use std::{
        future::pending,
        time::Duration,
    };

    #[tokio::test]
    async fn regular_buffered() {
        let mut senders = vec![];
        let mut receivers = vec![];

        for _ in 0..5 {
            let (s, r) = tokio::sync::watch::channel(false);
            senders.push(s);
            receivers.push(r);
        }

        let stream = futures_util::stream::iter(senders)
            .map(|sender| async move {
                sender.send_replace(true);
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
            // Given
            .buffered(2)
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
        // The buffered stream adds 2 tasks into its queue, and checks them periodically.
        // When one of them finishes, it is propagated to `filter_map`, which has infinite loop,
        // returning `Pending`. Because this future is not resolved, it blocks
        // the buffered stream from pulling more tasks.
        assert_eq!(values, vec![true, true, false, false, false]);
    }

    #[tokio::test]
    async fn concurrent() {
        let mut senders = vec![];
        let mut receivers = vec![];

        for _ in 0..5 {
            let (s, r) = tokio::sync::watch::channel(false);
            senders.push(s);
            receivers.push(r);
        }

        // Given
        let stream = futures_util::stream::iter(senders)
            .map(|sender| async move {
                sender.send_replace(true);
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
            .concurrent(2)
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
        // The concurrent stream adds 2 tasks into its queue, and checks them periodically.
        // When one of them finishes, it is added to the buffer, and immediately propagated to the
        // `mpsc` channel. After it is propagated to `mpsc`, we add another task to the queue.
        // At this point we have 3 started tasks.
        // The result from `mpsc` is then passed to `filter_map`, freeing `mpsc` channel.
        // Which allows us to add another task to the queue.
        // So at the end we have 4 started/finished tasks:
        // 2 in the queue, 1 finished in the `mpsc` channel, and 1 finished in `filter_map`.
        assert_eq!(values, vec![true, true, true, true, false]);
    }
}
