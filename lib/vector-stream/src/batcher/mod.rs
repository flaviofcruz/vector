pub mod config;
pub mod data;
pub mod limiter;

use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};

pub use config::BatchConfig;
use futures::{
    Future, StreamExt,
    stream::{Fuse, Stream},
};
use pin_project::pin_project;
use tokio::time::Sleep;
use vector_common::flush_signal::{self, FlushSignal};

#[pin_project]
pub struct Batcher<S, C> {
    state: C,

    #[pin]
    /// The stream this `Batcher` wraps
    stream: Fuse<S>,

    #[pin]
    timer: Maybe<Sleep>,

    /// Optional task-local signal, set by the buffer reader during shutdown,
    /// requesting that the open batch be flushed immediately rather than held
    /// until the batch timeout. See `poll_next`'s `Poll::Pending` arm.
    flush_signal: Option<FlushSignal>,
}

/// An `Option`, but with pin projection
#[pin_project(project = MaybeProj)]
pub enum Maybe<T> {
    Some(#[pin] T),
    None,
}

impl<S, C> Batcher<S, C>
where
    S: Stream,
    C: BatchConfig<S::Item>,
{
    pub fn new(stream: S, config: C) -> Self {
        Self {
            state: config,
            stream: stream.fuse(),
            timer: Maybe::None,
            flush_signal: flush_signal::get_task_flush_signal(),
        }
    }
}

impl<S, C> Stream for Batcher<S, C>
where
    S: Stream,
    C: BatchConfig<S::Item>,
{
    type Item = C::Batch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();
            match this.stream.poll_next(cx) {
                Poll::Ready(None) => {
                    return {
                        if this.state.len() == 0 {
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(this.state.take_batch()))
                        }
                    };
                }
                Poll::Ready(Some(item)) => {
                    let (item_fits, item_metadata) = this.state.item_fits_in_batch(&item);
                    if item_fits {
                        this.state.push(item, item_metadata);
                        if this.state.is_batch_full() {
                            this.timer.set(Maybe::None);
                            return Poll::Ready(Some(this.state.take_batch()));
                        } else if this.state.len() == 1 {
                            this.timer
                                .set(Maybe::Some(tokio::time::sleep(this.state.timeout())));
                        }
                    } else {
                        let output = Poll::Ready(Some(this.state.take_batch()));
                        this.state.push(item, item_metadata);
                        this.timer
                            .set(Maybe::Some(tokio::time::sleep(this.state.timeout())));
                        return output;
                    }
                }
                Poll::Pending => {
                    // Check if the buffer reader has signaled us to flush the open
                    // batch. This happens during shutdown when the writer is done but
                    // there are still unacknowledged records — flushing lets the sink
                    // process them and send acks back so the buffer can drain, instead
                    // of holding the partial batch until `batch.timeout_secs` (which can
                    // outlast the shutdown deadline). Mirrors `PartitionedBatcher`.
                    //
                    // Unconditional (matching `PartitionedBatcher`): the signal only
                    // exists for disk-buffered sinks and is only ever set at shutdown,
                    // so this has zero steady-state effect.
                    if let Some(signal) = this.flush_signal.as_ref() {
                        if signal.take() && this.state.len() != 0 {
                            this.timer.set(Maybe::None);
                            return Poll::Ready(Some(this.state.take_batch()));
                        }
                    }

                    return {
                        if let MaybeProj::Some(timer) = this.timer.as_mut().project() {
                            ready!(timer.poll(cx));
                            this.timer.set(Maybe::None);
                            debug_assert!(
                                this.state.len() != 0,
                                "timer should have been cancelled"
                            );
                            Poll::Ready(Some(this.state.take_batch()))
                        } else {
                            Poll::Pending
                        }
                    };
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[cfg(test)]
#[allow(clippy::similar_names)]
mod test {
    use std::{num::NonZeroUsize, time::Duration};

    use futures::stream;

    use super::*;
    use crate::BatcherSettings;

    #[tokio::test]
    async fn item_limit() {
        let stream = stream::iter([1, 2, 3]);
        let settings = BatcherSettings::new(
            Duration::from_millis(100),
            NonZeroUsize::new(10000).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        );
        let batcher = Batcher::new(stream, settings.as_item_size_config(|x: &u32| *x as usize));
        let batches: Vec<_> = batcher.collect().await;
        assert_eq!(batches, vec![vec![1, 2], vec![3],]);
    }

    #[tokio::test]
    async fn size_limit() {
        let batcher = Batcher::new(
            stream::iter([1, 2, 3, 4, 5, 6, 2, 3, 1]),
            BatcherSettings::new(
                Duration::from_millis(100),
                NonZeroUsize::new(5).unwrap(),
                NonZeroUsize::new(100).unwrap(),
            )
            .as_item_size_config(|x: &u32| *x as usize),
        );
        let batches: Vec<_> = batcher.collect().await;
        assert_eq!(
            batches,
            vec![
                vec![1, 2],
                vec![3],
                vec![4],
                vec![5],
                vec![6],
                vec![2, 3],
                vec![1],
            ]
        );
    }

    #[tokio::test]
    async fn timeout_limit() {
        tokio::time::pause();

        let timeout = Duration::from_millis(100);
        let stream = stream::iter([1, 2]).chain(stream::pending());
        let batcher = Batcher::new(
            stream,
            BatcherSettings::new(
                timeout,
                NonZeroUsize::new(5).unwrap(),
                NonZeroUsize::new(100).unwrap(),
            )
            .as_item_size_config(|x: &u32| *x as usize),
        );

        tokio::pin!(batcher);
        let mut next = batcher.next();
        assert_eq!(futures::poll!(&mut next), Poll::Pending);
        tokio::time::advance(timeout).await;
        let batch = next.await;
        assert_eq!(batch, Some(vec![1, 2]));
    }

    /// The shutdown flush signal must flush the open partial batch immediately
    /// (before the batch timeout fires), so a disk-buffered sink can drain and
    /// ack inside the wave-1 shutdown deadline instead of waiting out the full
    /// `batch.timeout_secs`. Mirrors `PartitionedBatcher`'s flush-signal behavior.
    #[tokio::test]
    async fn flush_signal_flushes_open_batch_immediately() {
        tokio::time::pause();

        // Long timeout so the batch would NOT flush on its own within the test.
        let timeout = Duration::from_secs(60);
        let signal = FlushSignal::new();

        flush_signal::with_flush_signal(signal.clone(), async {
            let stream = stream::iter([1, 2]).chain(stream::pending());
            let batcher = Batcher::new(
                stream,
                BatcherSettings::new(
                    timeout,
                    NonZeroUsize::new(5).unwrap(),
                    NonZeroUsize::new(100).unwrap(),
                )
                .as_item_size_config(|x: &u32| *x as usize),
            );

            tokio::pin!(batcher);
            let mut next = batcher.next();
            // Items are buffered but the (60s) timer has NOT expired: batch is held.
            assert_eq!(futures::poll!(&mut next), Poll::Pending);

            // Buffer reader requests a flush (shutdown).
            signal.set();

            // WITHOUT advancing time, the open batch must be emitted immediately.
            // (Polling rather than awaiting: under paused time an await would
            // auto-advance to the 60s timer and mask a missing flush hook.)
            assert_eq!(futures::poll!(&mut next), Poll::Ready(Some(vec![1, 2])));
        })
        .await;
    }

    /// Steady-state guard: with no flush signal set, an open partial batch stays
    /// held until the timeout (unchanged from the original behavior).
    #[tokio::test]
    async fn no_flush_when_signal_unset() {
        tokio::time::pause();

        let timeout = Duration::from_secs(60);
        let signal = FlushSignal::new();

        flush_signal::with_flush_signal(signal.clone(), async {
            let stream = stream::iter([1, 2]).chain(stream::pending());
            let batcher = Batcher::new(
                stream,
                BatcherSettings::new(
                    timeout,
                    NonZeroUsize::new(5).unwrap(),
                    NonZeroUsize::new(100).unwrap(),
                )
                .as_item_size_config(|x: &u32| *x as usize),
            );

            tokio::pin!(batcher);
            let mut next = batcher.next();
            // Signal never set: the batch must remain held (Pending), not flushed.
            assert_eq!(futures::poll!(&mut next), Poll::Pending);
            assert_eq!(futures::poll!(&mut next), Poll::Pending);
        })
        .await;
    }
}
