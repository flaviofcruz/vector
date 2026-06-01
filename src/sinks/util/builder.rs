use std::{
    convert::Infallible,
    fmt,
    future::Future,
    hash::Hash,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{Stream, StreamExt, stream::Map};
use pin_project::pin_project;
use tower::Service;
use tracing::Span;
use vector_lib::{
    ByteSizeOf, emit,
    event::{Finalizable, Metric},
    partition::Partitioner,
    stream::{
        ConcurrentMap, Driver, DriverResponse, ExpirationQueue, PartitionedBatcher,
        batcher::{Batcher, config::BatchConfig},
    },
};

use super::{
    IncrementalRequestBuilder, Normalizer, RequestBuilder, buffer::metrics::MetricNormalize,
};
use crate::internal_events::ComponentEncodeCpuTime;

// Calling thread's consumed CPU time; `None` if unavailable on this platform.
#[cfg(target_os = "linux")]
fn thread_cpu_time() -> Option<Duration> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `clock_gettime` only writes `ts`; the clock id is valid on Linux.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc == 0 {
        Some(Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32))
    } else {
        None
    }
}

#[cfg(not(target_os = "linux"))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

impl<T: ?Sized> SinkBuilderExt for T where T: Stream {}

pub trait SinkBuilderExt: Stream {
    /// Converts a stream of infallible results by unwrapping them.
    ///
    /// For a stream of `Result<T, Infallible>` items, this turns it into a stream of `T` items.
    fn unwrap_infallible<T>(self) -> UnwrapInfallible<Self>
    where
        Self: Stream<Item = Result<T, Infallible>> + Sized,
    {
        UnwrapInfallible { st: self }
    }

    /// Batches the stream based on the given partitioner and batch settings.
    ///
    /// The stream will yield batches of events, with their partition key, when either a batch fills
    /// up or times out. [`Partitioner`] operates on a per-event basis, and has access to the event
    /// itself, and so can access any and all fields of an event.
    fn batched_partitioned<P, C, F, B>(
        self,
        partitioner: P,
        settings: F,
    ) -> PartitionedBatcher<Self, P, ExpirationQueue<P::Key>, C, F, B>
    where
        Self: Stream<Item = P::Item> + Sized,
        P: Partitioner + Unpin,
        P::Key: Eq + Hash + Clone,
        P::Item: ByteSizeOf,
        C: BatchConfig<P::Item>,
        F: Fn() -> C + Send,
    {
        PartitionedBatcher::new(self, partitioner, settings)
    }

    /// Batches the stream based on the given batch settings and item size calculator.
    ///
    /// The stream will yield batches of events, when either a batch fills
    /// up or times out. The `item_size_calculator` determines the "size" of each input
    /// in a batch. The units of "size" are intentionally not defined, so you can choose
    /// whatever is needed.
    fn batched<C>(self, config: C) -> Batcher<Self, C>
    where
        C: BatchConfig<Self::Item>,
        Self: Sized,
    {
        Batcher::new(self, config)
    }

    /// Maps the items in the stream concurrently, up to the configured limit.
    ///
    /// For every item, the given mapper is invoked, and the future that is returned is spawned
    /// and awaited concurrently.  A limit can be passed: `None` is self-describing, as it imposes
    /// no concurrency limit, and `Some(n)` limits this stage to `n` concurrent operations at any
    /// given time.
    ///
    /// If the spawned future panics, the panic will be carried through and resumed on the task
    /// calling the stream.
    fn concurrent_map<F, T>(self, limit: NonZeroUsize, f: F) -> ConcurrentMap<Self, T>
    where
        Self: Sized,
        F: Fn(Self::Item) -> Pin<Box<dyn Future<Output = T> + Send + 'static>> + Send + 'static,
        T: Send + 'static,
    {
        ConcurrentMap::new(self, Some(limit), f)
    }

    /// Constructs a [`Stream`] which transforms the input into a request suitable for sending to
    /// downstream services.
    ///
    /// Each input is transformed concurrently, up to the given limit.  A limit of `n` limits
    /// this stage to `n` concurrent operations at any given time.
    ///
    /// Encoding and compression are handled internally, deferring to the builder at the necessary
    /// checkpoints for adjusting the event before encoding/compression, as well as generating the
    /// correct request object with the result of encoding/compressing the events.
    fn request_builder<B>(
        self,
        limit: NonZeroUsize,
        builder: B,
    ) -> ConcurrentMap<Self, Result<B::Request, B::Error>>
    where
        Self: Sized,
        Self::Item: Send + 'static,
        B: RequestBuilder<<Self as Stream>::Item> + Send + Sync + 'static,
        B::Error: Send,
        B::Request: Send,
    {
        let builder = Arc::new(builder);

        // The future passed into the concurrent map is spawned in a tokio thread so we must preserve
        // the span context in order to propagate the sink's automatic tags.
        let span = Arc::new(Span::current());

        self.concurrent_map(limit, move |input| {
            let builder = Arc::clone(&builder);
            let span = Arc::clone(&span);

            Box::pin(async move {
                let _entered = span.enter();

                // Split the input into metadata and events.
                let (metadata, request_metadata_builder, events) = builder.split_input(input);

                // Encode is synchronous, so the thread CPU delta is this component's encode cost.
                let cpu_before = thread_cpu_time();
                let payload = builder.encode_events(events)?;
                if let Some(cpu_time) = cpu_before
                    .and_then(|before| thread_cpu_time().map(|after| after.saturating_sub(before)))
                {
                    emit!(ComponentEncodeCpuTime { cpu_time });
                }

                // Note: it would be nice for the RequestMetadataBuilder to build be created from the
                // events here, and not need to be required by split_input(). But this then requires
                // each Event type to implement Serialize, and that causes conflicts with the Serialize
                // implementation for EstimatedJsonEncodedSizeOf.

                // Build the request metadata.
                let request_metadata = request_metadata_builder.build(&payload);

                // Now build the actual request.
                Ok(builder.build_request(metadata, request_metadata, payload))
            })
        })
    }

    /// Constructs a [`Stream`] which transforms the input into a number of requests suitable for
    /// sending to downstream services.
    ///
    /// Unlike `request_builder`, which depends on the `RequestBuilder` trait,
    /// `incremental_request_builder` depends on the `IncrementalRequestBuilder` trait, which is
    /// designed specifically for sinks that have more stringent requirements around the generated
    /// requests.
    ///
    /// As an example, the normal `request_builder` doesn't allow for a batch of input events to be
    /// split up: all events must be split at the beginning, encoded separately (and all together),
    /// and then reassembled into the request.  If the encoding of these events caused a payload to
    /// be generated that was, say, too large, you would have to back out the operation entirely by
    /// failing the batch.
    ///
    /// With `incremental_request_builder`, the builder is given all of the events in a single shot,
    /// and can generate multiple payloads.  This is the maximally flexible approach to encoding,
    /// but means that the trait doesn't provide any default methods like `RequestBuilder` does.
    ///
    /// Each input is transformed serially.
    ///
    /// Encoding and compression are handled internally, deferring to the builder at the necessary
    /// checkpoints for adjusting the event before encoding/compression, as well as generating the
    /// correct request object with the result of encoding/compressing the events.
    fn incremental_request_builder<B>(
        self,
        mut builder: B,
    ) -> Map<Self, Box<dyn FnMut(Self::Item) -> Vec<Result<B::Request, B::Error>> + Send + Sync>>
    where
        Self: Sized,
        Self::Item: Send + 'static,
        B: IncrementalRequestBuilder<<Self as Stream>::Item> + Send + Sync + 'static,
        B::Error: Send,
        B::Request: Send,
    {
        self.map(Box::new(move |input| {
            builder
                .encode_events_incremental(input)
                .into_iter()
                .map(|result| {
                    result.map(|(metadata, payload)| builder.build_request(metadata, payload))
                })
                .collect()
        }))
    }

    /// Normalizes a stream of [`Metric`] events with the provided normalizer.
    ///
    /// An implementation of [`MetricNormalize`] is used to either drop metrics which cannot be
    /// supported by the sink, or to modify them.  Such modifications typically include converting
    /// absolute metrics to incremental metrics by tracking the change over time for a particular
    /// series, or emitting absolute metrics based on incremental updates.
    fn normalized<N>(self, normalizer: N) -> Normalizer<Self, N>
    where
        Self: Stream<Item = Metric> + Unpin + Sized,
        N: MetricNormalize,
    {
        Normalizer::new(self, normalizer)
    }

    /// Normalizes a stream of [`Metric`] events with a default normalizer.
    ///
    /// An implementation of [`MetricNormalize`] is used to either drop metrics which cannot be
    /// supported by the sink, or to modify them.  Such modifications typically include converting
    /// absolute metrics to incremental metrics by tracking the change over time for a particular
    /// series, or emitting absolute metrics based on incremental updates.
    fn normalized_with_default<N>(self) -> Normalizer<Self, N>
    where
        Self: Stream<Item = Metric> + Unpin + Sized,
        N: MetricNormalize + Default,
    {
        Normalizer::new(self, N::default())
    }

    /// Normalizes a stream of [`Metric`] events with a normalizer and an optional TTL.
    fn normalized_with_ttl<N>(self, maybe_ttl_secs: Option<f64>) -> Normalizer<Self, N>
    where
        Self: Stream<Item = Metric> + Unpin + Sized,
        N: MetricNormalize + Default,
    {
        match maybe_ttl_secs {
            None => Normalizer::new(self, N::default()),
            Some(ttl) => {
                Normalizer::new_with_ttl(self, N::default(), Duration::from_secs(ttl as u64))
            }
        }
    }

    /// Creates a [`Driver`] that uses the configured event stream as the input to the given
    /// service.
    ///
    /// This is typically a terminal step in building a sink, bridging the gap from the processing
    /// that must be performed by Vector (in the stream) to the underlying sink itself (the
    /// service).
    fn into_driver<Svc>(self, service: Svc) -> Driver<Self, Svc>
    where
        Self: Sized,
        Self::Item: Finalizable,
        Svc: Service<Self::Item>,
        Svc::Error: fmt::Debug + 'static,
        Svc::Future: Send + 'static,
        Svc::Response: DriverResponse,
    {
        Driver::new(self, service)
    }
}

#[pin_project]
pub struct UnwrapInfallible<St> {
    #[pin]
    st: St,
}

impl<St, T> Stream for UnwrapInfallible<St>
where
    St: Stream<Item = Result<T, Infallible>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.st
            .poll_next(cx)
            .map(|maybe| maybe.map(|result| result.unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use std::{io, num::NonZeroUsize};

    use bytes::Bytes;
    use futures_util::{StreamExt, stream};

    use crate::metrics::{Controller, init_test};
    use crate::sinks::prelude::*;

    // No-op encoder and request builder, used only to drive the `request_builder` combinator.
    struct TestEncoder;

    impl encoding::Encoder<Vec<Event>> for TestEncoder {
        fn encode_input(
            &self,
            _input: Vec<Event>,
            _writer: &mut dyn io::Write,
        ) -> io::Result<(usize, GroupedCountByteSize)> {
            Ok((0, telemetry().create_request_count_byte_size()))
        }
    }

    struct TestRequestBuilder {
        encoder: TestEncoder,
    }

    impl RequestBuilder<Vec<Event>> for TestRequestBuilder {
        type Metadata = ();
        type Events = Vec<Event>;
        type Encoder = TestEncoder;
        type Payload = Bytes;
        type Request = ();
        type Error = io::Error;

        fn compression(&self) -> Compression {
            Compression::None
        }

        fn encoder(&self) -> &Self::Encoder {
            &self.encoder
        }

        fn split_input(
            &self,
            input: Vec<Event>,
        ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
            ((), RequestMetadataBuilder::from_events(&input), input)
        }

        fn build_request(
            &self,
            _metadata: Self::Metadata,
            _request_metadata: RequestMetadata,
            _payload: EncodeResult<Self::Payload>,
        ) -> Self::Request {
        }
    }

    // The `request_builder` combinator emits `component_encode_cpu_seconds` when it builds a request.
    #[tokio::test]
    async fn request_builder_emits_encode_cpu_metric() {
        init_test();

        let batch = vec![Event::Log(LogEvent::from("hello"))];
        let _requests: Vec<_> = stream::iter(vec![batch])
            .request_builder(
                NonZeroUsize::new(1).unwrap(),
                TestRequestBuilder {
                    encoder: TestEncoder,
                },
            )
            .collect()
            .await;

        let metrics = Controller::get().unwrap().capture_metrics();
        assert!(
            metrics
                .iter()
                .any(|m| m.name() == "component_encode_cpu_seconds"),
            "request_builder did not emit component_encode_cpu_seconds"
        );
    }
}
