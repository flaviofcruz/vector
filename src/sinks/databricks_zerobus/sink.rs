//! The main Zerobus sink implementation.

use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::stream::BoxStream;

use vector_lib::codecs::encoding::{BatchEncoder, BatchOutput};
use vector_lib::event::EventStatus;
use vector_lib::finalization::Finalizable;

use crate::sinks::prelude::*;
use crate::sinks::util::metadata::RequestMetadataBuilder;
use crate::sinks::util::request_builder::default_request_builder_concurrency_limit;
use crate::sinks::util::{RealtimeSizeBasedDefaultBatchSettings, TowerRequestSettings};

use super::service::{ZerobusPayload, ZerobusRequest, ZerobusRetryLogic, ZerobusService};
#[cfg(feature = "codecs-arrow")]
use super::wire_to_arrow::WireToArrowEncoder;

/// The main Zerobus sink.
pub struct ZerobusSink {
    service: ZerobusService,
    request_limits: TowerRequestSettings,
    batch_settings: BatcherSettings,
    encoder: BatchEncoder,
    /// Optional streaming wire-to-Arrow encoder used when events carry
    /// original proto wire bytes in `wire_to_arrow::WIRE_BYTES_FIELD`.
    /// Falls back to the generic `encoder` on a per-batch basis.
    #[cfg(feature = "codecs-arrow")]
    wire_encoder: Option<Arc<WireToArrowEncoder>>,
}

impl ZerobusSink {
    pub fn new(
        service: ZerobusService,
        request_limits: TowerRequestSettings,
        batch_config: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,
        encoder: BatchEncoder,
        #[cfg(feature = "codecs-arrow")] wire_encoder: Option<Arc<WireToArrowEncoder>>,
    ) -> Result<Self, crate::Error> {
        let batch_settings = batch_config.into_batcher_settings()?;

        Ok(Self {
            service,
            request_limits,
            batch_settings,
            encoder,
            #[cfg(feature = "codecs-arrow")]
            wire_encoder,
        })
    }

    fn encode_batch(
        encoder: &BatchEncoder,
        #[cfg(feature = "codecs-arrow")] wire_encoder: Option<&WireToArrowEncoder>,
        mut events: Vec<Event>,
    ) -> Result<ZerobusRequest, String> {
        let finalizers = events.take_finalizers();
        let metadata_builder = RequestMetadataBuilder::from_events(&events);

        // Fast path: wire-to-Arrow encoder, used when every event in the batch
        // carries original proto wire bytes. Any miss (field absent, wrong
        // type, or encoder error) falls back to the generic path below.
        #[cfg(feature = "codecs-arrow")]
        if let Some(wire_enc) = wire_encoder {
            if let Some(wire_bytes) = WireToArrowEncoder::try_extract_wire_bytes(&events) {
                match wire_enc.encode_batch(&wire_bytes) {
                    Ok(record_batch) => {
                        let byte_size = record_batch.get_array_memory_size();
                        let request_size =
                            NonZeroUsize::new(byte_size).unwrap_or(NonZeroUsize::MIN);
                        let metadata = metadata_builder.with_request_size(request_size);
                        return Ok(ZerobusRequest {
                            payload: ZerobusPayload::Arrow(record_batch),
                            metadata,
                            finalizers,
                        });
                    }
                    Err(e) => {
                        // TODO: rate-limit. Falling back is correct, but we
                        // want visibility into how often it happens so we can
                        // drive the rate toward zero as VRL upstream catches up.
                        warn!(
                            message = "wire-to-Arrow encoding failed; falling back to generic path",
                            error = %e,
                        );
                    }
                }
            } else {
                // Field absent on at least one event — fall through to the
                // generic path. Same TODO re: rate-limited metric.
                warn!(
                    message = "wire-to-Arrow field missing on at least one event in batch; \
                               falling back to generic path"
                );
            }
        }

        let batch_output = match encoder.encode_batch(&events) {
            Ok(output) => output,
            Err(e) => {
                finalizers.update_status(EventStatus::Rejected);
                return Err(format!("Failed to encode batch: {}", e));
            }
        };

        let (payload, byte_size) = match batch_output {
            BatchOutput::Records(records) => {
                let size = records.iter().map(|r| r.len()).sum::<usize>();
                (ZerobusPayload::Records(records), size)
            }
            #[cfg(feature = "codecs-arrow")]
            BatchOutput::Arrow(record_batch) => {
                let size = record_batch.get_array_memory_size();
                (ZerobusPayload::Arrow(record_batch), size)
            }
            #[allow(unreachable_patterns)]
            _ => {
                finalizers.update_status(EventStatus::Rejected);
                return Err("Unexpected batch output type".to_string());
            }
        };

        let request_size = NonZeroUsize::new(byte_size).unwrap_or(NonZeroUsize::MIN);
        let metadata = metadata_builder.with_request_size(request_size);

        Ok(ZerobusRequest {
            payload,
            metadata,
            finalizers,
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let encoder = Arc::new(self.encoder.clone());
        #[cfg(feature = "codecs-arrow")]
        let wire_encoder = self.wire_encoder.clone();

        let result = {
            let tower_service = ServiceBuilder::new()
                .settings(self.request_limits, ZerobusRetryLogic)
                .service(self.service.clone());

            input
                .batched(self.batch_settings.as_byte_size_config())
                .concurrent_map(default_request_builder_concurrency_limit(), move |events| {
                    let encoder = Arc::clone(&encoder);
                    #[cfg(feature = "codecs-arrow")]
                    let wire_encoder = wire_encoder.clone();
                    Box::pin(async move {
                        Self::encode_batch(
                            &encoder,
                            #[cfg(feature = "codecs-arrow")]
                            wire_encoder.as_deref(),
                            events,
                        )
                    })
                })
                .filter_map(|result| async move {
                    match result {
                        Err(error) => {
                            emit!(SinkRequestBuildError { error });
                            None
                        }
                        Ok(req) => Some(req),
                    }
                })
                .into_driver(tower_service)
                .run()
                .await
        };

        self.service.close_stream().await;

        result
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for ZerobusSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
