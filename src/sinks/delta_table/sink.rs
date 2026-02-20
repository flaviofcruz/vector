//! Azure Delta table sink event processing pipeline

use super::request_builder::AzureDeltaRequestOptions;
use super::service::{AzureDeltaRetryLogic, AzureDeltaService};
use crate::sinks::prelude::*;
use crate::sinks::util::service::Svc;
use futures::future;

/// Azure Delta table sink that processes events through batching and service layers
pub struct AzureDeltaSink {
    service: Svc<AzureDeltaService, AzureDeltaRetryLogic>,
    request_options: AzureDeltaRequestOptions,
    batch_settings: BatcherSettings,
}

impl AzureDeltaSink {
    pub const fn new(
        service: Svc<AzureDeltaService, AzureDeltaRetryLogic>,
        request_options: AzureDeltaRequestOptions,
        batch_settings: BatcherSettings,
    ) -> Self {
        Self {
            service,
            request_options,
            batch_settings,
        }
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for AzureDeltaSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let request_options = self.request_options;

        // Create the stream pipeline
        let stream = input
            .batched(self.batch_settings.as_byte_size_config())
            .request_builder(
                default_request_builder_concurrency_limit(),
                request_options,
            )
            .map(|request| {
                match request {
                    Err(error) => {
                        // Log detailed error information for debugging
                        error!(
                            message = "Request building failed - shutting down sink to prevent data loss",
                            error = ?error,
                            error_type = std::any::type_name_of_val(&error)
                        );
                        emit!(SinkRequestBuildError { error });
                        // Fail immediately - request builder errors are typically permanent
                        // for the given batch and retrying won't help
                        Err(())
                    }
                    Ok(req) => Ok(req),
                }
            })
            // Properly handle errors while maintaining Vector's required Result<(), ()> signature
            .scan(true, |should_continue, result| {
                match result {
                    Ok(request) => {
                        if *should_continue {
                            future::ready(Some(Ok(request)))
                        } else {
                            future::ready(None)
                        }
                    }
                    Err(()) => {
                        *should_continue = false;
                        future::ready(Some(Err(())))
                    }
                }
            })
            .take_while(|result| future::ready(result.is_ok()))
            .map(|result| result.unwrap()); // Safe because we filtered out errors

        let driver = stream.into_driver(self.service);
        driver.run().await
    }
}
