use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use vector_lib::{
    buffers::{
        config::MemoryBufferSize,
        topology::channel::{LimitedReceiver, limited},
    },
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    event::EventContainer,
    schema::Definition,
    source::Source,
    source_sender::SourceSenderItem,
};

use crate::config::{SourceConfig, SourceContext};

/// Configuration for the `test_deferred` source.
///
/// Identical to `BasicSourceConfig` but with `has_deferred_shutdown() = true`,
/// simulating internal sources like `internal_logs` and `internal_metrics`.
#[configurable_component(source("test_deferred", "Test (deferred shutdown)."))]
#[derive(Clone, Debug)]
#[serde(default)]
pub struct DeferredSourceConfig {
    #[serde(skip)]
    receiver: Arc<Mutex<Option<LimitedReceiver<SourceSenderItem>>>>,

    #[serde(skip)]
    event_counter: Option<Arc<AtomicUsize>>,

    #[serde(skip)]
    force_shutdown: bool,

    /// Meaningless field that only exists for triggering config diffs during topology reloading.
    data: Option<String>,
}

impl Default for DeferredSourceConfig {
    fn default() -> Self {
        let limit = MemoryBufferSize::MaxEvents(NonZeroUsize::new(1000).unwrap());
        let (_, receiver) = limited(limit, None, None);
        Self {
            receiver: Arc::new(Mutex::new(Some(receiver))),
            event_counter: None,
            force_shutdown: false,
            data: None,
        }
    }
}

impl_generate_config_from_default!(DeferredSourceConfig);

impl DeferredSourceConfig {
    pub fn new(receiver: LimitedReceiver<SourceSenderItem>) -> Self {
        Self {
            receiver: Arc::new(Mutex::new(Some(receiver))),
            event_counter: None,
            force_shutdown: false,
            data: None,
        }
    }

    pub fn new_with_event_counter(
        receiver: LimitedReceiver<SourceSenderItem>,
        event_counter: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            receiver: Arc::new(Mutex::new(Some(receiver))),
            event_counter: Some(event_counter),
            force_shutdown: false,
            data: None,
        }
    }

    pub fn set_force_shutdown(&mut self, force_shutdown: bool) {
        self.force_shutdown = force_shutdown;
    }
}

#[async_trait]
#[typetag::serde(name = "test_deferred")]
impl SourceConfig for DeferredSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let wrapped = Arc::clone(&self.receiver);
        let event_counter = self.event_counter.clone();
        let mut recv = wrapped.lock().unwrap().take().unwrap();
        let shutdown1 = cx.shutdown.clone();
        let shutdown2 = cx.shutdown;
        let mut out = cx.out;
        let force_shutdown = self.force_shutdown;

        Ok(Box::pin(async move {
            tokio::pin!(shutdown1);
            tokio::pin!(shutdown2);

            loop {
                tokio::select! {
                    biased;

                    _ = &mut shutdown1, if force_shutdown => break,

                    result = recv.next() => match result {
                        Some(array) => {
                            if let Some(counter) = &event_counter {
                                counter.fetch_add(array.len(), Ordering::Relaxed);
                            }

                            if let Err(e) = out.send_event(array).await {
                                error!(message = "Error sending in deferred source..", %e);
                                return Err(())
                            }
                        }
                        None => break,
                    },

                    _ = &mut shutdown2, if !force_shutdown => break,
                }
            }

            info!("Deferred source finished sending.");
            Ok(())
        }))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_maybe_logs(
            DataType::all_bits(),
            Definition::default_legacy_namespace(),
        )]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }

    fn has_deferred_shutdown(&self) -> bool {
        true
    }
}
