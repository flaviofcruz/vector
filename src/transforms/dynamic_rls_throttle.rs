use std::pin::Pin;

use async_stream::stream;
use futures::{Stream, StreamExt};
use vector_lib::{config::clone_input_definitions, configurable::configurable_component};

use crate::{
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    event::Event,
    schema,
    transforms::{TaskTransform, Transform},
};

/// Configuration for the `dynamic_rls_throttle` transform.
///
/// Skeleton: this transform currently forwards every event unchanged. It exists so
/// the component is registered in the binary and can be wired into the service-log
/// pipeline now. The throttling logic — per-`(topic, system)` counting, a background
/// task that reports counts to the sidecar and receives the over-limit set from the
/// rate-limit service (RLSv2), and dropping over-quota events — lands in a follow-up.
/// The config fields that logic needs (sidecar endpoint, report interval, staleness
/// budget, key fields) are added here then; for now the config is intentionally empty.
#[configurable_component(transform(
    "dynamic_rls_throttle",
    "Drop logs whose topic/system is over its online quota, as decided by the rate-limit service (skeleton: currently a no-op)."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct DynamicRlsThrottleConfig {}

impl_generate_config_from_default!(DynamicRlsThrottleConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "dynamic_rls_throttle")]
impl TransformConfig for DynamicRlsThrottleConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::event_task(DynamicRlsThrottle))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        // The event is not modified, so the definition is passed through as-is.
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }
}

#[derive(Clone)]
pub struct DynamicRlsThrottle;

impl TaskTransform<Event> for DynamicRlsThrottle {
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        // TODO(follow-up): hold the over-limit set + per-`(topic, system)` counts,
        // spawn a background reporter that POSTs counts to the sidecar and atomically
        // swaps in the over-limit set returned from the rate-limit service, then drop
        // events whose key is over quota. Skeleton behaviour: forward every event
        // unchanged.
        Box::pin(stream! {
            while let Some(event) = input_rx.next().await {
                yield event;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DynamicRlsThrottleConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<DynamicRlsThrottleConfig>();
    }
}
