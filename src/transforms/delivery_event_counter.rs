use vector_common::internal_event::vector_event::delivery_event::emit_delivery_counters;
use vector_lib::{
    config::clone_input_definitions, configurable::configurable_component,
    event::event_log::generate_count_map,
};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::Event,
    schema::Definition,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

const SINK_INPUT_ACCEPTED: &str = "VECTOR_SINK_INPUT_ACCEPTED";

/// Configuration for the `delivery_event_counter` transform.
#[configurable_component(transform(
    "delivery_event_counter",
    "Count delivery events admitted at a terminal sink boundary without changing them."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct DeliveryEventCounterConfig {}

impl GenerateConfig for DeliveryEventCounterConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {}).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "delivery_event_counter")]
impl TransformConfig for DeliveryEventCounterConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(DeliveryEventCounter))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _context: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug)]
struct DeliveryEventCounter;

impl FunctionTransform for DeliveryEventCounter {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let count_map = generate_count_map(std::slice::from_ref(&event), true);
        emit_delivery_counters(count_map.values(), SINK_INPUT_ACCEPTED);
        output.push(event);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;
    use crate::event::LogEvent;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<DeliveryEventCounterConfig>();
    }

    #[test]
    fn counts_preserved_lineage_and_passes_the_event_through() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut log = LogEvent::from("combined message");
        log.metadata_mut().set_delivery_event_count(4);
        let event = Event::Log(log);
        let mut output = OutputBuffer::with_capacity(1);

        metrics::with_local_recorder(&recorder, || {
            DeliveryEventCounter.transform(&mut output, event.clone());
        });

        assert_eq!(output.into_events().collect::<Vec<_>>(), vec![event]);
        let counters = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "delivery_events_total"
                    && key.key().labels().any(|label| {
                        label.key() == "delivery_event_type" && label.value() == SINK_INPUT_ACCEPTED
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(counters.len(), 2);
        for (key, _, _, value) in counters {
            assert_eq!(value, DebugValue::Counter(4));
            let labels = key
                .key()
                .labels()
                .map(|label| (label.key(), label.value()))
                .collect::<HashMap<_, _>>();
            assert_eq!(
                labels.get("delivery_event_type"),
                Some(&SINK_INPUT_ACCEPTED)
            );
        }
    }
}
