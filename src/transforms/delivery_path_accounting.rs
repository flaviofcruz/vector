use vector_common::internal_event::vector_event::delivery_event::DeliveryEventStage;
use vector_lib::{config::clone_input_definitions, configurable::configurable_component};

use crate::{
    conditions::{AnyCondition, Condition},
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::Event,
    schema::Definition,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

/// Operation applied to the delivery requirements carried by an event.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPathOperation {
    /// Retain and select this branch's primary paths for sink delivery credit.
    Select,
    /// Remove all primary requirements from a supplemental copy.
    Supplemental,
    /// Resolve paths through an approved intentional policy exclusion.
    IntentionalDrop,
}

/// Configuration for explicit delivery-path branch accounting.
///
/// Drops in other transforms do not automatically earn intentional-drop credit. Without an
/// explicit approved exclusion, an enrolled event's READ remains unmatched by delivery.
#[configurable_component(transform(
    "delivery_path_accounting",
    "Apply an explicit routing or disposition decision to source-created delivery requirements."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPathAccountingConfig {
    /// Accounting operation to apply.
    pub operation: DeliveryPathOperation,
    /// One opaque source-configured path name. Mutually exclusive with `paths`.
    #[serde(default)]
    pub path: Option<String>,
    /// Source-configured paths sharing this branch or policy decision.
    #[serde(default)]
    pub paths: Vec<String>,
    /// Bounded policy reason required by `intentional_drop`.
    #[serde(default)]
    pub reason: Option<String>,
    /// Optional predicate limiting which events receive the accounting operation.
    #[serde(default)]
    #[configurable(derived)]
    pub condition: Option<AnyCondition>,
}

impl GenerateConfig for DeliveryPathAccountingConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            operation: DeliveryPathOperation::Select,
            path: Some("primary".to_owned()),
            paths: Vec::new(),
            reason: None,
            condition: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "delivery_path_accounting")]
impl TransformConfig for DeliveryPathAccountingConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        if self.path.is_some() && !self.paths.is_empty() {
            return Err("delivery path accounting accepts either path or paths, not both".into());
        }
        let paths = self
            .path
            .iter()
            .cloned()
            .chain(self.paths.iter().cloned())
            .collect::<Vec<_>>();
        let paths = match self.operation {
            DeliveryPathOperation::Supplemental => {
                if !paths.is_empty() || self.reason.is_some() {
                    return Err("supplemental delivery accounting accepts no path or reason".into());
                }
                Vec::new()
            }
            DeliveryPathOperation::IntentionalDrop => {
                required_nonempty(&self.reason, "reason")?;
                paths
            }
            DeliveryPathOperation::Select => {
                if self.reason.is_some() {
                    return Err("only intentional_drop accepts a reason".into());
                }
                paths
            }
        };
        if !matches!(self.operation, DeliveryPathOperation::Supplemental)
            && (paths.is_empty() || paths.iter().any(String::is_empty))
        {
            return Err("delivery path accounting requires non-empty paths".into());
        }
        Ok(Transform::function(DeliveryPathAccountingTransform {
            operation: self.operation,
            paths,
            reason: self.reason.clone(),
            condition: self
                .condition
                .as_ref()
                .map(|condition| {
                    condition.build(&context.enrichment_tables, &context.metrics_storage)
                })
                .transpose()?,
        }))
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

fn required_nonempty(value: &Option<String>, name: &str) -> crate::Result<String> {
    value
        .as_ref()
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| format!("delivery path accounting requires a non-empty {name}").into())
}

#[derive(Clone, Debug)]
struct DeliveryPathAccountingTransform {
    operation: DeliveryPathOperation,
    paths: Vec<String>,
    reason: Option<String>,
    condition: Option<Condition>,
}

impl FunctionTransform for DeliveryPathAccountingTransform {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        if self
            .condition
            .as_ref()
            .is_some_and(|condition| !condition.check(event.clone()).0)
        {
            output.push(event);
            return;
        }
        if let Some(accounting) = event.metadata_mut().delivery_path_accounting_mut() {
            match self.operation {
                DeliveryPathOperation::Select => {
                    accounting.select_paths(&self.paths);
                }
                DeliveryPathOperation::Supplemental => accounting.clear_paths(),
                DeliveryPathOperation::IntentionalDrop => {
                    let reason = self.reason.as_deref().expect("validated reason");
                    for path in &self.paths {
                        for count in accounting.take_path(path) {
                            count.emit(DeliveryEventStage::IntentionalDrop { reason });
                        }
                    }
                }
            }
        }
        output.push(event);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use vector_common::internal_event::vector_event::delivery_path::{
        DeliveryPathAccounting, DeliverySourceLabels,
    };

    use super::*;
    use crate::event::LogEvent;

    fn event() -> Event {
        let mut event = LogEvent::from("message");
        event
            .metadata_mut()
            .set_delivery_path_accounting(Some(DeliveryPathAccounting::at_read(
                DeliverySourceLabels {
                    process_generation_id: "source-process".to_owned(),
                    time_parity: 3_600_000,
                    topic: "service-log".to_owned(),
                    delivery_method: "file".to_owned(),
                    service_system: "service".to_owned(),
                    source_pod_id: "pod".to_owned(),
                },
                ["blob".to_owned(), "online".to_owned()],
            )));
        event.into()
    }

    fn apply(operation: DeliveryPathOperation, path: Option<&str>, reason: Option<&str>) -> Event {
        let mut transform = DeliveryPathAccountingTransform {
            operation,
            paths: path.map(str::to_owned).into_iter().collect(),
            reason: reason.map(str::to_owned),
            condition: None,
        };
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event());
        output.into_events().next().expect("one output")
    }

    #[test]
    fn select_and_supplemental_limit_delivery_credit() {
        let selected = apply(DeliveryPathOperation::Select, Some("online"), None);
        let counts = &selected
            .metadata()
            .delivery_path_accounting()
            .expect("accounting")
            .counts;
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].path, "online");
        assert!(counts[0].selected);

        let supplemental = apply(DeliveryPathOperation::Supplemental, None, None);
        assert!(
            supplemental
                .metadata()
                .delivery_path_accounting()
                .expect("configured accounting")
                .counts
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delivery_path_shared_branch_selects_only_enrolled_paths() {
        let config: DeliveryPathAccountingConfig = serde_json::from_value(serde_json::json!({
            "operation": "select",
            "paths": ["online", "other-online"],
        }))
        .unwrap();
        let Transform::Function(mut transform) =
            config.build(&TransformContext::default()).await.unwrap()
        else {
            panic!("expected function transform");
        };
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event());
        let output = output.into_events().next().unwrap();
        let accounting = output.metadata().delivery_path_accounting().unwrap();
        assert_eq!(accounting.counts.len(), 1);
        assert_eq!(accounting.counts[0].path, "online");
        assert!(accounting.counts[0].selected);
    }

    #[test]
    fn intentional_drop_emits_resolution_and_removes_only_that_path() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let resolved = metrics::with_local_recorder(&recorder, || {
            apply(
                DeliveryPathOperation::IntentionalDrop,
                Some("online"),
                Some("lite_content_excluded"),
            )
        });
        let counts = &resolved
            .metadata()
            .delivery_path_accounting()
            .expect("accounting")
            .counts;
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].path, "blob");

        let counters = snapshotter.snapshot().into_vec();
        assert_eq!(counters.len(), 2);
        for (key, _, _, value) in counters {
            let labels = key
                .key()
                .labels()
                .map(|label| (label.key(), label.value()))
                .collect::<HashMap<_, _>>();
            assert_eq!(value, DebugValue::Counter(1));
            assert_eq!(
                labels["delivery_event_type"],
                "VECTOR_DELIVERY_INTENTIONAL_DROP"
            );
            assert_eq!(labels["delivery_path"], "online");
            assert_eq!(labels["reason"], "lite_content_excluded");
        }
    }

    #[tokio::test]
    async fn delivery_path_conditional_disposition_resolves_only_excluded_enrolled_events() {
        for eligible in [false, true] {
            let config: DeliveryPathAccountingConfig = serde_json::from_value(serde_json::json!({
                "operation": "intentional_drop",
                "paths": ["online"],
                "reason": "content_filter",
                "condition": "includes(array(%delivery_path_accounting.paths) ?? [], \"online\") && .is_fast_eligible != true",
            })).unwrap();
            let Transform::Function(mut transform) =
                config.build(&TransformContext::default()).await.unwrap()
            else {
                panic!("expected function transform")
            };
            let mut input = event();
            input.as_mut_log().insert("is_fast_eligible", eligible);
            input.metadata_mut().value_mut().insert(
                vrl::path!("delivery_path_accounting"),
                vrl::value!({"paths": ["blob", "online"]}),
            );
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let mut output = OutputBuffer::with_capacity(1);
            metrics::with_local_recorder(&recorder, || transform.transform(&mut output, input));
            let output = output.into_events().next().unwrap();
            let accounting = output.metadata().delivery_path_accounting().unwrap();
            assert!(accounting.has_path("blob"));
            assert_eq!(accounting.has_path("online"), eligible);
            let counters = snapshotter.snapshot().into_vec();
            assert_eq!(counters.len(), if eligible { 0 } else { 2 });
            for (_, _, _, value) in counters {
                assert_eq!(value, DebugValue::Counter(1));
            }
        }
    }
}
