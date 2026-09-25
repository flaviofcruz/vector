use std::time::Duration;

use governor::clock;
use serde_with::serde_as;
use vector_lib::{config::clone_input_definitions, configurable::configurable_component};

use super::transform::Throttle;
use crate::{
    conditions::AnyCondition,
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    schema,
    template::Template,
    transforms::Transform,
};

/// Configuration of internal metrics for the Throttle transform.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ThrottleInternalMetricsConfig {
    /// Whether or not to emit the `events_discarded_total` internal metric with the `key` tag.
    ///
    /// If true, the counter will be incremented for each discarded event, including the key value
    /// associated with the discarded event. If false, the counter will not be emitted. Instead, the
    /// number of discarded events can be seen through the `component_discarded_events_total` internal
    /// metric.
    ///
    /// Note that this defaults to false because the `key` tag has potentially unbounded cardinality.
    /// Only set this to true if you know that the number of unique keys is bounded.
    #[serde(default)]
    pub emit_events_discarded_per_key: bool,
}

/// Approved intentional-drop accounting emitted only when this throttle rejects an event.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThrottleDeliveryPathIntentionalDropConfig {
    /// Source-configured paths affected by this throttle's policy decision.
    pub paths: Vec<String>,
    /// Bounded policy reason attached to the intentional-drop counter.
    pub reason: String,
}

/// Configuration for the `throttle` transform.
#[serde_as]
#[configurable_component(transform("throttle", "Rate limit logs passing through a topology."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ThrottleConfig {
    /// The number of events allowed for a given bucket per configured `window_secs`.
    ///
    /// Each unique key has its own `threshold`.
    pub threshold: u32,

    /// The time window in which the configured `threshold` is applied, in seconds.
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[configurable(metadata(docs::human_name = "Time Window"))]
    pub window_secs: Duration,

    /// The value to group events into separate buckets to be rate limited independently.
    ///
    /// If left unspecified, or if the event doesn't have `key_field`, then the event is not rate
    /// limited separately.
    #[configurable(metadata(docs::examples = "{{ message }}", docs::examples = "{{ hostname }}",))]
    pub key_field: Option<Template>,

    /// A logical condition used to exclude events from sampling.
    pub exclude: Option<AnyCondition>,

    #[configurable(derived)]
    #[serde(default)]
    pub internal_metrics: ThrottleInternalMetricsConfig,

    /// The event status to set when dropping event due to throttling. One of (dropped, errored, rejected)
    #[serde(default = "default_throttled_event_status")]
    pub throttled_event_status: String,

    /// Optional approved intentional-drop accounting for source-created delivery paths.
    /// Does not change throttling behavior or count events that pass through.
    pub delivery_path_intentional_drop: Option<ThrottleDeliveryPathIntentionalDropConfig>,
}

impl_generate_config_from_default!(ThrottleConfig);

pub fn default_throttled_event_status() -> String {
    "dropped".to_string()
}

#[async_trait::async_trait]
#[typetag::serde(name = "throttle")]
impl TransformConfig for ThrottleConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        Throttle::new(self, context, clock::MonotonicClock).map(Transform::event_task)
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        // The event is not modified, so the definition is passed through as-is
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::ThrottleConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ThrottleConfig>();
    }

    #[test]
    fn intentional_drop_config_round_trips() {
        let intentional_drop = serde_json::json!({
            "paths": ["online"],
            "reason": "policy_rate_limit",
        });
        let config: ThrottleConfig = serde_json::from_value(serde_json::json!({
            "threshold": 10,
            "window_secs": 1,
            "delivery_path_intentional_drop": intentional_drop,
        }))
        .unwrap();
        let accounting = config.delivery_path_intentional_drop.as_ref().unwrap();
        assert_eq!(accounting.paths, ["online"]);
        assert_eq!(accounting.reason, "policy_rate_limit");
        assert_eq!(
            serde_json::to_value(config).unwrap()["delivery_path_intentional_drop"],
            intentional_drop,
        );
    }
}
