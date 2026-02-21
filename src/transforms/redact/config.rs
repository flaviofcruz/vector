use std::path::PathBuf;

use vector_lib::config::clone_input_definitions;
use vector_lib::configurable::configurable_component;

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    schema,
    transforms::Transform,
};

use super::transform::Redact;

/// Configuration for the `redact` transform.
#[configurable_component(transform(
    "redact",
    "Redact secrets and sensitive data from log fields using configurable regex patterns."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct RedactConfig {
    /// List of field names to apply redaction to.
    ///
    /// Each field specified will have the redaction rules applied.
    /// Fields are specified using Vector's field notation (e.g., "message", "body", "nested.field").
    #[configurable(metadata(docs::examples = "message", docs::examples = "body"))]
    pub fields: Vec<String>,

    /// Path to the TOML file containing redaction rules.
    ///
    /// The rules file should follow the format used by the Databricks redactor,
    /// with a `[patterns]` section containing named rules with pattern, replacement,
    /// and optional validation configurations.
    #[configurable(metadata(docs::examples = "/etc/vector/redaction-rules.toml"))]
    pub rules_file: PathBuf,

    /// Whether to validate examples in the rules file at startup.
    ///
    /// When enabled, the component will parse any examples defined in the rules file
    /// and verify that they redact correctly according to their expected output.
    /// This helps catch misconfigured rules early but adds startup time.
    #[serde(default)]
    #[configurable(metadata(docs::examples = true, docs::examples = false))]
    pub validate_examples: bool,
}

impl GenerateConfig for RedactConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"fields = ["message"]
rules_file = "/etc/vector/redaction-rules.toml""#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "redact")]
impl TransformConfig for RedactConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(Redact::new(self)?))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _context: &TransformContext,
        input_definitions: &[(OutputId, schema::Definition)],
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
