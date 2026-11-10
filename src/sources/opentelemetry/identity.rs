//! Certificate identity providers used after transport-level client verification.

use vector_lib::lookup::OwnedValuePath;
use vrl::value::Value;

use crate::databricks_uwi::UwiIdentityConfig;

/// Client certificate identity policy and metadata provider.
#[vector_lib::configurable::configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[configurable(metadata(docs::enum_tag_description = "The client certificate identity provider."))]
pub enum ClientIdentityConfig {
    /// Authenticate assigned Databricks serverless workloads using signed UWI attributes.
    DatabricksUwi(UwiIdentityConfig),
}

pub(super) struct VerifiedClientIdentity {
    pub metadata_path: OwnedValuePath,
    pub metadata: Value,
}

impl ClientIdentityConfig {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::DatabricksUwi(config) => config.validate(),
        }
    }

    pub(super) fn metadata_path(&self) -> OwnedValuePath {
        match self {
            Self::DatabricksUwi(config) => config.metadata_path(),
        }
    }

    pub(super) fn extract(&self, leaf_der: &[u8]) -> Result<VerifiedClientIdentity, &'static str> {
        let metadata = match self {
            Self::DatabricksUwi(config) => config.extract(leaf_der)?,
        };
        Ok(VerifiedClientIdentity {
            metadata_path: self.metadata_path(),
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ClientIdentityConfig;
    use serde_json::json;
    use vector_lib::lookup::owned_value_path;
    use vrl::value::Value;

    #[test]
    fn provider_config_roundtrips_and_preserves_authenticated_metadata() {
        let value = json!({
            "type": "databricks_uwi",
            "trust_domains": ["serverless.databricks.com"],
            "region_uri": "region:test",
            "allowed_apps": ["test-app"]
        });
        let config: ClientIdentityConfig = serde_json::from_value(value.clone()).unwrap();
        config.validate().unwrap();
        assert_eq!(serde_json::to_value(&config).unwrap(), value);
        let identity = config
            .extract(&crate::databricks_uwi::assigned_test_certificate())
            .unwrap();
        assert_eq!(
            identity.metadata_path,
            owned_value_path!("opentelemetry", "uwi")
        );
        assert_eq!(
            identity.metadata.get(&owned_value_path!("workspace_id")),
            Some(&Value::from("12345"))
        );
        assert_eq!(
            identity.metadata.get(&owned_value_path!("authenticated")),
            Some(&Value::Boolean(true))
        );

        for invalid in [
            json!({"type": "unknown"}),
            json!({"trust_domains": ["serverless.databricks.com"]}),
            {
                let mut unknown_field = value;
                unknown_field["unexpected"] = json!(true);
                unknown_field
            },
        ] {
            assert!(serde_json::from_value::<ClientIdentityConfig>(invalid).is_err());
        }
    }
}
