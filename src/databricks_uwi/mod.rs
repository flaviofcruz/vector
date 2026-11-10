//! Databricks UWI certificate policy, signed attributes, and receiver metadata.

mod uwi;

use std::collections::BTreeMap;
use vector_lib::lookup::{OwnedValuePath, owned_value_path};
use vrl::value::Value;

use uwi::VerifiedIdentity;

/// Policy for authenticated serverless OTLP producers. This does not identify a pod replica.
#[vector_lib::configurable::configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct UwiIdentityConfig {
    /// Accepted UWI URI SAN trust domains, matched exactly without wildcards.
    pub trust_domains: Vec<String>,
    /// Exact signed region URI expected on this receiver.
    pub region_uri: String,
    /// Applications permitted to submit telemetry to this receiver.
    pub allowed_apps: Vec<String>,
}

impl UwiIdentityConfig {
    fn policy(&self) -> uwi::UwiIdentityPolicy {
        uwi::UwiIdentityPolicy {
            trust_domains: self.trust_domains.clone(),
            region_uri: self.region_uri.clone(),
            allowed_apps: self.allowed_apps.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        self.policy().validate()
    }

    pub(crate) fn extract(&self, leaf_der: &[u8]) -> Result<Value, &'static str> {
        self.policy()
            .extract(leaf_der)
            .map(VerifiedIdentity::into_value)
    }

    pub(crate) fn metadata_path(&self) -> OwnedValuePath {
        owned_value_path!("opentelemetry", "uwi")
    }
}

#[cfg(test)]
pub(crate) fn assigned_test_certificate() -> Vec<u8> {
    uwi::tests::assigned_certificate()
}

#[cfg(test)]
pub(crate) fn assigned_test_client_identity() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    uwi::tests::assigned_client_identity()
}

impl VerifiedIdentity {
    fn into_value(self) -> Value {
        Value::Object(BTreeMap::from([
            ("authenticated".into(), Value::Boolean(true)),
            ("principal".into(), self.principal.into()),
            ("workspace_id".into(), self.workspace_id.into()),
            ("app".into(), self.app.into()),
            ("app_instance".into(), self.app_instance.into()),
            ("workload".into(), self.workload.into()),
            ("region_uri".into(), self.region_uri.into()),
            (
                "external_workload_uri".into(),
                self.external_workload_uri.into(),
            ),
            (
                "workspace_scoped_app_id".into(),
                self.workspace_scoped_app_id.into(),
            ),
        ]))
    }
}
