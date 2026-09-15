//! Central location for all VRL functions used in Vector.
//!
//! This crate provides a single source of truth for the complete set of VRL functions
//! available throughout Vector, combining:
//! - Standard VRL library functions (`vrl::stdlib::all`)
//! - Vector-specific functions (`vector_vrl::secret_functions`)
//! - Enrichment table functions (`enrichment::vrl_functions`)
//! - Databricks redaction functions (`redaction_functions`, optional)
//! - DNS tap parsing functions (optional, with `dnstap` feature)

#![deny(warnings)]

use vrl::{compiler::Function, path::OwnedTargetPath};

#[cfg(feature = "databricks-redaction")]
pub mod apply_redaction;
#[cfg(feature = "databricks-redaction")]
pub mod enforce_metadata_shape;
pub mod get_secret;
#[cfg(feature = "databricks-redaction")]
pub mod redaction;
pub mod remove_secret;
pub mod set_secret;
pub mod set_semantic_meaning;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum MetadataKey {
    Legacy(String),
    Query(OwnedTargetPath),
}

pub const LEGACY_METADATA_KEYS: [&str; 2] = ["datadog_api_key", "splunk_hec_token"];

/// Returns Vector-specific secret functions.
pub fn secret_functions() -> Vec<Box<dyn Function>> {
    vec![
        Box::new(set_semantic_meaning::SetSemanticMeaning) as _,
        Box::new(get_secret::GetSecret) as _,
        Box::new(remove_secret::RemoveSecret) as _,
        Box::new(set_secret::SetSecret) as _,
    ]
}

/// Returns Vector-specific redaction functions.
#[cfg(feature = "databricks-redaction")]
pub fn redaction_functions() -> Vec<Box<dyn Function>> {
    vec![
        Box::new(apply_redaction::ApplyRedaction) as _,
        Box::new(enforce_metadata_shape::EnforceMetadataShape) as _,
    ]
}

/// Returns no Databricks-only functions in standalone builds.
#[cfg(not(feature = "databricks-redaction"))]
pub fn redaction_functions() -> Vec<Box<dyn Function>> {
    Vec::new()
}

/// Returns all VRL functions available in Vector.
#[allow(clippy::disallowed_methods)]
pub fn all() -> Vec<Box<dyn Function>> {
    let functions = vrl::stdlib::all()
        .into_iter()
        .chain(secret_functions())
        .chain(redaction_functions())
        .chain(enrichment::vrl_functions());

    #[cfg(feature = "dnstap")]
    let functions = functions.chain(dnstap_parser::vrl_functions());

    #[cfg(feature = "vrl-metrics")]
    let functions = functions.chain(vector_vrl_metrics::all());

    functions.collect()
}

#[cfg(all(test, not(feature = "databricks-redaction")))]
mod tests {
    #[test]
    fn standalone_build_omits_databricks_redaction_functions() {
        assert!(super::redaction_functions().is_empty());
    }
}
