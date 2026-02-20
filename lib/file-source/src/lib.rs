#![deny(warnings)]
#![deny(clippy::all)]

pub mod file_server;
pub mod file_watcher;
pub mod paths_provider;

pub use self::file_server::{
    FileServer, Line, Shutdown as FileServerShutdown, calculate_ignore_before,
    parse_start_reading_at,
};

pub use file_source_common::{
    FileFingerprint, FingerprintStrategy, Fingerprinter, ReadFrom, ReadFromConfig,
    checkpointer::{CHECKPOINT_FILE_NAME, Checkpointer, CheckpointsView},
    internal_events::FileSourceInternalEvents,
};

use serde_with::serde_as;
use std::path::PathBuf;
use vector_config::configurable_component;

pub type FilePosition = u64;

// Config that specifies file TTL removal behavior
// action=Keep => Keep files matching {patterns} and remove the rest
// action=Remove => Remove files matching {patterns} and keep the rest
pub struct FileTTLRemovalConfig {
    pub action: FileTTLAction,
    pub patterns: Vec<glob::Pattern>,
}

/// Finer configuration of TTL Settings for the file source
/// {pattern} sets the files for which {action} should be done
/// If action = "keep", the specified files are not removed (the rest are)
/// If action = "remove", the specified files are removed (the rest are kept)
/// This config will be converted to FileTTLRemovalConfig, but is defined separately
/// as we want to use glob patterns for FileTTLRemovalConfig, but Pathbufs fit serialization/deserialization better
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TTLRemovalConfig {
    /// The action to take: either "keep" or "remove"
    #[configurable(derived)]
    pub action: FileTTLAction,
    /// File path patterns to match for this TTL action
    pub patterns: Vec<PathBuf>,
}

// Convert the configuration object TTLRemovalConfig to the FileTTLRemovalConfig struct type used by
// file_server. Primarily involves converting Pathbufs -> glob patterns.
pub fn convert_to_file_ttl_removal_config(config: &TTLRemovalConfig) -> FileTTLRemovalConfig {
    FileTTLRemovalConfig {
        action: config.action.clone(),
        patterns: config
            .patterns
            .iter()
            .filter_map(|pattern| glob::Pattern::new(&pattern.to_string_lossy()).ok())
            .collect(),
    }
}

/// Action to take for TTL configuration
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileTTLAction {
    /// Keep the files matching the patterns
    Keep,
    /// Remove the files matching the patterns
    Remove,
}
