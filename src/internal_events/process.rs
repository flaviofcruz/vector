use metrics::{counter, gauge};
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::{InternalEvent, error_stage, error_type};

use crate::{built_info, config};

#[derive(Debug, NamedInternalEvent)]
pub struct VectorStarted;

impl InternalEvent for VectorStarted {
    fn emit(self) {
        info!(
            target: "vector",
            message = "Vector has started.",
            debug = built_info::DEBUG,
            version = built_info::PKG_VERSION,
            arch = built_info::TARGET_ARCH,
            revision = built_info::VECTOR_BUILD_DESC.unwrap_or(""),
        );
        counter!("started_total").increment(1);
        // No-op increment to pre-populate the counter so the series is visible
        // at the first `:8687/metrics` scrape even on pods that never reload.
        counter!("reloaded_total").increment(0);
        gauge!("last_config_reload_success").set(1.0);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct VectorReloaded<'a> {
    pub config_paths: &'a [config::ConfigPath],
}

impl InternalEvent for VectorReloaded<'_> {
    fn emit(self) {
        info!(
            target: "vector",
            message = "Vector has reloaded.",
            path = ?self.config_paths,
            internal_log_rate_limit = false,
        );
        info!(
            message = "Vector has reloaded.",
            // VECTOR_SERVICE_EVENT
            vector_event_type = 2,
            // VECTOR_CONFIG_RELOAD_SUCCESS
            service_event = 6,
            internal_log_rate_limit = false,
        );
        counter!("reloaded_total").increment(1);
        gauge!("last_config_reload_success").set(1.0);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct VectorStopped;

impl InternalEvent for VectorStopped {
    fn emit(self) {
        info!(
            target: "vector",
            message = "Vector has stopped.",
        );
        counter!("stopped_total").increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct VectorQuit;

impl InternalEvent for VectorQuit {
    fn emit(self) {
        info!(
            target: "vector",
            message = "Vector has quit.",
        );
        counter!("quit_total").increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct VectorReloadError {
    pub reason: &'static str,
}

impl InternalEvent for VectorReloadError {
    fn emit(self) {
        error!(
            message = "Reload was not successful.",
            reason = self.reason,
            error_code = "reload",
            error_type = error_type::CONFIGURATION_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_limit = false,
        );
        info!(
            message = "Reload was not successful.",
            // VECTOR_SERVICE_EVENT
            vector_event_type = 2,
            // VECTOR_CONFIG_RELOAD_FAILURE
            service_event = 7,
            internal_log_rate_limit = false,
        );
        counter!(
            "component_errors_total",
            "error_code" => "reload",
            "error_type" => error_type::CONFIGURATION_FAILED,
            "stage" => error_stage::PROCESSING,
            "reason" => self.reason,
        )
        .increment(1);
        gauge!("last_config_reload_success").set(0.0);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct VectorConfigLoadError;

impl InternalEvent for VectorConfigLoadError {
    fn emit(self) {
        error!(
            message = "Failed to load config files, reload aborted.",
            error_code = "config_load",
            error_type = error_type::CONFIGURATION_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_limit = false,
        );
        info!(
            message = "Failed to load config files, reload aborted.",
            // VECTOR_SERVICE_EVENT
            vector_event_type = 2,
            // VECTOR_CONFIG_RELOAD_FAILURE (was erroneously 6 = SUCCESS)
            service_event = 7,
            internal_log_rate_limit = false,
        );
        counter!(
            "component_errors_total",
            "error_code" => "config_load",
            "error_type" => error_type::CONFIGURATION_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
        gauge!("last_config_reload_success").set(0.0);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct VectorRecoveryError;

impl InternalEvent for VectorRecoveryError {
    fn emit(self) {
        error!(
            message = "Vector has failed to recover from a failed reload.",
            error_code = "recovery",
            error_type = error_type::CONFIGURATION_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_limit = false,
        );
        counter!(
            "component_errors_total",
            "error_code" => "recovery",
            "error_type" => error_type::CONFIGURATION_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}
