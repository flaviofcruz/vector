//! Ingestion Callback Component
//!
//! A shared, config-driven HTTP callback mechanism for object storage sources
//! like AWS S3, Azure Blob, etc. After a file finishes processing (success or failure),
//! this component fires an HTTP request to a configured endpoint so the
//! upstream service knows the outcome.
//!
//! # Prerequisite
//!
//! This component is only triggered when a source processes a **custom
//! direct-ingest message** (`process_custom_message = true`). It is not
//! fired for standard S3 event notifications / Azure Event Grid messages.
//!
//! # Design
//!
//! The URI is a **template** rendered at runtime. The request body is
//! always JSON — defined as a map of JSON keys to template values. After
//! template rendering, the map is serialized via `serde_json` so all
//! values are properly escaped. `Content-Type: application/json` is
//! always set.
//!
//! Templates use Vector's existing `{{field}}` syntax. The available
//! fields are documented on [`CallbackContext`] and in the
//! [Template field reference](#template-field-reference) section below.
//!
//! # Template field layout
//!
//! Fields are split into two namespaces:
//!
//! - **`message.*`** — all fields from the original direct-ingest queue
//!   message, passed through as-is (e.g., `{{message.bucket}}`).
//! - **Top-level** — fields added by Vector after processing
//!   (e.g., `{{status}}`, `{{error_message}}`).
//!
//! The `message.*` fields are whatever the source passes in — this
//! component is agnostic to their names or meanings.
//!
//! # Template field reference
//!
//! ## `message.*` — Fields from the queue message
//!
//! All fields from the original direct-ingest queue message are passed
//! through verbatim and namespaced under `message`. The exact set of
//! fields depends on the source that populates [`CallbackContext`].
//! Templates reference them as `{{message.<field_name>}}`.
//!
//! ## Top-level — Fields added by Vector
//!
//! These are always present, computed after file processing completes:
//!
//! | Template variable | Type   | Description                                                        |
//! |-------------------|--------|--------------------------------------------------------------------|
//! | `status`          | String | Processing outcome: `"DELIVERED"`, `"ERRORED"`, or `"REJECTED"`.   |
//! | `error_message`   | String | Human-readable error description. Empty string on success.         |
//! | `timestamp`       | String | ISO-8601 UTC time when callback is fired.                          |
//! | `duration_ms`     | String | File processing duration in milliseconds.                          |
//!
//! ## Rendering rules
//!
//! - Templates use `{{field_name}}` syntax (Vector's standard template engine).
//! - Message fields are nested: `{{message.my_field}}`.
//! - Vector fields are top-level: `{{status}}`, `{{error_message}}`.
//! - If a referenced field is missing, the template render fails and the
//!   callback is skipped (with an error log).

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use http::{Method, Request, Uri};
use hyper::Body;
use snafu::Snafu;
use url::Url;
use vector_lib::configurable::configurable_component;

use tracing::{debug, error, trace, warn};

use crate::common::backoff::ExponentialBackoff;
use crate::config::ProxyConfig;
use crate::event::{BatchStatus, LogEvent};
use crate::http::{Auth, HttpClient, HttpError};
use crate::template::Template;
use crate::tls::{MaybeTlsSettings, TlsEnableableConfig};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Snafu)]
pub enum CallbackError {
    #[snafu(display("Failed to build callback HTTP client: {}", source))]
    BuildClient { source: HttpError },

    #[snafu(display(
        "Invalid callback base_url: must be an absolute http(s) URL with a host, got: {:?}. \
         Ensure `request.base_url` is set in your ingestion_callback config.",
        url
    ))]
    InvalidBaseUrl { url: String },

    #[snafu(display("Failed to parse callback URI template: {}", source))]
    InvalidUriTemplate {
        source: crate::template::TemplateParseError,
    },

    #[snafu(display("Failed to parse callback body value template: {}", source))]
    InvalidBodyValueTemplate {
        source: crate::template::TemplateParseError,
    },

    #[snafu(display(
        "Header {:?} is reserved and cannot be set via `headers`. \
         Use the `auth` config for Authorization; Content-Type is set automatically.",
        name
    ))]
    ReservedHeaderName { name: String },

    #[snafu(display("Invalid callback header name {:?}: {}", name, source))]
    InvalidHeaderName {
        name: String,
        source: http::header::InvalidHeaderName,
    },

    #[snafu(display("Invalid callback header value for {:?}: {}", name, source))]
    InvalidHeaderValue {
        name: String,
        source: http::header::InvalidHeaderValue,
    },

    #[snafu(display("Invalid URI after template rendering: {}", rendered))]
    InvalidRenderedUri { rendered: String },

    #[snafu(display("HTTP request failed: {}", source))]
    Request { source: HttpError },

    #[snafu(display("Callback returned non-success status: {}", status))]
    NonSuccessStatus { status: u16 },

    #[snafu(display("Callback request timed out"))]
    Timeout,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// HTTP method for the callback request.
#[configurable_component]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    /// HTTP POST (default).
    #[default]
    POST,
    /// HTTP PUT.
    PUT,
    /// HTTP PATCH.
    PATCH,
}

impl HttpMethod {
    fn to_method(&self) -> Method {
        match self {
            HttpMethod::POST => Method::POST,
            HttpMethod::PUT => Method::PUT,
            HttpMethod::PATCH => Method::PATCH,
        }
    }
}

/// Configuration for HTTP callbacks fired after a custom direct-ingest
/// message finishes processing (success or failure).
///
/// This component is only triggered for sources with custom message
/// processing enabled (`process_custom_message = true`).
///
/// The `uri` field and `body` map values support `{{field}}` template
/// interpolation. See the module-level template field reference for available fields.
///
/// The request body is always JSON. Define it as a map of JSON keys to
/// template values — the rendered values are serialized with `serde_json`
/// so special characters are always properly escaped.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct IngestionCallbackConfig {
    /// Endpoint to call when file ingestion **succeeds** (`BatchStatus::Delivered`).
    ///
    /// Omit to skip callbacks on success.
    #[configurable(derived)]
    pub on_success: Option<CallbackEndpointConfig>,

    /// Endpoint to call when file ingestion **fails** (`BatchStatus::Errored` or `Rejected`).
    ///
    /// Omit to skip callbacks on failure.
    #[configurable(derived)]
    pub on_failure: Option<CallbackEndpointConfig>,

    /// Shared HTTP request settings (base URL, timeout, retries).
    #[configurable(derived)]
    #[serde(default)]
    pub request: CallbackRequestConfig,

    /// Authentication for the callback HTTP request.
    ///
    /// Uses Vector's standard [`Auth`] enum, which supports `bearer`, `basic`,
    /// and `custom` strategies. The auth header is applied automatically.
    #[configurable(derived)]
    pub auth: Option<Auth>,

    /// Optional TLS configuration for the callback HTTP client.
    #[configurable(derived)]
    pub tls: Option<TlsEnableableConfig>,
}

/// Defines a single callback endpoint.
///
/// The `uri` is a template string. The `body` is a JSON object defined as
/// a map of keys to template values — each value is rendered, then the
/// whole map is serialized as JSON. If `body` is empty/omitted, no request
/// body is sent.
///
/// **Template fields:** see the module-level template field reference.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct CallbackEndpointConfig {
    /// URI path template, appended to `request.base_url`.
    ///
    /// Supports `{{field}}` interpolation.
    #[configurable(metadata(
        docs::examples = "/v2/ingestion/files/{{message.file_id}}/mark-successful",
        docs::examples = "/v2/ingestion/files/{{message.file_id}}/mark-failed"
    ))]
    pub uri: String,

    /// HTTP method. Defaults to POST.
    #[serde(default)]
    #[configurable(derived)]
    pub method: HttpMethod,

    /// JSON request body as a flat map of keys to template values.
    ///
    /// Each value supports `{{field}}` interpolation. After rendering,
    /// the map is serialized as a JSON object via `serde_json`, ensuring
    /// all values are properly escaped.
    ///
    /// **No nesting is supported** — all keys are top-level string values
    /// in the resulting JSON object (e.g., `{"file_id": "abc", "error": "msg"}`).
    ///
    /// Omit or leave empty for requests with no body (e.g., mark-successful).
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "A JSON key and its template value."
    ))]
    #[configurable(metadata(docs::examples = "body_examples()"))]
    pub body: BTreeMap<String, String>,

    /// Additional HTTP headers to include in the callback request.
    ///
    /// Header names and values are static strings — template interpolation is not supported.
    /// Headers are validated at startup.
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "An HTTP header name and its static value."
    ))]
    #[configurable(metadata(docs::examples = "header_examples()"))]
    pub headers: BTreeMap<String, String>,
}

fn body_examples() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("file_id".to_string(), "{{message.file_id}}".to_string()),
        ("error_message".to_string(), "{{error_message}}".to_string()),
    ])
}

fn header_examples() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("X-Tenant-Id".to_string(), "acme-corp".to_string()),
        ("X-Environment".to_string(), "production".to_string()),
    ])
}

/// Shared HTTP settings for all callback endpoints.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct CallbackRequestConfig {
    /// Base URL prepended to every endpoint `uri`.
    #[configurable(metadata(docs::examples = "https://log-access.example.com"))]
    #[configurable(validation(format = "uri"))]
    pub base_url: String,

    /// Request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub timeout_secs: u64,

    /// Maximum number of retry attempts on transient failure (non-2xx or network error).
    ///
    /// Uses exponential backoff starting at `retry_initial_backoff_secs`.
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: u32,

    /// Initial backoff between retries, in seconds. Doubles on each attempt.
    #[serde(default = "default_retry_initial_backoff_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub retry_initial_backoff_secs: u64,
}

impl Default for CallbackRequestConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            timeout_secs: default_timeout_secs(),
            retry_max_attempts: default_retry_max_attempts(),
            retry_initial_backoff_secs: default_retry_initial_backoff_secs(),
        }
    }
}

const fn default_timeout_secs() -> u64 {
    10
}

const fn default_retry_max_attempts() -> u32 {
    3
}

const fn default_retry_initial_backoff_secs() -> u64 {
    1
}

/// Headers that Vector manages itself and that users cannot override via `headers`.
/// `content-type` is set automatically when a body is present; `authorization` is
/// handled by the `auth` config field.
const RESERVED_HEADERS: &[&str] = &["content-type", "authorization"];

// ---------------------------------------------------------------------------
// Parsed endpoint (templates pre-compiled at startup)
// ---------------------------------------------------------------------------

/// A [`CallbackEndpointConfig`] with its templates pre-parsed for fast rendering.
#[derive(Clone, Debug)]
struct ParsedEndpoint {
    uri_template: Template,
    /// Body key-value pairs with pre-parsed template values.
    /// Empty vec means no body.
    body_templates: Vec<(String, Template)>,
    /// Pre-validated static headers applied to every request for this endpoint.
    headers: Vec<(http::header::HeaderName, http::header::HeaderValue)>,
    method: Method,
}

impl ParsedEndpoint {
    fn try_from_config(cfg: &CallbackEndpointConfig) -> Result<Self, CallbackError> {
        let uri_template = Template::try_from(cfg.uri.as_str())
            .map_err(|source| CallbackError::InvalidUriTemplate { source })?;

        let mut body_templates = Vec::with_capacity(cfg.body.len());
        for (key, value_tpl) in &cfg.body {
            let tpl = Template::try_from(value_tpl.as_str())
                .map_err(|source| CallbackError::InvalidBodyValueTemplate { source })?;
            body_templates.push((key.clone(), tpl));
        }

        let mut headers = Vec::with_capacity(cfg.headers.len());
        for (name, value) in &cfg.headers {
            if RESERVED_HEADERS.contains(&name.to_lowercase().as_str()) {
                return Err(CallbackError::ReservedHeaderName { name: name.clone() });
            }
            let header_name =
                http::header::HeaderName::from_bytes(name.as_bytes()).map_err(|source| {
                    CallbackError::InvalidHeaderName {
                        name: name.clone(),
                        source,
                    }
                })?;
            let header_value = http::header::HeaderValue::from_str(value).map_err(|source| {
                CallbackError::InvalidHeaderValue {
                    name: name.clone(),
                    source,
                }
            })?;
            headers.push((header_name, header_value));
        }

        Ok(Self {
            uri_template,
            body_templates,
            headers,
            method: cfg.method.to_method(),
        })
    }
}

// ---------------------------------------------------------------------------
// Callback context (the data available to templates)
// ---------------------------------------------------------------------------

/// Runtime context passed to the callback after file processing completes.
///
/// This struct is converted into a [`LogEvent`] for template rendering.
/// The resulting event has two levels:
///
/// - **`message.*`** — All fields from `message_fields`, nested under
///   the `message` key. The exact fields depend on the source.
/// - **Top-level** — Vector-computed processing outcome fields:
///   `status`, `error_message`, `timestamp`, `duration_ms`.
pub struct CallbackContext {
    /// Processing outcome.
    pub status: BatchStatus,

    /// Error description. Empty on success.
    pub error_message: String,

    /// How long file processing took.
    pub duration: Duration,

    /// Timestamp when the callback is being fired.
    pub timestamp: DateTime<Utc>,

    /// Fields from the original queue message, provided by the source.
    ///
    /// These are placed under the `message` key in the template context,
    /// so they are referenced as `{{message.<field_name>}}`.
    pub message_fields: HashMap<String, String>,
}

impl CallbackContext {
    /// Convert this context into a [`LogEvent`] that Vector's [`Template`]
    /// engine can render against.
    ///
    /// Layout:
    /// ```text
    /// {
    ///   "message": { "field1": "value1", ... },
    ///   "status": "DELIVERED",
    ///   "error_message": "",
    ///   "timestamp": "2026-03-16T...",
    ///   "duration_ms": "1500"
    /// }
    /// ```
    fn to_log_event(&self) -> LogEvent {
        let mut log = LogEvent::default();

        // Message fields nested under "message.*"
        for (k, v) in &self.message_fields {
            let path = format!("message.{k}");
            log.insert(path.as_str(), v.clone());
        }

        // Vector-computed fields at top level
        log.insert("status", batch_status_str(self.status));
        log.insert("error_message", self.error_message.clone());
        log.insert("timestamp", self.timestamp.to_rfc3339());
        log.insert("duration_ms", self.duration.as_millis().to_string());

        log
    }
}

// ---------------------------------------------------------------------------
// Client (runtime component)
// ---------------------------------------------------------------------------

/// The runtime callback client. Built once at source startup from
/// [`IngestionCallbackConfig`], then cloned into each concurrent ingestor task.
///
/// Calling [`notify`] is **fire-and-forget with retries**: failures are
/// appropriately logged but never propagated to the ingestion pipeline.
#[derive(Clone, Debug)]
pub struct IngestionCallbackClient {
    http_client: HttpClient,
    base_url: Url,
    timeout: Duration,
    retry_max_attempts: u32,
    retry_initial_backoff_secs: u64,
    auth: Option<Auth>,
    on_success: Option<ParsedEndpoint>,
    on_failure: Option<ParsedEndpoint>,
}

impl IngestionCallbackClient {
    /// Build a new client from config. Called once during source startup.
    ///
    /// Validates all templates eagerly so config errors surface immediately
    /// rather than at runtime when the first callback fires.
    pub fn new(
        config: &IngestionCallbackConfig,
        proxy: &ProxyConfig,
    ) -> Result<Self, CallbackError> {
        // Validate base_url is a non-empty absolute http(s) URL with a host.
        let base_url = config.request.base_url.trim_end_matches('/');
        let parsed_base = Url::parse(base_url)
            .ok()
            .filter(|u| matches!(u.scheme(), "http" | "https") && u.has_host())
            .ok_or_else(|| CallbackError::InvalidBaseUrl {
                url: config.request.base_url.clone(),
            })?;

        let tls_settings =
            MaybeTlsSettings::from_config(config.tls.as_ref(), false).map_err(|e| {
                CallbackError::BuildClient {
                    source: HttpError::BuildTlsConnector { source: e },
                }
            })?;

        let http_client = HttpClient::new(tls_settings, proxy)
            .map_err(|source| CallbackError::BuildClient { source })?;

        let on_success = config
            .on_success
            .as_ref()
            .map(ParsedEndpoint::try_from_config)
            .transpose()?;

        let on_failure = config
            .on_failure
            .as_ref()
            .map(ParsedEndpoint::try_from_config)
            .transpose()?;

        Ok(Self {
            http_client,
            base_url: parsed_base,
            timeout: Duration::from_secs(config.request.timeout_secs),
            retry_max_attempts: config.request.retry_max_attempts,
            retry_initial_backoff_secs: config.request.retry_initial_backoff_secs,
            auth: config.auth.clone(),
            on_success,
            on_failure,
        })
    }

    /// Convenience method: builds a [`CallbackContext`] from a processing result
    /// and spawns [`notify`](Self::notify) as a detached tokio task.
    ///
    /// This is the primary integration point for sources. Call it after
    /// `process_s3_object` / `process_blob_object` returns with the
    /// processing result and the original message fields.
    ///
    /// `processing_error` should be `None` on success, or `Some(error_string)`
    /// on failure. The `message_fields` are the fields from the original
    /// direct-ingest queue message (e.g., file_id, bucket, key, etc.).
    pub fn spawn_notify(
        &self,
        result: &Result<(), impl std::fmt::Display>,
        processing_duration: Duration,
        message_fields: HashMap<String, String>,
    ) -> tokio::task::JoinHandle<()> {
        let status = match result {
            Ok(()) => BatchStatus::Delivered,
            Err(_) => BatchStatus::Errored,
        };
        let error_message = match result {
            Ok(()) => String::new(),
            Err(err) => format!("{err}"),
        };
        let ctx = CallbackContext {
            status,
            error_message,
            duration: processing_duration,
            timestamp: Utc::now(),
            message_fields,
        };
        let cb = self.clone();
        tokio::spawn(async move { cb.notify(&ctx).await })
    }

    /// Fire the appropriate callback (success or failure) based on the context.
    ///
    /// This method is designed to be called from `tokio::spawn` — it never
    /// returns an error to the caller, the errors are appropriately logged.
    pub async fn notify(&self, ctx: &CallbackContext) {
        let endpoint = match ctx.status {
            BatchStatus::Delivered => &self.on_success,
            BatchStatus::Errored | BatchStatus::Rejected => &self.on_failure,
        };

        let Some(endpoint) = endpoint else {
            trace!(
                message = "No callback endpoint configured for this status.",
                status = %batch_status_str(ctx.status),
            );
            return;
        };

        let event = ctx.to_log_event();

        // Render URI template
        let uri_path = match endpoint.uri_template.render_string(&event) {
            Ok(v) => v,
            Err(err) => {
                error!(
                    message = "Failed to render callback URI template. Skipping callback.",
                    error = %err,
                    status = %batch_status_str(ctx.status),
                );
                return;
            }
        };

        // Render body: each value is a template, rendered then collected
        // into a BTreeMap, then serialized as JSON via serde_json.
        let body = if endpoint.body_templates.is_empty() {
            String::new()
        } else {
            let mut rendered_body = BTreeMap::new();
            for (key, tpl) in &endpoint.body_templates {
                match tpl.render_string(&event) {
                    Ok(v) => {
                        rendered_body.insert(key.clone(), v);
                    }
                    Err(err) => {
                        error!(
                            message = "Failed to render callback body value template. Skipping callback.",
                            key = %key,
                            error = %err,
                            status = %batch_status_str(ctx.status),
                        );
                        return;
                    }
                }
            }
            // serde_json handles all escaping (quotes, newlines, etc.)
            match serde_json::to_string(&rendered_body) {
                Ok(json) => json,
                Err(err) => {
                    error!(
                        message = "Failed to serialize callback body as JSON. Skipping callback.",
                        error = %err,
                    );
                    return;
                }
            }
        };

        let full_url = match self.base_url.join(&uri_path) {
            Ok(url) => url.to_string(),
            Err(_) => {
                error!(
                    message = "Failed to join base_url with rendered URI path. Skipping callback.",
                    base_url = %self.base_url,
                    uri_path = %uri_path,
                );
                return;
            }
        };

        // Retry with exponential backoff using Vector's ExponentialBackoff.
        // from_millis(base) produces delays of base^1, base^2, ... ms before
        // applying the factor. With base=2 and factor=initial_backoff_ms/2,
        // the first delay is 2 * (initial_backoff_ms/2) = initial_backoff_ms.
        // saturating_mul prevents overflow; max(1) ensures non-zero backoff.
        let initial_backoff_ms = self.retry_initial_backoff_secs.saturating_mul(1000);
        let mut backoff = ExponentialBackoff::from_millis(2)
            .factor((initial_backoff_ms / 2).max(1))
            .max_delay(Duration::from_secs(30));

        let max_attempts = self.retry_max_attempts.max(1);
        for attempt in 1..=max_attempts {
            match self
                .send_request(&full_url, &endpoint.method, &body, &endpoint.headers)
                .await
            {
                Ok(()) => {
                    debug!(
                        message = "Ingestion callback succeeded.",
                        url = %full_url,
                        status = %batch_status_str(ctx.status),
                        attempt,
                    );
                    return;
                }
                Err(err) => {
                    if attempt >= max_attempts {
                        warn!(
                            message = "Ingestion callback failed after all retries.",
                            url = %full_url,
                            status = %batch_status_str(ctx.status),
                            error = %err,
                            attempts = attempt,
                        );
                        return;
                    }
                    let delay = backoff.next().unwrap_or(Duration::from_secs(1));
                    warn!(
                        message = "Ingestion callback attempt failed. Retrying.",
                        url = %full_url,
                        error = %err,
                        attempt,
                        next_backoff_ms = delay.as_millis(),
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn send_request(
        &self,
        url: &str,
        method: &Method,
        body: &str,
        extra_headers: &[(http::header::HeaderName, http::header::HeaderValue)],
    ) -> Result<(), CallbackError> {
        let uri: Uri = url.parse().map_err(|_| CallbackError::InvalidRenderedUri {
            rendered: url.to_string(),
        })?;

        let http_body = if body.is_empty() {
            Body::empty()
        } else {
            Body::from(body.as_bytes().to_vec())
        };

        let mut builder = Request::builder().method(method).uri(&uri);

        if !body.is_empty() {
            builder = builder.header("Content-Type", "application/json");
        }

        for (name, value) in extra_headers {
            builder = builder.header(name, value);
        }

        if let Some(ref auth) = self.auth {
            builder = auth.apply_builder(builder);
        }

        let request = builder
            .body(http_body)
            .map_err(|e| CallbackError::Request {
                source: HttpError::BuildRequest { source: e },
            })?;

        let response = tokio::time::timeout(self.timeout, self.http_client.send(request))
            .await
            .map_err(|_| CallbackError::Timeout)?
            .map_err(|source| CallbackError::Request { source })?;

        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(CallbackError::NonSuccessStatus { status });
        }

        Ok(())
    }
}

fn batch_status_str(status: BatchStatus) -> &'static str {
    match status {
        BatchStatus::Delivered => "DELIVERED",
        BatchStatus::Errored => "ERRORED",
        BatchStatus::Rejected => "REJECTED",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helper to build a CallbackContext quickly
    // -----------------------------------------------------------------------

    fn make_context(
        status: BatchStatus,
        error_message: &str,
        message_fields: Vec<(&str, &str)>,
    ) -> CallbackContext {
        CallbackContext {
            status,
            error_message: error_message.to_string(),
            duration: Duration::from_millis(1500),
            timestamp: Utc::now(),
            message_fields: message_fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn make_endpoint(uri: &str, body: Vec<(&str, &str)>) -> CallbackEndpointConfig {
        CallbackEndpointConfig {
            uri: uri.to_string(),
            method: HttpMethod::POST,
            body: body
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            headers: BTreeMap::new(),
        }
    }

    // -----------------------------------------------------------------------
    // CallbackContext → LogEvent conversion
    // -----------------------------------------------------------------------

    #[test]
    fn context_to_event_message_fields_namespaced_under_message() {
        let ctx = make_context(
            BatchStatus::Delivered,
            "",
            vec![("file_id", "f-1"), ("bucket", "b"), ("key", "k")],
        );
        let event = ctx.to_log_event();

        assert_eq!(
            event.get("message.file_id").unwrap().to_string_lossy(),
            "f-1"
        );
        assert_eq!(event.get("message.bucket").unwrap().to_string_lossy(), "b");
        assert_eq!(event.get("message.key").unwrap().to_string_lossy(), "k");
        // Top-level "file_id" should NOT exist
        assert!(event.get("file_id").is_none());
    }

    #[test]
    fn context_to_event_vector_fields_at_top_level() {
        let ctx = make_context(BatchStatus::Errored, "something broke", vec![]);
        let event = ctx.to_log_event();

        assert_eq!(event.get("status").unwrap().to_string_lossy(), "ERRORED");
        assert_eq!(
            event.get("error_message").unwrap().to_string_lossy(),
            "something broke"
        );
        assert_eq!(event.get("duration_ms").unwrap().to_string_lossy(), "1500");
        assert!(event.get("timestamp").is_some());
    }

    #[test]
    fn context_to_event_all_batch_statuses() {
        for (status, expected) in [
            (BatchStatus::Delivered, "DELIVERED"),
            (BatchStatus::Errored, "ERRORED"),
            (BatchStatus::Rejected, "REJECTED"),
        ] {
            let ctx = make_context(status, "", vec![]);
            let event = ctx.to_log_event();
            assert_eq!(event.get("status").unwrap().to_string_lossy(), expected);
        }
    }

    #[test]
    fn context_to_event_empty_message_fields() {
        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        let event = ctx.to_log_event();

        // Should still have Vector fields
        assert!(event.get("status").is_some());
        assert!(event.get("error_message").is_some());
        // No message.* fields
        assert!(event.get("message").is_none());
    }

    #[test]
    fn context_to_event_duration_precision() {
        let ctx = CallbackContext {
            status: BatchStatus::Delivered,
            error_message: String::new(),
            duration: Duration::from_millis(42),
            timestamp: Utc::now(),
            message_fields: HashMap::new(),
        };
        let event = ctx.to_log_event();
        assert_eq!(event.get("duration_ms").unwrap().to_string_lossy(), "42");
    }

    // -----------------------------------------------------------------------
    // URI template rendering
    // -----------------------------------------------------------------------

    #[test]
    fn uri_template_renders_message_fields() {
        let tpl = Template::try_from("/files/{{message.file_id}}/action").unwrap();
        let ctx = make_context(BatchStatus::Delivered, "", vec![("file_id", "abc-123")]);
        let event = ctx.to_log_event();

        assert_eq!(tpl.render_string(&event).unwrap(), "/files/abc-123/action");
    }

    #[test]
    fn uri_template_renders_vector_fields() {
        let tpl = Template::try_from("/callback/{{status}}").unwrap();
        let ctx = make_context(BatchStatus::Rejected, "", vec![]);
        let event = ctx.to_log_event();

        assert_eq!(tpl.render_string(&event).unwrap(), "/callback/REJECTED");
    }

    #[test]
    fn uri_template_missing_field_returns_error() {
        let tpl = Template::try_from("/files/{{message.file_id}}/action").unwrap();
        // No file_id in message_fields
        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        let event = ctx.to_log_event();

        assert!(tpl.render_string(&event).is_err());
    }

    #[test]
    fn uri_template_with_multiple_fields() {
        let tpl =
            Template::try_from("/{{message.region}}/files/{{message.file_id}}/{{status}}").unwrap();
        let ctx = make_context(
            BatchStatus::Delivered,
            "",
            vec![("file_id", "f-1"), ("region", "us-west-2")],
        );
        let event = ctx.to_log_event();

        assert_eq!(
            tpl.render_string(&event).unwrap(),
            "/us-west-2/files/f-1/DELIVERED"
        );
    }

    // -----------------------------------------------------------------------
    // Body JSON serialization
    // -----------------------------------------------------------------------

    #[test]
    fn body_renders_and_serializes_as_valid_json() {
        let ctx = make_context(BatchStatus::Errored, "timeout", vec![("file_id", "f-99")]);
        let event = ctx.to_log_event();

        let body_cfg: BTreeMap<String, String> = BTreeMap::from([
            ("file_id".into(), "{{message.file_id}}".into()),
            ("error_message".into(), "{{error_message}}".into()),
            (
                "custom_field".into(),
                "ID = {{message.file_id}} AND status = {{status}}".into(),
            ),
        ]);

        let mut rendered = BTreeMap::new();
        for (k, v) in &body_cfg {
            let tpl = Template::try_from(v.as_str()).unwrap();
            rendered.insert(k.clone(), tpl.render_string(&event).unwrap());
        }

        let json = serde_json::to_string(&rendered).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["file_id"], "f-99");
        assert_eq!(parsed["error_message"], "timeout");
        assert_eq!(parsed["custom_field"], "ID = f-99 AND status = ERRORED");
    }

    #[test]
    fn body_empty_map_produces_no_body() {
        let cfg = make_endpoint("/test", vec![]);
        let parsed = ParsedEndpoint::try_from_config(&cfg).unwrap();
        assert!(parsed.body_templates.is_empty());
    }

    #[test]
    fn body_missing_template_field_returns_error() {
        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        let event = ctx.to_log_event();

        let tpl = Template::try_from("{{message.nonexistent}}").unwrap();
        assert!(tpl.render_string(&event).is_err());
    }

    // -----------------------------------------------------------------------
    // ParsedEndpoint
    // -----------------------------------------------------------------------

    #[test]
    fn parsed_endpoint_validates_uri_template() {
        let cfg = make_endpoint("/v2/files/{{message.id}}/ok", vec![]);
        assert!(ParsedEndpoint::try_from_config(&cfg).is_ok());
    }

    #[test]
    fn parsed_endpoint_validates_body_templates() {
        let cfg = make_endpoint(
            "/test",
            vec![
                ("a", "{{message.x}}"),
                ("b", "{{status}}"),
                ("c", "static_value"),
            ],
        );
        let parsed = ParsedEndpoint::try_from_config(&cfg).unwrap();
        assert_eq!(parsed.body_templates.len(), 3);
    }

    #[test]
    fn parsed_endpoint_preserves_method() {
        let mut cfg = make_endpoint("/test", vec![]);
        cfg.method = HttpMethod::PUT;
        let parsed = ParsedEndpoint::try_from_config(&cfg).unwrap();
        assert_eq!(parsed.method, Method::PUT);
    }

    // -----------------------------------------------------------------------
    // Config deserialization
    // -----------------------------------------------------------------------

    #[test]
    fn config_full_deserialization() {
        let json = r#"{
            "on_success": {
                "uri": "/v2/files/{{message.file_id}}/mark-successful",
                "method": "POST",
                "headers": {
                    "X-Tenant-Id": "acme-corp",
                    "X-Environment": "production"
                }
            },
            "on_failure": {
                "uri": "/v2/files/{{message.file_id}}/mark-failed",
                "method": "PUT",
                "body": {
                    "file_id": "{{message.file_id}}",
                    "error_message": "{{error_message}}"
                }
            },
            "request": {
                "base_url": "https://log-access.example.com",
                "timeout_secs": 5,
                "retry_max_attempts": 5,
                "retry_initial_backoff_secs": 2
            },
            "auth": {
                "strategy": "bearer",
                "token": "my-token"
            }
        }"#;

        let config: IngestionCallbackConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.request.base_url, "https://log-access.example.com");
        assert_eq!(config.request.timeout_secs, 5);
        assert_eq!(config.request.retry_max_attempts, 5);
        assert_eq!(config.request.retry_initial_backoff_secs, 2);
        assert!(config.auth.is_some());

        let on_success = config.on_success.unwrap();
        assert!(on_success.body.is_empty());
        assert_eq!(on_success.method, HttpMethod::POST);
        assert_eq!(on_success.headers["X-Tenant-Id"], "acme-corp");
        assert_eq!(on_success.headers["X-Environment"], "production");

        let on_failure = config.on_failure.unwrap();
        assert_eq!(on_failure.method, HttpMethod::PUT);
        assert_eq!(on_failure.body.len(), 2);
        assert_eq!(on_failure.body["file_id"], "{{message.file_id}}");
        assert_eq!(on_failure.body["error_message"], "{{error_message}}");
        assert!(on_failure.headers.is_empty());
    }

    #[test]
    fn config_defaults_applied_when_omitted() {
        let json = r#"{
            "request": { "base_url": "https://example.com" }
        }"#;

        let config: IngestionCallbackConfig = serde_json::from_str(json).unwrap();

        assert!(config.on_success.is_none());
        assert!(config.on_failure.is_none());
        assert_eq!(config.request.timeout_secs, 10);
        assert_eq!(config.request.retry_max_attempts, 3);
        assert_eq!(config.request.retry_initial_backoff_secs, 1);
        assert!(config.auth.is_none());
        assert!(config.tls.is_none());
    }

    #[test]
    fn config_endpoint_defaults() {
        let json = r#"{ "uri": "/test" }"#;
        let cfg: CallbackEndpointConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.method, HttpMethod::POST);
        assert!(cfg.body.is_empty());
        assert!(cfg.headers.is_empty());
    }

    // -----------------------------------------------------------------------
    // Headers: config, validation, and request application
    // -----------------------------------------------------------------------

    #[test]
    fn config_rejects_unknown_fields() {
        let json = r#"{
            "unknown_field": true,
            "request": { "base_url": "https://example.com" }
        }"#;
        assert!(serde_json::from_str::<IngestionCallbackConfig>(json).is_err());
    }

    #[test]
    fn config_base_url_validated_with_url_parser() {
        // Invalid base URLs must all be rejected
        let invalid_cases = [
            ("empty string", ""),
            ("no scheme", "example.com/path"),
            ("ftp scheme", "ftp://example.com"),
            ("scheme only", "https://"),
            ("path only", "/just/a/path"),
        ];
        for (name, url) in invalid_cases {
            let config = IngestionCallbackConfig {
                on_success: Some(make_endpoint("/test", vec![])),
                on_failure: None,
                request: CallbackRequestConfig {
                    base_url: url.to_string(),
                    ..Default::default()
                },
                auth: None,
                tls: None,
            };
            assert!(
                IngestionCallbackClient::new(&config, &ProxyConfig::default()).is_err(),
                "expected error for case: {name}"
            );
        }

        // Valid http and https base URLs must be accepted.
        for url in ["http://example.com", "https://example.com:8080"] {
            let config = IngestionCallbackConfig {
                on_success: Some(make_endpoint("/test", vec![])),
                on_failure: None,
                request: CallbackRequestConfig {
                    base_url: url.to_string(),
                    ..Default::default()
                },
                auth: None,
                tls: None,
            };
            assert!(
                IngestionCallbackClient::new(&config, &ProxyConfig::default()).is_ok(),
                "expected ok for url: {url}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // E2E unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn e2e_success_endpoint_renders_uri_with_no_body() {
        let cfg = make_endpoint("/v2/files/{{message.file_id}}/mark-successful", vec![]);
        let parsed = ParsedEndpoint::try_from_config(&cfg).unwrap();
        let ctx = make_context(BatchStatus::Delivered, "", vec![("file_id", "f-1")]);
        let event = ctx.to_log_event();

        assert_eq!(
            parsed.uri_template.render_string(&event).unwrap(),
            "/v2/files/f-1/mark-successful"
        );
        assert!(parsed.body_templates.is_empty());
    }

    #[test]
    fn e2e_failure_endpoint_renders_uri_and_json_body() {
        let cfg = make_endpoint(
            "/v2/files/{{message.file_id}}/mark-failed",
            vec![
                ("file_id", "{{message.file_id}}"),
                ("error_message", "{{error_message}}"),
            ],
        );
        let parsed = ParsedEndpoint::try_from_config(&cfg).unwrap();
        let ctx = make_context(
            BatchStatus::Errored,
            "connection reset",
            vec![("file_id", "f-2")],
        );
        let event = ctx.to_log_event();

        assert_eq!(
            parsed.uri_template.render_string(&event).unwrap(),
            "/v2/files/f-2/mark-failed"
        );

        let mut body = BTreeMap::new();
        for (k, tpl) in &parsed.body_templates {
            body.insert(k.clone(), tpl.render_string(&event).unwrap());
        }
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&body).unwrap()).unwrap();
        assert_eq!(json["file_id"], "f-2");
        assert_eq!(json["error_message"], "connection reset");
    }

    #[test]
    fn e2e_special_chars_in_body_produce_valid_json() {
        let cfg = make_endpoint("/cb", vec![("msg", "{{error_message}}")]);
        let parsed = ParsedEndpoint::try_from_config(&cfg).unwrap();
        let ctx = make_context(
            BatchStatus::Rejected,
            "line1\nline2\t\"quoted\"\\end",
            vec![],
        );
        let event = ctx.to_log_event();

        let mut body = BTreeMap::new();
        for (k, tpl) in &parsed.body_templates {
            body.insert(k.clone(), tpl.render_string(&event).unwrap());
        }
        let json: serde_json::Value = serde_json::from_str(&serde_json::to_string(&body).unwrap())
            .expect("must be valid JSON");
        assert_eq!(json["msg"], "line1\nline2\t\"quoted\"\\end");
    }

    // -----------------------------------------------------------------------
    // Integration tests — real HTTP server
    // -----------------------------------------------------------------------

    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Response, Server, StatusCode};
    use std::net::SocketAddr;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    };
    use tokio::sync::oneshot;

    /// A recorded HTTP request: method, URI, body, and headers.
    #[derive(Debug, Clone)]
    struct RecordedRequest {
        method: String,
        uri: String,
        body: String,
        headers: HashMap<String, String>,
    }

    type RequestLog = Arc<Mutex<Vec<RecordedRequest>>>;

    /// Spin up a local HTTP server that records requests.
    /// Returns (addr, request_log, shutdown_tx).
    async fn start_test_server(
        status_codes: Vec<StatusCode>,
    ) -> (SocketAddr, RequestLog, oneshot::Sender<()>) {
        let request_log: RequestLog = Arc::new(Mutex::new(Vec::new()));
        let call_count = Arc::new(AtomicU32::new(0));

        let log_clone = request_log.clone();
        let codes = Arc::new(status_codes);
        let count_clone = call_count.clone();

        let make_svc = make_service_fn(move |_| {
            let log = log_clone.clone();
            let codes = codes.clone();
            let count = count_clone.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let log = log.clone();
                    let codes = codes.clone();
                    let count = count.clone();
                    async move {
                        let method = req.method().to_string();
                        let uri = req.uri().to_string();
                        let headers: HashMap<String, String> = req
                            .headers()
                            .iter()
                            .map(|(k, v)| {
                                (k.as_str().to_string(), v.to_str().unwrap_or("").to_string())
                            })
                            .collect();
                        let mut body_data = Vec::new();
                        let mut body_stream = req.into_body();
                        while let Some(chunk) = futures::StreamExt::next(&mut body_stream).await {
                            body_data.extend_from_slice(&chunk.unwrap());
                        }
                        let body_bytes = bytes::Bytes::from(body_data);
                        let body = String::from_utf8_lossy(&body_bytes).to_string();

                        log.lock().unwrap().push(RecordedRequest {
                            method,
                            uri,
                            body,
                            headers,
                        });

                        let idx = count.fetch_add(1, Ordering::SeqCst) as usize;
                        let status = codes.get(idx).copied().unwrap_or(StatusCode::OK);

                        Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(status)
                                .body(Body::empty())
                                .unwrap(),
                        )
                    }
                }))
            }
        });

        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let graceful = server.with_graceful_shutdown(async {
            shutdown_rx.await.ok();
        });

        tokio::spawn(graceful);

        (addr, request_log, shutdown_tx)
    }

    fn make_test_client(
        base_url: &str,
        on_success_uri: Option<&str>,
        on_failure_uri: Option<&str>,
        on_failure_body: Vec<(&str, &str)>,
    ) -> IngestionCallbackClient {
        let config = IngestionCallbackConfig {
            on_success: on_success_uri.map(|uri| CallbackEndpointConfig {
                uri: uri.to_string(),
                method: HttpMethod::POST,
                body: BTreeMap::new(),
                headers: BTreeMap::new(),
            }),
            on_failure: on_failure_uri.map(|uri| CallbackEndpointConfig {
                uri: uri.to_string(),
                method: HttpMethod::POST,
                body: on_failure_body
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                headers: BTreeMap::new(),
            }),
            request: CallbackRequestConfig {
                base_url: base_url.to_string(),
                timeout_secs: 5,
                retry_max_attempts: 1,
                retry_initial_backoff_secs: 1,
            },
            auth: None,
            tls: None,
        };

        IngestionCallbackClient::new(&config, &ProxyConfig::default()).unwrap()
    }

    /// Minimal helper for retry-specific tests — only configures the success endpoint.
    fn make_retry_client(
        base_url: &str,
        on_success_uri: &str,
        retry_max_attempts: u32,
    ) -> IngestionCallbackClient {
        let config = IngestionCallbackConfig {
            on_success: Some(CallbackEndpointConfig {
                uri: on_success_uri.to_string(),
                method: HttpMethod::POST,
                body: BTreeMap::new(),
                headers: BTreeMap::new(),
            }),
            on_failure: None,
            request: CallbackRequestConfig {
                base_url: base_url.to_string(),
                timeout_secs: 5,
                retry_max_attempts,
                retry_initial_backoff_secs: 1,
            },
            auth: None,
            tls: None,
        };
        IngestionCallbackClient::new(&config, &ProxyConfig::default()).unwrap()
    }

    #[tokio::test]
    async fn integration_success_callback_verifies_method_uri_headers_and_empty_body() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        let client = make_test_client(
            &format!("http://{addr}"),
            Some("/v2/files/{{message.file_id}}/mark-successful"),
            None,
            vec![],
        );

        let ctx = make_context(BatchStatus::Delivered, "", vec![("file_id", "f-integ-1")]);
        client.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let req = &requests[0];
        assert_eq!(req.method, "POST");
        assert_eq!(req.uri, "/v2/files/f-integ-1/mark-successful");
        assert!(req.body.is_empty(), "success endpoint should have no body");
        assert!(
            req.headers.get("content-type").is_none(),
            "Content-Type must not be set when body is empty"
        );

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_failure_callback_sends_json_body_and_checks_method() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        let client = make_test_client(
            &format!("http://{addr}"),
            None,
            Some("/v2/files/{{message.file_id}}/mark-failed"),
            vec![
                ("file_id", "{{message.file_id}}"),
                ("error_message", "{{error_message}}"),
            ],
        );

        let ctx = make_context(
            BatchStatus::Errored,
            "read timeout",
            vec![("file_id", "f-integ-2")],
        );
        client.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let req = &requests[0];
        assert_eq!(req.method, "POST");
        assert_eq!(req.uri, "/v2/files/f-integ-2/mark-failed");

        let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
        assert_eq!(body["file_id"], "f-integ-2");
        assert_eq!(body["error_message"], "read timeout");

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_body_with_special_chars_arrives_as_valid_json() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        let client = make_test_client(
            &format!("http://{addr}"),
            None,
            Some("/callback"),
            vec![("msg", "{{error_message}}")],
        );

        let ctx = make_context(
            BatchStatus::Errored,
            "line1\nline2\t\"quoted\"\t\\end",
            vec![],
        );
        client.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value = serde_json::from_str(&requests[0].body)
            .expect("body with special chars must be valid JSON");
        assert_eq!(body["msg"], "line1\nline2\t\"quoted\"\t\\end");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn integration_retries_on_server_error_then_succeeds() {
        // First two calls return 500, third returns 200
        let (addr, log, shutdown) = start_test_server(vec![
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::OK,
        ])
        .await;

        // start_paused = true: Tokio auto-advances paused time when the only pending
        // work is timer-based (the retry sleep), making retries instant without wall-clock waits.
        let client = make_retry_client(&format!("http://{addr}"), "/ok", 3);

        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        client.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(
            requests.len(),
            3,
            "should have retried twice then succeeded"
        );
        // All 3 attempts hit the same URI
        for req in requests.iter() {
            assert_eq!(req.uri, "/ok");
        }

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn integration_stops_after_max_retries() {
        let (addr, log, shutdown) = start_test_server(vec![
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::INTERNAL_SERVER_ERROR,
        ])
        .await;

        let client = make_retry_client(&format!("http://{addr}"), "/fail", 2);

        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        client.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 2, "should stop after max retries");

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_zero_retry_attempts_still_makes_one_request() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        let client = make_retry_client(&format!("http://{addr}"), "/once", 0);

        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        client.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 1, "max(1) ensures at least one attempt");

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_content_type_set_iff_body_is_nonempty() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK, StatusCode::OK]).await;

        // on_success has no body fields → empty body; on_failure has body fields → JSON body
        let client = make_test_client(
            &format!("http://{addr}"),
            Some("/success"),
            Some("/failure"),
            vec![("reason", "{{error_message}}")],
        );

        client
            .notify(&make_context(BatchStatus::Delivered, "", vec![]))
            .await;
        client
            .notify(&make_context(BatchStatus::Errored, "oops", vec![]))
            .await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 2);

        // Empty-body request must NOT carry Content-Type
        assert!(
            requests[0].headers.get("content-type").is_none(),
            "Content-Type must be absent when body is empty"
        );

        // Non-empty-body request MUST carry Content-Type: application/json
        assert_eq!(
            requests[1].headers.get("content-type").map(|s| s.as_str()),
            Some("application/json"),
            "Content-Type must be application/json when body is non-empty"
        );

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_no_callback_when_endpoint_not_configured() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        // Only on_failure configured, but status is Delivered
        let client = make_test_client(&format!("http://{addr}"), None, Some("/fail"), vec![]);

        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        client.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 0, "should not call any endpoint");

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_all_three_statuses_route_correctly() {
        let (addr, log, shutdown) =
            start_test_server(vec![StatusCode::OK, StatusCode::OK, StatusCode::OK]).await;

        let client = make_test_client(
            &format!("http://{addr}"),
            Some("/success"),
            Some("/failure"),
            vec![],
        );

        // Delivered → on_success
        client
            .notify(&make_context(BatchStatus::Delivered, "", vec![]))
            .await;
        // Errored → on_failure
        client
            .notify(&make_context(BatchStatus::Errored, "err", vec![]))
            .await;
        // Rejected → on_failure
        client
            .notify(&make_context(BatchStatus::Rejected, "rej", vec![]))
            .await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].uri, "/success");
        assert_eq!(requests[1].uri, "/failure");
        assert_eq!(requests[2].uri, "/failure");

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_url_join_normalizes_path_separator() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK, StatusCode::OK]).await;

        // Case 1: base_url has trailing slash, uri has leading slash → single slash in result.
        let client_trailing = make_test_client(
            &format!("http://{addr}/"),
            Some("/v1/callback"),
            None,
            vec![],
        );
        let ctx = make_context(BatchStatus::Delivered, "", vec![]);
        client_trailing.notify(&ctx).await;

        // Case 2: base_url has no trailing slash, uri has no leading slash → join still works.
        let client_no_slash =
            make_test_client(&format!("http://{addr}"), Some("v1/callback"), None, vec![]);
        client_no_slash.notify(&ctx).await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 2);
        // Both must resolve to the same path without double-slash.
        assert_eq!(requests[0].uri, "/v1/callback");
        assert_eq!(requests[1].uri, "/v1/callback");

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_spawn_notify_success_fires_callback() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        let client = make_test_client(
            &format!("http://{addr}"),
            Some("/mark-ok"),
            Some("/mark-fail"),
            vec![("error_message", "{{error_message}}")],
        );

        // Simulate a successful processing result
        let result: Result<(), String> = Ok(());
        let message_fields = HashMap::from([("file_id".to_string(), "f-spawn-1".to_string())]);
        client
            .spawn_notify(&result, Duration::ZERO, message_fields)
            .await
            .unwrap();

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].uri, "/mark-ok");
        assert!(requests[0].body.is_empty());

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn integration_spawn_notify_failure_fires_callback_with_error() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        let client = make_test_client(
            &format!("http://{addr}"),
            Some("/mark-ok"),
            Some("/mark-fail"),
            vec![
                ("file_id", "{{message.file_id}}"),
                ("error_message", "{{error_message}}"),
            ],
        );

        // Simulate a failed processing result
        let result: Result<(), String> = Err("S3 read timeout".to_string());
        let message_fields = HashMap::from([("file_id".to_string(), "f-spawn-2".to_string())]);
        client
            .spawn_notify(&result, Duration::ZERO, message_fields)
            .await
            .unwrap();

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].uri, "/mark-fail");

        let body: serde_json::Value = serde_json::from_str(&requests[0].body).unwrap();
        assert_eq!(body["file_id"], "f-spawn-2");
        assert_eq!(body["error_message"], "S3 read timeout");

        let _ = shutdown.send(());
    }

    // -----------------------------------------------------------------------
    // Headers: config validation grid + integration
    // -----------------------------------------------------------------------

    #[test]
    fn headers_validation_grid() {
        // (header_name, header_value, expect_ok, description)
        let cases: &[(&str, &str, bool, &str)] = &[
            // Valid
            ("X-Tenant-Id", "acme-corp", true, "simple custom header"),
            ("x-lowercase", "value", true, "lowercase name"),
            ("X-Empty-Value", "", true, "empty value is allowed"),
            // Reserved — blocked regardless of validity
            (
                "content-type",
                "text/plain",
                false,
                "content-type is reserved",
            ),
            (
                "Content-Type",
                "text/plain",
                false,
                "content-type is reserved (mixed case)",
            ),
            (
                "authorization",
                "Bearer tok",
                false,
                "authorization is reserved",
            ),
            (
                "Authorization",
                "Bearer tok",
                false,
                "authorization is reserved (mixed case)",
            ),
            // Invalid names
            ("invalid name", "value", false, "space in header name"),
            ("", "value", false, "empty header name"),
            // Invalid values
            ("X-Bad", "val\nue", false, "newline in value"),
        ];

        for (name, value, expect_ok, desc) in cases {
            let cfg = CallbackEndpointConfig {
                uri: "/test".to_string(),
                method: HttpMethod::POST,
                body: BTreeMap::new(),
                headers: BTreeMap::from([(name.to_string(), value.to_string())]),
            };
            let result = ParsedEndpoint::try_from_config(&cfg);
            assert_eq!(
                result.is_ok(),
                *expect_ok,
                "case {desc:?} failed: name={name:?} value={value:?}",
            );
        }
    }

    /// Custom headers configured on an endpoint arrive on the wire with correct values.
    #[tokio::test]
    async fn integration_custom_headers_are_sent() {
        let (addr, log, shutdown) = start_test_server(vec![StatusCode::OK]).await;

        let config = IngestionCallbackConfig {
            on_success: Some(CallbackEndpointConfig {
                uri: "/callback".to_string(),
                method: HttpMethod::POST,
                body: BTreeMap::new(),
                headers: BTreeMap::from([
                    ("X-Tenant-Id".to_string(), "acme-corp".to_string()),
                    ("X-Source".to_string(), "vector".to_string()),
                ]),
            }),
            on_failure: None,
            request: CallbackRequestConfig {
                base_url: format!("http://{addr}"),
                timeout_secs: 5,
                retry_max_attempts: 1,
                retry_initial_backoff_secs: 1,
            },
            auth: None,
            tls: None,
        };
        let client = IngestionCallbackClient::new(&config, &ProxyConfig::default()).unwrap();

        client
            .notify(&make_context(BatchStatus::Delivered, "", vec![]))
            .await;

        let requests = log.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].headers.get("x-tenant-id").map(String::as_str),
            Some("acme-corp")
        );
        assert_eq!(
            requests[0].headers.get("x-source").map(String::as_str),
            Some("vector")
        );

        let _ = shutdown.send(());
    }

    /// Reserved headers (`content-type`, `authorization`) are rejected at client build time,
    /// regardless of casing, so there is never a conflict with Vector-managed headers.
    #[test]
    fn reserved_headers_rejected_at_build() {
        for reserved in [
            "content-type",
            "Content-Type",
            "authorization",
            "Authorization",
        ] {
            let config = IngestionCallbackConfig {
                on_success: Some(CallbackEndpointConfig {
                    uri: "/test".to_string(),
                    method: HttpMethod::POST,
                    body: BTreeMap::new(),
                    headers: BTreeMap::from([(reserved.to_string(), "some-value".to_string())]),
                }),
                on_failure: None,
                request: CallbackRequestConfig {
                    base_url: "https://example.com".to_string(),
                    ..Default::default()
                },
                auth: None,
                tls: None,
            };
            assert!(
                IngestionCallbackClient::new(&config, &ProxyConfig::default()).is_err(),
                "expected error for reserved header {reserved:?}"
            );
        }
    }
}
