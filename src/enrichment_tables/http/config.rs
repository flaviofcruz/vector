//! Configuration for the `http` enrichment table.

use std::collections::HashMap;

use vector_lib::configurable::configurable_component;

use crate::config::{EnrichmentTableConfig, GlobalOptions};
use crate::http::Auth;
use crate::sources::util::http::HttpMethod;
use crate::tls::TlsConfig;

use super::table::HttpTable;

/// How to locate the array of row objects within the HTTP response body.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct ResponseConfig {
    /// A [JSON Pointer][json_pointer] (RFC 6901) to the array of row objects within the
    /// response body.
    ///
    /// The default (empty string) treats the entire response body as the array — i.e. the
    /// endpoint returns a bare JSON array of objects. Set this when the array is nested, for
    /// example `/data` for a body like `{"data": [ ... ]}`.
    ///
    /// [json_pointer]: https://datatracker.ietf.org/doc/html/rfc6901
    #[serde(default)]
    #[configurable(metadata(docs::examples = "/data"))]
    #[configurable(metadata(docs::examples = "/results/items"))]
    pub items_pointer: String,
}

/// The pagination strategy used to fetch all pages of the dataset.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "strategy", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The pagination strategy to use."))]
pub enum PaginationConfig {
    /// No pagination: a single request returns the full dataset.
    None,

    /// Cursor/token pagination: the response carries a token pointing at the next page, which
    /// is sent back as a query parameter on the following request. Paging stops when the token
    /// is absent, `null`, or empty.
    Cursor {
        /// A [JSON Pointer][json_pointer] (RFC 6901) to the next-page token in the response
        /// body.
        ///
        /// [json_pointer]: https://datatracker.ietf.org/doc/html/rfc6901
        #[configurable(metadata(docs::examples = "/next_cursor"))]
        #[configurable(metadata(docs::examples = "/paging/next"))]
        token_pointer: String,

        /// The query parameter to send the token back in on the next request.
        #[configurable(metadata(docs::examples = "cursor"))]
        #[configurable(metadata(docs::examples = "page_token"))]
        token_param: String,
    },

    /// Offset/limit pagination: each request advances `offset` by `page_size` and requests
    /// `page_size` rows via `limit`. Paging stops when a page returns fewer rows than
    /// `page_size`.
    Offset {
        /// The query parameter carrying the row offset (incremented by `page_size` each page).
        #[configurable(metadata(docs::examples = "offset"))]
        offset_param: String,

        /// The query parameter carrying the page size.
        #[configurable(metadata(docs::examples = "limit"))]
        limit_param: String,

        /// The number of rows to request per page.
        #[configurable(metadata(docs::examples = 1000))]
        page_size: usize,
    },
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self::None
    }
}

/// Guardrails bounding how much data a single refresh will fetch.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct LimitsConfig {
    /// The maximum number of pages to fetch in one refresh. When reached, the refresh stops
    /// and a warning is logged (the data is not silently truncated without notice).
    #[serde(default = "default_max_pages")]
    #[configurable(metadata(docs::examples = 1000))]
    pub max_pages: usize,

    /// The maximum number of rows to accumulate in one refresh. When reached, the refresh
    /// stops and a warning is logged.
    #[serde(default = "default_max_rows")]
    #[configurable(metadata(docs::examples = 1_000_000))]
    pub max_rows: usize,
}

const fn default_max_pages() -> usize {
    1000
}

const fn default_max_rows() -> usize {
    1_000_000
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_pages: default_max_pages(),
            max_rows: default_max_rows(),
        }
    }
}

/// Configuration for the `http` enrichment table.
///
/// Fetches a dataset from an HTTP endpoint and exposes it as an enrichment table. The data is
/// loaded into memory and refreshed periodically in the background, so lookups are fast and
/// never block on the network. This makes the `http` table a good fit for small, relatively
/// static reference datasets (mappings, allow-lists, small dimension tables) served over a
/// REST-style API.
///
/// The endpoint is expected to return a JSON array of objects (optionally nested within the
/// body — see `response.items_pointer`). Each object becomes a row; the union of object keys
/// across all rows becomes the set of columns, and a key missing from a given row is `null`.
#[configurable_component(enrichment_table("http"))]
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// The URL to fetch the dataset from.
    #[configurable(metadata(docs::examples = "https://api.example.com/v1/users"))]
    pub url: String,

    /// The HTTP method to use for the request.
    #[serde(default = "default_method")]
    pub method: HttpMethod,

    /// Additional headers to send with each request.
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "An HTTP request header and its value."
    ))]
    pub headers: HashMap<String, String>,

    /// An optional request body sent with each request (for example a JSON query for a `POST`
    /// endpoint).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,

    /// Authentication to apply to each request.
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,

    /// TLS configuration for the request.
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    /// How to locate the array of row objects within the response.
    #[configurable(derived)]
    #[serde(default)]
    pub response: ResponseConfig,

    /// The pagination strategy for fetching the full dataset.
    #[configurable(derived)]
    #[serde(default)]
    pub pagination: PaginationConfig,

    /// Guardrails bounding a single refresh.
    #[configurable(derived)]
    #[serde(default)]
    pub limits: LimitsConfig,

    /// How often, in seconds, to refresh the dataset from the endpoint.
    ///
    /// When unset, the dataset is fetched once at startup and never refreshed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[configurable(metadata(docs::examples = 3600))]
    pub refresh_interval_secs: Option<u64>,

    /// The timeout, in seconds, for each individual HTTP request made while fetching the
    /// dataset (applied per page).
    ///
    /// The timeout covers the whole request — both sending it and reading the response body —
    /// so an endpoint that stalls partway through streaming a response cannot hang the fetch. If
    /// a request does not complete within this window it is aborted and the refresh fails,
    /// leaving the previous snapshot in place rather than hanging indefinitely on an
    /// unresponsive endpoint.
    #[serde(default = "default_request_timeout_secs")]
    #[configurable(metadata(docs::examples = 30))]
    pub request_timeout_secs: u64,

    /// Whether to persist the fetched dataset to disk (under the global `data_dir`) so it can
    /// be served immediately on restart without waiting for the first fetch.
    #[serde(default = "crate::serde::default_true")]
    pub persist: bool,

    /// The maximum age, in seconds, of a persisted snapshot that will be trusted on startup.
    ///
    /// On startup, if a persisted snapshot exists and is younger than this, it is loaded
    /// immediately and a fresh fetch happens in the background. Older snapshots are ignored in
    /// favor of a fresh fetch. When unset, any persisted snapshot is trusted on startup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[configurable(metadata(docs::examples = 86400))]
    pub max_cache_age_secs: Option<u64>,

    /// Key/value pairs mapping column names to types, used to coerce string cell values into
    /// their proper types.
    ///
    /// Uses the same type syntax as the `file` enrichment table (for example `integer`,
    /// `float`, `boolean`, `timestamp|%+`). Columns not listed keep the type inferred from the
    /// JSON value.
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "Represents mapped column names and types."
    ))]
    pub schema: HashMap<String, String>,
}

const fn default_method() -> HttpMethod {
    HttpMethod::Get
}

const fn default_request_timeout_secs() -> u64 {
    30
}

impl EnrichmentTableConfig for HttpConfig {
    async fn build(
        &self,
        globals: &GlobalOptions,
    ) -> crate::Result<Box<dyn vector_lib::enrichment::Table + Send + Sync>> {
        let table = HttpTable::new(self.clone(), globals).await?;
        Ok(Box::new(table))
    }
}

impl_generate_config_from_default!(HttpConfig);

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            url: "https://api.example.com/v1/data".to_string(),
            method: default_method(),
            headers: HashMap::new(),
            body: None,
            auth: None,
            tls: None,
            response: ResponseConfig::default(),
            pagination: PaginationConfig::default(),
            limits: LimitsConfig::default(),
            refresh_interval_secs: None,
            request_timeout_secs: default_request_timeout_secs(),
            persist: true,
            max_cache_age_secs: None,
            schema: HashMap::new(),
        }
    }
}
