//! The `http` enrichment table: fetches a dataset over HTTP, holds it in an indexed in-memory
//! snapshot, and refreshes it in the background.
//!
//! Lookups read a lock-free [`ArcSwap`] snapshot via the shared [`IndexedData`] engine, so they
//! never block on the network. A detached background task re-fetches on the configured
//! interval, rebuilds the indexes that were registered against the previous snapshot, and
//! atomically swaps in the new data. A failed refresh is logged and the last good snapshot is
//! retained (fail-open). Optionally, the fetched dataset is persisted to disk so a restart can
//! serve data immediately instead of waiting for the first fetch.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwap;
use bytes::{Buf, Bytes};
use http::{Request, Uri};
use http_body::Body as HttpBody;
use hyper::Body;
use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
use serde::{Deserialize, Serialize};
use vector_lib::{
    TimeZone,
    conversion::Conversion,
    enrichment::{Case, Condition, IndexHandle, Table},
};
use vrl::value::{ObjectMap, Value};

use super::config::{HttpConfig, PaginationConfig};
use crate::config::ProxyConfig;
use crate::enrichment_tables::file::json_to_vrl_value;
use crate::enrichment_tables::indexed_data::IndexedData;
use crate::http::HttpClient;
use crate::tls::TlsSettings;

/// Exponential-backoff bounds for the background recovery retry used when the table started
/// empty (its initial load failed and no cache was available). The first retry fires quickly so
/// a brief upstream blip is recovered almost immediately; the delay then doubles on each failure
/// up to the cap, so a longer outage settles to at most one attempt per minute. Recovery stops
/// as soon as one fetch succeeds.
const INITIAL_RETRY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(60);

/// Shared state between the table handle(s) and the background refresh task.
struct Shared {
    /// The current indexed snapshot. Read lock-free by lookups, replaced atomically by
    /// `add_index` and the refresh task.
    data: ArcSwap<IndexedData>,
    /// The authoritative list of index specifications registered via `add_index`, in
    /// registration order. Also serves as the write-lock serializing snapshot replacement so a
    /// concurrent `add_index` and refresh can't clobber each other. Index handles are positions
    /// into this list, and every snapshot rebuilds indexes in this order, so a handle stays
    /// valid across refreshes.
    index_specs: Mutex<Vec<(Case, Vec<String>)>>,
}

/// An enrichment table backed by a periodically-refreshed HTTP dataset.
#[derive(Clone)]
pub struct HttpTable {
    shared: Arc<Shared>,
}

impl HttpTable {
    /// Build the table: load an initial snapshot (from a fresh on-disk cache if available,
    /// otherwise by fetching), then spawn the background refresh task when a refresh interval
    /// is configured. If the initial load fails and no cache is available, the table starts
    /// empty rather than failing the build (which would abort the whole topology at startup),
    /// and a background retry recovers the data — see the fail-open comment inside. Set
    /// `require_initial_load` to opt back into failing the build in that case.
    pub async fn new(
        config: HttpConfig,
        globals: &crate::config::GlobalOptions,
    ) -> crate::Result<Self> {
        let timezone = globals.timezone();
        let ctx = FetchContext::new(&config, timezone, &globals.proxy)?;

        let cache_path = resolve_cache_path(&config, globals);

        // Prefer a fresh cache for an instant start; otherwise fetch now.
        //
        // `started_empty` tracks the fail-open case where neither a fresh fetch nor any cache
        // (fresh or stale) could produce data. Building must not fail in that case: an
        // enrichment table that returns `Err` here aborts the whole topology at startup (the
        // builder collects it as a configuration error, and the process exits with
        // `exitcode::CONFIG`). A transient upstream outage — a 404/503 from the endpoint, DNS
        // failure, etc. — would then crashloop Vector rather than degrade one lookup source.
        // Instead we start with an empty snapshot (lookups simply match nothing, exactly as an
        // empty dataset would) and rely on the background refresh to populate it once the
        // endpoint recovers. This mirrors the fail-open behavior of every other path in this
        // table (stale-cache fallback, keep-last-good on refresh failure) and the config-reload
        // path, which already logs and keeps serving rather than aborting.
        let (rows, from_fresh_cache, started_empty) =
            match fresh_cache(&config, cache_path.as_ref()) {
                Some(rows) => (rows, true, false),
                None => match ctx.fetch_all_pages().await {
                    Ok(rows) => (rows, false, false),
                    Err(e) => {
                        // Fetch failed on first build: fall back to a stale cache if one exists,
                        // otherwise start empty and let the background refresh recover.
                        match any_cache(cache_path.as_ref()) {
                            Some(stale) => {
                                warn!(
                                    message = "Initial HTTP enrichment table fetch failed; serving stale cached snapshot.",
                                    url = %config.url,
                                    error = %e,
                                );
                                (stale.rows, true, false)
                            }
                            None if config.require_initial_load => {
                                // Opt-in fail-fast: the dataset is a hard dependency, so a
                                // failed initial load with no cache aborts startup (the topology
                                // builder turns this Err into `exitcode::CONFIG`).
                                return Err(format!(
                                    "failed to load HTTP enrichment table from {}: {e}",
                                    config.url
                                )
                                .into());
                            }
                            None => {
                                error!(
                                    message = "Initial HTTP enrichment table load failed and no cache is available; starting empty and will retry in the background. Lookups match nothing until a refresh succeeds.",
                                    url = %config.url,
                                    error = %e,
                                    internal_log_rate_limit = false,
                                );
                                (Vec::new(), false, true)
                            }
                        }
                    }
                },
            };

        let indexed = decode_indexed(&rows, &config.schema, timezone);
        let shared = Arc::new(Shared {
            data: ArcSwap::from_pointee(indexed),
            index_specs: Mutex::new(Vec::new()),
        });

        // Persist a freshly fetched snapshot (a cache-sourced one is already on disk). Never
        // persist the empty placeholder — that would clobber a good on-disk snapshot with
        // nothing and defeat the instant-start cache on the next restart.
        if !from_fresh_cache && !started_empty {
            persist(&config, cache_path.as_ref(), &rows).await;
        }

        // Schedule the background refresh task. It has up to two phases:
        //
        // - Recovery (only when `started_empty`, i.e. the synchronous initial load failed): retry
        //   with exponential backoff until one fetch succeeds. This fills the empty placeholder
        //   fast after a transient outage — a brief blip recovers in ~1s — without hammering a
        //   still-down endpoint (the delay doubles, capped at one attempt per minute). It applies
        //   whether or not an interval is configured, so a long `refresh_interval_secs` no longer
        //   means minutes/hours of an empty table after a failed start.
        // - Steady state (only when `refresh_interval_secs` is set): refresh periodically forever,
        //   as before.
        //
        // With no configured interval, an empty-start table stops once recovery succeeds — that
        // is exactly the "fetch once at startup, never refresh" semantics of an unset interval
        // (the failed initial fetch just left that one fetch undone), so it ends up in the same
        // state as a table that had loaded cleanly. With no interval and a successful load there
        // is nothing to do, so no task is spawned.
        let steady_interval = config
            .refresh_interval_secs
            .map(|secs| Duration::from_secs(secs.max(1)));
        if steady_interval.is_some() || started_empty {
            spawn_refresh_task(
                Arc::downgrade(&shared),
                ctx,
                config.clone(),
                cache_path,
                steady_interval,
                started_empty,
            );
        }

        Ok(Self { shared })
    }
}

impl Table for HttpTable {
    fn find_table_row<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<ObjectMap, String> {
        self.shared
            .data
            .load()
            .find_table_row(case, condition, select, wildcard, index)
    }

    fn find_table_rows<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<Vec<ObjectMap>, String> {
        self.shared
            .data
            .load()
            .find_table_rows(case, condition, select, wildcard, index)
    }

    fn add_index(&mut self, case: Case, fields: &[&str]) -> Result<IndexHandle, String> {
        let mut specs = self
            .shared
            .index_specs
            .lock()
            .expect("index_specs poisoned");

        // Delegate handle allocation entirely to `IndexedData::add_index` so there is a single
        // source of truth. Keep `index_specs` strictly 1:1 with the live `indexes` by mirroring
        // exactly what it did: a genuinely new index returns a handle at `indexes.len() - 1`
        // (== the old length, == the current `specs.len()`), so we push its spec at that same
        // position; a reused handle (its own dedup by normalized column positions matched an
        // existing index) returns an in-range handle and we must NOT push, or `specs` would grow
        // past `indexes` and every later handle would shift after the next `reapply_indexes`.
        let mut new = (**self.shared.data.load()).clone();
        let handle = new.add_index(case, fields)?;
        self.shared.data.store(Arc::new(new));

        let normalized = fields.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        if handle.0 == specs.len() {
            specs.push((case, normalized));
        }
        debug_assert!(
            handle.0 < specs.len(),
            "index handle {} out of sync with specs (len {})",
            handle.0,
            specs.len()
        );
        Ok(handle)
    }

    fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
        self.shared.data.load().index_fields()
    }

    /// The table refreshes itself on its own interval, so it never needs the topology to
    /// reload it in response to external file changes.
    fn needs_reload(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for HttpTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.shared.data.load();
        write!(
            f,
            "Http {} row(s) {} index(es)",
            data.len(),
            data.index_count()
        )
    }
}

/// Perform one fetch and, on success, atomically swap in the new indexed snapshot (rebuilding
/// the registered indexes) and persist it. Returns `true` on a successful fetch, `false` on a
/// failed one (the previous snapshot is left in place). Takes the already-upgraded `Shared` so
/// the caller controls the `Weak` lifetime check.
async fn refresh_once(
    shared: &Shared,
    ctx: &FetchContext,
    config: &HttpConfig,
    cache_path: Option<&std::path::PathBuf>,
) -> bool {
    match ctx.fetch_all_pages().await {
        Ok(rows) => {
            let mut new = decode_indexed(&rows, &config.schema, ctx.timezone);
            {
                let specs = shared.index_specs.lock().expect("index_specs poisoned");
                new.reapply_indexes(&specs);
                shared.data.store(Arc::new(new));
            }
            persist(config, cache_path, &rows).await;
            trace!(message = "Refreshed HTTP enrichment table.", url = %config.url);
            true
        }
        Err(e) => {
            warn!(
                message = "Failed to refresh HTTP enrichment table; keeping previous data.",
                url = %config.url,
                error = %e,
            );
            false
        }
    }
}

/// Spawn the detached background refresh task. It exits when every table handle has been
/// dropped (the `Weak` upgrade fails), for example on a config reload.
///
/// The task has up to two phases:
/// - **Recovery** (when `recover_empty` is set, i.e. the table started empty because its initial
///   load failed): retry with exponential backoff — [`INITIAL_RETRY_BACKOFF`], doubling, capped
///   at [`MAX_RETRY_BACKOFF`] — until one fetch succeeds. This populates the empty placeholder
///   quickly after a transient outage without hammering a still-down endpoint.
/// - **Steady state** (when `steady_interval` is `Some`): refresh periodically forever.
///
/// If `steady_interval` is `None`, the task ends once recovery succeeds (an unset
/// `refresh_interval_secs` is "fetch once, never refresh"). At least one of `recover_empty` /
/// `steady_interval` is always meaningfully set by the caller.
fn spawn_refresh_task(
    shared: std::sync::Weak<Shared>,
    ctx: FetchContext,
    config: HttpConfig,
    cache_path: Option<std::path::PathBuf>,
    steady_interval: Option<Duration>,
    recover_empty: bool,
) {
    tokio::spawn(async move {
        // Recovery phase: exponential backoff until the first successful fetch.
        if recover_empty {
            let mut backoff = INITIAL_RETRY_BACKOFF;
            loop {
                tokio::time::sleep(backoff).await;
                // Stop once the table is gone (all handles dropped, e.g. a config reload).
                let Some(shared) = shared.upgrade() else {
                    return;
                };
                if refresh_once(&shared, &ctx, &config, cache_path.as_ref()).await {
                    debug!(
                        message = "HTTP enrichment table recovered its initial snapshot.",
                        url = %config.url,
                    );
                    break;
                }
                backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
            }
        }

        // Steady-state phase: periodic refresh on the configured interval, if any.
        let Some(interval) = steady_interval else {
            return;
        };
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately; skip it since we already have data at this point
        // (either loaded at build time or just recovered above).
        ticker.tick().await;
        loop {
            ticker.tick().await;
            // Stop once the table is gone.
            let Some(shared) = shared.upgrade() else {
                break;
            };
            refresh_once(&shared, &ctx, &config, cache_path.as_ref()).await;
        }
    });
}

/// Everything needed to perform a (possibly paginated) fetch, independent of the mutable
/// snapshot state.
struct FetchContext {
    client: HttpClient,
    timezone: TimeZone,
    config: HttpConfig,
}

impl FetchContext {
    fn new(config: &HttpConfig, timezone: TimeZone, proxy: &ProxyConfig) -> crate::Result<Self> {
        let tls = TlsSettings::from_options(config.tls.as_ref())?;
        // Honor the global `proxy` config, overlaid with the standard HTTP_PROXY/HTTPS_PROXY/
        // NO_PROXY environment variables — otherwise enrichment fetches would bypass a
        // deployment's configured egress proxy. The table has no per-component proxy config, so
        // the component layer is the default.
        let proxy = ProxyConfig::merge_with_env(proxy, &ProxyConfig::default());
        let client = HttpClient::new(tls, &proxy)?;
        Ok(Self {
            client,
            timezone,
            config: config.clone(),
        })
    }

    /// Fetch every page of the dataset and return the accumulated row objects.
    async fn fetch_all_pages(&self) -> Result<Vec<serde_json::Value>, String> {
        let mut rows = Vec::new();
        // Query parameters that change per page (cursor token / offset). For offset pagination
        // the very first request must already carry `offset=0` and the configured `limit` (see
        // `initial_page_params`); otherwise page 0 is fetched with no bounds and the server
        // applies its own default page size, which either truncates the dataset (a short first
        // page trips the early-exit below) or re-collects rows on the next page (duplicates).
        let mut extra_params = initial_page_params(&self.config.pagination);

        for page in 0..self.config.limits.max_pages {
            let body = self.fetch_page(&extra_params).await?;
            let items = extract_items(&body, &self.config.response.items_pointer)?;
            let page_len = items.len();
            rows.extend(items);

            if rows.len() >= self.config.limits.max_rows {
                warn!(
                    message = "HTTP enrichment table reached max_rows; truncating.",
                    url = %self.config.url,
                    max_rows = self.config.limits.max_rows,
                    rows = rows.len(),
                );
                break;
            }

            match &self.config.pagination {
                PaginationConfig::None => break,
                PaginationConfig::Cursor {
                    token_pointer,
                    token_param,
                } => match next_cursor_token(&body, token_pointer) {
                    Some(token) => {
                        extra_params = vec![(token_param.clone(), token)];
                    }
                    None => break,
                },
                PaginationConfig::Offset {
                    offset_param,
                    limit_param,
                    page_size,
                } => {
                    // A short page means we've reached the end.
                    if page_len < *page_size {
                        break;
                    }
                    let next_offset = (page + 1) * *page_size;
                    extra_params = vec![
                        (offset_param.clone(), next_offset.to_string()),
                        (limit_param.clone(), page_size.to_string()),
                    ];
                }
            }

            if page + 1 == self.config.limits.max_pages {
                warn!(
                    message = "HTTP enrichment table reached max_pages; truncating.",
                    url = %self.config.url,
                    max_pages = self.config.limits.max_pages,
                );
            }
        }

        Ok(rows)
    }

    /// Perform a single HTTP request with the given extra query parameters and parse the JSON
    /// response body.
    async fn fetch_page(
        &self,
        extra_params: &[(String, String)],
    ) -> Result<serde_json::Value, String> {
        let uri = build_uri(&self.config.url, extra_params)?;

        let method: http::Method = self.config.method.into();
        let mut builder = Request::builder().method(method).uri(uri);
        for (name, value) in &self.config.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let body = self
            .config
            .body
            .clone()
            .map(Body::from)
            .unwrap_or_else(Body::empty);
        let mut request = builder
            .body(body)
            .map_err(|e| format!("failed to build request: {e}"))?;

        if let Some(auth) = &self.config.auth {
            auth.apply(&mut request);
        }

        // The timeout must cover the whole request — both sending it and reading the response
        // body. Scoping it to `send` alone would leave an endpoint that returns headers and then
        // stalls the body stream able to hang this fetch (and therefore all future refreshes)
        // indefinitely.
        let url = &self.config.url;
        let read = async {
            let response = self
                .client
                .send(request)
                .await
                .map_err(|e| format!("request to {url} failed: {e}"))?;
            let status = response.status();
            let body_bytes = response
                .into_body()
                .collect()
                .await
                .map(|c| c.to_bytes())
                .map_err(|e| format!("failed to read response body: {e}"))?;
            Ok::<_, String>((status, body_bytes))
        };

        let timeout = Duration::from_secs(self.config.request_timeout_secs.max(1));
        let (status, body_bytes) = match tokio::time::timeout(timeout, read).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(format!(
                    "request to {} timed out after {}s",
                    self.config.url, self.config.request_timeout_secs
                ));
            }
        };

        if !status.is_success() {
            let text = String::from_utf8_lossy(&body_bytes);
            return Err(format!("{} returned {}: {}", self.config.url, status, text));
        }

        serde_json::from_reader(body_bytes.reader())
            .map_err(|e| format!("failed to parse JSON response: {e}"))
    }
}

/// Extract the array of row objects from a response body using the configured JSON Pointer.
/// An empty pointer means the whole body is the array.
fn extract_items(
    body: &serde_json::Value,
    pointer: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let target = if pointer.is_empty() {
        body
    } else {
        body.pointer(pointer)
            .ok_or_else(|| format!("items_pointer '{pointer}' not found in response"))?
    };

    match target {
        serde_json::Value::Array(arr) => Ok(arr.clone()),
        _ => Err(format!(
            "expected a JSON array at items_pointer '{pointer}', found a different type"
        )),
    }
}

/// Read the next-page token from a response body. Returns `None` (stop paging) when the token
/// is absent, `null`, or empty.
fn next_cursor_token(body: &serde_json::Value, pointer: &str) -> Option<String> {
    match body.pointer(pointer) {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) if s.is_empty() => None,
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        Some(_) => None,
    }
}

/// The query parameters the first page request must carry for a given pagination strategy.
///
/// Offset pagination must send `offset=0` and the configured `limit` from the very first
/// request so the server uses the configured page size rather than its own default. `None` and
/// `Cursor` strategies start with no extra parameters (a cursor is only known after the first
/// response).
fn initial_page_params(pagination: &PaginationConfig) -> Vec<(String, String)> {
    match pagination {
        PaginationConfig::Offset {
            offset_param,
            limit_param,
            page_size,
        } => vec![
            (offset_param.clone(), "0".to_string()),
            (limit_param.clone(), page_size.to_string()),
        ],
        PaginationConfig::None | PaginationConfig::Cursor { .. } => Vec::new(),
    }
}

/// Build a URI from the base URL plus extra query parameters (appended to any already present).
fn build_uri(base: &str, extra_params: &[(String, String)]) -> Result<Uri, String> {
    if extra_params.is_empty() {
        return base
            .parse()
            .map_err(|e| format!("invalid url '{base}': {e}"));
    }

    let query = extra_params
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                percent_encode(k.as_bytes(), NON_ALPHANUMERIC),
                percent_encode(v.as_bytes(), NON_ALPHANUMERIC)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    let sep = if base.contains('?') { '&' } else { '?' };
    let url = format!("{base}{sep}{query}");
    url.parse().map_err(|e| format!("invalid url '{url}': {e}"))
}

/// Decode row objects into an indexed table body, applying schema-based type coercion.
fn decode_indexed(
    rows: &[serde_json::Value],
    schema: &HashMap<String, String>,
    timezone: TimeZone,
) -> IndexedData {
    // Column set is the union of object keys across all rows, in first-seen order.
    let mut columns: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for key in obj.keys() {
                if seen.insert(key.clone()) {
                    columns.push(key.clone());
                }
            }
        }
    }

    let null = serde_json::Value::Null;
    let data = rows
        .iter()
        .filter_map(|row| row.as_object())
        .map(|obj| {
            columns
                .iter()
                .map(|col| coerce_cell(schema, timezone, col, obj.get(col).unwrap_or(&null)))
                .collect::<Vec<Value>>()
        })
        .collect::<Vec<_>>();

    IndexedData::new(columns, data)
}

/// Coerce a single JSON cell into a VRL [`Value`], honoring the configured schema type for the
/// column. Non-schema columns use the natural JSON→VRL mapping.
///
/// The schema type is applied to any scalar JSON value — string, number, or boolean — not just
/// strings. A JSON number or boolean is rendered to its textual form and run through the same
/// conversion a string would be, so (for example) a `string` schema on a numeric column yields
/// `Bytes` rather than `Integer`. This matters because lookup keys are hashed from the VRL
/// value's type-specific encoding, so a mismatch between the declared and actual type would make
/// lookups silently never match. `null` and non-scalar values (arrays/objects) cannot take a
/// scalar schema type and keep the natural mapping. On conversion failure we fall back to the
/// natural mapping so one bad cell doesn't drop the whole dataset.
fn coerce_cell(
    schema: &HashMap<String, String>,
    timezone: TimeZone,
    column: &str,
    value: &serde_json::Value,
) -> Value {
    let Some(format) = schema.get(column) else {
        return json_to_vrl_value(value);
    };

    // The textual form to feed the conversion, for scalar cells only.
    let text = match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        // null / array / object: a scalar schema type doesn't apply — keep the natural mapping.
        _ => return json_to_vrl_value(value),
    };

    match parse_typed(format, timezone, &text) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                message = "Failed to coerce HTTP enrichment table column; using raw value.",
                column = %column,
                error = %e,
            );
            json_to_vrl_value(value)
        }
    }
}

/// Parse a string value according to a schema format spec, mirroring the `file` table's
/// `date` / `date|<fmt>` / `<conversion>` handling.
fn parse_typed(format: &str, timezone: TimeZone, value: &str) -> Result<Value, String> {
    use chrono::TimeZone as _;

    let mut split = format.splitn(2, '|').map(|segment| segment.trim());
    Ok(match (split.next(), split.next()) {
        (Some("date"), None) => Value::Timestamp(
            chrono::FixedOffset::east_opt(0)
                .expect("invalid timestamp")
                .from_utc_datetime(
                    &chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                        .map_err(|_| format!("unable to parse date {value}"))?
                        .and_hms_opt(0, 0, 0)
                        .expect("invalid timestamp"),
                )
                .into(),
        ),
        (Some("date"), Some(fmt)) => Value::Timestamp(
            chrono::FixedOffset::east_opt(0)
                .expect("invalid timestamp")
                .from_utc_datetime(
                    &chrono::NaiveDate::parse_from_str(value, fmt)
                        .map_err(|_| format!("unable to parse date {value}"))?
                        .and_hms_opt(0, 0, 0)
                        .expect("invalid timestamp"),
                )
                .into(),
        ),
        _ => {
            let conversion = Conversion::parse(format, timezone).map_err(|e| e.to_string())?;
            conversion
                .convert(Bytes::copy_from_slice(value.as_bytes()))
                .map_err(|_| format!("unable to parse {value}"))?
        }
    })
}

// ---- On-disk snapshot cache ------------------------------------------------------------

/// A persisted snapshot: the raw fetched row objects plus the time they were fetched.
#[derive(Serialize, Deserialize)]
struct CachedSnapshot {
    /// Seconds since the Unix epoch when the snapshot was fetched.
    fetched_at_secs: u64,
    /// The raw row objects, exactly as fetched (decoding/coercion is re-run on load).
    rows: Vec<serde_json::Value>,
}

/// Resolve the on-disk cache path for this table, or `None` if persistence is disabled or no
/// writable `data_dir` is available.
fn resolve_cache_path(
    config: &HttpConfig,
    globals: &crate::config::GlobalOptions,
) -> Option<std::path::PathBuf> {
    if !config.persist {
        return None;
    }
    match globals.resolve_and_make_data_subdir(None, "enrichment_tables/http") {
        Ok(dir) => {
            // The table's own component name isn't available here, so derive a stable filename
            // from everything that determines the fetched dataset. Hashing only the URL would
            // make two tables that share a URL but differ in headers/body/pagination/method/
            // schema collide on the same file, clobbering each other's snapshot.
            let name = format!("{:016x}.json", cache_key(config));
            Some(dir.join(name))
        }
        Err(e) => {
            warn!(
                message = "Persistence enabled for HTTP enrichment table but no writable data_dir; disabling cache.",
                url = %config.url,
                error = %e,
            );
            None
        }
    }
}

/// A stable hash over the fields that determine what dataset a fetch returns, used to name the
/// on-disk snapshot so distinct tables get distinct cache files.
///
/// `headers` and `schema` are `HashMap`s whose iteration order is not stable across processes,
/// so they are sorted before hashing — otherwise the key would vary run to run and the cache
/// would never hit on restart. Fields that don't affect the returned data (`persist`,
/// `refresh_interval_secs`, `max_cache_age_secs`, `limits`) are intentionally excluded; changing
/// them only causes a harmless cache miss, never a collision.
fn cache_key(config: &HttpConfig) -> u64 {
    use std::hash::Hash;

    let mut hasher = seahash::SeaHasher::default();
    config.url.hash(&mut hasher);
    format!("{:?}", config.method).hash(&mut hasher);
    config.body.hash(&mut hasher);

    let mut headers: Vec<_> = config.headers.iter().collect();
    headers.sort();
    headers.hash(&mut hasher);

    let mut schema: Vec<_> = config.schema.iter().collect();
    schema.sort();
    schema.hash(&mut hasher);

    // Enums/structs with deterministic serialization (no maps) — hash their serialized form so
    // any future variant/field is covered without enumerating it here.
    serde_json::to_string(&config.pagination)
        .unwrap_or_default()
        .hash(&mut hasher);
    serde_json::to_string(&config.response)
        .unwrap_or_default()
        .hash(&mut hasher);
    serde_json::to_string(&config.auth)
        .unwrap_or_default()
        .hash(&mut hasher);

    std::hash::Hasher::finish(&hasher)
}

/// Load the cache only if it exists and is within `max_cache_age_secs`.
fn fresh_cache(
    config: &HttpConfig,
    cache_path: Option<&std::path::PathBuf>,
) -> Option<Vec<serde_json::Value>> {
    let snapshot = any_cache(cache_path)?;
    match config.max_cache_age_secs {
        None => Some(snapshot.rows),
        Some(max_age) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if now.saturating_sub(snapshot.fetched_at_secs) <= max_age {
                Some(snapshot.rows)
            } else {
                None
            }
        }
    }
}

/// Load the cached snapshot regardless of age.
fn any_cache(cache_path: Option<&std::path::PathBuf>) -> Option<CachedSnapshot> {
    let path = cache_path?;
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Persist the fetched rows to the cache path, if configured. Best-effort: failures are logged
/// but never fatal.
///
/// The serialization and file write can be large (up to `max_rows`), and both are blocking
/// operations, so they run on a blocking thread via `spawn_blocking` rather than on the async
/// runtime worker — otherwise a big snapshot would stall every other task on that worker for the
/// duration of the write.
async fn persist(
    config: &HttpConfig,
    cache_path: Option<&std::path::PathBuf>,
    rows: &[serde_json::Value],
) {
    let Some(path) = cache_path else {
        return;
    };
    let fetched_at_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let snapshot = CachedSnapshot {
        fetched_at_secs,
        rows: rows.to_vec(),
    };
    let path = path.clone();
    let url = config.url.clone();

    let result = tokio::task::spawn_blocking(move || {
        let bytes = serde_json::to_vec(&snapshot).map_err(|e| e.to_string())?;
        std::fs::write(&path, bytes).map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(
                message = "Failed to persist HTTP enrichment table snapshot.",
                url = %url,
                error = %e,
            );
        }
        Err(e) => {
            warn!(
                message = "HTTP enrichment table persist task failed to join.",
                url = %url,
                error = %e,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(json: &str) -> Vec<serde_json::Value> {
        match serde_json::from_str(json).unwrap() {
            serde_json::Value::Array(a) => a,
            other => vec![other],
        }
    }

    #[test]
    fn decodes_array_of_objects_with_column_union() {
        let data = decode_indexed(
            &rows(r#"[{"id": 1, "name": "a"}, {"id": 2, "team": "x"}]"#),
            &HashMap::new(),
            TimeZone::default(),
        );
        // Columns are the union in first-seen order: id, name, team.
        assert_eq!(data.headers(), &["id", "name", "team"]);
        assert_eq!(data.len(), 2);
    }

    #[test]
    fn missing_keys_default_to_null() {
        let data = decode_indexed(
            &rows(r#"[{"id": 1, "name": "a"}, {"id": 2}]"#),
            &HashMap::new(),
            TimeZone::default(),
        );
        let row = data
            .find_table_row(
                Case::Sensitive,
                &[Condition::Equals {
                    field: "id",
                    value: Value::from(2),
                }],
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(row.get("name"), Some(&Value::Null));
    }

    #[test]
    fn coerces_schema_typed_columns() {
        let mut schema = HashMap::new();
        schema.insert("count".to_string(), "integer".to_string());
        let data = decode_indexed(
            &rows(r#"[{"id": "k", "count": "42"}]"#),
            &schema,
            TimeZone::default(),
        );
        let row = data
            .find_table_row(
                Case::Sensitive,
                &[Condition::Equals {
                    field: "id",
                    value: Value::from("k"),
                }],
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(row.get("count"), Some(&Value::Integer(42)));
    }

    #[test]
    fn schema_coerces_non_string_json_cells() {
        // A `string` schema on a column whose JSON value is a number must yield Bytes, not
        // Integer — otherwise a lookup keyed by string would silently never match.
        let mut schema = HashMap::new();
        schema.insert("id".to_string(), "string".to_string());
        let data = decode_indexed(
            &rows(r#"[{"id": 42, "name": "a"}]"#),
            &schema,
            TimeZone::default(),
        );
        // Look the row up by the string form of the numeric id; it must match.
        let row = data
            .find_table_row(
                Case::Sensitive,
                &[Condition::Equals {
                    field: "id",
                    value: Value::from("42"),
                }],
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(row.get("id"), Some(&Value::from("42")));
        assert_eq!(row.get("name"), Some(&Value::from("a")));
    }

    #[test]
    fn extract_items_whole_body() {
        let body = serde_json::json!([{"a": 1}]);
        assert_eq!(extract_items(&body, "").unwrap().len(), 1);
    }

    #[test]
    fn extract_items_nested_pointer() {
        let body = serde_json::json!({"data": [{"a": 1}, {"a": 2}]});
        assert_eq!(extract_items(&body, "/data").unwrap().len(), 2);
    }

    #[test]
    fn extract_items_wrong_type_errors() {
        let body = serde_json::json!({"data": 5});
        assert!(extract_items(&body, "/data").is_err());
    }

    #[test]
    fn cursor_token_stops_on_absent_or_empty() {
        let body = serde_json::json!({"next": "abc"});
        assert_eq!(next_cursor_token(&body, "/next"), Some("abc".to_string()));
        let body = serde_json::json!({"next": ""});
        assert_eq!(next_cursor_token(&body, "/next"), None);
        let body = serde_json::json!({"other": "abc"});
        assert_eq!(next_cursor_token(&body, "/next"), None);
    }

    #[test]
    fn build_uri_appends_query() {
        let uri = build_uri(
            "https://x.test/api",
            &[("cursor".to_string(), "a b".to_string())],
        )
        .unwrap();
        assert_eq!(uri.query(), Some("cursor=a%20b"));
        let uri = build_uri(
            "https://x.test/api?foo=1",
            &[("cursor".to_string(), "z".to_string())],
        )
        .unwrap();
        assert_eq!(uri.query(), Some("foo=1&cursor=z"));
    }

    #[test]
    fn offset_pagination_seeds_first_request_with_offset_and_limit() {
        let params = initial_page_params(&PaginationConfig::Offset {
            offset_param: "offset".to_string(),
            limit_param: "limit".to_string(),
            page_size: 500,
        });
        assert_eq!(
            params,
            vec![
                ("offset".to_string(), "0".to_string()),
                ("limit".to_string(), "500".to_string()),
            ]
        );
    }

    #[test]
    fn none_and_cursor_pagination_seed_no_first_request_params() {
        assert!(initial_page_params(&PaginationConfig::None).is_empty());
        assert!(
            initial_page_params(&PaginationConfig::Cursor {
                token_pointer: "/next".to_string(),
                token_param: "cursor".to_string(),
            })
            .is_empty()
        );
    }

    #[test]
    fn cache_key_distinguishes_same_url_different_config() {
        let base = HttpConfig {
            url: "https://api.test/data".to_string(),
            ..Default::default()
        };

        // Same config → same key (must be stable so the cache hits on restart).
        assert_eq!(cache_key(&base), cache_key(&base.clone()));

        // Differing headers must not collide.
        let mut with_header = base.clone();
        with_header
            .headers
            .insert("x-tenant".to_string(), "a".to_string());
        assert_ne!(cache_key(&base), cache_key(&with_header));

        // Differing schema must not collide.
        let mut with_schema = base.clone();
        with_schema
            .schema
            .insert("id".to_string(), "integer".to_string());
        assert_ne!(cache_key(&base), cache_key(&with_schema));

        // Differing pagination must not collide.
        let mut with_pagination = base.clone();
        with_pagination.pagination = PaginationConfig::Cursor {
            token_pointer: "/next".to_string(),
            token_param: "cursor".to_string(),
        };
        assert_ne!(cache_key(&base), cache_key(&with_pagination));
    }

    #[test]
    fn cache_key_stable_regardless_of_header_insertion_order() {
        let mut a = HttpConfig {
            url: "https://api.test/data".to_string(),
            ..Default::default()
        };
        a.headers.insert("a".to_string(), "1".to_string());
        a.headers.insert("b".to_string(), "2".to_string());

        let mut b = HttpConfig {
            url: "https://api.test/data".to_string(),
            ..Default::default()
        };
        b.headers.insert("b".to_string(), "2".to_string());
        b.headers.insert("a".to_string(), "1".to_string());

        assert_eq!(cache_key(&a), cache_key(&b));
    }

    #[tokio::test]
    async fn fetch_page_times_out_on_unresponsive_endpoint() {
        // 192.0.2.0/24 is TEST-NET-1 (RFC 5737): guaranteed not routable, so the
        // connection attempt hangs and must be cut off by the request timeout rather
        // than blocking forever.
        let config = HttpConfig {
            url: "http://192.0.2.1/api".to_string(),
            request_timeout_secs: 1,
            ..Default::default()
        };
        let ctx = FetchContext::new(&config, TimeZone::default(), &ProxyConfig::default()).unwrap();
        let err = ctx.fetch_page(&[]).await.unwrap_err();
        assert!(err.contains("timed out"), "unexpected error: {err}");
    }

    // Regression test for the startup crash: an initial load failure with no cache must NOT
    // fail the build. Before the fail-open change, `HttpTable::new` returned `Err` here, which
    // the topology builder surfaced as a configuration error and turned into a process exit
    // (`exitcode::CONFIG`) — so a 404 (or any transient upstream error) from the enrichment
    // endpoint crashlooped the whole Vector process. It must instead start with an empty
    // snapshot and recover in the background.
    #[tokio::test]
    async fn initial_load_failure_starts_empty_instead_of_failing_build() {
        // Reproduce the reported failure exactly: the endpoint answers, but with 404.
        let uri = crate::test_util::http::spawn_blackhole_http_server(|_req| async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(hyper::Body::from(
                        r#"{"error_code":"ENDPOINT_NOT_FOUND","message":"No API found"}"#,
                    ))
                    .unwrap(),
            )
        })
        .await;

        let config = HttpConfig {
            url: uri.to_string(),
            // No refresh interval configured: the "fetch once at startup" case. The build must
            // still succeed despite the failed fetch.
            refresh_interval_secs: None,
            // Disable persistence so no data_dir / cache can mask the failure path.
            persist: false,
            ..Default::default()
        };

        let table = HttpTable::new(config, &crate::config::GlobalOptions::default())
            .await
            .expect("build must succeed (start empty) when the initial fetch fails with no cache");

        // The table is empty: a `find_table_rows` returns no rows (never panics), and a
        // `find_table_row` reports "no rows found" rather than crashing the lookup path.
        assert_eq!(table.shared.data.load().len(), 0);
        let rows = table
            .find_table_rows(Case::Sensitive, &[], None, None, None)
            .unwrap();
        assert!(rows.is_empty());
    }

    // A DNS/connection failure (not just an HTTP error status) on the initial load must also be
    // fail-open, and when a refresh interval IS configured the build still succeeds empty and
    // the periodic refresh is left to recover the data.
    #[tokio::test]
    async fn initial_load_failure_with_refresh_interval_starts_empty() {
        let config = HttpConfig {
            // TEST-NET-1 (RFC 5737): guaranteed unroutable, so the connection attempt fails.
            url: "http://192.0.2.1/api".to_string(),
            request_timeout_secs: 1,
            refresh_interval_secs: Some(3600),
            persist: false,
            ..Default::default()
        };

        let table = HttpTable::new(config, &crate::config::GlobalOptions::default())
            .await
            .expect("build must succeed (start empty) on an unreachable endpoint");
        assert_eq!(table.shared.data.load().len(), 0);
    }

    // Opt-in fail-fast: with `require_initial_load = true`, a failed initial load and no cache
    // must return `Err` (the topology builder turns that into a startup abort) rather than
    // starting empty. This is the behavior a deployment selects when the dataset is a hard
    // dependency.
    #[tokio::test]
    async fn require_initial_load_fails_build_when_initial_fetch_fails() {
        let config = HttpConfig {
            // TEST-NET-1 (RFC 5737): guaranteed unroutable, so the initial fetch fails.
            url: "http://192.0.2.1/api".to_string(),
            request_timeout_secs: 1,
            require_initial_load: true,
            persist: false,
            ..Default::default()
        };

        let err = HttpTable::new(config, &crate::config::GlobalOptions::default())
            .await
            .expect_err("build must fail when require_initial_load is set and the fetch fails");
        assert!(
            err.to_string().contains("failed to load HTTP enrichment table"),
            "unexpected error: {err}"
        );
    }

    // A table that started empty because its initial load failed must recover in the background
    // via the exponential-backoff retry: the endpoint fails the first (synchronous) fetch, then
    // starts serving data, and the table populates without a configured refresh interval. With
    // `INITIAL_RETRY_BACKOFF` at 1s, recovery lands within a couple of seconds.
    #[tokio::test]
    async fn empty_start_recovers_in_background_after_transient_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Fail the first request (the synchronous initial load), then serve one row.
        let calls = Arc::new(AtomicUsize::new(0));
        let uri = {
            let calls = Arc::clone(&calls);
            crate::test_util::http::spawn_blackhole_http_server(move |_req| {
                let calls = Arc::clone(&calls);
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    let resp = if n == 0 {
                        http::Response::builder()
                            .status(http::StatusCode::SERVICE_UNAVAILABLE)
                            .body(hyper::Body::empty())
                            .unwrap()
                    } else {
                        http::Response::builder()
                            .status(http::StatusCode::OK)
                            .body(hyper::Body::from(r#"[{"id": "a", "v": "1"}]"#))
                            .unwrap()
                    };
                    Ok::<_, std::convert::Infallible>(resp)
                }
            })
            .await
        };

        let config = HttpConfig {
            url: uri.to_string(),
            // No refresh interval: recovery must still run and then stop after the first success.
            refresh_interval_secs: None,
            persist: false,
            ..Default::default()
        };

        let table = HttpTable::new(config, &crate::config::GlobalOptions::default())
            .await
            .expect("build must succeed (start empty)");
        // The synchronous load failed, so the table starts empty.
        assert_eq!(table.shared.data.load().len(), 0);

        // The background retry (first backoff ~1s) should populate the table shortly.
        let mut recovered = false;
        for _ in 0..40 {
            if table.shared.data.load().len() == 1 {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(recovered, "table did not recover in the background within the deadline");
        let row = table
            .find_table_row(
                Case::Sensitive,
                &[Condition::Equals {
                    field: "id",
                    value: Value::from("a"),
                }],
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(row.get("v"), Some(&Value::from("1")));
    }

    // Build a table directly from decoded data, bypassing the network, for lookup/index tests.
    fn table_from(data: IndexedData) -> HttpTable {
        HttpTable {
            shared: Arc::new(Shared {
                data: ArcSwap::from_pointee(data),
                index_specs: Mutex::new(Vec::new()),
            }),
        }
    }

    // Regression test for the handle/spec divergence: `IndexedData::add_index` dedups by
    // normalized column positions, so registering the same columns in a different order returns
    // the *same* handle. `HttpTable::add_index` must not push a duplicate spec in that case, or
    // `index_specs` would grow past the live indexes and later handles would shift after a
    // refresh — silently resolving to the wrong index.
    #[test]
    fn add_index_keeps_specs_aligned_when_same_columns_registered_in_different_order() {
        let mut table = table_from(decode_indexed(
            &rows(r#"[{"a": "1", "b": "2", "c": "3"}]"#),
            &HashMap::new(),
            TimeZone::default(),
        ));

        // ["a","b"] and ["b","a"] normalize to the same column positions → same handle.
        let h_ab = table.add_index(Case::Sensitive, &["a", "b"]).unwrap();
        let h_ba = table.add_index(Case::Sensitive, &["b", "a"]).unwrap();
        assert_eq!(h_ab, h_ba, "same columns must dedup to the same handle");

        // specs must stay 1:1 with the live indexes (one entry, not two).
        assert_eq!(table.shared.index_specs.lock().unwrap().len(), 1);
        assert_eq!(table.shared.data.load().index_count(), 1);

        // A genuinely different index gets the next handle.
        let h_c = table.add_index(Case::Sensitive, &["c"]).unwrap();
        assert_eq!(h_c, IndexHandle(1));
        assert_eq!(table.shared.index_specs.lock().unwrap().len(), 2);

        // After a refresh rebuilds indexes from specs, the c-handle must still resolve to c.
        {
            let specs = table.shared.index_specs.lock().unwrap();
            let mut refreshed = decode_indexed(
                &rows(r#"[{"a": "1", "b": "2", "c": "3"}]"#),
                &HashMap::new(),
                TimeZone::default(),
            );
            refreshed.reapply_indexes(&specs);
            table.shared.data.store(Arc::new(refreshed));
        }
        let row = table
            .find_table_row(
                Case::Sensitive,
                &[Condition::Equals {
                    field: "c",
                    value: Value::from("3"),
                }],
                None,
                None,
                Some(h_c),
            )
            .unwrap();
        assert_eq!(row.get("a"), Some(&Value::from("1")));
    }
}
