//! Schema fetching and Arrow schema construction for ClickHouse tables.

use std::collections::HashMap;
use std::time::Duration;

use arrow::datatypes::{Field, Schema};
use async_trait::async_trait;
use http::{Request, StatusCode};
use hyper::Body;
use serde::Deserialize;
use vector_lib::codecs::encoding::format::{
    ArrowEncodingError, COERCE_DEFAULT_METADATA_KEY, SchemaProvider,
};

use crate::{
    common::backoff::ExponentialBackoff,
    http::{Auth, HttpClient},
    sinks::{
        clickhouse::service::ClickhouseRetryLogic,
        util::{TowerRequestSettings, http::HttpResponse, retries::RetryLogic},
    },
};

use super::parser::{clickhouse_type_to_arrow, unwrap_type_modifiers};

#[derive(Debug, Deserialize)]
struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
}

/// URL-encodes a string for use in HTTP query parameters.
fn url_encode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

/// Retry policy for the startup schema fetch.
///
/// If every attempt fails the sink falls back to JSONEachRow until restart. Sinks are built one at
/// a time, so this budget also delays topology start.
#[derive(Clone, Copy, Debug)]
pub struct SchemaFetchRetryPolicy {
    /// Total attempts, including the first. At least 1.
    pub max_attempts: usize,
    /// Delay before the first retry; doubles on each subsequent retry.
    pub initial_backoff: Duration,
    /// Upper bound on any single retry delay.
    pub max_backoff: Duration,
    /// Deadline for one attempt (request + body). `HttpClient` has no timeout of its own, so a
    /// blackholed endpoint would otherwise hang sink construction indefinitely.
    pub attempt_timeout: Duration,
}

impl SchemaFetchRetryPolicy {
    /// The production policy. The fetch bypasses the sink's Tower stack, so its timeout and
    /// backoff settings are applied here instead.
    pub const fn from_request_settings(
        request: &TowerRequestSettings,
        max_attempts: usize,
    ) -> Self {
        Self {
            max_attempts,
            initial_backoff: request.retry_initial_backoff,
            max_backoff: request.retry_max_duration,
            attempt_timeout: request.timeout,
        }
    }
}

/// Outcome of a single failed schema-fetch attempt.
#[derive(Debug)]
enum SchemaFetchAttemptError {
    /// Transport error, timeout, truncated body, or a status the insert path would retry.
    Retriable(String),
    /// Deterministic failure (4xx, non-retriable ClickHouse code, unparseable or unsupported
    /// schema) that would fail identically on every attempt.
    Permanent(String),
}

/// Longest error-body excerpt carried into log lines; ClickHouse exceptions can embed stack traces.
const MAX_ERROR_BODY_CHARS: usize = 512;

/// Fetches the table schema, retrying retriable failures per `policy`. Permanent failures and
/// exhausted retries return the last error, and the caller falls back to JSONEachRow.
pub async fn fetch_table_schema_with_retry(
    client: &HttpClient,
    endpoint: &str,
    database: &str,
    table: &str,
    auth: Option<&Auth>,
    retry_logic: &ClickhouseRetryLogic,
    policy: SchemaFetchRetryPolicy,
) -> crate::Result<Schema> {
    let max_attempts = policy.max_attempts.max(1);
    // `from_millis(2)` doubles each step; `factor` scales it so the first delay is `initial_backoff`.
    let mut backoff = ExponentialBackoff::from_millis(2)
        .factor((policy.initial_backoff.as_millis() as u64 / 2).max(1))
        .max_delay(policy.max_backoff);

    for attempt in 1..=max_attempts {
        let result = tokio::time::timeout(
            policy.attempt_timeout,
            fetch_table_schema(client, endpoint, database, table, auth, retry_logic),
        )
        .await
        .unwrap_or_else(|_| {
            Err(SchemaFetchAttemptError::Retriable(format!(
                "schema fetch timed out after {:?}",
                policy.attempt_timeout
            )))
        });

        match result {
            Ok(schema) => {
                if attempt > 1 {
                    info!(
                        message = "ClickHouse Arrow schema fetch succeeded after retry.",
                        attempt, max_attempts,
                    );
                }
                return Ok(schema);
            }
            Err(SchemaFetchAttemptError::Permanent(error)) => return Err(error.into()),
            Err(SchemaFetchAttemptError::Retriable(error)) if attempt == max_attempts => {
                return Err(format!("{error} (gave up after {attempt} attempts)").into());
            }
            Err(SchemaFetchAttemptError::Retriable(error)) => {
                let delay = backoff.next().expect("ExponentialBackoff is unbounded");
                warn!(
                    message = "ClickHouse Arrow schema fetch failed; retrying.",
                    attempt,
                    max_attempts,
                    retry_in_ms = delay.as_millis() as u64,
                    %error,
                    // At most `max_attempts - 1` lines per sink build; the default limiter would
                    // hide every retry after the first.
                    internal_log_rate_limit = false,
                );
                metrics::counter!("clickhouse_arrow_schema_fetch_retries_total").increment(1);
                tokio::time::sleep(delay).await;
            }
        }
    }
    unreachable!("loop returns on the final attempt")
}

/// One attempt at fetching the schema for a ClickHouse table and converting it to an Arrow schema.
async fn fetch_table_schema(
    client: &HttpClient,
    endpoint: &str,
    database: &str,
    table: &str,
    auth: Option<&Auth>,
    retry_logic: &ClickhouseRetryLogic,
) -> Result<Schema, SchemaFetchAttemptError> {
    use SchemaFetchAttemptError::{Permanent, Retriable};

    // Skip non-insertable columns (MATERIALIZED/ALIAS/EPHEMERAL): the stream matches columns by
    // name, so including them fails the insert.
    let query = "SELECT name, type \
                 FROM system.columns \
                 WHERE database = {db:String} AND table = {tbl:String} \
                   AND default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL') \
                 ORDER BY position \
                 FORMAT JSONEachRow";

    // Build URI with query and parameters
    let uri = format!(
        "{}?query={}&param_db={}&param_tbl={}",
        endpoint,
        url_encode(query),
        url_encode(database),
        url_encode(table)
    );
    let mut request = Request::get(&uri).body(Body::empty()).unwrap();

    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await.map_err(|error| {
        let message = format!("Failed to fetch schema from ClickHouse: {error}");
        if error.is_retriable() {
            Retriable(message)
        } else {
            Permanent(message)
        }
    })?;

    let (parts, body) = response.into_parts();
    let status = parts.status;
    // A body cut off mid-read (`unexpected EOF`, connection reset) is a transport blip.
    let body_bytes = http_body::Body::collect(body)
        .await
        .map_err(|e| Retriable(format!("Failed to read schema response body: {e}")))?
        .to_bytes();

    if status != StatusCode::OK {
        let excerpt: String = String::from_utf8_lossy(&body_bytes)
            .trim()
            .chars()
            .take(MAX_ERROR_BODY_CHARS)
            .collect();
        let message = format!("Failed to fetch schema from ClickHouse: HTTP {status}: {excerpt}");
        // Classify exactly as the insert path would, so `non_retriable_error_codes` applies here too.
        let probe = HttpResponse {
            http_response: http::Response::from_parts(parts, body_bytes),
            events_byte_size: Default::default(),
            raw_byte_size: 0,
        };
        return Err(
            if retry_logic.should_retry_response(&probe).is_retryable() {
                Retriable(message)
            } else {
                Permanent(message)
            },
        );
    }

    let body_str = String::from_utf8(body_bytes.into())
        .map_err(|e| Permanent(format!("Failed to parse response as UTF-8: {e}")))?;
    parse_schema_from_response(&body_str).map_err(|e| Permanent(e.to_string()))
}

/// Parses the JSON response from ClickHouse and builds an Arrow schema.
fn parse_schema_from_response(response: &str) -> crate::Result<Schema> {
    let mut columns: Vec<ColumnInfo> = Vec::new();

    for line in response.lines() {
        if line.trim().is_empty() {
            continue;
        }

        let column: ColumnInfo = serde_json::from_str(line)
            .map_err(|e| format!("Failed to parse column info: {}", e))?;
        columns.push(column);
    }

    if columns.is_empty() {
        return Err("No columns found in table schema".into());
    }

    let mut fields = Vec::new();
    for column in columns {
        let (arrow_type, nullable) = clickhouse_type_to_arrow(&column.column_type)
            .map_err(|e| format!("Failed to convert column '{}': {}", column.name, e))?;
        let mut field = Field::new(&column.name, arrow_type, nullable);
        // Tag the column with the value to substitute for a missing/null instance (used only
        // when coercion is on).
        if let Some(default) = clickhouse_omitted_default(&column.column_type) {
            field = field.with_metadata(HashMap::from([(
                COERCE_DEFAULT_METADATA_KEY.to_string(),
                default,
            )]));
        }
        fields.push(field);
    }

    Ok(Schema::new(fields))
}

/// Value to substitute for a missing/null instance of a non-nullable column, mirroring the
/// type's ClickHouse omitted-column default. String/FixedString/LowCardinality -> `""`,
/// JSON -> `"{}"`, numeric/temporal scalars -> the zero value, and Map/Array/Tuple -> an
/// empty-collection signal (the codec emits an empty Map/List or a Struct of child defaults).
/// `None` for nullable columns (they stay NULL) and for still-unsupported types, where a
/// missing value errors the batch. A DEFAULT column gets this type-default, not its DEFAULT expr.
fn clickhouse_omitted_default(ch_type: &str) -> Option<String> {
    let (base, is_nullable) = unwrap_type_modifiers(ch_type);
    if is_nullable {
        return None;
    }
    if base.starts_with("JSON") {
        Some("{}".to_string())
    } else if base == "String" || base.starts_with("FixedString") {
        Some(String::new())
    } else if is_zero_default_scalar(base) {
        Some("0".to_string())
    } else if base.starts_with("Map") || base.starts_with("Array") || base.starts_with("Tuple") {
        // Empty string is a coercion signal; the codec fills an empty Map/List (or a Struct of
        // child defaults) for collection columns.
        Some(String::new())
    } else {
        None
    }
}

/// Numeric and temporal scalar types whose omitted-column default is the zero value.
fn is_zero_default_scalar(base: &str) -> bool {
    matches!(
        base,
        "Int8"
            | "Int16"
            | "Int32"
            | "Int64"
            | "UInt8"
            | "UInt16"
            | "UInt32"
            | "UInt64"
            | "Float32"
            | "Float64"
            | "Bool"
            | "Boolean"
            | "Date"
            | "Date32"
    ) || base.starts_with("DateTime")
        || base.starts_with("Decimal")
}

/// Schema provider implementation for ClickHouse tables.
#[derive(Clone, Debug)]
pub struct ClickHouseSchemaProvider {
    client: HttpClient,
    endpoint: String,
    database: String,
    table: String,
    auth: Option<Auth>,
    retry_logic: ClickhouseRetryLogic,
    retry_policy: SchemaFetchRetryPolicy,
}

impl ClickHouseSchemaProvider {
    /// Create a new ClickHouse schema provider.
    pub const fn new(
        client: HttpClient,
        endpoint: String,
        database: String,
        table: String,
        auth: Option<Auth>,
        retry_logic: ClickhouseRetryLogic,
        retry_policy: SchemaFetchRetryPolicy,
    ) -> Self {
        Self {
            client,
            endpoint,
            database,
            table,
            auth,
            retry_logic,
            retry_policy,
        }
    }
}

#[async_trait]
impl SchemaProvider for ClickHouseSchemaProvider {
    async fn get_schema(&self) -> Result<Schema, ArrowEncodingError> {
        fetch_table_schema_with_retry(
            &self.client,
            &self.endpoint,
            &self.database,
            &self.table,
            self.auth.as_ref(),
            &self.retry_logic,
            self.retry_policy,
        )
        .await
        .map_err(|e| ArrowEncodingError::SchemaFetchError {
            message: e.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use rstest::rstest;
    use std::sync::Arc;

    fn string_dictionary_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    fn map_type(key_type: DataType, value_type: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key_type, false),
                        Field::new("value", value_type, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[rstest]
    #[case("Int64", DataType::Int64)]
    #[case("String", DataType::Utf8)]
    #[case("DateTime", DataType::Timestamp(TimeUnit::Second, None))]
    #[case("Decimal(18, 2)", DataType::Decimal128(18, 2))]
    #[case("DateTime64(6)", DataType::Timestamp(TimeUnit::Microsecond, None))]
    #[case("Decimal32(4)", DataType::Decimal128(9, 4))]
    #[case("Enum8('first' = 0, 'second' = 1)", DataType::Utf8)]
    #[case(
        "Map(LowCardinality(String), String)",
        map_type(string_dictionary_type(), DataType::Utf8)
    )]
    #[case(
        "Array(LowCardinality(String))",
        DataType::List(Arc::new(Field::new("item", string_dictionary_type(), false)))
    )]
    #[case(
        "Array(Map(LowCardinality(String), String))",
        DataType::List(Arc::new(Field::new(
            "item",
            map_type(string_dictionary_type(), DataType::Utf8),
            false,
        )))
    )]
    #[case(
        "Array(DateTime64(9))",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )))
    )]
    fn test_parse_schema(#[case] clickhouse_type: &str, #[case] expected: DataType) {
        let response = serde_json::json!({"name": "column", "type": clickhouse_type}).to_string();
        let schema = parse_schema_from_response(&response).unwrap();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "column");
        assert_eq!(schema.field(0).data_type(), &expected);
    }

    #[test]
    fn test_coerce_default_metadata_tagging() {
        // Each non-nullable coercible column is tagged with its omitted-column default; nullable
        // columns and unsupported types get no tag.
        let response = r#"{"name":"s","type":"String"}
{"name":"lc","type":"LowCardinality(String)"}
{"name":"j","type":"JSON(a UInt64, b UInt64)"}
{"name":"ws","type":"Int64"}
{"name":"n","type":"Nullable(String)"}
{"name":"f","type":"Float64"}
{"name":"i32","type":"Int32"}
{"name":"b","type":"Bool"}
{"name":"dt","type":"DateTime64(3)"}
{"name":"dec","type":"Decimal(18, 2)"}
{"name":"nn","type":"Nullable(Int32)"}
"#;
        let schema = parse_schema_from_response(response).unwrap();
        let default = |i: usize| {
            schema
                .field(i)
                .metadata()
                .get(COERCE_DEFAULT_METADATA_KEY)
                .cloned()
        };
        assert_eq!(default(0).as_deref(), Some("")); // String -> ""
        assert_eq!(default(1).as_deref(), Some("")); // LowCardinality(String) -> ""
        assert_eq!(default(2).as_deref(), Some("{}")); // JSON -> "{}" (CH rejects "")
        assert_eq!(default(3).as_deref(), Some("0")); // Int64 -> "0"
        assert_eq!(default(4), None); // Nullable(String) -> stays NULL
        assert_eq!(default(5).as_deref(), Some("0")); // Float64 -> "0"
        assert_eq!(default(6).as_deref(), Some("0")); // Int32 -> "0"
        assert_eq!(default(7).as_deref(), Some("0")); // Bool -> "0"
        assert_eq!(default(8).as_deref(), Some("0")); // DateTime64 -> "0"
        assert_eq!(default(9).as_deref(), Some("0")); // Decimal -> "0"
        assert_eq!(default(10), None); // Nullable(Int32) -> stays NULL
    }

    #[test]
    fn test_schema_field_ordering() {
        let response = r#"{"name":"timestamp","type":"DateTime64(3)"}
{"name":"host","type":"String"}
{"name":"message","type":"String"}
{"name":"id","type":"Int64"}
{"name":"score","type":"Float64"}
{"name":"active","type":"Bool"}
{"name":"name","type":"String"}
"#;

        let schema = parse_schema_from_response(response).unwrap();
        assert_eq!(schema.fields().len(), 7);

        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "host");
        assert_eq!(schema.field(2).name(), "message");
        assert_eq!(schema.field(3).name(), "id");
        assert_eq!(schema.field(4).name(), "score");
        assert_eq!(schema.field(5).name(), "active");
        assert_eq!(schema.field(6).name(), "name");

        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Int64);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert_eq!(schema.field(5).data_type(), &DataType::Boolean);
    }

    mod fetch_retry {
        use std::convert::Infallible;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use hyper::service::{make_service_fn, service_fn};
        use hyper::{Body, Response, Server};

        use super::super::{
            ClickhouseRetryLogic, SchemaFetchRetryPolicy, fetch_table_schema_with_retry,
        };
        use crate::http::HttpClient;
        use crate::test_util::addr::{PortGuard, next_addr};
        use crate::tls::TlsSettings;

        const SCHEMA_BODY: &str = r#"{"name":"message","type":"String"}"#;

        /// A scripted response: `None` never answers (blackholed request).
        type Step = Option<(u16, &'static str)>;

        /// Serves `script` in order (the last step repeats). Returns the port reservation (keep
        /// it alive for the test), the endpoint, and the request counter.
        fn spawn_server(script: Vec<Step>) -> (PortGuard, String, Arc<AtomicUsize>) {
            let (guard, addr) = next_addr();
            let hits = Arc::new(AtomicUsize::new(0));
            let script = Arc::new(script);
            let hits_srv = Arc::clone(&hits);
            let make = make_service_fn(move |_| {
                let hits = Arc::clone(&hits_srv);
                let script = Arc::clone(&script);
                async move {
                    Ok::<_, Infallible>(service_fn(move |_req| {
                        let n = hits.fetch_add(1, Ordering::SeqCst);
                        let step = script[n.min(script.len() - 1)];
                        async move {
                            let Some((status, body)) = step else {
                                std::future::pending::<()>().await;
                                unreachable!()
                            };
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(Body::from(body))
                                    .unwrap(),
                            )
                        }
                    }))
                }
            });
            // Bind before returning so the first request can't race the listener.
            let server = Server::bind(&addr).serve(make);
            tokio::spawn(async move {
                let _ = server.await;
            });
            (guard, format!("http://{addr}/"), hits)
        }

        fn policy(max_attempts: usize) -> SchemaFetchRetryPolicy {
            SchemaFetchRetryPolicy {
                max_attempts,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(5),
                attempt_timeout: Duration::from_millis(500),
            }
        }

        async fn fetch(
            endpoint: &str,
            policy: SchemaFetchRetryPolicy,
        ) -> crate::Result<arrow::datatypes::Schema> {
            let client = HttpClient::new(TlsSettings::default(), &Default::default()).unwrap();
            fetch_table_schema_with_retry(
                &client,
                endpoint,
                "db",
                "tbl",
                None,
                &ClickhouseRetryLogic::default(),
                policy,
            )
            .await
        }

        #[test]
        fn production_policy_uses_request_settings() {
            let request: crate::sinks::util::TowerRequestConfig = toml::from_str(
                "timeout_secs = 7\nretry_initial_backoff_secs = 2\nretry_max_duration_secs = 9",
            )
            .unwrap();
            let policy = SchemaFetchRetryPolicy::from_request_settings(&request.into_settings(), 4);
            assert_eq!(policy.max_attempts, 4);
            assert_eq!(policy.attempt_timeout, Duration::from_secs(7));
            assert_eq!(policy.initial_backoff, Duration::from_secs(2));
            assert_eq!(policy.max_backoff, Duration::from_secs(9));
        }

        #[tokio::test]
        async fn retries_transient_failures_until_success() {
            // Proxy-relayed TOO_MANY_SIMULTANEOUS_QUERIES, then a 503, then the schema.
            let (_guard, endpoint, hits) = spawn_server(vec![
                Some((
                    500,
                    "Code: 202. DB::Exception: Too many simultaneous queries.",
                )),
                Some((503, "")),
                Some((200, SCHEMA_BODY)),
            ]);
            let schema = fetch(&endpoint, policy(5))
                .await
                .expect("third attempt succeeds");
            assert_eq!(schema.field(0).name(), "message");
            assert_eq!(hits.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn gives_up_after_max_attempts() {
            let (_guard, endpoint, hits) = spawn_server(vec![Some((503, "unavailable"))]);
            let err = fetch(&endpoint, policy(3)).await.unwrap_err().to_string();
            assert_eq!(hits.load(Ordering::SeqCst), 3);
            assert!(err.contains("HTTP 503"), "{err}");
            assert!(
                err.contains("unavailable"),
                "error should carry the body: {err}"
            );
            assert!(err.contains("gave up after 3 attempts"), "{err}");
        }

        #[tokio::test]
        async fn max_attempts_one_disables_retries() {
            let (_guard, endpoint, hits) = spawn_server(vec![Some((503, ""))]);
            fetch(&endpoint, policy(1)).await.unwrap_err();
            assert_eq!(hits.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn does_not_retry_permanent_failures() {
            for step in [
                // 4xx, e.g. UNKNOWN_TABLE / auth failure.
                (404, "Code: 60. DB::Exception: Table db.tbl does not exist."),
                // 500 carrying a code in the non-retriable set.
                (500, "Code: 469. DB::Exception: Constraint violated."),
                // Schema that can never be encoded.
                (
                    200,
                    r#"{"name":"c","type":"AggregateFunction(uniq, String)"}"#,
                ),
                // Table exists in no form the query can see.
                (200, ""),
            ] {
                let (_guard, endpoint, hits) = spawn_server(vec![Some(step)]);
                fetch(&endpoint, policy(5)).await.unwrap_err();
                assert_eq!(hits.load(Ordering::SeqCst), 1, "retried {step:?}");
            }
        }

        #[tokio::test]
        async fn retries_attempt_timeout() {
            let (_guard, endpoint, hits) = spawn_server(vec![None, Some((200, SCHEMA_BODY))]);
            let policy = SchemaFetchRetryPolicy {
                attempt_timeout: Duration::from_millis(100),
                ..policy(2)
            };
            fetch(&endpoint, policy)
                .await
                .expect("second attempt succeeds");
            assert_eq!(hits.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn retries_connection_errors() {
            // Nothing listens here: every attempt is connection-refused.
            let (_guard, addr) = next_addr();
            let endpoint = format!("http://{addr}/");
            let err = fetch(&endpoint, policy(2)).await.unwrap_err().to_string();
            assert!(err.contains("gave up after 2 attempts"), "{err}");
        }
    }
}
