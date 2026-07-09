//! Service implementation for the `Clickhouse` sink.

use bytes::Bytes;
use http::{
    Request, StatusCode, Uri,
    header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE},
};
use snafu::ResultExt;

use super::{config::QuerySettingsConfig, sink::PartitionKey};
use crate::{
    http::{Auth, HttpError},
    sinks::{
        HTTPRequestBuilderSnafu, UriParseSnafu,
        clickhouse::config::Format,
        prelude::*,
        util::{
            http::{HttpRequest, HttpResponse, HttpRetryLogic, HttpServiceRequestBuilder},
            retries::RetryAction,
        },
    },
};

#[derive(Debug, Default, Clone)]
pub struct ClickhouseRetryLogic {
    inner: HttpRetryLogic<HttpRequest<PartitionKey>>,
}

impl RetryLogic for ClickhouseRetryLogic {
    type Error = HttpError;
    type Request = HttpRequest<PartitionKey>;
    type Response = HttpResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        self.inner.is_retriable_error(error)
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction<Self::Request> {
        match response.http_response.status() {
            StatusCode::INTERNAL_SERVER_ERROR => {
                let body = response.http_response.body();

                // ClickHouse surfaces most deterministic data-value errors (bad type,
                // constraint violation, overflow, ...) as HTTP 500 with a body that
                // begins `Code: {code_num}. DB::Exception: ...`. Retrying these is
                // futile — the same rows fail identically every time — and in headless
                // mode each retry fans out to another pod, wasting capacity before the
                // request is dropped anyway. Parse the numeric code and refuse to retry
                // the known-deterministic ones. Any other 500 (e.g. MEMORY_LIMIT_EXCEEDED,
                // transient replication/IO errors) stays retriable.
                //
                // Note: codes returned by ClickHouse as 4xx (e.g. 53 TYPE_MISMATCH,
                // 117 INCORRECT_DATA in current versions) never reach this arm; they are
                // handled as non-retriable by the fallback below. They are kept in the
                // set as defense-in-depth in case a version maps them back to 500.
                //
                // Reference: https://github.com/vectordotdev/vector/pull/693#issuecomment-517332654
                // Status mapping: src/Server/HTTP/exceptionCodeToHTTPStatus.cpp
                match parse_clickhouse_error_code(body) {
                    Some(code) if NON_RETRIABLE_CLICKHOUSE_ERROR_CODES.contains(&code) => {
                        RetryAction::DontRetry(
                            format!("non-retriable ClickHouse error code {code}").into(),
                        )
                    }
                    _ => RetryAction::Retry(String::from_utf8_lossy(body).to_string().into()),
                }
            }
            _ => self.inner.should_retry_response(&response.http_response),
        }
    }
}

/// ClickHouse error codes that represent deterministic, non-transient failures
/// which ClickHouse returns over HTTP as 500. Retrying them can never succeed,
/// so the sink drops the request immediately instead of exhausting its retry
/// budget. Codes ClickHouse already returns as 4xx are intentionally not listed
/// here (they short-circuit before this check) except where kept as
/// defense-in-depth against version-dependent status remapping.
///
/// Codes (see ClickHouse `src/Common/ErrorCodes.cpp`):
/// - 469 VIOLATED_CONSTRAINT       — row violates a table CHECK constraint
/// - 70  CANNOT_CONVERT_TYPE       — value not convertible to the column type
/// - 69  ARGUMENT_OUT_OF_BOUND     — value outside the type's allowed range
/// - 407 DECIMAL_OVERFLOW          — decimal value overflows its precision/scale
/// - 131 TOO_LARGE_STRING_SIZE     — string exceeds the column's fixed size
/// - 53  TYPE_MISMATCH             — 4xx today; kept as defense-in-depth
/// - 117 INCORRECT_DATA            — 4xx today; kept as defense-in-depth
const NON_RETRIABLE_CLICKHOUSE_ERROR_CODES: &[u32] = &[469, 70, 69, 407, 131, 53, 117];

/// Parses the leading numeric error code from a ClickHouse HTTP error body.
///
/// ClickHouse error bodies begin with `Code: {n}. DB::Exception: ...`. Returns
/// `None` if the body does not start with that exact prefix or the code is not a
/// parseable integer. Parsing the full integer (rather than prefix-matching the
/// bytes) avoids matching unintended codes — e.g. `b"Code: 53"` as a prefix also
/// matches `Code: 530`..`539`.
fn parse_clickhouse_error_code(body: &[u8]) -> Option<u32> {
    let rest = body.strip_prefix(b"Code: ")?;
    let digits_end = rest
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(rest.len());
    if digits_end == 0 {
        return None;
    }
    std::str::from_utf8(&rest[..digits_end]).ok()?.parse().ok()
}

#[derive(Debug, Clone)]
pub(super) struct ClickhouseServiceRequestBuilder {
    pub(super) auth: Option<Auth>,
    pub(super) endpoint: Uri,
    pub(super) skip_unknown_fields: Option<bool>,
    pub(super) date_time_best_effort: bool,
    pub(super) insert_random_shard: bool,
    pub(super) compression: Compression,
    pub(super) query_settings: QuerySettingsConfig,
}

impl HttpServiceRequestBuilder<PartitionKey> for ClickhouseServiceRequestBuilder {
    fn build(
        &self,
        mut request: HttpRequest<PartitionKey>,
    ) -> Result<Request<Bytes>, crate::Error> {
        let metadata = request.get_additional_metadata();

        let uri = set_uri_query(
            &self.endpoint,
            &metadata.database,
            &metadata.table,
            metadata.format,
            self.skip_unknown_fields,
            self.date_time_best_effort,
            self.insert_random_shard,
            self.query_settings,
        )?;

        let auth: Option<Auth> = self.auth.clone();

        // Extract format before taking payload to avoid borrow checker issues
        let format = metadata.format;
        let payload = request.take_payload();

        // Set content type based on format
        let content_type = match format {
            Format::ArrowStream => "application/vnd.apache.arrow.stream",
            _ => "application/x-ndjson",
        };

        let mut builder = Request::post(&uri)
            .header(CONTENT_TYPE, content_type)
            .header(CONTENT_LENGTH, payload.len());
        if let Some(ce) = self.compression.content_encoding() {
            builder = builder.header(CONTENT_ENCODING, ce);
        }
        if let Some(auth) = auth {
            builder = auth.apply_builder(builder);
        }

        builder
            .body(payload)
            .context(HTTPRequestBuilderSnafu)
            .map_err(Into::into)
    }
}

fn append_param<T: ToString>(uri: &mut String, key: &str, value: Option<T>) {
    if let Some(val) = value {
        uri.push_str(&format!("{}={}&", key, val.to_string()));
    }
}
fn append_param_bool(uri: &mut String, key: &str, value: Option<bool>) {
    if let Some(val) = value {
        uri.push_str(&format!("{}={}&", key, if val { 1 } else { 0 }));
    }
}

#[allow(clippy::too_many_arguments)]
fn set_uri_query(
    uri: &Uri,
    database: &str,
    table: &str,
    format: Format,
    skip_unknown: Option<bool>,
    date_time_best_effort: bool,
    insert_random_shard: bool,
    query_settings: QuerySettingsConfig,
) -> crate::Result<Uri> {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(
            "query",
            format!(
                "INSERT INTO \"{}\".\"{}\" FORMAT {}",
                database,
                table.replace('\"', "\\\""),
                format
            )
            .as_str(),
        )
        .finish();

    let mut uri = uri.to_string();
    if !uri.ends_with('/') {
        uri.push('/');
    }

    uri.push_str("?input_format_import_nested_json=1&");
    append_param_bool(&mut uri, "input_format_skip_unknown_fields", skip_unknown);
    if date_time_best_effort {
        uri.push_str("date_time_input_format=best_effort&")
    }
    if insert_random_shard {
        uri.push_str("insert_distributed_one_random_shard=1&")
    }
    append_param_bool(
        &mut uri,
        "async_insert",
        query_settings.async_insert_settings.enabled,
    );
    append_param_bool(
        &mut uri,
        "wait_for_async_insert",
        query_settings.async_insert_settings.wait_for_processing,
    );
    append_param(
        &mut uri,
        "wait_for_async_insert_timeout",
        query_settings
            .async_insert_settings
            .wait_for_processing_timeout,
    );
    append_param_bool(
        &mut uri,
        "async_insert_deduplicate",
        query_settings.async_insert_settings.deduplicate,
    );
    append_param(
        &mut uri,
        "async_insert_max_data_size",
        query_settings.async_insert_settings.max_data_size,
    );
    append_param(
        &mut uri,
        "async_insert_max_query_number",
        query_settings.async_insert_settings.max_query_number,
    );
    uri.push_str(query.as_str());

    uri.parse::<Uri>()
        .context(UriParseSnafu)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::super::config::AsyncInsertSettingsConfig;
    use super::*;
    use crate::sinks::util::retries::RetryLogic;

    fn http_response(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            http_response: http::Response::builder()
                .status(status)
                .body(Bytes::from(body.to_owned()))
                .unwrap(),
            events_byte_size: Default::default(),
            raw_byte_size: 0,
        }
    }

    fn ch_500(code: u32) -> HttpResponse {
        http_response(
            500,
            &format!("Code: {code}. DB::Exception: something happened. ({code})"),
        )
    }

    #[test]
    fn parses_leading_error_code() {
        assert_eq!(
            parse_clickhouse_error_code(b"Code: 469. DB::Exception: ..."),
            Some(469)
        );
        assert_eq!(parse_clickhouse_error_code(b"Code: 53. foo"), Some(53));
    }

    #[test]
    fn parse_rejects_non_matching_bodies() {
        assert_eq!(parse_clickhouse_error_code(b"no code here"), None);
        assert_eq!(parse_clickhouse_error_code(b"Code: abc"), None);
        assert_eq!(parse_clickhouse_error_code(b"Code: "), None);
    }

    #[test]
    fn parse_handles_code_with_no_trailing_text() {
        // Body that is exactly the code with nothing after it.
        assert_eq!(parse_clickhouse_error_code(b"Code: 469"), Some(469));
    }

    #[test]
    fn parse_does_not_confuse_53_with_530() {
        // The old prefix match on `b"Code: 53"` would have matched 530..539.
        assert_eq!(parse_clickhouse_error_code(b"Code: 530. foo"), Some(530));
        assert!(!NON_RETRIABLE_CLICKHOUSE_ERROR_CODES.contains(&530));
    }

    #[test]
    fn does_not_retry_deterministic_500_error_codes() {
        let logic = ClickhouseRetryLogic::default();
        // 469 is the observed VIOLATED_CONSTRAINT case; the rest are the other
        // deterministic data-value errors ClickHouse returns as 500.
        for code in [469, 70, 69, 407, 131, 53, 117] {
            assert!(
                logic
                    .should_retry_response(&ch_500(code))
                    .is_not_retryable(),
                "expected code {code} to be non-retriable"
            );
        }
    }

    #[test]
    fn retries_unknown_500_error_codes() {
        let logic = ClickhouseRetryLogic::default();
        // e.g. 241 MEMORY_LIMIT_EXCEEDED — transient, must keep retrying.
        assert!(logic.should_retry_response(&ch_500(241)).is_retryable());
        // A 500 without a parseable code body is also retried (fail open).
        assert!(
            logic
                .should_retry_response(&http_response(500, "opaque error"))
                .is_retryable()
        );
    }

    #[test]
    fn retries_500_with_code_prefixed_substring_of_blacklisted() {
        let logic = ClickhouseRetryLogic::default();
        // 4690 must not be treated as 469.
        assert!(logic.should_retry_response(&ch_500(4690)).is_retryable());
    }

    #[test]
    fn does_not_retry_4xx() {
        let logic = ClickhouseRetryLogic::default();
        // Client errors (e.g. 400 for TYPE_MISMATCH in current CH) fall through
        // to the inner logic, which does not retry them.
        assert!(
            logic
                .should_retry_response(&http_response(400, "Code: 53. type mismatch"))
                .is_not_retryable()
        );
    }

    #[test]
    fn retries_generic_5xx() {
        let logic = ClickhouseRetryLogic::default();
        // 503 is transient and not the special-cased 500 path.
        assert!(
            logic
                .should_retry_response(&http_response(503, "unavailable"))
                .is_retryable()
        );
    }

    #[test]
    fn encode_valid() {
        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_table",
            Format::JsonEachRow,
            Some(false),
            true,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     input_format_skip_unknown_fields=0&\
                                     date_time_input_format=best_effort&\
                                     query=INSERT+INTO+%22my_database%22.%22my_table%22+FORMAT+JSONEachRow"
        );

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonEachRow,
            Some(false),
            false,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     input_format_skip_unknown_fields=0&\
                                     query=INSERT+INTO+%22my_database%22.%22my_%5C%22table%5C%22%22+FORMAT+JSONEachRow"
        );

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonAsObject,
            Some(true),
            true,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     input_format_skip_unknown_fields=1&\
                                     date_time_input_format=best_effort&\
                                     query=INSERT+INTO+%22my_database%22.%22my_%5C%22table%5C%22%22+FORMAT+JSONAsObject"
        );

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonAsObject,
            None,
            true,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     date_time_input_format=best_effort&\
                                     query=INSERT+INTO+%22my_database%22.%22my_%5C%22table%5C%22%22+FORMAT+JSONAsObject"
        );

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonAsObject,
            None,
            true,
            false,
            QuerySettingsConfig {
                async_insert_settings: AsyncInsertSettingsConfig {
                    enabled: Some(true),
                    wait_for_processing: Some(true),
                    wait_for_processing_timeout: Some(500),
                    ..AsyncInsertSettingsConfig::default()
                },
            },
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     date_time_input_format=best_effort&\
                                     async_insert=1&\
                                     wait_for_async_insert=1&\
                                     wait_for_async_insert_timeout=500&\
                                     query=INSERT+INTO+%22my_database%22.%22my_%5C%22table%5C%22%22+FORMAT+JSONAsObject"
        );
    }

    #[test]
    fn encode_invalid() {
        set_uri_query(
            &"localhost:80".parse().unwrap(),
            "my_database",
            "my_table",
            Format::JsonEachRow,
            Some(false),
            false,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap_err();
    }
}
