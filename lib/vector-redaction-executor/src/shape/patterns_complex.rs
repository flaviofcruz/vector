//! Complex / composite pattern validators.
//!
//! The multi-part structural validators from `DataShapeValidators.java` — gRPC names, image
//! labels, stack traces, kube context, Databricks/Google hostnames, branch tags. Split out of
//! `patterns.rs` only for file size; same faithful-port discipline applies (see that module's
//! header). Validators that also consult runtime resources (NEXUS_ATTRIBUTE_ID, OOM_REASON_PIPED,
//! INFRA_DATA_MODEL_URI, RELEASE_STEP_NAME) live in `enumerated.rs`, which owns the loaded data.

use super::patterns;

fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

fn is_upper(c: u8) -> bool {
    c.is_ascii_uppercase()
}

fn is_lower(c: u8) -> bool {
    c.is_ascii_lowercase()
}

fn is_ascii_letter(c: u8) -> bool {
    c.is_ascii_alphabetic()
}

fn is_lower_hex(c: u8) -> bool {
    c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
}

/// Non-empty run of ASCII digits only (mirrors the Java isAsciiDigits used by the stack-trace port).
fn is_ascii_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// Spark Thrift structured fields.
// ---------------------------------------------------------------------------

const SPARK_THRIFT_HIVE_TYPE_NAMES: [&str; 22] = [
    "VOID",
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "STRING",
    "CHAR",
    "VARCHAR",
    "DATE",
    "TIMESTAMP",
    "INTERVAL_YEAR_MONTH",
    "INTERVAL_DAY_TIME",
    "BINARY",
    "DECIMAL",
    "ARRAY",
    "MAP",
    "STRUCT",
    "UNIONTYPE",
    "USER_DEFINED",
];

/// Semicolon-delimited Hive type names emitted for a Spark Thrift result schema.
pub fn spark_thrift_result_schema(value: &str) -> bool {
    const MAX_TOTAL_LENGTH: usize = 200;
    const TRUNCATED_SUFFIX: &str = "[TRUNCATED]";
    if !value.is_ascii() || value.len() > MAX_TOTAL_LENGTH {
        return false;
    }
    if value.len() == MAX_TOTAL_LENGTH && value.ends_with(TRUNCATED_SUFFIX) {
        let prefix = &value[..MAX_TOTAL_LENGTH - TRUNCATED_SUFFIX.len()];
        let mut tokens = prefix.split(';').peekable();
        while let Some(token) = tokens.next() {
            if tokens.peek().is_some() {
                if !SPARK_THRIFT_HIVE_TYPE_NAMES.contains(&token) {
                    return false;
                }
            } else {
                return token.is_empty()
                    || SPARK_THRIFT_HIVE_TYPE_NAMES
                        .iter()
                        .any(|type_name| type_name.starts_with(token));
            }
        }
        return false;
    }
    value.is_empty()
        || value
            .split(';')
            .all(|token| SPARK_THRIFT_HIVE_TYPE_NAMES.contains(&token))
}

/// Field-specific JSON parser for the Spark Thrift partition-size histogram.
pub fn spark_thrift_partition_size_buckets(value: &str) -> bool {
    const MAX_TOTAL_LENGTH: usize = 147;
    if value.is_empty() || value.len() > MAX_TOTAL_LENGTH || !value.is_ascii() {
        return false;
    }
    let bytes = value.as_bytes();
    let mut index = skip_json_whitespace(bytes, 0);
    if index >= bytes.len() || bytes[index] != b'{' {
        return false;
    }
    index = skip_json_whitespace(bytes, index + 1);
    if index < bytes.len() && bytes[index] == b'}' {
        return skip_json_whitespace(bytes, index + 1) == bytes.len();
    }

    let mut seen_keys: u8 = 0;
    loop {
        if index >= bytes.len() || bytes[index] != b'"' {
            return false;
        }
        let key_start = index + 1;
        index = key_start;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if index == key_start || index >= bytes.len() || bytes[index] != b'"' {
            return false;
        }
        let key = &value[key_start..index];
        let key_bit = match spark_thrift_partition_bucket_bit(key) {
            Some(bit) => bit,
            None => return false,
        };
        if seen_keys & key_bit != 0 {
            return false;
        }
        seen_keys |= key_bit;

        index = skip_json_whitespace(bytes, index + 1);
        if index >= bytes.len() || bytes[index] != b':' {
            return false;
        }
        index = skip_json_whitespace(bytes, index + 1);
        if index >= bytes.len() || !(b'1'..=b'9').contains(&bytes[index]) {
            return false;
        }
        let mut count: u64 = 0;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            count = count * 10 + u64::from(bytes[index] - b'0');
            if count > i32::MAX as u64 {
                return false;
            }
            index += 1;
        }

        index = skip_json_whitespace(bytes, index);
        if index >= bytes.len() {
            return false;
        }
        match bytes[index] {
            b',' => {
                index = skip_json_whitespace(bytes, index + 1);
            }
            b'}' => return skip_json_whitespace(bytes, index + 1) == bytes.len(),
            _ => return false,
        }
    }
}

fn skip_json_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t' | b'\n' | b'\r') {
        index += 1;
    }
    index
}

fn spark_thrift_partition_bucket_bit(key: &str) -> Option<u8> {
    match key {
        "0" => Some(1 << 0),
        "1048576" => Some(1 << 1),
        "5242880" => Some(1 << 2),
        "10485760" => Some(1 << 3),
        "20971520" => Some(1 << 4),
        "52480000" => Some(1 << 5),
        "104857600" => Some(1 << 6),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// gRPC / proto names.
// ---------------------------------------------------------------------------

/// gRPC fully-qualified service name: lowercase dotted package + UpperCamel type ending "Service".
pub fn grpc_service(v: &str) -> bool {
    const MAX_TOTAL_LEN: usize = 128;
    const MAX_SEGMENT_LEN: usize = 40;
    const MAX_TYPE_LEN: usize = 64;
    const MAX_PACKAGE_SEGMENTS: usize = 7;
    const SUFFIX: &str = "Service";
    fqn_like(
        v,
        MAX_TOTAL_LEN,
        MAX_SEGMENT_LEN,
        MAX_TYPE_LEN,
        MAX_PACKAGE_SEGMENTS,
        None,
        Some(SUFFIX),
    )
}

/// Proto fully-qualified message name: "com.databricks." + dotted package + UpperCamel type.
pub fn proto_message_fqn(v: &str) -> bool {
    const REQUIRED_PREFIX: &str = "com.databricks.";
    const MAX_TOTAL_LEN: usize = 256;
    const MAX_SEGMENT_LEN: usize = 40;
    const MAX_TYPE_LEN: usize = 80;
    const MAX_PACKAGE_SEGMENTS: usize = 9;
    fqn_like(
        v,
        MAX_TOTAL_LEN,
        MAX_SEGMENT_LEN,
        MAX_TYPE_LEN,
        MAX_PACKAGE_SEGMENTS,
        Some(REQUIRED_PREFIX),
        None,
    )
}

/// Shared body of GrpcServiceValidator / ProtoMessageFqnValidator: a lowercase dotted package path
/// (`[a-z][a-z0-9_]*` segments) followed by an UpperCamel type (`[A-Z][A-Za-z0-9]*`), with optional
/// required literal prefix and optional required type suffix.
fn fqn_like(
    v: &str,
    max_total_len: usize,
    max_segment_len: usize,
    max_type_len: usize,
    max_package_segments: usize,
    required_prefix: Option<&str>,
    type_suffix: Option<&str>,
) -> bool {
    let b = v.as_bytes();
    let len = b.len();
    if len == 0 || len > max_total_len {
        return false;
    }
    if let Some(prefix) = required_prefix {
        if !v.starts_with(prefix) {
            return false;
        }
    }
    let last_dot = match v.rfind('.') {
        Some(d) => d,
        None => return false,
    };
    // Need at least one package segment before the dot and a non-empty type name after it.
    if last_dot == 0 || last_dot == len - 1 {
        return false;
    }
    // Type name: [A-Z][A-Za-z0-9]*, <= max_type_len [, ending in the suffix].
    let type_start = last_dot + 1;
    if len - type_start > max_type_len {
        return false;
    }
    if !is_upper(b[type_start]) {
        return false;
    }
    for &c in &b[type_start + 1..] {
        if !(c.is_ascii_lowercase() || c.is_ascii_uppercase() || is_digit(c)) {
            return false;
        }
    }
    if let Some(suffix) = type_suffix {
        if !v.ends_with(suffix) {
            return false;
        }
    }
    // Package path: 1 to max_package_segments segments of [a-z][a-z0-9_]*.
    let mut segment_count = 0;
    let mut i = 0;
    while i < last_dot {
        if !is_lower(b[i]) {
            return false;
        }
        i += 1;
        let mut seg_len = 1;
        let mut at_dot = false;
        while i < last_dot && !at_dot {
            let c = b[i];
            if c == b'.' {
                at_dot = true;
            } else {
                if !(c.is_ascii_lowercase() || is_digit(c) || c == b'_') {
                    return false;
                }
                seg_len += 1;
                if seg_len > max_segment_len {
                    return false;
                }
                i += 1;
            }
        }
        segment_count += 1;
        if segment_count > max_package_segments {
            return false;
        }
        if at_dot {
            i += 1; // consume the '.'
            if i >= last_dot {
                return false; // dangling separator before the type name
            }
        }
    }
    segment_count >= 1
}

/// gRPC method (RPC) name: a camelCase identifier whose leading word is a known RPC verb.
pub fn grpc_method(v: &str) -> bool {
    const MAX_LEN: usize = 64;
    let b = v.as_bytes();
    let len = b.len();
    if len == 0 || len > MAX_LEN {
        return false;
    }
    let first = b[0];
    if !is_ascii_letter(first) {
        return false;
    }
    let mut has_lower = is_lower(first);
    for &c in &b[1..] {
        if c.is_ascii_lowercase() {
            has_lower = true;
        } else if !(c.is_ascii_uppercase() || is_digit(c)) {
            return false;
        }
    }
    // Reject ALL-CAPS tokens (e.g. SQL keywords / customer CONSTANTS).
    if !has_lower {
        return false;
    }
    // Leading camelCase word (first char + following lowercase letters, lowercased) must be a verb.
    let mut k = 1;
    while k < len && b[k].is_ascii_lowercase() {
        k += 1;
    }
    let head = v[..k].to_ascii_lowercase();
    GRPC_METHOD_HEAD_VERBS.binary_search(&head.as_str()).is_ok()
}

/// Generic error/outcome code: 1-8 UPPER_SNAKE tokens; first a known domain, last a known mode.
pub fn generic_error_code(v: &str) -> bool {
    const MAX_TOKENS: usize = 8;
    const MAX_LEN: usize = 50;
    let b = v.as_bytes();
    let len = b.len();
    if len == 0 || len > MAX_LEN {
        return false;
    }
    for &c in b {
        if !(c.is_ascii_uppercase() || c == b'_') {
            return false;
        }
    }
    // Split on '_', tracking only the first/last token bounds and count, rejecting empty tokens.
    let mut token_count = 0;
    let first_start = 0;
    let mut first_end: i64 = -1;
    let mut last_start: i64 = -1;
    let mut seg_start = 0;
    // Inclusive range: `i == len` is a sentinel end-of-string position that closes the final token
    // (b[i] is never indexed there, guarded by the `i == len ||` short-circuit), and `i` is used as
    // a value in first_end/last_start/seg_start — so this is not a plain index loop.
    #[allow(clippy::needless_range_loop)]
    for i in 0..=len {
        if i == len || b[i] == b'_' {
            if i == seg_start {
                return false; // empty token
            }
            token_count += 1;
            if token_count > MAX_TOKENS {
                return false;
            }
            if first_end < 0 {
                first_end = i as i64;
            }
            last_start = seg_start as i64;
            seg_start = i + 1;
        }
    }
    let first = &v[first_start..first_end as usize];
    let last = &v[last_start as usize..len];
    GENERIC_ERROR_PREFIX.binary_search(&first).is_ok()
        && GENERIC_ERROR_SUFFIX.binary_search(&last).is_ok()
}

// ---------------------------------------------------------------------------
// DBR / DLT / dblet image & version labels.
// ---------------------------------------------------------------------------

/// DBR runtime image label. Union of DBR release, custom build, DLT release forms.
pub fn dbr_image_label(v: &str) -> bool {
    const CUSTOM_PREFIX: &str = "custom:";
    const DLT_PREFIX: &str = "dlt:";
    const LZ4_SUFFIX: &str = ".lz4";
    const BUILD_PREFIXES: [&str; 4] = ["snapshot__", "release__", "custom-prod__", "custom-ci__"];
    const NEPHOS_PLACEHOLDER: &str = "nephos-spark-version-placeholder";
    if v.is_empty() {
        return false;
    }
    if v == NEPHOS_PLACEHOLDER {
        return true;
    }
    let mut s: &str = v;
    if let Some(rest) = s.strip_prefix(CUSTOM_PREFIX) {
        s = rest;
    } else if let Some(rest) = s.strip_prefix(DLT_PREFIX) {
        s = rest;
    }
    if let Some(rest) = s.strip_suffix(LZ4_SUFFIX) {
        s = rest;
    }
    for prefix in BUILD_PREFIXES {
        if let Some(rest) = s.strip_prefix(prefix) {
            s = rest;
            break;
        }
    }
    !s.is_empty() && dbr_label_charset(s) && dbr_starts_with_version(s) && dbr_ends_with_version(s)
}

fn dbr_label_charset(v: &str) -> bool {
    v.bytes()
        .all(|c| is_digit(c) || is_ascii_letter(c) || c == b'.' || c == b'_' || c == b'-')
}

fn dbr_starts_with_version(v: &str) -> bool {
    const CLIENT_PREFIX: &str = "client.";
    if starts_with_digits_dot(v) {
        return true;
    }
    let b = v.as_bytes();
    v.starts_with(CLIENT_PREFIX)
        && b.len() > CLIENT_PREFIX.len()
        && (is_digit(b[CLIENT_PREFIX.len()]) || b[CLIENT_PREFIX.len()] == b'x')
}

fn dbr_ends_with_version(v: &str) -> bool {
    ends_with_scala_version(v) || ends_with_image_hex_run(v) || ends_with_format_number(v)
}

/// True iff `v` starts with 1+ digits then a '.'.
fn starts_with_digits_dot(v: &str) -> bool {
    let b = v.as_bytes();
    if b.is_empty() || !is_digit(b[0]) {
        return false;
    }
    let mut i = 1;
    while i < b.len() && is_digit(b[i]) {
        i += 1;
    }
    i < b.len() && b[i] == b'.'
}

/// True iff `v` ends with "-scala" + 1+ digits + '.' + 1+ digits.
fn ends_with_scala_version(v: &str) -> bool {
    const SCALA: &str = "-scala";
    let b = v.as_bytes();
    let mut i = b.len() as i64 - 1;
    if i < 0 || !is_digit(b[i as usize]) {
        return false;
    }
    while i >= 0 && is_digit(b[i as usize]) {
        i -= 1;
    }
    if i < 0 || b[i as usize] != b'.' {
        return false;
    }
    i -= 1;
    if i < 0 || !is_digit(b[i as usize]) {
        return false;
    }
    while i >= 0 && is_digit(b[i as usize]) {
        i -= 1;
    }
    let start = i + 1 - SCALA.len() as i64;
    start >= 0 && &v[start as usize..start as usize + SCALA.len()] == SCALA
}

/// True iff `v` ends with "-image-" + 1+ lowercase hex (the DbrImageLabel "image" branch).
fn ends_with_image_hex_run(v: &str) -> bool {
    const IMAGE: &str = "-image-";
    let b = v.as_bytes();
    let mut i = b.len() as i64 - 1;
    if i < 0 || !is_lower_hex(b[i as usize]) {
        return false;
    }
    while i >= 0 && is_lower_hex(b[i as usize]) {
        i -= 1;
    }
    let start = i + 1 - IMAGE.len() as i64;
    start >= 0 && &v[start as usize..start as usize + IMAGE.len()] == IMAGE
}

fn ends_with_format_number(v: &str) -> bool {
    const FORMAT: &str = "__format-";
    const TRIMMED_SUFFIX: &str = ".trimmed";
    let s = v.strip_suffix(TRIMMED_SUFFIX).unwrap_or(v);
    let b = s.as_bytes();
    let mut i = b.len() as i64 - 1;
    if i < 0 || !is_digit(b[i as usize]) {
        return false;
    }
    while i >= 0 && is_digit(b[i as usize]) {
        i -= 1;
    }
    let start = i + 1 - FORMAT.len() as i64;
    start >= 0 && &s[start as usize..start as usize + FORMAT.len()] == FORMAT
}

/// Databricks Spark runtime version string. Four accepted families.
pub fn spark_version(v: &str) -> bool {
    const CLIENT_PREFIX: &str = "client.";
    const CUSTOM_PREFIX: &str = "custom:";
    const DLT_PREFIX: &str = "dlt:";
    if v.is_empty() {
        return false;
    }
    if v.starts_with(CUSTOM_PREFIX) {
        return v.contains("-snapshot-") && v.contains("__databricks__");
    }
    if v.starts_with(DLT_PREFIX) {
        return spark_ends_with_image_hex(v);
    }
    if v.starts_with(CLIENT_PREFIX) {
        return ends_with_scala_version(v);
    }
    if starts_with_digits_dot(v) {
        return ends_with_scala_version(v);
    }
    false
}

/// True iff `v` ends with "-image-" + exactly 7 lowercase hex chars (SparkVersion's image branch).
fn spark_ends_with_image_hex(v: &str) -> bool {
    const IMAGE: &str = "-image-";
    const IMAGE_HEX_LEN: usize = 7;
    let b = v.as_bytes();
    let n = b.len();
    if n < IMAGE.len() + IMAGE_HEX_LEN {
        return false;
    }
    for &c in &b[n - IMAGE_HEX_LEN..] {
        if !is_lower_hex(c) {
            return false;
        }
    }
    let start = n - IMAGE_HEX_LEN - IMAGE.len();
    &v[start..start + IMAGE.len()] == IMAGE
}

/// Databricks dblet machine-image label: base tokens + "-<8 hex>-<12 digits>s".
pub fn dblet_image_label(v: &str) -> bool {
    const COMMIT_LEN: usize = 8;
    const TIMESTAMP_LEN: usize = 12;
    const MIN_LEN: usize = 1 + 1 + COMMIT_LEN + 1 + TIMESTAMP_LEN + 1;
    let b = v.as_bytes();
    let n = b.len();
    if n < MIN_LEN {
        return false;
    }
    if b[n - 1] != b's' {
        return false;
    }
    let ts_start = n - 1 - TIMESTAMP_LEN;
    if ts_start == 0 || b[ts_start - 1] != b'-' {
        return false;
    }
    for &c in &b[ts_start..n - 1] {
        if !is_digit(c) {
            return false;
        }
    }
    let commit_start = ts_start - 1 - COMMIT_LEN;
    if commit_start == 0 || b[commit_start - 1] != b'-' {
        return false;
    }
    for &c in &b[commit_start..ts_start - 1] {
        if !is_lower_hex(c) {
            return false;
        }
    }
    dblet_base_tokens(b, 0, commit_start - 1)
}

/// Base: one or more non-empty "[a-z0-9_]" tokens joined by a single '-'.
fn dblet_base_tokens(b: &[u8], from: usize, to: usize) -> bool {
    if to <= from {
        return false;
    }
    let mut at_token_start = true;
    for &c in &b[from..to] {
        if c == b'-' {
            if at_token_start {
                return false;
            }
            at_token_start = true;
        } else {
            if !(is_digit(c) || c.is_ascii_lowercase() || c == b'_') {
                return false;
            }
            at_token_start = false;
        }
    }
    !at_token_start
}

/// Databricks platform channel string. Three accepted forms.
pub fn platform_channel(v: &str) -> bool {
    const CHANNEL_PREFIX: &str = "CHANNEL_NAME_";
    const CLIENT_PREFIX: &str = "CLIENT-";
    const CUSTOM_PREFIX: &str = "custom";
    const CUSTOM_SUFFIX: &str = "___default";
    const MAX_LEN: usize = 255;
    if v.is_empty() || v.len() > MAX_LEN {
        return false;
    }
    if v.starts_with(CHANNEL_PREFIX) {
        return all_from(v, CHANNEL_PREFIX.len(), |c| {
            is_digit(c) || is_upper(c) || c == b'.' || c == b'_'
        });
    }
    if v.starts_with(CLIENT_PREFIX) {
        return platform_channel_client_body(v.as_bytes(), CLIENT_PREFIX.len());
    }
    if v.starts_with(CUSTOM_PREFIX) {
        return v.ends_with(CUSTOM_SUFFIX);
    }
    false
}

/// True iff `b` from `start` is one or two non-empty "[0-9A-Z]" runs joined by a single '-'.
fn platform_channel_client_body(b: &[u8], start: usize) -> bool {
    if start >= b.len() {
        return false;
    }
    let mut seen_dash = false;
    let mut at_run_start = true;
    for &c in &b[start..] {
        if c == b'-' {
            if seen_dash || at_run_start {
                return false;
            }
            seen_dash = true;
            at_run_start = true;
        } else {
            if !(is_digit(c) || is_upper(c)) {
                return false;
            }
            at_run_start = false;
        }
    }
    !at_run_start
}

/// True iff `v` has >=1 byte at/after `start` and every such byte satisfies `pred`.
fn all_from(v: &str, start: usize, pred: impl Fn(u8) -> bool) -> bool {
    let b = v.as_bytes();
    if start >= b.len() {
        return false;
    }
    b[start..].iter().all(|&c| pred(c))
}

// ---------------------------------------------------------------------------
// Branch tag.
// ---------------------------------------------------------------------------

/// Databricks build/deploy branch tag: literal branch sentinels or the structured deploy tag.
pub fn branch_name(v: &str) -> bool {
    const MASTER: &str = "master";
    const DEVELOPMENT: &str = "development";
    const MIN_LEN: usize = 35;
    const SHA_LEN: usize = 8;
    if v == MASTER || v == DEVELOPMENT {
        return true;
    }
    let b = v.as_bytes();
    let len = b.len();
    if len < MIN_LEN {
        return false;
    }
    // Service: chars up to the first '_', all in [a-z0-9-], non-empty.
    let service_end = match v.find('_') {
        Some(i) if i >= 1 => i,
        _ => return false,
    };
    for &c in &b[..service_end] {
        if !(c.is_ascii_lowercase() || is_digit(c) || c == b'-') {
            return false;
        }
    }
    // Date block "YYYY-MM-DD" then '_'.
    let d = service_end + 1;
    if d + 10 >= len {
        return false;
    }
    if !digit_run(b, d, d + 4)
        || b[d + 4] != b'-'
        || !digit_run(b, d + 5, d + 7)
        || b[d + 7] != b'-'
        || !digit_run(b, d + 8, d + 10)
        || b[d + 10] != b'_'
    {
        return false;
    }
    // Time block "HH.MM.SS" + 'Z' then '_'.
    let t = d + 11;
    if t + 9 >= len {
        return false;
    }
    if !digit_run(b, t, t + 2)
        || b[t + 2] != b'.'
        || !digit_run(b, t + 3, t + 5)
        || b[t + 5] != b'.'
        || !digit_run(b, t + 6, t + 8)
        || b[t + 8] != b'Z'
        || b[t + 9] != b'_'
    {
        return false;
    }
    let branch_start = t + 10;
    // Trailing "_<8 lowercase hex>_<digits>", parsed from the end.
    let mut i = len as i64 - 1;
    while i >= 0 && is_digit(b[i as usize]) {
        i -= 1;
    }
    if i == len as i64 - 1 {
        return false; // no build-number digits
    }
    if i < 0 || b[i as usize] != b'_' {
        return false;
    }
    let sha_sep = i; // the '_' before the build number
    let sha_start = sha_sep - SHA_LEN as i64;
    if sha_start < 0 {
        return false;
    }
    for j in sha_start..sha_sep {
        if !is_lower_hex(b[j as usize]) {
            return false;
        }
    }
    let branch_sep = sha_start - 1; // the '_' before the sha
    if branch_sep < 0 || b[branch_sep as usize] != b'_' {
        return false;
    }
    // Branch span [branch_start, branch_sep): non-empty, chars in [A-Za-z0-9_.-].
    if branch_start as i64 >= branch_sep {
        return false;
    }
    for &c in b.iter().take(branch_sep as usize).skip(branch_start) {
        if !(is_digit(c)
            || c.is_ascii_lowercase()
            || c.is_ascii_uppercase()
            || c == b'_'
            || c == b'-'
            || c == b'.')
        {
            return false;
        }
    }
    true
}

fn digit_run(b: &[u8], from: usize, to: usize) -> bool {
    b[from..to].iter().all(|&c| is_digit(c))
}

// ---------------------------------------------------------------------------
// Hostnames.
// ---------------------------------------------------------------------------

/// Databricks-owned FQDN: lowercase [a-z0-9-] dotted labels ending in a Databricks-owned domain.
pub fn databricks_fqdn(v: &str) -> bool {
    const SUFFIXES: [&str; 6] = [
        ".databricks.com",
        ".azuredatabricks.net",
        ".databricks.us",
        ".databricks.mil",
        ".databricks.azure.us",
        ".databricks.azure.cn",
    ];
    const MAX_LEN: usize = 255;
    if v.len() > MAX_LEN || !SUFFIXES.iter().any(|s| v.ends_with(s)) {
        return false;
    }
    let mut label_empty = true;
    for &c in v.as_bytes() {
        if c == b'.' {
            if label_empty {
                return false;
            }
            label_empty = true;
        } else if c == b'-' || is_digit(c) || c.is_ascii_lowercase() {
            label_empty = false;
        } else {
            return false;
        }
    }
    !label_empty
}

/// Google API endpoint hostname: "googleapis.com" or a subdomain of ".googleapis.com".
pub fn google_api_endpoint(v: &str) -> bool {
    const ROOT: &str = "googleapis.com";
    const SUFFIX: &str = ".googleapis.com";
    const MAX_LEN: usize = 255;
    if v.len() > MAX_LEN {
        return false;
    }
    if v == ROOT {
        return true;
    }
    if !v.ends_with(SUFFIX) {
        return false;
    }
    let mut label_empty = true;
    let mut previous_char = 0u8;
    for &c in v.as_bytes() {
        if c == b'.' {
            if label_empty || previous_char == b'-' {
                return false;
            }
            label_empty = true;
        } else if c == b'-' || is_digit(c) || c.is_ascii_lowercase() {
            if label_empty && c == b'-' {
                return false;
            }
            label_empty = false;
            previous_char = c;
        } else {
            return false;
        }
    }
    !label_empty && previous_char != b'-'
}

// ---------------------------------------------------------------------------
// Redacted stack trace.
// ---------------------------------------------------------------------------

const STACK_CLASS_PREFIXES: [&str; 16] = [
    "com.databricks.",
    "org.apache.",
    "scala.",
    "java.",
    "shaded.databricks.",
    "py4j.",
    "sun.",
    "jdk.",
    "com.google.",
    "com.microsoft.",
    "com.amazonaws.",
    "net.snowflake.",
    "io.delta.",
    "org.antlr.",
    "org.postgresql.",
    "bigquery.",
];
const STACK_SENTINEL: &str = "<Redacted Exception Message>";
const STACK_CAUSED_BY: &str = "Caused by: ";
const STACK_SUPPRESSED: &str = "Suppressed: ";

/// One line of a redacted DBR/JVM stack trace (one frame per `repeated string` element).
pub fn redacted_stack_trace(v: &str) -> bool {
    if v.is_empty() {
        return true; // empty element carries no frame to leak
    }
    stack_matches_line(stack_strip_leading_whitespace(v))
}

fn stack_strip_leading_whitespace(raw: &str) -> &str {
    let b = raw.as_bytes();
    let mut start = 0;
    let mut end = b.len();
    while start < end && (b[start] == b' ' || b[start] == b'\t') {
        start += 1;
    }
    if end > start && b[end - 1] == b'\r' {
        end -= 1;
    }
    &raw[start..end]
}

fn stack_matches_line(line: &str) -> bool {
    // Cause header (also handling DBR's "at Caused by:" rendering), checked before the frame branch.
    let after_at = line.strip_prefix("at ").unwrap_or(line);
    if let Some(body) = after_at.strip_prefix(STACK_CAUSED_BY) {
        return stack_is_cause_body(body);
    }
    if let Some(body) = after_at.strip_prefix(STACK_SUPPRESSED) {
        return stack_is_cause_body(body);
    }
    if let Some(rest) = line.strip_prefix("at ") {
        return stack_is_frame(rest);
    }
    if stack_is_truncation_marker(line) {
        return true;
    }
    // Bare JFR frame: `pkg.Class.method[:line]`, no `at ` and no parens.
    stack_is_bare_frame(line)
}

/// A bare `pkg.Class.method[:line]` frame, as JFR emits them. Mirrors isBareFrame (Java).
fn stack_is_bare_frame(line: &str) -> bool {
    if line.is_empty() {
        return false;
    }
    // A frame token never contains ':', so a trailing ":<digits>" is the line number.
    let frame = match line.rfind(':') {
        Some(colon) => {
            let digits = &line[colon + 1..];
            if digits.is_empty() || !is_ascii_digits(digits) {
                return false;
            }
            &line[..colon]
        }
        None => line,
    };
    // The class is everything before the trailing `.method`, so a frame needs two dots: a bare
    // `Class.method` is not fully qualified and is rejected.
    let dot = match frame.rfind('.') {
        Some(d) if d > 0 && d + 1 < frame.len() => d,
        _ => return false,
    };
    let class = &frame[..dot];
    let method = &frame[dot + 1..];
    if !class.contains('.') {
        return false; // not fully qualified
    }
    // stack_is_allowed_class_token permits '.' but does not reject an empty segment, which the
    // "at Class(File:line)" grammar cannot produce, so check it here.
    if class.starts_with('.') || class.ends_with('.') || class.contains("..") {
        return false;
    }
    // '<' and '>' for the synthetic <init>/<clinit>.
    let method_ok = method.bytes().all(|c| {
        is_ascii_letter(c) || is_digit(c) || c == b'_' || c == b'$' || c == b'<' || c == b'>'
    });
    method_ok && stack_is_allowed_class_token(class)
}

fn stack_is_cause_body(body: &str) -> bool {
    let mut body = body;
    if let Some(colon) = body.find(": ") {
        if &body[colon + 2..] != STACK_SENTINEL {
            return false;
        }
        body = &body[..colon];
    }
    stack_is_allowed_class_token(body) || stack_is_signed_int(body)
}

fn stack_is_frame(rest: &str) -> bool {
    let paren = match rest.find('(') {
        None => return stack_is_signed_int(rest), // synthetic frame id
        Some(p) => p,
    };
    if !rest.ends_with(')') {
        return false;
    }
    stack_is_allowed_class_token(&rest[..paren])
        && stack_is_location(&rest[paren + 1..rest.len() - 1])
}

fn stack_is_location(loc: &str) -> bool {
    if loc == "Native Method" || loc == "Unknown Source" {
        return true;
    }
    let colon = loc.find(':');
    let file_and_ext = match colon {
        Some(c) => &loc[..c],
        None => loc,
    };
    if let Some(c) = colon {
        if !is_ascii_digits(&loc[c + 1..]) {
            return false;
        }
    }
    let dot = match file_and_ext.rfind('.') {
        Some(d) if d > 0 => d,
        _ => return false, // need a non-empty base and an extension
    };
    let ext = &file_and_ext[dot + 1..];
    if ext != "scala" && ext != "java" {
        return false;
    }
    file_and_ext.as_bytes()[..dot]
        .iter()
        .all(|&c| is_ascii_letter(c) || is_digit(c) || c == b'_' || c == b'$')
}

fn stack_is_allowed_class_token(cls: &str) -> bool {
    if !STACK_CLASS_PREFIXES.iter().any(|p| cls.starts_with(p)) {
        return false;
    }
    cls.bytes().all(|c| {
        is_ascii_letter(c)
            || is_digit(c)
            || c == b'_'
            || c == b'$'
            || c == b'.'
            || c == b'<'
            || c == b'>'
    })
}

fn stack_is_truncation_marker(s: &str) -> bool {
    const PREFIX: &str = "... ";
    const SUFFIX: &str = " more";
    if !s.starts_with(PREFIX) || !s.ends_with(SUFFIX) {
        return false;
    }
    let start = PREFIX.len();
    let end = s.len() - SUFFIX.len();
    start < end && is_ascii_digits(&s[start..end])
}

fn stack_is_signed_int(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let start = if s.as_bytes()[0] == b'-' { 1 } else { 0 };
    start < s.len() && is_ascii_digits(&s[start..])
}

// ---------------------------------------------------------------------------
// Curated word lists (sorted for binary_search — verified sorted by a unit test below).
// ---------------------------------------------------------------------------

/// RPC action-verb head words for `grpc_method`. MUST stay sorted (asserted in tests).
static GRPC_METHOD_HEAD_VERBS: [&str; 108] = [
    "acquire",
    "add",
    "analyze",
    "apply",
    "approve",
    "assign",
    "authorize",
    "backfill",
    "batch",
    "bulk",
    "cancel",
    "change",
    "check",
    "cleanup",
    "clear",
    "close",
    "commit",
    "configure",
    "convert",
    "count",
    "create",
    "delete",
    "describe",
    "disable",
    "edit",
    "enable",
    "evaluate",
    "exchange",
    "execute",
    "export",
    "fetch",
    "filter",
    "find",
    "force",
    "generate",
    "get",
    "grant",
    "heartbeat",
    "import",
    "increment",
    "insert",
    "install",
    "internal",
    "intra",
    "is",
    "keep",
    "launch",
    "list",
    "log",
    "lookup",
    "migrate",
    "modify",
    "multi",
    "notify",
    "open",
    "patch",
    "pause",
    "ping",
    "poll",
    "process",
    "proxy",
    "publish",
    "purge",
    "put",
    "query",
    "read",
    "reclaim",
    "reconcile",
    "record",
    "refresh",
    "register",
    "release",
    "remove",
    "replace",
    "report",
    "reset",
    "resize",
    "resolve",
    "restore",
    "resume",
    "revoke",
    "rotate",
    "run",
    "scan",
    "search",
    "send",
    "set",
    "setup",
    "start",
    "stop",
    "stream",
    "submit",
    "subscribe",
    "sync",
    "terminate",
    "trigger",
    "try",
    "undelete",
    "uninstall",
    "unregister",
    "update",
    "upsert",
    "validate",
    "verify",
    "wait",
    "warmup",
    "watch",
    "write",
];

/// Error-domain head words for `generic_error_code`. MUST stay sorted (asserted in tests).
static GENERIC_ERROR_PREFIX: [&str; 38] = [
    "AWS",
    "AZURE",
    "CANNOT",
    "CATALOG",
    "CF",
    "CLOUD",
    "CLUSTER",
    "COLUMN",
    "COMMAND",
    "DELTA",
    "ERROR",
    "FAILED",
    "FAULT",
    "GCP",
    "GIT",
    "INGESTION",
    "INVALID",
    "MISSING",
    "NETWORK",
    "NO",
    "NOT",
    "NOTEBOOK",
    "PERMISSIONS",
    "PIPELINE",
    "RESOURCE",
    "SCHEMA",
    "SESSION",
    "SPARK",
    "STORAGE",
    "STREAMING",
    "TABLE",
    "UC",
    "UNCLASSIFIED",
    "UNEXPECTED",
    "UNITY",
    "UNRESOLVED",
    "UNSUPPORTED",
    "USER",
];

/// Failure-mode tail words for `generic_error_code`. MUST stay sorted (asserted in tests).
static GENERIC_ERROR_SUFFIX: [&str; 32] = [
    "BLOCKED",
    "CANCELLED",
    "CHANGE",
    "CHANGED",
    "COLUMN",
    "DENIED",
    "DISABLED",
    "ENABLED",
    "ERROR",
    "EXCEEDED",
    "EXCEPTION",
    "EXHAUSTED",
    "EXIST",
    "EXISTS",
    "EXPRESSION",
    "FAILED",
    "FAILURE",
    "FOUND",
    "INSTALLATION",
    "LOG",
    "MISMATCH",
    "PATH",
    "REQUEST",
    "SCHEMA",
    "SUPPORTED",
    "TABLE",
    "TERMINATION",
    "TIMEOUT",
    "TYPE",
    "UNAVAILABLE",
    "VIOLATION",
    "WRITE",
];

// ---------------------------------------------------------------------------
// Engine-request id / exception type.
// ---------------------------------------------------------------------------

/// Every byte in `s` is a hex digit (any case) and `s` has length `n`. Mirrors the Java
/// `isHexRun(String, int)` (any-case) used by the engine-request-id token check — distinct from
/// `patterns::hex64`, which requires lowercase.
fn is_hex_len(s: &str, n: usize) -> bool {
    s.len() == n && s.bytes().all(|c| c.is_ascii_hexdigit())
}

/// Java identifier part: ASCII letter, digit, `_`, or `$`.
fn is_java_ident_part(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$'
}

// Leading product prefixes for engine_request_id (first token must be one of these). Sorted for
// binary_search; a subset of ENGINE_REQUEST_ID_KEYWORDS.
static ENGINE_REQUEST_ID_PREFIXES: [&str; 6] =
    ["command-api", "dbsql", "dlt", "job", "notebook", "unknown"];

// Structural keyword tokens baked into the engine_request_id templates (path separators and the
// recorder source names after "/unknown/"), including the prefixes. Sorted for binary_search.
static ENGINE_REQUEST_ID_KEYWORDS: [&str; 25] = [
    "c",
    "command-api",
    "d",
    "dbsql",
    "dlt",
    "driverlocal",
    "e",
    "execution",
    "f",
    "jg",
    "job",
    "notebook",
    "o",
    "p",
    "r",
    "s",
    "s-u",
    "sc",
    "sparkconnect",
    "t",
    "thrift-handler",
    "thriftserver",
    "u",
    "unknown",
    "ve",
];

/// Databricks engine-request ID (EngineRequest.engine_request_id). Must start with a known product
/// prefix and decompose into "/"-delimited tokens that are each a structural keyword or a
/// constrained value token (decimal digits, UUID, 32-/64-char hex, or the "-" placeholder), so no
/// free-form customer input passes. Mirrors `EngineRequestIdValidator` in DataShapeValidators.java.
pub fn engine_request_id(v: &str) -> bool {
    const MAX_LEN: usize = 512;
    const MAX_TOKENS: usize = 12;
    if v.len() < 2 || v.len() > MAX_LEN || !v.starts_with('/') {
        return false;
    }
    // Split on '/'; the leading '/' yields an empty tokens[0] that the per-token loop skips.
    let tokens: Vec<&str> = v.split('/').collect();
    if tokens.len() < 2 || tokens.len() - 1 > MAX_TOKENS {
        return false;
    }
    if ENGINE_REQUEST_ID_PREFIXES
        .binary_search(&tokens[1])
        .is_err()
    {
        return false;
    }
    for tok in &tokens[1..] {
        if tok.is_empty() {
            return false; // reject "//" and a trailing '/'
        }
        if *tok == "-" || ENGINE_REQUEST_ID_KEYWORDS.binary_search(tok).is_ok() {
            continue;
        }
        if is_ascii_digits(tok)
            || patterns::uuid(tok)
            || is_hex_len(tok, 32)
            || is_hex_len(tok, 64)
        {
            continue;
        }
        return false;
    }
    true
}

// Closed set of bare simple exception names safe to centralize: the CPython builtin exception
// hierarchy plus Spark / PySpark exception simple names. Sorted for binary_search; mirrors
// `ExceptionTypeValidator.SIMPLE_NAMES` in DataShapeValidators.java.
static EXCEPTION_SIMPLE_NAMES: [&str; 83] = [
    "AnalysisException",
    "ArithmeticError",
    "AssertionError",
    "AttributeError",
    "BaseException",
    "BaseExceptionGroup",
    "BlockingIOError",
    "BrokenPipeError",
    "BufferError",
    "BytesWarning",
    "ChildProcessError",
    "ConnectionAbortedError",
    "ConnectionError",
    "ConnectionRefusedError",
    "ConnectionResetError",
    "DeprecationWarning",
    "EOFError",
    "EncodingWarning",
    "EnvironmentError",
    "Exception",
    "ExceptionGroup",
    "FileExistsError",
    "FileNotFoundError",
    "FloatingPointError",
    "FutureWarning",
    "GeneratorExit",
    "IOError",
    "ImportError",
    "ImportWarning",
    "IndentationError",
    "IndexError",
    "InterruptedError",
    "IsADirectoryError",
    "KeyError",
    "KeyboardInterrupt",
    "LookupError",
    "MemoryError",
    "ModuleNotFoundError",
    "NameError",
    "NotADirectoryError",
    "NotImplementedError",
    "OSError",
    "OverflowError",
    "ParseException",
    "PendingDeprecationWarning",
    "PermissionError",
    "ProcessLookupError",
    "PySparkAssertionError",
    "PySparkAttributeError",
    "PySparkException",
    "PySparkImportError",
    "PySparkIndexError",
    "PySparkKeyError",
    "PySparkNotImplementedError",
    "PySparkPicklingError",
    "PySparkRuntimeError",
    "PySparkTypeError",
    "PySparkValueError",
    "RecursionError",
    "ReferenceError",
    "ResourceWarning",
    "RuntimeError",
    "RuntimeWarning",
    "StopAsyncIteration",
    "StopIteration",
    "StreamingQueryException",
    "SyntaxError",
    "SyntaxWarning",
    "SystemError",
    "SystemExit",
    "TabError",
    "TimeoutError",
    "TypeError",
    "UnboundLocalError",
    "UnicodeDecodeError",
    "UnicodeEncodeError",
    "UnicodeError",
    "UnicodeTranslateError",
    "UnicodeWarning",
    "UserWarning",
    "ValueError",
    "Warning",
    "ZeroDivisionError",
];

// Platform package prefixes whose classes are Databricks/Spark/JVM-controlled (centralizable). Not
// sorted: matched by first-prefix-wins iteration, mirroring the Java array order.
static EXCEPTION_FQN_PREFIXES: [&str; 10] = [
    "org.apache.spark.",
    "org.apache.hadoop.",
    "org.apache.hive.",
    "org.sparkproject.",
    "com.databricks.",
    "io.delta.",
    "java.",
    "javax.",
    "scala.",
    "py4j.",
];

/// A fully-qualified class name under a known platform prefix whose remaining components are dotted
/// Java/Scala identifiers with no empty segments. Mirrors `ExceptionTypeValidator.isFqnUnderKnownPrefix`.
fn is_fqn_under_known_prefix(v: &str) -> bool {
    let matched = match EXCEPTION_FQN_PREFIXES.iter().find(|p| v.starts_with(**p)) {
        Some(p) => *p,
        None => return false,
    };
    let rest = &v[matched.len()..];
    if rest.is_empty() {
        return false; // nothing after the prefix
    }
    let mut seg_has_char = false;
    for c in rest.bytes() {
        if c == b'.' {
            if !seg_has_char {
                return false; // empty segment ("..", leading '.')
            }
            seg_has_char = false;
        } else if is_java_ident_part(c) {
            seg_has_char = true;
        } else {
            return false;
        }
    }
    seg_has_char // reject a trailing '.'
}

/// Exception class name (ErrorInfo.exception_type): a FQN under a known platform prefix, or a bare
/// simple name in the closed CPython/Spark/PySpark set. User-defined exceptions match neither and
/// are redacted. Mirrors `ExceptionTypeValidator` in DataShapeValidators.java.
pub fn exception_type(v: &str) -> bool {
    const MAX_LEN: usize = 256;
    if v.is_empty() || v.len() > MAX_LEN {
        return false;
    }
    if v.contains('.') {
        return is_fqn_under_known_prefix(v);
    }
    EXCEPTION_SIMPLE_NAMES.binary_search(&v).is_ok()
}

// ---------------------------------------------------------------------------
// JFR redacted-field shapes. Each accepts only the KEPT form the producer emits; the hash it emits
// for everything else is covered by DATA_SHAPE_NUMERIC, declared alongside on the column. Java is
// the reference impl (DataShapeValidators.java); these mirror it and share the corpus.
// ---------------------------------------------------------------------------

/// A kept (already-redacted) JVM class token: a dotted FQN of token chars (`[A-Za-z0-9_$]`) on shape
/// 102's package allowlist. Mirrors DataShapeValidators.isJfrClassToken.
fn jfr_is_class_token(cls: &str) -> bool {
    if cls.is_empty() {
        return false;
    }
    let b = cls.as_bytes();
    let mut saw_dot = false;
    for i in 0..b.len() {
        let c = b[i];
        if c == b'.' {
            if i == 0 || i == b.len() - 1 || b[i - 1] == b'.' {
                return false;
            }
            saw_dot = true;
        } else if !(is_ascii_letter(c) || is_digit(c) || c == b'_' || c == b'$') {
            return false;
        }
    }
    saw_dot && STACK_CLASS_PREFIXES.iter().any(|p| cls.starts_with(p))
}

// The standalone JFR stack-frame shape was folded into DATA_SHAPE_REDACTED_STACK_TRACE, which now
// accepts the bare `pkg.Class.method[:line]` grammar in addition to the `at Class(File:line)` form —
// see redacted_stack_trace above. Its package allowlist governs both grammars.

/// A redacted JFR object/allocation type name: a dotted FQN, an object-array `[Lpkg.Class;`, a
/// primitive/primitive-array descriptor (`[B`, `[[I`), or a plain primitive/void name.
pub fn jfr_redacted_type_name(v: &str) -> bool {
    if v.is_empty() {
        return true;
    }
    if matches!(
        v,
        "boolean" | "byte" | "char" | "short" | "int" | "long" | "float" | "double" | "void"
    ) {
        return true;
    }
    let b = v.as_bytes();
    if b[0] == b'[' {
        let mut i = 0;
        while i < b.len() && b[i] == b'[' {
            i += 1;
        }
        let component = &v[i..];
        if component.len() == 1
            && matches!(
                component.as_bytes()[0],
                b'B' | b'S' | b'I' | b'J' | b'Z' | b'C' | b'F' | b'D'
            )
        {
            return true; // primitive array
        }
        if component.len() > 2 && component.as_bytes()[0] == b'L' && component.ends_with(';') {
            return jfr_is_class_token(&component[1..component.len() - 1]);
        }
        return false;
    }
    jfr_is_class_token(v)
}

/// Databricks account-console URL: a Databricks-owned FQDN, the literal `/?account_id=`, then a
/// hyphenated UUID that is the entire remainder. The host is delegated to [`databricks_fqdn`].
pub fn databricks_account_console_url(v: &str) -> bool {
    const SEPARATOR: &str = "/?account_id=";
    const UUID_LEN: usize = 36;

    let Some(sep) = v.find(SEPARATOR) else {
        return false;
    };
    // The account id must be the entire remainder -- one UUID, no extra query params.
    if v.len() != sep + SEPARATOR.len() + UUID_LEN {
        return false;
    }
    patterns::uuid(&v[sep + SEPARATOR.len()..]) && databricks_fqdn(&v[..sep])
}

/// Azure subscription scope path: the literal `/subscriptions/` followed by a hyphenated UUID, with
/// nothing after it. The longer ARM forms are not accepted: their trailing segments are
/// customer-chosen resource names.
pub fn azure_subscription_path(v: &str) -> bool {
    const PREFIX: &str = "/subscriptions/";
    const UUID_LEN: usize = 36;

    v.len() == PREFIX.len() + UUID_LEN
        && match v.strip_prefix(PREFIX) {
            Some(uuid_part) => patterns::uuid(uuid_part),
            None => false,
        }
}

/// Lifecyclemanagement partner change-event registration id: the literal
/// `"PARTNER_CHANGE_EVENT_REGISTRATION_V"` followed by a positive integer with no leading zero.
pub fn partner_change_event_registration_id(v: &str) -> bool {
    const PREFIX: &str = "PARTNER_CHANGE_EVENT_REGISTRATION_V";

    let Some(version) = v.strip_prefix(PREFIX) else {
        return false;
    };
    !version.is_empty()
        && version.as_bytes()[0] != b'0'
        && version.bytes().all(|c| c.is_ascii_digit())
}

/// Data Rooms embedding cache key (`datarooms.embedding.path`): a `/`-delimited path whose every
/// token is either a structural keyword or a hashed/opaque id. Accepted forms, where `<sha256>` is
/// 64 lowercase hex and `<hex32>` is 32 lowercase hex:
///
/// - `table/<sha256>` and `table/<sha256>/(column|synonym)/<sha256>`
/// - `message/<sha256>/type/(sql|column)`
/// - `instructions/<hex32>/title_with_contents`, `sql-instructions/<hex32>/title`,
///   `sql-snippets/<hex32>/display_name-with-code`, `from-snippets/<hex32>/asset-with-metadata`
/// - `space/<hex32>/(metadata_2|metadata_3|metadata_v2)` and
///   `space/<hex32>/sample_question/<hex32>`
///
/// Customer-derived tokens (table names, column names, synonyms) only ever appear hashed, so no
/// customer-controlled string can match. The pre-Nov-2025 raw-text synonym path and
/// `snippets/<snippet name>/snippet_key` embed free text and are deliberately rejected.
pub fn data_rooms_embedding_path(v: &str) -> bool {
    fn is_lower_hex_run(token: &str, expected_len: usize) -> bool {
        let b = token.as_bytes();
        b.len() == expected_len
            && b.iter()
                .all(|&c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    }

    const SHA256_LEN: usize = 64;
    const HEX32_LEN: usize = 32;

    let parts: Vec<&str> = v.split('/').collect();
    if parts.len() < 2 || parts.len() > 4 {
        return false;
    }

    let id_len = match parts[0] {
        "table" | "message" => SHA256_LEN,
        "instructions" | "sql-instructions" | "sql-snippets" | "from-snippets" | "space" => {
            HEX32_LEN
        }
        _ => return false,
    };
    // "message" takes a sha256 in the current form but a hex32 in the legacy bare form, so its id
    // width is checked per-branch below rather than by the single up-front gate.
    if parts[0] != "message" && !is_lower_hex_run(parts[1], id_len) {
        return false;
    }

    match parts[0] {
        // table/<sha256>, or table/<sha256>/(column|synonym)/<sha256>.
        "table" => match parts.len() {
            2 => true,
            4 => matches!(parts[2], "column" | "synonym") && is_lower_hex_run(parts[3], SHA256_LEN),
            _ => false,
        },
        // message/<sha256>/type/(sql|column). The bare "message/<hex32>" form is the
        // pre-Sept-2024 message-id path (superseded by 65ba26d43f1ee); ~318k legacy rows remain,
        // and a message id is HEX() of a binary(16), so it is equally safe.
        "message" => match parts.len() {
            2 => is_lower_hex_run(parts[1], HEX32_LEN),
            4 => {
                is_lower_hex_run(parts[1], SHA256_LEN)
                    && parts[2] == "type"
                    && matches!(parts[3], "sql" | "column")
            }
            _ => false,
        },
        // space/<hex32>/<metadata leaf>, or space/<hex32>/sample_question/<hex32>.
        "space" => match parts.len() {
            3 => matches!(parts[2], "metadata_2" | "metadata_3" | "metadata_v2"),
            4 => parts[2] == "sample_question" && is_lower_hex_run(parts[3], HEX32_LEN),
            _ => false,
        },
        // The remaining roots each take exactly one fixed trailing keyword.
        "instructions" => parts.len() == 3 && parts[2] == "title_with_contents",
        "sql-instructions" => parts.len() == 3 && parts[2] == "title",
        "sql-snippets" => parts.len() == 3 && parts[2] == "display_name-with-code",
        "from-snippets" => parts.len() == 3 && parts[2] == "asset-with-metadata",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_sorted(a: &[&str]) -> bool {
        a.windows(2).all(|w| w[0] < w[1])
    }

    #[test]
    fn word_lists_are_sorted_and_unique() {
        assert!(is_sorted(&GRPC_METHOD_HEAD_VERBS), "GRPC_METHOD_HEAD_VERBS must be sorted+unique");
        assert!(is_sorted(&GENERIC_ERROR_PREFIX), "GENERIC_ERROR_PREFIX must be sorted+unique");
        assert!(is_sorted(&GENERIC_ERROR_SUFFIX), "GENERIC_ERROR_SUFFIX must be sorted+unique");
        assert!(
            is_sorted(&ENGINE_REQUEST_ID_PREFIXES),
            "ENGINE_REQUEST_ID_PREFIXES must be sorted+unique"
        );
        assert!(
            is_sorted(&ENGINE_REQUEST_ID_KEYWORDS),
            "ENGINE_REQUEST_ID_KEYWORDS must be sorted+unique"
        );
        assert!(is_sorted(&EXCEPTION_SIMPLE_NAMES), "EXCEPTION_SIMPLE_NAMES must be sorted+unique");
    }
}
