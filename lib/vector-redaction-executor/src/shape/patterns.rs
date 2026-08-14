//! Pattern / structural data-shape validators.
//!
//! Direct char-level ports of the reference implementation in
//! `compliance/shape/java/src/DataShapeValidators.java`. Each `fn` here corresponds 1:1 to a Java
//! nested `*Validator` class — same length checks, same branching, same exits — so the shared
//! corpus at `compliance/shape/testdata/cases.json` validates all four ports (Java, Scala, Python,
//! Rust) identically. Keep them structurally aligned with the Java source; do not "optimize" a
//! validator in a way that changes which strings it accepts.
//!
//! Byte-oriented: every validator treats the input as ASCII bytes. The Java code indexes UTF-16
//! `char`s but every accept path is pure ASCII, so a multi-byte UTF-8 sequence can only *fail* a
//! byte check — never spuriously match — which preserves the accept set exactly.

// ---------------------------------------------------------------------------
// Character-class predicates shared by the per-character validators.
// ---------------------------------------------------------------------------

fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

fn is_upper(c: u8) -> bool {
    c.is_ascii_uppercase()
}

fn is_ascii_letter(c: u8) -> bool {
    c.is_ascii_alphabetic()
}

fn is_lower_hex(c: u8) -> bool {
    c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
}

fn is_hex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}

fn is_crockford_base32(c: u8) -> bool {
    c.is_ascii_digit()
        || (b'A'..=b'H').contains(&c)
        || (b'J'..=b'K').contains(&c)
        || (b'M'..=b'N').contains(&c)
        || (b'P'..=b'T').contains(&c)
        || (b'V'..=b'Z').contains(&c)
}

/// Every byte in `s[from..to)` is a hex digit (any case).
fn is_hex_run(s: &[u8], from: usize, to: usize) -> bool {
    s[from..to].iter().all(|&c| is_hex(c))
}

// ---------------------------------------------------------------------------
// UUID / hex.
// ---------------------------------------------------------------------------

/// UUID with hyphens: 8-4-4-4-12 hex characters.
pub fn uuid(v: &str) -> bool {
    let b = v.as_bytes();
    if b.len() != 36 {
        return false;
    }
    is_hex_run(b, 0, 8)
        && b[8] == b'-'
        && is_hex_run(b, 9, 13)
        && b[13] == b'-'
        && is_hex_run(b, 14, 18)
        && b[18] == b'-'
        && is_hex_run(b, 19, 23)
        && b[23] == b'-'
        && is_hex_run(b, 24, 36)
}

/// Nimbus REPL ID: a bounded IDM region name followed by a UUID.
pub fn nimbus_repl_id(v: &str) -> bool {
    const PREFIX: &[u8] = b"pzp";
    const MIN_REGION_LEN: usize = 5;
    const MAX_REGION_LEN: usize = 16;
    const UUID_LEN: usize = 36;

    let b = v.as_bytes();
    let Some(region_len) = b.len().checked_sub(UUID_LEN + 1) else {
        return false;
    };
    if !(MIN_REGION_LEN..=MAX_REGION_LEN).contains(&region_len) || !b.starts_with(PREFIX) {
        return false;
    }
    if !b[PREFIX.len()..region_len]
        .iter()
        .all(|c| is_digit(*c) || c.is_ascii_lowercase())
    {
        return false;
    }
    b[region_len] == b'-' && uuid(&v[region_len + 1..])
}

/// 32-character hex string (UUID without hyphens).
pub fn hex32(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 32 && b.iter().all(|&c| is_hex(c))
}

pub fn google_cloud_hex32_id(v: &str) -> bool {
    const PREFIX: &str = "GoogleCloud-";
    const SUFFIX_LEN: usize = 32;

    let Some(suffix) = v.strip_prefix(PREFIX) else {
        return false;
    };
    suffix.len() == SUFFIX_LEN && hex32(suffix)
}

/// 40-character lowercase hex string, such as a git commit SHA-1.
pub fn hex40(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 40 && b.iter().all(|&c| is_lower_hex(c))
}

/// ULID (Universally Unique Lexicographically Sortable Identifier): exactly 26
/// characters from Crockford's Base32 alphabet (0-9, A-Z excluding I, L, O, U).
/// Uppercase canonical form only.
pub fn ulid(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 26 && b.iter().all(|&c| is_crockford_base32(c))
}

/// 8-character lowercase hex string.
pub fn hex8(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 8 && b.iter().all(|&c| is_lower_hex(c))
}

/// 16-character lowercase hex string.
pub fn hex16(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 16 && b.iter().all(|&c| is_lower_hex(c))
}

/// 64-character lowercase hex string.
pub fn hex64(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 64 && b.iter().all(|&c| is_lower_hex(c))
}

/// A pair of 16-char lowercase hex strings joined by a single '-' (total length 33).
pub fn hex16_pair(v: &str) -> bool {
    let b = v.as_bytes();
    if b.len() != 33 || b[16] != b'-' {
        return false;
    }
    (0..33).all(|i| i == 16 || is_lower_hex(b[i]))
}

/// A 32-character lowercase hex string and a 16-character lowercase hex string joined by `_`.
pub fn hex32_hex16(v: &str) -> bool {
    let b = v.as_bytes();
    if b.len() != 49 || b[32] != b'_' {
        return false;
    }
    (0..49).all(|i| i == 32 || is_lower_hex(b[i]))
}

/// Databricks workspace deployment name in its system-generated AWS or Azure form.
pub fn deployment_name(v: &str) -> bool {
    const AWS_PREFIX: &str = "dbc-";
    const AWS_ID_LEN: usize = 8;
    const AWS_SUFFIX_LEN: usize = 4;
    const AZURE_PREFIX: &str = "adb-";
    const MAX_AZURE_ID_LEN: usize = 16;
    const MAX_AZURE_SUFFIX_LEN: usize = 2;

    if let Some(suffix) = v.strip_prefix(AWS_PREFIX) {
        let b = suffix.as_bytes();
        return b.len() == AWS_ID_LEN + 1 + AWS_SUFFIX_LEN
            && b[AWS_ID_LEN] == b'-'
            && b[..AWS_ID_LEN].iter().all(|&c| is_lower_hex(c))
            && b[AWS_ID_LEN + 1..].iter().all(|&c| is_lower_hex(c));
    }

    let Some(suffix) = v.strip_prefix(AZURE_PREFIX) else {
        return false;
    };
    let Some(dot) = suffix.find('.') else {
        return false;
    };
    let id = &suffix[..dot];
    let shard = &suffix[dot + 1..];
    !id.is_empty()
        && id.len() <= MAX_AZURE_ID_LEN
        && !shard.is_empty()
        && shard.len() <= MAX_AZURE_SUFFIX_LEN
        && id.bytes().all(is_digit)
        && shard.bytes().all(is_digit)
}

/// Two decimal digits, a hyphen, then a hyphenated UUID.
pub fn prefixed_uuid(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 39 && is_digit(b[0]) && is_digit(b[1]) && b[2] == b'-' && uuid(&v[3..])
}

/// SQL Gateway endpoint ID: "endpoint-" followed by 16 lowercase hex characters.
pub fn sql_gateway_endpoint_id(v: &str) -> bool {
    v.strip_prefix("endpoint-").is_some_and(hex16)
}

/// UUID with a positive i16 version suffix separated by a colon.
pub fn uuid_with_version(v: &str) -> bool {
    const UUID_LEN: usize = 36;
    const MIN_TOTAL_LEN: usize = 38;
    const MAX_TOTAL_LEN: usize = 42;
    const MAX_VERSION: u32 = i16::MAX as u32;

    let b = v.as_bytes();
    if !(MIN_TOTAL_LEN..=MAX_TOTAL_LEN).contains(&b.len())
        || b[UUID_LEN] != b':'
        || !uuid(&v[..UUID_LEN])
    {
        return false;
    }
    let version = &v[UUID_LEN + 1..];
    if !numeric(version) || !matches!(version.as_bytes()[0], b'1'..=b'9') {
        return false;
    }
    let mut parsed = 0u32;
    for c in version.bytes() {
        parsed = parsed * 10 + u32::from(c - b'0');
        if parsed > MAX_VERSION {
            return false;
        }
    }
    parsed > 0
}

// ---------------------------------------------------------------------------
// Numeric.
// ---------------------------------------------------------------------------

/// Digits with an optional single leading '-'. Empty string passes; a bare "-" does not.
pub fn numeric(v: &str) -> bool {
    let b = v.as_bytes();
    let mut start = 0;
    if !b.is_empty() && b[0] == b'-' {
        if b.len() == 1 {
            return false;
        }
        start = 1;
    }
    b[start..].iter().all(|&c| is_digit(c))
}

/// Generic dotted version number: 1-3 components of 1-3 digits each.
pub fn version_number(v: &str) -> bool {
    let b = v.as_bytes();
    if b.is_empty() {
        return false;
    }
    const MAX_COMPONENT_DIGITS: i32 = 3;
    const MAX_COMPONENTS: i32 = 3;
    let mut component_count = 0;
    let mut component_digits = 0;
    for &c in b {
        if c == b'.' {
            if component_digits == 0 {
                return false;
            }
            component_count += 1;
            if component_count >= MAX_COMPONENTS {
                return false;
            }
            component_digits = 0;
        } else if is_digit(c) {
            component_digits += 1;
            if component_digits > MAX_COMPONENT_DIGITS {
                return false;
            }
        } else {
            return false;
        }
    }
    component_digits > 0
}

const THRIFT_DRIVERS: &[&str] = &["Thrift (Java)", "Thrift (C++)", "Thrift (Python)"];
const SPARK_XDBC_DRIVERS: &[&str] = &["SparkJDBCDriver", "SparkODBCDriver"];
const DATABRICKS_XDBC_DRIVERS: &[&str] = &["DatabricksJDBCDriver", "DatabricksODBCDriver"];
const ADBC_DRIVERS: &[&str] = &["ADBCDatabricksDriver", "ADBCSparkDriver"];
const SPARK_XDBC_VENDORS: &[&str] =
    &["", "CData", "Databricks", "Microsoft", "MicroStrategy", "Qlik", "Simba"];
const KNOWN_BI_TOOLS: &[&str] = &[
    "",
    "other",
    "unknown",
    "ADBCDatabricksDriver",
    "ADBCSparkDriver",
    "Adverity",
    "Airbyte",
    "Alation",
    "Alteryx",
    "Anomalo",
    "Arcion",
    "Ascend",
    "Atlan",
    "AtlanCatalog",
    "BigID",
    "Bigeye",
    "BoostKPI",
    "CData",
    "CartoDB",
    "Census",
    "Collibra",
    "Confluent",
    "Datafi",
    "Databricks dbt",
    "Databricks go-dbsql",
    "Databricks Google Sheet AddOn",
    "Databricks Power Automate Connector",
    "Databricks PowerFx Connector",
    "Databricks SQL MCP",
    "Databricks Sql Notebooks",
    "DatabricksGenie",
    "DatabricksSqlExecApi",
    "dbt Cloud",
    "Deepiq",
    "Domo",
    "Erwin",
    "Fishtown Analytics dbt",
    "Fivetran",
    "GoDatabricksSqlConnector",
    "Google Apps Script",
    "GreatExpectations",
    "Hevodata",
    "Hex",
    "hightouch",
    "Hunters",
    "HVR",
    "Immuta",
    "Informatica",
    "Informatica_CDI",
    "Kyvos",
    "Lightup",
    "Looker",
    "Looker Studio",
    "Lytics",
    "Macheye",
    "Matillion",
    "Mathworks",
    "Matlab",
    "MicroStrategy",
    "MonteCarlo",
    "NodejsDatabricksSqlConnector",
    "OSS/SaaS Redash",
    "OvalEdge_OvalEdge",
    "Panther",
    "PowerBI",
    "Precisely",
    "Preset",
    "Prophecy",
    "Protegrity",
    "PyDatabricksSqlConnector",
    "Qlik",
    "Qlik_QCS",
    "Qlik_QSD",
    "Qlik_QSE",
    "Qlik_QSEfW",
    "Quest",
    "Rivery",
    "RudderStack",
    "Securonix",
    "Sigma",
    "Sisense",
    "Sisu",
    "Snaplogic",
    "Soda",
    "Splunk",
    "SQL Analytics",
    "sqlalchemy",
    "Stardog",
    "Stitch",
    "Streamsets",
    "Striim",
    "Superconductive",
    "Tableau",
    "Talend",
    "Tellius",
    "ThoughtSpot",
    "Tibco Spotfire",
    "VaultSpeed",
    "Wandisco",
    "amperity",
    "atscale",
    "databricks-sat",
    "datadotworld",
    "dataiku",
    "dqlabs",
    "etleap",
    "privacera",
    "robustintelligence",
    "snowplow-rdbloader-bdp",
    "snowplow-rdbloader-oss",
];

enum ThriftDriverFamily {
    Empty,
    LegacyThrift,
    Other,
    SparkXdbc,
    DatabricksXdbc,
    DatabricksJdbcOss,
    DatabricksSqlExecApi,
    GoConnector,
    NodejsConnector,
    PythonConnector,
    Adbc,
    Unknown,
}

/// Four-part tuple emitted by the Spark Thrift user-agent redactor.
pub fn spark_thrift_user_agent(v: &str) -> bool {
    const MAX_TOTAL_LENGTH: usize = 256;
    if v.len() > MAX_TOTAL_LENGTH {
        return false;
    }
    let mut parts = v.split(';');
    let (Some(driver), Some(version), Some(vendor), Some(bi_tool)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }

    match thrift_driver_family(driver) {
        ThriftDriverFamily::Empty | ThriftDriverFamily::LegacyThrift => {
            version.is_empty() && vendor.is_empty() && bi_tool.is_empty()
        }
        ThriftDriverFamily::Other => {
            version.is_empty() && vendor.is_empty() && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::SparkXdbc | ThriftDriverFamily::DatabricksXdbc => {
            numeric_version_with_optional_revision(version)
                && SPARK_XDBC_VENDORS.contains(&vendor)
                && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::DatabricksJdbcOss => {
            oss_jdbc_version(version) && vendor == "Databricks" && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::DatabricksSqlExecApi => {
            version == "2.0" && vendor == "Databricks" && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::GoConnector => {
            numeric_version(version) && vendor == "Databricks" && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::NodejsConnector => {
            node_connector_version(version)
                && vendor == "Databricks"
                && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::PythonConnector => {
            python_connector_version(version)
                && vendor == "Databricks"
                && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::Adbc => {
            numeric_version_with_optional_revision(version)
                && vendor == "Apache Arrow"
                && KNOWN_BI_TOOLS.contains(&bi_tool)
        }
        ThriftDriverFamily::Unknown => false,
    }
}

fn thrift_driver_family(driver: &str) -> ThriftDriverFamily {
    if driver.is_empty() {
        ThriftDriverFamily::Empty
    } else if THRIFT_DRIVERS.contains(&driver) {
        ThriftDriverFamily::LegacyThrift
    } else if driver == "other" {
        ThriftDriverFamily::Other
    } else if SPARK_XDBC_DRIVERS.contains(&driver) {
        ThriftDriverFamily::SparkXdbc
    } else if DATABRICKS_XDBC_DRIVERS.contains(&driver) {
        ThriftDriverFamily::DatabricksXdbc
    } else if driver == "DatabricksJDBCDriverOSS" {
        ThriftDriverFamily::DatabricksJdbcOss
    } else if driver == "DatabricksSqlExecApi" {
        ThriftDriverFamily::DatabricksSqlExecApi
    } else if driver == "GoDatabricksSqlConnector" {
        ThriftDriverFamily::GoConnector
    } else if driver == "NodejsDatabricksSqlConnector" {
        ThriftDriverFamily::NodejsConnector
    } else if driver == "PyDatabricksSqlConnector" {
        ThriftDriverFamily::PythonConnector
    } else if ADBC_DRIVERS.contains(&driver) {
        ThriftDriverFamily::Adbc
    } else {
        ThriftDriverFamily::Unknown
    }
}

fn numeric_version_with_optional_revision(v: &str) -> bool {
    let Some(end) = consume_numeric_core(v, 0) else {
        return false;
    };
    end == v.len()
        || (v.as_bytes()[end] == b'-' && consume_digits(v.as_bytes(), end + 1) == Some(v.len()))
}

fn oss_jdbc_version(v: &str) -> bool {
    let Some(end) = consume_numeric_core(v, 0) else {
        return false;
    };
    end == v.len() || matches!(&v[end..], "-oss" | "-oss-beta")
}

fn numeric_version(v: &str) -> bool {
    consume_numeric_core(v, 0) == Some(v.len())
}

fn node_connector_version(v: &str) -> bool {
    let Some(end) = consume_numeric_core(v, 0) else {
        return false;
    };
    if end == v.len() {
        return true;
    }
    ["-beta.", "-rc.", "-obs."].iter().any(|suffix| {
        v[end..].starts_with(suffix)
            && consume_digits(v.as_bytes(), end + suffix.len()) == Some(v.len())
    })
}

fn python_connector_version(v: &str) -> bool {
    if !valid_thrift_version_length(v) {
        return false;
    }
    let Some(mut index) = consume_digits(v.as_bytes(), 0) else {
        return false;
    };
    if index < v.len() && v.as_bytes()[index] == b'!' {
        let Some(end) = consume_numeric_core(v, index + 1) else {
            return false;
        };
        index = end;
    } else {
        let Some(end) = consume_numeric_core(v, 0) else {
            return false;
        };
        index = end;
    }

    for marker in ["rc", "a", "b"] {
        if let Some(end) = consume_marked_digits(v, index, marker) {
            index = end;
            break;
        }
    }
    if let Some(end) = consume_marked_digits(v, index, ".post") {
        index = end;
    }
    if let Some(end) = consume_marked_digits(v, index, ".dev") {
        index = end;
    }
    const LOCAL_SUFFIX: &str = "+abnormal";
    if v[index..].starts_with(LOCAL_SUFFIX) {
        index += LOCAL_SUFFIX.len();
        if index < v.len() && v.as_bytes()[index] == b'.' {
            let Some(end) = consume_lower_hex(v.as_bytes(), index + 1) else {
                return false;
            };
            index = end;
        }
    }
    index == v.len()
}

fn consume_numeric_core(v: &str, start: usize) -> Option<usize> {
    if !valid_thrift_version_length(v) {
        return None;
    }
    let b = v.as_bytes();
    let mut index = consume_digits(b, start)?;
    while index + 1 < b.len() && b[index] == b'.' && is_digit(b[index + 1]) {
        index = consume_digits(b, index + 1)?;
    }
    Some(index)
}

fn consume_marked_digits(v: &str, start: usize, marker: &str) -> Option<usize> {
    v[start..]
        .starts_with(marker)
        .then(|| consume_digits(v.as_bytes(), start + marker.len()))
        .flatten()
}

fn consume_digits(v: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    while index < v.len() && is_digit(v[index]) {
        index += 1;
    }
    (index != start).then_some(index)
}

fn consume_lower_hex(v: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    while index < v.len() && is_lower_hex(v[index]) {
        index += 1;
    }
    (index != start).then_some(index)
}

fn valid_thrift_version_length(v: &str) -> bool {
    !v.is_empty() && v.len() <= 64
}

/// Spark Thrift metadata-operation parameter selectivity summary.
pub fn spark_thrift_metadata_ops_param_selectivity(v: &str) -> bool {
    const STANDARD_KEYS: [&str; 5] =
        ["catalogName", "schemaName", "tableName", "columnName", "functionName"];
    const CROSS_REFERENCE_KEYS: [&str; 6] = [
        "parentCatalogName",
        "parentSchemaName",
        "parentTableName",
        "foreignCatalog",
        "foreignSchema",
        "foreignTable",
    ];
    const VALUES: [&str; 11] = [
        "Null",
        "Wildcard",
        "Empty",
        "PrefixMatch",
        "PostfixMatch",
        "ContainsMatch",
        "ExactMatch",
        "Underscore",
        "UnderscoreWithMatchingResult",
        "OtherMatch",
        "WildcardInMiddle",
    ];
    const CROSS_REFERENCE_VALUES: [&str; 2] = ["Null", "ExactMatch"];

    if v.is_empty() || v.len() > 210 {
        return false;
    }
    let mut seen_keys = [""; 6];
    let mut pair_count = 0usize;
    let mut cross_reference = None;
    for pair in v.split(", ") {
        let Some(separator) = pair.find(": ") else {
            return false;
        };
        let key = &pair[..separator];
        let selectivity = &pair[separator + 2..];
        let is_cross_reference = *cross_reference.get_or_insert_with(|| {
            if STANDARD_KEYS.contains(&key) {
                false
            } else {
                CROSS_REFERENCE_KEYS.contains(&key)
            }
        });
        let (allowed_keys, max_pairs): (&[&str], usize) = if is_cross_reference {
            (&CROSS_REFERENCE_KEYS, 6)
        } else {
            (&STANDARD_KEYS, 5)
        };
        let allowed_value = if is_cross_reference {
            CROSS_REFERENCE_VALUES.contains(&selectivity)
        } else {
            VALUES.contains(&selectivity)
        };
        if !allowed_keys.contains(&key)
            || pair_count == max_pairs
            || seen_keys[..pair_count].contains(&key)
            || !allowed_value
        {
            return false;
        }
        seen_keys[pair_count] = key;
        pair_count += 1;
    }
    match cross_reference {
        Some(true) => pair_count == 6,
        Some(false) => true,
        None => false,
    }
}

// ---------------------------------------------------------------------------
// IDs anchored by a literal prefix.
// ---------------------------------------------------------------------------

/// AWS EC2 instance ID: "i-" followed by 17 lowercase hex characters.
pub fn aws_ec2_instance_id(v: &str) -> bool {
    let b = v.as_bytes();
    if b.len() != 19 || b[0] != b'i' || b[1] != b'-' {
        return false;
    }
    b[2..19].iter().all(|&c| is_lower_hex(c))
}

/// AWS resource id of the form `<prefix>` + exactly 17 lowercase hex characters (the modern
/// long-form AWS id). Shared by EBS volume ("vol-") and EBS snapshot ("snap-") ids.
fn aws_hex_resource_id(v: &str, prefix: &str) -> bool {
    let expected_len = prefix.len() + 17;
    if v.len() != expected_len || !v.starts_with(prefix) {
        return false;
    }
    v.as_bytes()[prefix.len()..]
        .iter()
        .all(|&c| is_lower_hex(c))
}

/// AWS EBS volume ID: "vol-" followed by 17 lowercase hex characters.
pub fn aws_ebs_volume_id(v: &str) -> bool {
    aws_hex_resource_id(v, "vol-")
}

/// AWS EBS snapshot ID: "snap-" followed by 17 lowercase hex characters.
pub fn aws_ebs_snapshot_id(v: &str) -> bool {
    aws_hex_resource_id(v, "snap-")
}

/// Databricks task run id: "TaskRunId-" + 1+ digits.
pub fn task_run_id(v: &str) -> bool {
    prefixed_digits(v, "TaskRunId-")
}

/// Databricks Notebooks workload id: "Notebooks-" + 1+ digits.
pub fn notebooks_id(v: &str) -> bool {
    prefixed_digits(v, "Notebooks-")
}

/// Per-user AI Parse Document tile id: "pd-" + 1+ digits (the numeric workspace user id).
pub fn pd_tile_id(v: &str) -> bool {
    prefixed_digits(v, "pd-")
}

/// Notifications-service idempotency key: `notifsvc-<middle>-<token>`, with a trailing UUID or
/// 16-char hex token and a bounded `[A-Za-z0-9_-]` middle.
pub fn notifsvc_idempotency_key(v: &str) -> bool {
    const PREFIX: &str = "notifsvc-";
    const HASH_LEN: usize = 16;
    const UUID_LEN: usize = 36;
    // 120 leaves headroom over the 100-char worst case today: the 47-char prefix bound plus
    // the longest 52-char event name.
    const MAX_MIDDLE_LEN: usize = 120;

    let b = v.as_bytes();
    if !v.starts_with(PREFIX) {
        return false;
    }

    // Trailing, not merely embedded: an embedded token would let anything precede it.
    let mut token_start = None;
    if b.len() > PREFIX.len() + UUID_LEN
        && b[b.len() - UUID_LEN - 1] == b'-'
        && is_uuid_window(b, b.len() - UUID_LEN)
    {
        token_start = Some(b.len() - UUID_LEN);
    } else if b.len() > PREFIX.len() + HASH_LEN
        && b[b.len() - HASH_LEN - 1] == b'-'
        && is_hex_hash(&v[b.len() - HASH_LEN..], HASH_LEN)
    {
        token_start = Some(b.len() - HASH_LEN);
    }
    let Some(token_start) = token_start else {
        return false;
    };

    let middle = &b[PREFIX.len()..token_start - 1];
    (1..=MAX_MIDDLE_LEN).contains(&middle.len())
        && middle
            .iter()
            .all(|&c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

/// Whether `s` is exactly `n` case-insensitive hex characters. Distinct from `hex16`
/// (lowercase-only); the NPS idempotency-key ports accept mixed-case hash tokens.
fn is_hex_hash(s: &str, n: usize) -> bool {
    let b = s.as_bytes();
    b.len() == n && b.iter().all(|&c| is_hex(c))
}

/// Whether the 36-byte window of `b` starting at `off` is a hyphenated UUID (8-4-4-4-12).
fn is_uuid_window(b: &[u8], off: usize) -> bool {
    is_hex_run(b, off, off + 8)
        && b[off + 8] == b'-'
        && is_hex_run(b, off + 9, off + 13)
        && b[off + 13] == b'-'
        && is_hex_run(b, off + 14, off + 18)
        && b[off + 18] == b'-'
        && is_hex_run(b, off + 19, off + 23)
        && b[off + 23] == b'-'
        && is_hex_run(b, off + 24, off + 36)
}

/// Shared helper: a literal `prefix` followed by one or more decimal digits.
fn prefixed_digits(v: &str, prefix: &str) -> bool {
    let b = v.as_bytes();
    if b.len() <= prefix.len() || !v.starts_with(prefix) {
        return false;
    }
    b[prefix.len()..].iter().all(|&c| is_digit(c))
}

/// Logging file source id: "checksum:" + a u64 (1-20 decimal digits).
pub fn logging_file_id(v: &str) -> bool {
    const PREFIX: &str = "checksum:";
    const MAX_U64_DIGITS: usize = 20;
    if !v.starts_with(PREFIX) {
        return false;
    }
    let b = v.as_bytes();
    let from = PREFIX.len();
    let len = b.len() - from;
    if !(1..=MAX_U64_DIGITS).contains(&len) {
        return false;
    }
    b[from..].iter().all(|&c| is_digit(c))
}

/// Databricks approval id: literal "1," then exactly 24 chars from [A-Za-z0-9_].
pub fn approval_id(v: &str) -> bool {
    const PREFIX: &str = "1,";
    const SUFFIX_LEN: usize = 24;
    let expected = PREFIX.len() + SUFFIX_LEN;
    let b = v.as_bytes();
    if b.len() != expected || !v.starts_with(PREFIX) {
        return false;
    }
    b[PREFIX.len()..]
        .iter()
        .all(|&c| is_digit(c) || c.is_ascii_lowercase() || c.is_ascii_uppercase() || c == b'_')
}

/// Estore schema-qualified namespace: "entitystore_" + 1-30 lowercase letters.
pub fn estore_namespace(v: &str) -> bool {
    const PREFIX: &str = "entitystore_";
    const MAX_SUFFIX_LEN: usize = 30;
    if !v.starts_with(PREFIX) {
        return false;
    }
    let b = v.as_bytes();
    let suffix_len = b.len() - PREFIX.len();
    if !(1..=MAX_SUFFIX_LEN).contains(&suffix_len) {
        return false;
    }
    b[PREFIX.len()..].iter().all(|&c| c.is_ascii_lowercase())
}

/// Databricks system actor URI: "system:" or "system:" + lowercase kebab-case ([a-z0-9-]+).
pub fn system_uri(v: &str) -> bool {
    const PREFIX: &str = "system:";
    let b = v.as_bytes();
    if !v.starts_with(PREFIX) {
        return false;
    }
    b[PREFIX.len()..]
        .iter()
        .all(|&c| is_digit(c) || c.is_ascii_lowercase() || c == b'-')
}

// ---------------------------------------------------------------------------
// Timestamps.
// ---------------------------------------------------------------------------

/// Date-only calendar date: YYYY-MM-DD, zero-padded, month and day range-checked (01-12, 01-31).
/// Day-of-month is NOT checked against the month, so "2026-02-31" passes.
pub fn calendar_date(v: &str) -> bool {
    const LEN: usize = 10;
    let b = v.as_bytes();
    if b.len() != LEN {
        return false;
    }
    if b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    for (i, &c) in b.iter().enumerate() {
        if i != 4 && i != 7 && !is_digit(c) {
            return false;
        }
    }
    let month = (b[5] - b'0') * 10 + (b[6] - b'0');
    if !(1..=12).contains(&month) {
        return false;
    }
    let day = (b[8] - b'0') * 10 + (b[9] - b'0');
    (1..=31).contains(&day)
}

/// ISO-8601 UTC timestamp with trailing 'Z'; optional 1-9 fractional digits.
pub fn iso8601_timestamp(v: &str) -> bool {
    const NO_FRACTION_LEN: usize = 20;
    const MAX_FRACTION_DIGITS: usize = 9;
    const MAX_LEN: usize = NO_FRACTION_LEN + 1 + MAX_FRACTION_DIGITS;
    let b = v.as_bytes();
    let len = b.len();
    if !(NO_FRACTION_LEN..=MAX_LEN).contains(&len) {
        return false;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return false;
    }
    if b[len - 1] != b'Z' {
        return false;
    }
    for (i, &c) in b.iter().enumerate().take(19) {
        if i != 4 && i != 7 && i != 10 && i != 13 && i != 16 && !is_digit(c) {
            return false;
        }
    }
    if len > NO_FRACTION_LEN {
        if b[19] != b'.' {
            return false;
        }
        if len < NO_FRACTION_LEN + 2 {
            return false;
        }
        if !b[20..len - 1].iter().all(|&c| is_digit(c)) {
            return false;
        }
    }
    true
}

/// Postgres-style timestamp with a named UTC timezone token (GMT|UTC).
pub fn postgres_timestamp_tz(v: &str) -> bool {
    const NO_FRACTION_LEN: usize = 23;
    const MAX_FRACTION_DIGITS: usize = 9;
    const MAX_LEN: usize = NO_FRACTION_LEN + 1 + MAX_FRACTION_DIGITS;
    let b = v.as_bytes();
    let len = b.len();
    if !(NO_FRACTION_LEN..=MAX_LEN).contains(&len) {
        return false;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b' ' || b[13] != b':' || b[16] != b':' {
        return false;
    }
    for (i, &c) in b.iter().enumerate().take(19) {
        if i != 4 && i != 7 && i != 10 && i != 13 && i != 16 && !is_digit(c) {
            return false;
        }
    }
    if b[len - 4] != b' ' {
        return false;
    }
    let (tz0, tz1, tz2) = (b[len - 3], b[len - 2], b[len - 1]);
    let tz_ok =
        (tz0 == b'G' && tz1 == b'M' && tz2 == b'T') || (tz0 == b'U' && tz1 == b'T' && tz2 == b'C');
    if !tz_ok {
        return false;
    }
    if len == NO_FRACTION_LEN {
        return true;
    }
    if b[19] != b'.' {
        return false;
    }
    // len - 4 - 20 < 1  ->  need at least one fractional digit.
    if (len as i64) - 4 - 20 < 1 {
        return false;
    }
    b[20..len - 4].iter().all(|&c| is_digit(c))
}

// ---------------------------------------------------------------------------
// Tracing / fingerprints.
// ---------------------------------------------------------------------------

/// W3C traceparent: <2 hex>-<32 lowercase alnum>-<16 hex>-<2 hex>, total length 55.
pub fn w3c_traceparent(v: &str) -> bool {
    let b = v.as_bytes();
    if b.len() != 55 {
        return false;
    }
    if b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
        return false;
    }
    (0..55).all(|i| {
        i == 2
            || i == 35
            || i == 52
            || if (3..35).contains(&i) {
                is_digit(b[i]) || b[i].is_ascii_lowercase()
            } else {
                is_lower_hex(b[i])
            }
    })
}

/// JA4 TLS client fingerprint. Fixed length 36.
pub fn ja4_fingerprint(v: &str) -> bool {
    let b = v.as_bytes();
    if b.len() != 36 || b[0] != b't' {
        return false;
    }
    if !is_digit(b[1]) || !is_digit(b[2]) {
        return false;
    }
    let proto_char = b[3];
    if proto_char != b'd' && proto_char != b'i' && proto_char != b'q' {
        return false;
    }
    for &c in &b[4..8] {
        if !is_digit(c) {
            return false;
        }
    }
    let ch8 = b[8];
    let ch9 = b[9];
    if ch8 != b'h' && !is_digit(ch8) {
        return false;
    }
    if !is_digit(ch9) {
        return false;
    }
    if b[10] != b'_' {
        return false;
    }
    for &c in &b[11..23] {
        if !is_lower_hex(c) {
            return false;
        }
    }
    if b[23] != b'_' {
        return false;
    }
    b[24..36].iter().all(|&c| is_lower_hex(c))
}

// ---------------------------------------------------------------------------
// IP addresses.
// ---------------------------------------------------------------------------

/// Truncated IP: an IPv4 "a.b.c.x" or IPv6 (8 groups, last is "x") whose final octet/group is 'x'.
pub fn truncated_ip(v: &str) -> bool {
    let b = v.as_bytes();
    let len = b.len();
    if len < 5 || b[len - 1] != b'x' {
        return false;
    }
    let sep = b[len - 2];
    if sep == b'.' {
        truncated_ipv4(b, len)
    } else if sep == b':' {
        truncated_ipv6(b, len)
    } else {
        false
    }
}

fn truncated_ipv4(b: &[u8], len: usize) -> bool {
    let prefix_end = len - 2;
    let mut dot_count = 0;
    let mut octet_len = 0;
    for &c in &b[..prefix_end] {
        if c == b'.' {
            if octet_len == 0 || octet_len > 3 {
                return false;
            }
            dot_count += 1;
            if dot_count > 2 {
                return false;
            }
            octet_len = 0;
        } else if is_digit(c) {
            octet_len += 1;
            if octet_len > 3 {
                return false;
            }
        } else {
            return false;
        }
    }
    dot_count == 2 && octet_len > 0 && octet_len <= 3
}

fn truncated_ipv6(b: &[u8], len: usize) -> bool {
    let prefix_end = len - 2;
    let mut colon_count = 0;
    let mut group_len = 0;
    for &c in &b[..prefix_end] {
        if c == b':' {
            if group_len > 4 {
                return false;
            }
            colon_count += 1;
            if colon_count > 6 {
                return false;
            }
            group_len = 0;
        } else if is_hex(c) {
            group_len += 1;
            if group_len > 4 {
                return false;
            }
        } else {
            return false;
        }
    }
    colon_count == 6 && group_len <= 4
}

/// Private / non-routable IPv4 address (RFC 1918 + loopback + link-local + wildcard).
pub fn private_ip_address(v: &str) -> bool {
    let bytes = match parse_ipv4(v) {
        Some(b) => b,
        None => return false,
    };
    let [a, b, c, _d] = bytes;
    // Mirrors the java.net.InetAddress predicate set: site-local (RFC 1918), loopback, link-local,
    // and the any-local wildcard 0.0.0.0.
    let site_local = a == 10 || (a == 172 && (16..=31).contains(&b)) || (a == 192 && b == 168);
    let loopback = a == 127;
    let link_local = a == 169 && b == 254;
    let any_local = a == 0 && b == 0 && c == 0 && _d == 0;
    site_local || loopback || link_local || any_local
}

/// Parses a canonical dotted-quad IPv4 literal (no leading zeros, 0-255 per octet).
fn parse_ipv4(v: &str) -> Option<[u8; 4]> {
    // split('.', -1) semantics: a trailing/leading dot yields an empty field → parse fails.
    let parts: Vec<&str> = v.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    let mut out = [0u8; 4];
    for (i, octet) in parts.iter().enumerate() {
        // Reject non-canonical forms the way the Java round-trip check does: leading zeros,
        // signs, empty fields, non-ASCII digits.
        if octet.is_empty() || octet.len() > 3 || !octet.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let val: u32 = octet.parse().ok()?;
        if val > 255 || val.to_string() != *octet {
            return None;
        }
        out[i] = val as u8;
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Cluster ids.
// ---------------------------------------------------------------------------

/// Databricks Runtime cluster id: MMDD-HHMMSS-<1-8 lowercase alnum>[-v2n|-v3n].
pub fn dbr_cluster_id(v: &str) -> bool {
    const MIN_LEN: usize = 13;
    const MAX_LEN: usize = 24;
    let b = v.as_bytes();
    let len = b.len();
    if !(MIN_LEN..=MAX_LEN).contains(&len) {
        return false;
    }
    if b[4] != b'-' || b[11] != b'-' {
        return false;
    }
    for (i, &c) in b.iter().enumerate().take(11) {
        if i != 4 && !is_digit(c) {
            return false;
        }
    }
    let alnum_end = if len >= 17
        && b[len - 4] == b'-'
        && b[len - 3] == b'v'
        && (b[len - 2] == b'2' || b[len - 2] == b'3')
        && b[len - 1] == b'n'
    {
        len - 4
    } else {
        len
    };
    let alnum_len = alnum_end - 12;
    if !(1..=8).contains(&alnum_len) {
        return false;
    }
    b[12..alnum_end]
        .iter()
        .all(|&c| is_digit(c) || c.is_ascii_lowercase())
}

/// Databricks internal cluster id: <6 alnum>--i<10 digits>-<1-8 alnum> (length 21-28).
pub fn internal_cluster_id(v: &str) -> bool {
    const MIN_LEN: usize = 21;
    const MAX_LEN: usize = 28;
    let b = v.as_bytes();
    let len = b.len();
    if !(MIN_LEN..=MAX_LEN).contains(&len) {
        return false;
    }
    for &c in &b[..6] {
        if !(is_digit(c) || c.is_ascii_lowercase()) {
            return false;
        }
    }
    if b[6] != b'-' || b[7] != b'-' || b[8] != b'i' {
        return false;
    }
    for &c in &b[9..19] {
        if !is_digit(c) {
            return false;
        }
    }
    if b[19] != b'-' {
        return false;
    }
    b[20..len]
        .iter()
        .all(|&c| is_digit(c) || c.is_ascii_lowercase())
}

// ---------------------------------------------------------------------------
// Small structural shapes.
// ---------------------------------------------------------------------------

/// ISO 3166-1 alpha-2 country code: exactly two uppercase ASCII letters.
pub fn iso_country_code(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 2 && is_upper(b[0]) && is_upper(b[1])
}

/// ANSI/ISO SQLSTATE: five [0-9A-Z] characters whose two-character class prefix is one Databricks
/// emits (per https://docs.databricks.com/aws/en/error-messages/sqlstates). The three-char subclass
/// is left free-form within [0-9A-Z] to avoid coupling to a runtime error-registry. "XX" is included
/// for internal errors per the doc.
pub fn sql_state(v: &str) -> bool {
    // Two-character class prefixes Databricks emits. Sorted for binary_search (verified by a unit
    // test below); mirrors SqlStateValidator.VALID_CLASSES in DataShapeValidators.java.
    const VALID_CLASSES: [&str; 40] = [
        "01", "02", "07", "08", "0A", "0B", "0K", "0N", "21", "22", "23", "24", "25", "28", "2B",
        "2D", "35", "38", "39", "3D", "3F", "40", "42", "44", "46", "51", "53", "54", "55", "56",
        "57", "58", "82", "F0", "HV", "HY", "KC", "KD", "P0", "XX",
    ];
    let b = v.as_bytes();
    if b.len() != 5 || !b.iter().all(|&c| is_digit(c) || is_upper(c)) {
        return false;
    }
    VALID_CLASSES.binary_search(&&v[0..2]).is_ok()
}

/// Boolean-typed string. Case-insensitive membership in a small fixed set.
pub fn boolean_string(v: &str) -> bool {
    const ALLOWED: [&str; 10] =
        ["true", "false", "yes", "no", "unknown", "1", "0", "success", "failure", "null"];
    let lower = v.to_ascii_lowercase();
    ALLOWED.contains(&lower.as_str())
}

/// HTTP protocol identifier: "HTTP/<major>" or "HTTP/<major>.<minor>" (each a non-negative int).
pub fn http_protocol(v: &str) -> bool {
    const PREFIX: &str = "HTTP/";
    if !v.starts_with(PREFIX) {
        return false;
    }
    let b = v.as_bytes();
    let len = b.len();
    let mut i = PREFIX.len();
    let major_start = i;
    while i < len && is_digit(b[i]) {
        i += 1;
    }
    if i == major_start {
        return false;
    }
    if i == len {
        return true;
    }
    if b[i] != b'.' {
        return false;
    }
    i += 1;
    let minor_start = i;
    while i < len && is_digit(b[i]) {
        i += 1;
    }
    i != minor_start && i == len
}

/// TLS protocol version: "TLSv<digit>.<digit>".
pub fn tls_version(v: &str) -> bool {
    const PREFIX: &str = "TLSv";
    let expected_len = PREFIX.len() + 3;
    let b = v.as_bytes();
    if b.len() != expected_len || !v.starts_with(PREFIX) {
        return false;
    }
    let major = b[PREFIX.len()];
    let dot = b[PREFIX.len() + 1];
    let minor = b[PREFIX.len() + 2];
    is_digit(major) && dot == b'.' && is_digit(minor)
}

/// Kubernetes API version: 'v' + 1+ digits, optionally 'alpha'/'beta' + 1+ digits.
pub fn k8s_api_version(v: &str) -> bool {
    let b = v.as_bytes();
    let len = b.len();
    if len < 2 || b[0] != b'v' {
        return false;
    }
    let mut i = 1;
    while i < len && is_digit(b[i]) {
        i += 1;
    }
    if i == 1 {
        return false;
    }
    if i == len {
        return true;
    }
    let tail = &v[i..];
    let prerelease_len = if tail.starts_with("alpha") {
        5
    } else if tail.starts_with("beta") {
        4
    } else {
        return false;
    };
    i += prerelease_len;
    let pre_start = i;
    while i < len && is_digit(b[i]) {
        i += 1;
    }
    i != pre_start && i == len
}

/// Salesforce account id: exactly 15 or 18 alphanumeric characters.
pub fn salesforce_account_id(v: &str) -> bool {
    let b = v.as_bytes();
    let len = b.len();
    if len != 15 && len != 18 {
        return false;
    }
    b.iter()
        .all(|&c| is_digit(c) || c.is_ascii_lowercase() || c.is_ascii_uppercase())
}

/// Alphanumeric string ([A-Za-z0-9]) whose length is one of a fixed set (currently 9, 10, 11, or
/// 25). Bounded to those exact lengths (not a general token) to stay non-permissive; more lengths
/// can be added to the set as needed. E.g. the AWS customer_identifier (11 chars), the AWS
/// Marketplace product code (25 chars), and the GCP Marketplace procurement account id (9-11 chars).
pub fn bounded_alphanumeric(v: &str) -> bool {
    let b = v.as_bytes();
    let len = b.len();
    if len != 9 && len != 10 && len != 11 && len != 25 {
        return false;
    }
    b.iter().all(|&c| is_digit(c) || is_ascii_letter(c))
}

/// Databricks employee email: non-empty [A-Za-z0-9._%+-] local part + "@databricks.com".
pub fn brickster_email(v: &str) -> bool {
    const SUFFIX: &str = "@databricks.com";
    let b = v.as_bytes();
    let len = b.len();
    if len <= SUFFIX.len() || !v.ends_with(SUFFIX) {
        return false;
    }
    let local_len = len - SUFFIX.len();
    b[..local_len].iter().all(|&c| {
        is_digit(c)
            || c.is_ascii_lowercase()
            || c.is_ascii_uppercase()
            || c == b'.'
            || c == b'_'
            || c == b'%'
            || c == b'+'
            || c == b'-'
    })
}

/// JIRA ticket key: 1-9 uppercase letters, '-', 1-9 decimal digits.
pub fn jira_ticket_key(v: &str) -> bool {
    const MAX_PREFIX_LEN: usize = 9;
    const MAX_DIGITS_LEN: usize = 9;
    const MAX_LEN: usize = MAX_PREFIX_LEN + 1 + MAX_DIGITS_LEN;
    let b = v.as_bytes();
    let len = b.len();
    if !(3..=MAX_LEN).contains(&len) {
        return false;
    }
    let mut i = 0;
    while i < len {
        let c = b[i];
        if c == b'-' {
            if i == 0 || i > MAX_PREFIX_LEN {
                return false;
            }
            i += 1;
            let digits_start = i;
            if i >= len {
                return false;
            }
            while i < len {
                if !is_digit(b[i]) {
                    return false;
                }
                i += 1;
            }
            return i - digits_start <= MAX_DIGITS_LEN;
        }
        if !c.is_ascii_uppercase() {
            return false;
        }
        i += 1;
    }
    false
}

// ---------------------------------------------------------------------------
// Databricks principal / config / pool identifiers.
// ---------------------------------------------------------------------------

/// Databricks principal URI: one of three literal prefixes + non-empty [A-Za-z0-9._\-@/%+] suffix.
pub fn databricks_principal_uri(v: &str) -> bool {
    const PREFIXES: [&str; 3] = [
        "principal://corp.databricks.com/users/",
        "principal://corp.microsoft.com/users/",
        "principal://prod.s2s.databricks.com/services/",
    ];
    let matched_prefix_len = PREFIXES
        .iter()
        .find(|p| v.starts_with(**p))
        .map(|p| p.len());
    let matched_prefix_len = match matched_prefix_len {
        Some(l) => l,
        None => return false,
    };
    let b = v.as_bytes();
    if b.len() <= matched_prefix_len {
        return false;
    }
    b[matched_prefix_len..].iter().all(|&c| {
        is_digit(c)
            || c.is_ascii_lowercase()
            || c.is_ascii_uppercase()
            || c == b'.'
            || c == b'_'
            || c == b'-'
            || c == b'@'
            || c == b'/'
            || c == b'%'
            || c == b'+'
    })
}

/// Apache Spark / DLT config key: "spark." or "pipelines." + letter + [A-Za-z0-9._-]*.
pub fn spark_config_name(v: &str) -> bool {
    const MAX_LEN: usize = 256;
    const SPARK_PREFIX: &str = "spark.";
    const PIPELINES_PREFIX: &str = "pipelines.";
    let b = v.as_bytes();
    let len = b.len();
    if len > MAX_LEN {
        return false;
    }
    let start = if v.starts_with(SPARK_PREFIX) {
        SPARK_PREFIX.len()
    } else if v.starts_with(PIPELINES_PREFIX) {
        PIPELINES_PREFIX.len()
    } else {
        return false;
    };
    if start >= len {
        return false;
    }
    if !is_ascii_letter(b[start]) {
        return false;
    }
    b[start + 1..]
        .iter()
        .all(|&c| is_digit(c) || is_ascii_letter(c) || c == b'.' || c == b'_' || c == b'-')
}

/// Databricks cluster-pool / serverless pod name: fixed pool prefix + version + "-<alnum>" segments.
pub fn cluster_pool_name(v: &str) -> bool {
    const PREFIXES: [&str; 3] = ["cluster-pool-v", "driver-pool-v", "executor-pool-v"];
    const MAX_LEN: usize = 63;
    let b = v.as_bytes();
    let len = b.len();
    if len > MAX_LEN {
        return false;
    }
    let mut i = match PREFIXES.iter().find(|p| v.starts_with(**p)) {
        Some(p) => p.len(),
        None => return false,
    };
    let version_start = i;
    while i < len && is_digit(b[i]) {
        i += 1;
    }
    if i == version_start {
        return false;
    }
    if i >= len || b[i] != b'-' {
        return false;
    }
    i += 1;
    let mut seg_chars_since_hyphen = 0;
    while i < len {
        let c = b[i];
        if c == b'-' {
            if seg_chars_since_hyphen == 0 {
                return false;
            }
            seg_chars_since_hyphen = 0;
        } else if is_digit(c) || c.is_ascii_lowercase() {
            seg_chars_since_hyphen += 1;
        } else {
            return false;
        }
        i += 1;
    }
    seg_chars_since_hyphen > 0
}

/// Databricks serverless node-type id: "serverless-(driver|executor)-" + segments + "-(az|aws|gcp)".
pub fn serverless_node_type_id(v: &str) -> bool {
    const DRIVER_PREFIX: &str = "serverless-driver-";
    const EXECUTOR_PREFIX: &str = "serverless-executor-";
    let b = v.as_bytes();
    let len = b.len();
    let prefix_len = if v.starts_with(DRIVER_PREFIX) {
        DRIVER_PREFIX.len()
    } else if v.starts_with(EXECUTOR_PREFIX) {
        EXECUTOR_PREFIX.len()
    } else {
        return false;
    };
    let suffix_len = if v.ends_with("-aws") {
        4
    } else if v.ends_with("-az") {
        3
    } else if v.ends_with("-gcp") {
        4
    } else {
        return false;
    };
    let middle_end = len - suffix_len;
    if middle_end <= prefix_len {
        return false;
    }
    let mut seg_chars_since_hyphen = 0;
    for &c in &b[prefix_len..middle_end] {
        if c == b'-' {
            if seg_chars_since_hyphen == 0 {
                return false;
            }
            seg_chars_since_hyphen = 0;
        } else if is_digit(c) || c.is_ascii_lowercase() {
            seg_chars_since_hyphen += 1;
        } else {
            return false;
        }
    }
    seg_chars_since_hyphen > 0
}

/// KaaS controllers release version: "kaas-controllers-r" + "<digits>-<digits>" [+ "-rollbacked-to"].
pub fn kaas_release_version_name(v: &str) -> bool {
    const PREFIX: &str = "kaas-controllers-r";
    const SUFFIX: &str = "-rollbacked-to";
    if !v.starts_with(PREFIX) {
        return false;
    }
    let b = v.as_bytes();
    let mut end = b.len();
    if v.ends_with(SUFFIX) {
        end -= SUFFIX.len();
    }
    if end < PREFIX.len() {
        return false;
    }
    // Core between prefix and (optional) suffix must be "<1+ digits>-<1+ digits>".
    let mut i = PREFIX.len();
    let release_start = i;
    while i < end && is_digit(b[i]) {
        i += 1;
    }
    if i == release_start {
        return false;
    }
    if i >= end || b[i] != b'-' {
        return false;
    }
    i += 1;
    let build_start = i;
    while i < end && is_digit(b[i]) {
        i += 1;
    }
    if i == build_start {
        return false;
    }
    i == end
}

// ---------------------------------------------------------------------------
// Regex-backed shapes (rare — kept as `regex` for a faithful port).
// ---------------------------------------------------------------------------

/// OpenAI/Bedrock chat-completion response id.
pub fn chat_completion_response_id(v: &str) -> bool {
    // ^(chatcmpl-[A-Za-z0-9]{29}|msg_bdrk_[A-Za-z0-9]{24})$
    fn alnum_run(s: &str, n: usize) -> bool {
        s.len() == n && s.bytes().all(|c| c.is_ascii_alphanumeric())
    }
    if let Some(rest) = v.strip_prefix("chatcmpl-") {
        alnum_run(rest, 29)
    } else if let Some(rest) = v.strip_prefix("msg_bdrk_") {
        alnum_run(rest, 24)
    } else {
        false
    }
}

#[cfg(test)]
mod spark_thrift_user_agent_tests {
    use super::spark_thrift_user_agent;

    #[test]
    fn accepts_reference_vocabulary_not_covered_by_corpus() {
        for value in [
            "Thrift (C++);;;",
            "Thrift (Python);;;",
            "SparkJDBCDriver;1.2.3;;unknown",
            "SparkODBCDriver;1.2.3;Databricks;Alation",
            "DatabricksODBCDriver;2.8.2-4;MicroStrategy;Looker",
            "ADBCSparkDriver;1.1.0-2;Apache Arrow;Airbyte",
            "other;;;AtlanCatalog",
        ] {
            assert!(spark_thrift_user_agent(value), "expected match: {value}");
        }
    }

    #[test]
    fn preserves_reference_driver_specific_rules() {
        assert!(!spark_thrift_user_agent("Thrift (Python);1.0;;"));
        assert!(!spark_thrift_user_agent("DatabricksODBCDriver;2.8.2;Apache Arrow;Looker"));
        assert!(!spark_thrift_user_agent("ADBCSparkDriver;1.1.0;Databricks;Airbyte"));
    }
}
