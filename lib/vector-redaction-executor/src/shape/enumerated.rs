//! Enumerated-set validators and the shapes that depend on runtime-loaded vocabularies.
//!
//! ~51 shapes are enumerated sets (allowed-value membership) whose values are NOT hand-written:
//! they are generated at build time into `static-shapes.json` by
//! `compliance/shape/java:static_shapes_json` (pulling from proto enums + registries across the
//! repo). `DATA_SHAPE_RELEASE_STEP_NAME` similarly loads its vocabulary from
//! `release-step-names.json`, and `DATA_SHAPE_JFR_REDACTED_THREAD_NAME` from the generated
//! `jfr-thread-name-allowlist.txt`. Java and Python both consume those same artifacts at runtime;
//! this crate embeds them at compile time via `include_str!` of vendored files.
//!
//! A few pattern validators here (`nexus_attribute_id`, `oom_reason_piped`, `infra_data_model_uri`,
//! `release_step_name`, `jfr_redacted_thread_name`, `k8s_node_name`) consult that loaded data, so
//! they live in this module rather than `patterns.rs`.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use serde_json::Value;

use super::patterns;

// Embedded from vendored files. These are point-in-time snapshots from the universe build.
// See src/shape/vendored/README for details on regeneration.
const STATIC_SHAPES_JSON: &str = include_str!("vendored/static-shapes.json");
const RELEASE_STEP_NAMES_JSON: &str = include_str!("vendored/release-step-names.json");
const JFR_THREAD_NAME_ALLOWLIST: &str = include_str!("vendored/jfr-thread-name-allowlist.txt");

// ---------------------------------------------------------------------------
// Static enumerated sets.
// ---------------------------------------------------------------------------

/// Parsed `static-shapes.json`: shape name (`DATA_SHAPE_*`) → set of allowed values, lower-cased.
/// Matching is case-insensitive (mirrors EnumeratedSetValidator: both set and input lower-cased).
struct StaticShapes {
    by_shape: HashMap<String, HashSet<String>>,
}

fn static_shapes() -> &'static StaticShapes {
    static CELL: OnceLock<StaticShapes> = OnceLock::new();
    CELL.get_or_init(|| {
        let root: Value = serde_json::from_str(STATIC_SHAPES_JSON)
            .expect("static-shapes.json embedded at build time must be valid JSON");
        let obj = root
            .as_object()
            .expect("static-shapes.json must be a top-level object");
        let mut by_shape = HashMap::new();
        for (shape_name, values) in obj {
            let set: HashSet<String> = values
                .as_array()
                .expect("static-shapes.json values must be arrays")
                .iter()
                .map(|v| {
                    v.as_str()
                        .expect("static-shapes.json entries must be strings")
                        .to_ascii_lowercase()
                })
                .collect();
            by_shape.insert(shape_name.clone(), set);
        }
        StaticShapes { by_shape }
    })
}

/// Validates `value` against the enumerated set for `shape_name` (a `DATA_SHAPE_*` string).
/// Returns `false` if the shape has no enumerated set (i.e. it is a pattern shape).
pub fn enumerated_set_contains(shape_name: &str, value: &str) -> bool {
    match static_shapes().by_shape.get(shape_name) {
        Some(set) => set.contains(&value.to_ascii_lowercase()),
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Validators that consult a static-shapes set (or another shape) directly.
// ---------------------------------------------------------------------------

/// Nexus attribute id: strip known suffixes / ClusterCreator prefix / "JOBLEVEL_", then every
/// remaining '_'-separated token must be a known token type.
pub fn nexus_attribute_id(value: &str) -> bool {
    const SUFFIXES: [&str; 4] = ["-offline", "-down", "-dryrun", "_JOB_LEVEL"];
    const JOBLEVEL_PREFIX: &str = "JOBLEVEL_";
    if value.is_empty() {
        return false;
    }
    // Strip known suffixes iteratively.
    let mut v = value;
    let mut stripped = true;
    while stripped {
        stripped = false;
        for suffix in SUFFIXES {
            if let Some(rest) = v.strip_suffix(suffix) {
                v = rest;
                stripped = true;
                break;
            }
        }
    }
    // Strip optional ClusterCreator prefix: "{Creator}-{rest}" → "{rest}".
    if let Some(hyphen) = v.find('-') {
        if hyphen > 0
            && enumerated_set_contains("DATA_SHAPE_CLUSTER_CREATOR", &v[..hyphen])
        {
            v = &v[hyphen + 1..];
        }
    }
    // Strip optional "JOBLEVEL_" prefix.
    if let Some(rest) = v.strip_prefix(JOBLEVEL_PREFIX) {
        v = rest;
    }
    // Empty after stripping (bare ClusterCreator, or ClusterCreator + suffixes only).
    if v.is_empty() {
        return true;
    }
    // Split on '_' (split(-1) semantics) — each section must be a known token type.
    v.split('_').all(nexus_is_known_token)
}

fn nexus_is_known_token(t: &str) -> bool {
    if t.is_empty() {
        return true;
    }
    patterns::numeric(t)
        || patterns::hex16(t)
        || patterns::uuid(t)
        || patterns::internal_cluster_id(t)
        || patterns::notebooks_id(t)
        || enumerated_set_contains("DATA_SHAPE_CLUSTER_CREATOR", t)
        || t.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// OOM reason piped: '|'-separated tokens, each a member of DATA_SHAPE_OOM_REASON. Empty passes.
pub fn oom_reason_piped(value: &str) -> bool {
    if value.is_empty() {
        return true;
    }
    value
        .split('|')
        .all(|token| !token.is_empty() && enumerated_set_contains("DATA_SHAPE_OOM_REASON", token))
}

/// Databricks engine-request outcome (EngineRequest.outcome): a non-composite outcome literal, or
/// the composite failure form "<sql_state>:<error_class>" whose left part is a SQLSTATE (or the
/// "MissingSqlState" filler) and right part is a Spark error class from DATA_SHAPE_DBR_ERROR_CLASS
/// (or the "MissingErrorClass" filler). Delegating both parts to shape validators keeps free-form
/// text out. Mirrors `EngineRequestOutcomeValidator` in DataShapeValidators.java.
pub fn engine_request_outcome(value: &str) -> bool {
    const WORDS: [&str; 4] = ["Success", "Canceled", "Interrupted", "UnknownError"];
    const MISSING_SQL_STATE: &str = "MissingSqlState";
    const MISSING_ERROR_CLASS: &str = "MissingErrorClass";
    if value.is_empty() {
        return false;
    }
    if WORDS.contains(&value) {
        return true;
    }
    // Composite failure form "<sql_state>:<error_class>" — exactly one ':', neither part empty.
    let colon = match value.find(':') {
        Some(0) => return false, // empty left part
        Some(i) => i,
        None => return false,
    };
    if colon == value.len() - 1 {
        return false; // empty right part
    }
    let left = &value[..colon];
    let right = &value[colon + 1..];
    if right.contains(':') {
        return false; // more than one ':'
    }
    let left_ok = left == MISSING_SQL_STATE || patterns::sql_state(left);
    let right_ok = right == MISSING_ERROR_CLASS
        || enumerated_set_contains("DATA_SHAPE_DBR_ERROR_CLASS", right);
    left_ok && right_ok
}

// ---------------------------------------------------------------------------
// Infra tokens shared by kube_context and infra_data_model_uri.
// ---------------------------------------------------------------------------

const INFRA_ENVS: [&str; 3] = ["dev", "staging", "prod"];
const INFRA_CLOUDS: [&str; 3] = ["aws", "azure", "gcp"];
const INFRA_CLUSTER_TYPES: [&str; 28] = [
    "general",
    "gc",
    "meta",
    "pdo",
    "ri",
    "s1",
    "nephos",
    "obs",
    "kafka",
    "mlserving",
    "mlserv",
    "tidb",
    "prototype",
    "rcp",
    "ingress",
    "sawless",
    "brickstore",
    "teleport",
    "generic",
    "brickindex",
    "nephos-kata",
    "genai-serving",
    "genai-training",
    "ff",
    "vdb",
    "brc",
    "dataplane",
    "dod",
];

fn infra_env(s: &str) -> bool {
    INFRA_ENVS.contains(&s)
}

fn infra_cloud(s: &str) -> bool {
    INFRA_CLOUDS.contains(&s)
}

fn infra_cluster_type(s: &str) -> bool {
    INFRA_CLUSTER_TYPES.contains(&s)
}

/// Databricks Kubernetes context name: optional env prefix, fixed cloud, then region/type/shard.
pub fn kube_context(value: &str) -> bool {
    const MAX_LEN: usize = 100;
    const MAX_TOKENS: usize = 7;
    const MAX_TOKEN_LEN: usize = 25;
    const MIN_REGION_LEN: usize = 2;
    const MAX_TOKENS_AFTER_CT: usize = 3;
    if value.len() > MAX_LEN {
        return false;
    }
    let dash = value.find('-');
    let offset = match dash {
        Some(d) if d > 0 && infra_env(&value[..d]) => d + 1,
        _ => 0,
    };
    let dash2 = value[offset..].find('-').map(|i| i + offset);
    let dash2 = match dash2 {
        Some(d) if infra_cloud(&value[offset..d]) => d,
        _ => return false,
    };
    let suffix = &value[dash2 + 1..];
    let tokens: Vec<&str> = suffix.split('-').collect();
    if tokens.is_empty() || tokens.len() > MAX_TOKENS {
        return false;
    }
    for t in &tokens {
        if t.is_empty() || t.len() > MAX_TOKEN_LEN || !is_lower_alnum(t) {
            return false;
        }
    }
    if tokens[0].len() < MIN_REGION_LEN {
        return false;
    }
    let ct_index = tokens.iter().position(|t| kube_is_cluster_type(t));
    if let Some(idx) = ct_index {
        if tokens.len() - idx - 1 > MAX_TOKENS_AFTER_CT {
            return false;
        }
    }
    true
}

fn is_lower_alnum(s: &str) -> bool {
    s.bytes()
        .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
}

/// Cluster-type token match for kube_context: exact, or a cluster-type prefix + all-digit tail.
fn kube_is_cluster_type(token: &str) -> bool {
    if infra_cluster_type(token) {
        return true;
    }
    for ct in INFRA_CLUSTER_TYPES {
        if let Some(rest) = token.strip_prefix(ct) {
            if !rest.is_empty() && rest.bytes().all(|c| c.is_ascii_digit()) {
                return true;
            }
        }
    }
    false
}

/// AWS availability zone: a recognized cloud region immediately followed by a single lowercase
/// zone letter, with no separator (e.g. "us-west-2a" = region "us-west-2" + "a"). The region prefix
/// is validated against `DATA_SHAPE_CLOUD_REGION`, so the shape tracks the region set; the trailing
/// letter is the structural anchor.
pub fn cloud_availability_zone(value: &str) -> bool {
    if value.len() < 2 {
        return false;
    }
    let zone = value.as_bytes()[value.len() - 1];
    if !zone.is_ascii_lowercase() {
        return false;
    }
    enumerated_set_contains("DATA_SHAPE_CLOUD_REGION", &value[..value.len() - 1])
}

pub fn gcp_compute_instance_id(value: &str) -> bool {
    const MIN_PROJECT_NUMBER_DIGITS: usize = 8;
    const MAX_PROJECT_NUMBER_DIGITS: usize = 16;
    const MAX_INSTANCE_ID_DIGITS: usize = 20;

    let mut parts = value.split('_');
    let (Some(project_number), Some(zone), Some(instance_id)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    let project_len = project_number.len();
    if !(MIN_PROJECT_NUMBER_DIGITS..=MAX_PROJECT_NUMBER_DIGITS).contains(&project_len)
        || !project_number.bytes().all(|c| c.is_ascii_digit())
    {
        return false;
    }
    let instance_len = instance_id.len();
    (1..=MAX_INSTANCE_ID_DIGITS).contains(&instance_len)
        && gcp_zone(zone)
        && instance_id.bytes().all(|c| c.is_ascii_digit())
}

fn gcp_zone(value: &str) -> bool {
    const MAX_ZONE_SUFFIX_LENGTH: usize = 4;

    let Some(last_hyphen) = value.rfind('-') else {
        return false;
    };
    let zone_suffix = &value[last_hyphen + 1..];
    if !(1..=MAX_ZONE_SUFFIX_LENGTH).contains(&zone_suffix.len())
        || !zone_suffix
            .bytes()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
    {
        return false;
    }
    let region = &value[..last_hyphen];
    enumerated_set_contains("DATA_SHAPE_CLOUD_REGION", region)
}

/// Kubernetes node name in its EC2 private-DNS form: "ip-" + a dashed IPv4 + one of the two AWS
/// terminal domains -- ".ec2.internal" (us-east-1, GovCloud, older default VPCs) or
/// ".<region>.compute.internal" (every other region). The dashed IPv4 head is dotted and validated
/// by the private-IP shape; the region label (compute form) is validated against the closed
/// cloud-region set.
/// Examples: "ip-10-51-52-176.ec2.internal", "ip-10-20-8-229.us-west-2.compute.internal"
pub fn k8s_node_name(value: &str) -> bool {
    let rest = match value.strip_prefix("ip-") {
        Some(r) => r,
        None => return false,
    };
    let head = if let Some(head_and_region) = rest.strip_suffix(".compute.internal") {
        // ip-<ipv4>.<region>.compute.internal -- peel the region label off the tail.
        match head_and_region.rfind('.') {
            Some(dot) => {
                let region = &head_and_region[dot + 1..];
                if !enumerated_set_contains("DATA_SHAPE_CLOUD_REGION", region) {
                    return false;
                }
                &head_and_region[..dot]
            }
            None => return false,
        }
    } else if let Some(h) = rest.strip_suffix(".ec2.internal") {
        h
    } else {
        return false;
    };
    // Dot the dashed IPv4 head and reuse the private-IP shape (four-octet form + private ranges).
    patterns::private_ip_address(&head.replace('-', "."))
}

/// Infra data-model URI: "<scheme>:<env>[/<cloud>[/<domain>[/<region>[/<type>[/<code>]]]]]".
pub fn infra_data_model_uri(value: &str) -> bool {
    const REG_DOMAINS: [&str; 7] =
        ["public", "single-tenant", "gov", "dod", "mooncake", "fedramp", "usgov"];
    let colon = match value.find(':') {
        Some(c) => c,
        None => return false,
    };
    let scheme = &value[..colon];
    let expected = match scheme {
        "environment" => 1,
        "cloud" => 2,
        "regulatory-domain" => 3,
        "region" => 4,
        "kubernetes-cluster-type" => 5,
        "kubernetes-cluster" => 6,
        _ => return false,
    };
    let path = &value[colon + 1..];
    if path.is_empty() {
        return false;
    }
    // Java uses split("/") with the default limit — trailing empty strings ARE stripped. Emulate.
    let parts = split_default(path, '/');
    if parts.len() != expected {
        return false;
    }
    if !infra_env(parts[0]) {
        return false;
    }
    if expected >= 2 && !infra_cloud(parts[1]) {
        return false;
    }
    if expected >= 3 && !REG_DOMAINS.contains(&parts[2]) {
        return false;
    }
    if expected >= 4 && !enumerated_set_contains("DATA_SHAPE_CLOUD_REGION", parts[3]) {
        return false;
    }
    if expected >= 5 && !infra_cluster_type(parts[4]) {
        return false;
    }
    if expected >= 6 && !cluster_code(parts[5]) {
        return false;
    }
    true
}

/// `[a-z0-9]{2,6}` — the kubernetes-cluster code component of infra_data_model_uri.
fn cluster_code(s: &str) -> bool {
    let len = s.len();
    (2..=6).contains(&len)
        && s.bytes()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
}

/// Java `String.split(regex)` with the default limit: trailing empty strings are removed.
fn split_default(s: &str, sep: char) -> Vec<&str> {
    let mut parts: Vec<&str> = s.split(sep).collect();
    while parts.len() > 1 && parts.last() == Some(&"") {
        parts.pop();
    }
    // Java's split also collapses a single empty result for "" input, but callers guard non-empty.
    parts
}

// ---------------------------------------------------------------------------
// Release step name.
// ---------------------------------------------------------------------------

struct ReleaseStepNames {
    constants: HashSet<String>,
    // Region-URI prefixes stored WITH the trailing '-' (e.g. "deploy-").
    region_uri_prefixes: Vec<String>,
    region_uri_anchor: String,
    // Stage-completed prefix stored WITH the trailing '-' (e.g. "stage-completed-").
    stage_completed_prefix: String,
    stage_enum_values: HashSet<String>,
    replay_diff_slugs: Vec<String>,
    // Roles stored as "-<role>-" infixes.
    replay_diff_role_infixes: Vec<String>,
    // Bucket-number marker stored as "-<marker>-" for the generated child-step suffix.
    bucket_number_infix: String,
}

fn release_step_names() -> &'static ReleaseStepNames {
    static CELL: OnceLock<ReleaseStepNames> = OnceLock::new();
    CELL.get_or_init(|| {
        let root: Value = serde_json::from_str(RELEASE_STEP_NAMES_JSON)
            .expect("release-step-names.json embedded at build time must be valid JSON");
        let str_array = |field: &str| -> Vec<String> {
            root.get(field)
                .and_then(Value::as_array)
                .unwrap_or_else(|| panic!("release-step-names.json missing array field '{field}'"))
                .iter()
                .map(|v| {
                    v.as_str()
                        .expect("release-step-names entries are strings")
                        .to_string()
                })
                .collect()
        };
        let str_field = |field: &str| -> String {
            root.get(field)
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("release-step-names.json missing string field '{field}'"))
                .to_string()
        };
        let constants: HashSet<String> = str_array("constants").into_iter().collect();
        let region_uri_prefixes: Vec<String> = str_array("regionUriPrefixes")
            .into_iter()
            .map(|p| format!("{p}-"))
            .collect();
        let region_uri_anchor = str_field("regionUriAnchor");
        let stage_completed_prefix = format!("{}-", str_field("stageCompletedPrefix"));
        let stage_enum_values: HashSet<String> = str_array("stageEnumValues").into_iter().collect();
        let replay_diff_slugs = str_array("replayDiffSlugs");
        let replay_diff_role_infixes: Vec<String> = str_array("replayDiffRoles")
            .into_iter()
            .map(|r| format!("-{r}-"))
            .collect();
        let bucket_number_infix = format!("-{}-", str_field("bucketNumberMarker"));
        ReleaseStepNames {
            constants,
            region_uri_prefixes,
            region_uri_anchor,
            stage_completed_prefix,
            stage_enum_values,
            replay_diff_slugs,
            replay_diff_role_infixes,
            bucket_number_infix,
        }
    })
}

/// Release-engineering step name. Exact constant, stage-completed form, region-URI form, or
/// replay-diff form, optionally followed by one generated bucket-number suffix.
pub fn release_step_name(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let r = release_step_names();
    if release_step_base_name(r, value) {
        return true;
    }
    release_strip_bucket_number_suffix(r, value)
        .is_some_and(|base_name| release_step_base_name(r, base_name))
}

fn release_step_base_name(r: &ReleaseStepNames, value: &str) -> bool {
    if r.constants.contains(value) {
        return true;
    }
    if let Some(suffix) = value.strip_prefix(&r.stage_completed_prefix) {
        return r.stage_enum_values.contains(suffix);
    }
    for prefix in &r.region_uri_prefixes {
        if let Some(suffix) = value.strip_prefix(prefix) {
            return release_matches_region_uri(r, suffix);
        }
    }
    for slug in &r.replay_diff_slugs {
        if release_matches_replay_diff(r, value, slug) {
            return true;
        }
    }
    false
}

fn release_strip_bucket_number_suffix<'a>(r: &ReleaseStepNames, value: &'a str) -> Option<&'a str> {
    let (base_name, bucket_number) = value.rsplit_once(&r.bucket_number_infix)?;
    let first_digit = bucket_number.as_bytes().first()?;
    if base_name.is_empty()
        || *first_digit == b'0'
        || !bucket_number.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    Some(base_name)
}

fn release_matches_region_uri(r: &ReleaseStepNames, suffix: &str) -> bool {
    if !suffix.starts_with(&r.region_uri_anchor) {
        return false;
    }
    // Region names contain dashes, so probe splits at each '-' from the end; at most one validates.
    let mut cut = suffix.len();
    loop {
        let candidate = &suffix[..cut];
        if infra_data_model_uri(candidate) {
            return cut == suffix.len() || release_is_region_uri_tail(suffix, cut + 1);
        }
        // Move `cut` to the previous '-' before the current one.
        match suffix[..cut].rfind('-') {
            Some(prev) if prev > 0 => cut = prev,
            _ => return false,
        }
    }
}

/// Tail after a region URI: lowercase/uppercase/digit kebab tokens (mixedCase dust setups allowed).
fn release_is_region_uri_tail(value: &str, from: usize) -> bool {
    if from > value.len() {
        return false;
    }
    let b = value.as_bytes();
    let mut token_len = 0;
    for &c in &b[from..] {
        if c == b'-' {
            if token_len == 0 {
                return false;
            }
            token_len = 0;
        } else if c.is_ascii_digit() || c.is_ascii_lowercase() || c.is_ascii_uppercase() {
            token_len += 1;
        } else {
            return false;
        }
    }
    token_len > 0
}

fn release_matches_replay_diff(r: &ReleaseStepNames, value: &str, slug: &str) -> bool {
    for role_infix in &r.replay_diff_role_infixes {
        let prefix = format!("{slug}{role_infix}");
        if let Some(rest) = value.strip_prefix(&prefix) {
            return release_is_lowercase_kebab_run(rest);
        }
    }
    false
}

fn release_is_lowercase_kebab_run(value: &str) -> bool {
    let b = value.as_bytes();
    let mut token_len = 0;
    for &c in b {
        if c == b'-' {
            if token_len == 0 {
                return false;
            }
            token_len = 0;
        } else if c.is_ascii_digit() || c.is_ascii_lowercase() {
            token_len += 1;
        } else {
            return false;
        }
    }
    token_len > 0
}

// ---------------------------------------------------------------------------
// JFR redacted thread name.
// ---------------------------------------------------------------------------

/// The placeholder that marks a counter position in an allowlist pattern; everything else in a
/// pattern is literal text. Must match the generator's constant and the Java/Python validators'.
const JFR_DIGITS: &str = "{DIGITS}";
/// What the producer masks a 6+ digit run to, and the only non-numeric span `{DIGITS}` accepts.
const JFR_LONG_DIGITS: &str = "<longDigits>";
const JFR_MAX_COUNTER_DIGITS: usize = 5;

/// The allowlist patterns, each pre-split on `{DIGITS}` into its literal segments, so matching does
/// no allocation and no parsing per call.
fn jfr_thread_name_patterns() -> &'static [Vec<&'static str>] {
    static CELL: OnceLock<Vec<Vec<&'static str>>> = OnceLock::new();
    CELL.get_or_init(|| {
        JFR_THREAD_NAME_ALLOWLIST
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| line.split(JFR_DIGITS).collect())
            .collect()
    })
}

/// A counter span: 1-5 ASCII digits, or the placeholder a masked 6+ digit run becomes.
fn jfr_is_counter(span: &str) -> bool {
    if span == JFR_LONG_DIGITS {
        return true;
    }
    !span.is_empty()
        && span.len() <= JFR_MAX_COUNTER_DIGITS
        && span.bytes().all(|c| c.is_ascii_digit())
}

/// Matches `value` against one pre-split pattern: the literal segments must appear in order, flush at
/// both ends, with a counter filling every gap.
fn jfr_matches_pattern(value: &str, segments: &[&str]) -> bool {
    // A pattern with no placeholder is pure literal text.
    if segments.len() == 1 {
        return value == segments[0];
    }
    if !value.starts_with(segments[0]) {
        return false;
    }
    let mut pos = segments[0].len();
    for segment in &segments[1..] {
        if segment.is_empty() {
            // Trailing placeholder: the rest of the value is the counter.
            return jfr_is_counter(&value[pos..]);
        }
        // Literal segments are platform text, so the first occurrence is the only viable split: a
        // counter is digits-only and cannot contain the segment's leading non-digit.
        match value[pos..].find(segment) {
            Some(offset) => {
                if !jfr_is_counter(&value[pos..pos + offset]) {
                    return false;
                }
                pos += offset + segment.len();
            }
            None => return false,
        }
    }
    pos == value.len()
}

/// A redacted JFR JVM thread name: a name on the producer's platform-thread allowlist, read from
/// the vendored allowlist. The format and the reasoning behind it are documented in the universe
/// source; the part that matters at this call site is that a raw 6+ digit run is REJECTED rather
/// than re-masked, so a producer that failed to mask surfaces instead of being silently repaired.
///
/// Mirrors JfrRedactedThreadNameValidator (Java).
pub fn jfr_redacted_thread_name(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    jfr_thread_name_patterns()
        .iter()
        .any(|segments| jfr_matches_pattern(value, segments))
}
