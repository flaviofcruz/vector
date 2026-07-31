//! `bricklens_config_enricher` — per-packet, multitenant routing for the bricklens-agent OTLP
//! export path.
//!
//! Events flow in; each is tagged with the transport config the downstream `bricklens_ingest`
//! sink needs (`.destination = { workspace_id, uc_resource_target, service_principal_resource,
//! service_principal_type }`) and forwarded, or dropped when logging is disabled for the tenant.
//!
//! Identity = `(source_pod, config_key)`:
//!   * `source_pod` — trusted half: the sender's pod IP (non-spoofable under host-enforced
//!     networking; NOT the microVM-TAP guest IP, cf. SDR-3916), mapped to `{tenant, trust_tier}`
//!     via trusted k8s pod labels (local, k8s-watch — an enrichment table).
//!   * `config_key` — from the OTLP payload (VS-instance id).
//!
//! Trust policy: an untrusted (single-tenant) pod is pinned to its label tenant (payload claim
//! ignored); a trusted-multitenant pod is believed. Then `(tenant, config_key) -> action` is
//! resolved from bricklens-config (remote, async, TTL-cached; a cold miss awaits + caches, so a
//! new tenant resolves on its first packet — no snapshot, no drop).
//!
//! Why a custom `TaskTransform` and not the `http` enrichment table (#612) or a VRL function:
//!   * A VRL function is sync + pure — it cannot `await` a live per-event lookup. A `TaskTransform`
//!     is the only place we can.
//!   * The `http` enrichment table is the wrong shape here. It bulk-fetches a dataset into an
//!     in-memory snapshot and refreshes it on an interval (`find_table_row` is a sync lookup over
//!     preloaded rows); it is built for "small, relatively static reference datasets". Our lookup
//!     key is `(workload_type, config_id, workspace_id)` where `config_id` is per-VS-instance —
//!     HIGH CARDINALITY and CHURNING (instances come and go at runtime on a multitenant node). A
//!     preloaded snapshot would (a) be perpetually stale/incomplete for brand-new config_ids until
//!     the next refresh — dropping their events in the meantime — and (b) be impossible to populate
//!     completely anyway: bricklens-config exposes only a per-key `GetTelemetryConfig`, no
//!     list/batch RPC to enumerate a node's configs. A read-through cache with synchronous on-demand
//!     resolution (below) is the correct pattern for high-cardinality dynamic keys: a cold key
//!     resolves on its first event (fail-closed on error), and repeats are served from cache.
//!   * Keeping the gRPC call in a vector component (vs. an operator sidecar writing an enrichment
//!     file) also gives the routing decision + drop native vector `events_sent`/`events_received`/
//!     `ComponentEventsDropped` telemetry in one place, rather than split across two processes.
//!   Trade-off accepted: a cold miss blocks this event on a synchronous gRPC round-trip (bounded by
//!   `request_timeout` + the single-flight + TTL cache below), rather than never blocking as a
//!   preloaded table would.
//!
//! The bricklens-config gRPC `ConfigResolver` is implemented, reusing the `bricklens_ingest`
//! sink's dynamic-proto transport (hyper + hyper_openssl mTLS, prost_reflect encode/decode).
//!
//! PoC wiring: EVERY event flows through a `GetTelemetryConfig` lookup. The request inputs
//! `(workload_type, config_id, workspace_id)` are read per-event from fields a preceding remap
//! annotates (hardcoded for the PoC — see the pipeline's `bricklens_external_otlp_identity`
//! transform). This is the trusted-multitenant model (the emitter presents its identity in the
//! request); the `resolve_tenant` / `PodIdentity` / `TrustTier` trust machinery below is the
//! eventual pod-IP-anchored path (k8s-watch table) for untrusted single-tenant pods, kept +
//! unit-tested but not yet on the hot path.

use std::{
    collections::HashMap,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_stream::stream;
use futures::{Stream, StreamExt};
use hyper::{Body, Request, Uri, body::HttpBody as _};
use prost_reflect::{
    DescriptorPool, DynamicMessage, MethodDescriptor, Value as ProtoValue, prost::Message,
};
use vector_lib::{
    config::clone_input_definitions,
    configurable::configurable_component,
    internal_event::{ComponentEventsDropped, INTENTIONAL, UNINTENTIONAL},
};

use crate::{
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    event::{Event, EventStatus},
    schema,
    sinks::bricklens_ingest::resolve_grpc_status,
    transforms::{TaskTransform, Transform},
};

/// Default TTL for a cached `(workload_type, config_id, workspace_id) -> action` resolution.
/// 5 minutes: high-cardinality multitenant nodes see many distinct keys, so a short TTL would
/// re-hit bricklens-config constantly; 300s bounds config staleness (a changed/disabled config
/// takes effect within ~5m) while collapsing the vast majority of repeats. Tunable per deploy via
/// the `cache_ttl_secs` config field.
const DEFAULT_CACHE_TTL_SECS: u64 = 300;

/// Default retries (beyond the first attempt) for a failed GetTelemetryConfig → 3 attempts total.
/// Retries use exponential backoff from `RETRY_BACKOFF_BASE`.
const DEFAULT_MAX_RETRIES: u32 = 2;

/// Base backoff before the first retry; doubles each subsequent retry (100ms, 200ms, ...). Kept
/// small: the transform stream is serial, so this delay is head-of-line for later events.
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(100);

/// Ceiling on a single retry's backoff. `max_retries` is operator-configurable, and an uncapped
/// `base * 2^attempt` would overflow `u32::pow` / the `Duration` multiply for large attempt counts
/// (panicking in debug, wrapping in release). Capping both bounds the delay and removes that
/// overflow. 30s is far beyond any useful per-retry wait given the serial head-of-line cost.
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// SP type to assume when bricklens-config does not supply one. The vector-search SSP is a System
/// SP, so default to it. TEMPORARY: the bricklens-config `AuthConfig` proto has no
/// `service_principal_type` field yet (it is being added; cf. bricklens-ingest-external's
/// `UnityCatalogTableConfig.service_principal_type`). Once the config supplies the type, this
/// fallback should be removed so a mis/unconfigured type is not silently coerced to System.
const DEFAULT_SERVICE_PRINCIPAL_TYPE: &str = "SERVICE_PRINCIPAL_TYPE_SYSTEM";

/// `users.ServicePrincipalType` enum-number -> name, for reading the SP type when bricklens-config
/// delivers it as a proto enum (wire form is the number). Mirrors central/api/users/
/// service_principal_type.proto. Kept as a small explicit map (not a generated binding) because the
/// transform only needs the name string to hand to the downstream sink.
fn service_principal_type_name(number: i32) -> Option<&'static str> {
    match number {
        0 => Some("SERVICE_PRINCIPAL_TYPE_UNSPECIFIED"),
        1 => Some("SERVICE_PRINCIPAL_TYPE_USER_MANAGED"),
        2 => Some("SERVICE_PRINCIPAL_TYPE_APPLICATION"),
        3 => Some("SERVICE_PRINCIPAL_TYPE_SYSTEM"),
        4 => Some("SERVICE_PRINCIPAL_TYPE_USER_MANAGED_CLOUD"),
        5 => Some("SERVICE_PRINCIPAL_TYPE_CUSTOMER_GRANTABLE_APPLICATION"),
        6 => Some("SERVICE_PRINCIPAL_TYPE_AGENT"),
        _ => None,
    }
}

/// Read `service_principal_type` from a `TelemetryConfig` `AuthConfig` message, accepting either the
/// enum name (string) or its number (proto enum wire form). Returns `None` when the field is absent
/// or resolves to `SERVICE_PRINCIPAL_TYPE_UNSPECIFIED` so the caller can apply its default.
/// `prost_reflect` decodes an enum field as `Value::EnumNumber(i32)`; a string-typed field decodes
/// as `Value::String`. Note a proto3 enum has no presence — an unset field reads as number 0
/// (UNSPECIFIED), not "missing" — so treat UNSPECIFIED (by number OR name) as unset here.
fn read_service_principal_type(auth: &DynamicMessage) -> Option<String> {
    const UNSPECIFIED: &str = "SERVICE_PRINCIPAL_TYPE_UNSPECIFIED";
    let field = auth.get_field_by_name("service_principal_type")?;
    match field.as_ref() {
        ProtoValue::EnumNumber(0) => None,
        ProtoValue::EnumNumber(n) => service_principal_type_name(*n).map(str::to_string),
        ProtoValue::String(s) if !s.is_empty() && s != UNSPECIFIED => Some(s.clone()),
        _ => None,
    }
}

/// Configuration for the `bricklens_config_enricher` transform.
#[configurable_component(transform(
    "bricklens_config_enricher",
    "Tag each OTLP export event with its per-tenant destination (table + SSP) resolved live from bricklens-config, or drop it when disabled (skeleton)."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct BricklensConfigEnricherConfig {
    /// gRPC endpoint of `bricklens-config`. REQUIRED (together with `proto_descriptor_path`): the
    /// transform builds a live gRPC resolver (hyper + mTLS) that calls `GetTelemetryConfig`. There
    /// is no no-op fallback; `build()` errors if this or `proto_descriptor_path` is unset (rather
    /// than silently dropping every event).
    #[serde(default)]
    pub bricklens_config_endpoint: Option<String>,

    /// TTL, in seconds, for cached `(tenant, config_key) -> action` resolutions (default 300).
    /// Raise it to cut GetTelemetryConfig load on high-churn multitenant nodes; lower it to make
    /// config changes take effect sooner. See `DEFAULT_CACHE_TTL_SECS`.
    #[serde(default)]
    pub cache_ttl_secs: Option<u64>,

    /// Path to the bricklens-config `FileDescriptorSet` (.pb), shipped into the vector image from
    /// universe (same mechanism as the `bricklens_ingest` sink's descriptor). REQUIRED together with
    /// `bricklens_config_endpoint`; `build()` errors if either is unset (no no-op fallback).
    #[serde(default)]
    pub proto_descriptor_path: Option<PathBuf>,

    /// Fully-qualified bricklens-config gRPC service name (from universe jsonnet).
    #[serde(default)]
    pub service_name: Option<String>,

    /// gRPC method name, e.g. `GetTelemetryConfig`.
    #[serde(default)]
    pub method_name: Option<String>,

    /// TLS/mTLS config for the bricklens-config connection (client cert + s2s-proxy SNI override),
    /// same shape as the `bricklens_ingest` sink.
    #[serde(default)]
    pub tls: Option<crate::tls::TlsEnableableConfig>,

    /// Per-request timeout (seconds) for the bricklens-config call (default 5).
    ///
    /// The `GetTelemetryConfig` request inputs (workload_type / config_id / workspace_id) are NOT
    /// config here — they are read per-event from fields a preceding remap annotates, so every
    /// event drives its own lookup.
    #[serde(default)]
    pub request_timeout_secs: Option<u64>,

    /// Number of RETRIES (not counting the first attempt) for a failed `GetTelemetryConfig`, with
    /// exponential backoff (default 2 → up to 3 attempts total). A retry only helps a transient
    /// failure (connect/timeout/transport); it also adds up to `sum(backoff)` of extra latency on a
    /// persistently-failing call before the event fails closed, and because the transform stream is
    /// serial, that latency is head-of-line. Set to 0 to disable. See `DEFAULT_MAX_RETRIES`.
    #[serde(default)]
    pub max_retries: Option<u32>,
}

impl_generate_config_from_default!(BricklensConfigEnricherConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "bricklens_config_enricher")]
impl TransformConfig for BricklensConfigEnricherConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        let ttl = Duration::from_secs(self.cache_ttl_secs.unwrap_or(DEFAULT_CACHE_TTL_SECS));

        // Both bricklens_config_endpoint and proto_descriptor_path are REQUIRED: the transform only
        // does one thing -- resolve each event's destination from a live bricklens-config -- so a
        // build with either unset has no safe behavior. There is intentionally no no-op / stub
        // fallback resolver: an empty resolver returns Ok(None), which route() treats as "no config
        // -> drop", so a mis/partially-configured transform would silently drop EVERY event. Fail
        // loudly at build() instead of black-holing the pipeline in production.
        let (Some(_endpoint), Some(_descriptor)) =
            (&self.bricklens_config_endpoint, &self.proto_descriptor_path)
        else {
            return Err("bricklens_config_enricher requires both bricklens_config_endpoint and \
                        proto_descriptor_path to be set (there is no no-op fallback; a partial or \
                        empty config would silently drop all events)"
                .into());
        };
        // Resolver stack (outer -> inner): cache -> retry -> live gRPC. A cache hit skips both retry
        // and the RPC; on a miss, RetryResolver retries the gRPC call with backoff, and only the
        // final (post-retry) result is cached.
        let max_retries = self.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let grpc: Arc<dyn ConfigResolver> = Arc::new(GrpcConfigResolver::new(self)?);
        let retrying: Arc<dyn ConfigResolver> = Arc::new(RetryResolver::new(grpc, max_retries));
        let configs = Arc::new(CachedConfigResolver::new(retrying, ttl));

        Ok(Transform::event_task(BricklensConfigEnricher { configs }))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        // Adds `.destination` sub-fields but does not change the top-level type; pass the schema
        // definition through (the destination shape can be pinned in a follow-up).
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }
}

// ---------------------------------------------------------------------------
// Identity + action model
// ---------------------------------------------------------------------------

/// How much to trust a pod's self-asserted tenant, decided from its (trusted) k8s labels.
///
/// The only distinction that actually drives routing is TRUSTED vs UNTRUSTED — can we believe the
/// tenant the payload asserts, or must we pin it to what the (trusted) pod labels say? Tenancy
/// (single- vs multi-tenant) is orthogonal: a *trusted* source is believed whether it is single- or
/// multi-tenant (a trusted single-tenant pod behaves like a trusted multi-tenant one), and an
/// *untrusted* source is pinned regardless. These two variants name the two cases the first
/// iteration supports; if we later need e.g. untrusted-multitenant (microVMs on trusted nodes) it
/// is a new variant, but `resolve_tenant` still only branches on the trusted/untrusted axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustTier {
    /// Untrusted source (e.g. a single-tenant customer dblet): pin to the label-derived tenant,
    /// ignore any tenant the payload claims.
    UntrustedSingleTenant,
    /// Trusted source (a Databricks-controlled component; single- or multi-tenant): believe the
    /// tenant it asserts in the payload.
    TrustedMultiTenant,
}

/// Trusted identity for a source pod, resolved from k8s pod labels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodIdentity {
    pub tenant: String,
    pub trust_tier: TrustTier,
}

/// Resolved routing/transport config for a `(tenant, config_key)`, from bricklens-config.
///
/// TODO(follow-up): this currently models only the Unity Catalog **table** destination. Generalize
/// it to the other destinations bricklens-ingest-external's `Destination` oneof will support — UC
/// **volume** and UC **connection** (third-party, e.g. Datadog) — e.g. an enum over
/// table/volume/connection rather than a bare `uc_resource_target`. Table-only is the first
/// iteration; volume/connection land with the corresponding ingest-external handlers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteInfo {
    /// Whether logging is currently enabled for this tenant/config.
    pub enabled: bool,
    /// Fully-qualified target table `<catalog>.<schema>.<table>`.
    pub uc_resource_target: String,
    /// Managed SP resource path to authenticate as, e.g. `accounts/<account>/bricklens`.
    pub service_principal_resource: String,
    /// `users.ServicePrincipalType` enum name, e.g. `SERVICE_PRINCIPAL_TYPE_SYSTEM`.
    pub service_principal_type: String,
}

/// Trust policy: choose the tenant to route as.
/// - trusted-multitenant pod: believe the payload's tenant (fall back to the pod's own tenant);
/// - untrusted pod: pin to the label-derived tenant, ignoring any payload claim.
pub fn resolve_tenant(identity: &PodIdentity, payload_tenant: Option<&str>) -> String {
    match identity.trust_tier {
        TrustTier::TrustedMultiTenant => payload_tenant
            .map(str::to_string)
            .unwrap_or_else(|| identity.tenant.clone()),
        TrustTier::UntrustedSingleTenant => identity.tenant.clone(),
    }
}

// ---------------------------------------------------------------------------
// Config resolution (bricklens-config GetTelemetryConfig)
// ---------------------------------------------------------------------------

/// The identity inputs for a `GetTelemetryConfig` request, carried per-event. In the multitenant
/// end state these are derived from the trusted pod identity + the OTLP payload; for the PoC a
/// preceding remap annotates them (hardcoded workload_type, config_id, workspace_id).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConfigLookupKey {
    pub workload_type: i32,
    pub config_id: String,
    pub workspace_id: i64,
}

/// `lookup key -> action`, from bricklens-config `GetTelemetryConfig`. Remote + async.
#[async_trait::async_trait]
pub trait ConfigResolver: Send + Sync {
    async fn resolve(&self, key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>>;
}

/// Live `ConfigResolver` that calls bricklens-config `GetTelemetryConfig` over gRPC. Reuses the
/// `bricklens_ingest` sink's dynamic-proto transport (hyper + hyper_openssl mTLS, prost_reflect
/// encode/decode) so DBNS / s2s-proxy routing + the client cert are identical.
///
/// The request inputs (workload_type / config_id / workspace_id) come per-event via the
/// `ConfigLookupKey`. Note bricklens-config verifies `workspace_id` against the caller cert
/// server-side, so a wrong workspace_id fails there rather than silently mis-routing.
pub struct GrpcConfigResolver {
    client: hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>,
    endpoint: Uri,
    method: MethodDescriptor,
    request_timeout: Duration,
}

impl GrpcConfigResolver {
    pub fn new(config: &BricklensConfigEnricherConfig) -> crate::Result<Self> {
        let endpoint_str = config.bricklens_config_endpoint.clone().ok_or_else(|| {
            "bricklens_config_endpoint is required for the gRPC resolver".to_string()
        })?;
        let descriptor_path = config
            .proto_descriptor_path
            .clone()
            .ok_or_else(|| "proto_descriptor_path is required for the gRPC resolver".to_string())?;
        let service_name = config
            .service_name
            .clone()
            .ok_or_else(|| "service_name is required for the gRPC resolver".to_string())?;
        let method_name = config
            .method_name
            .clone()
            .ok_or_else(|| "method_name is required for the gRPC resolver".to_string())?;

        let descriptor_bytes = std::fs::read(&descriptor_path).map_err(|e| {
            format!(
                "Failed to read proto descriptor {:?}: {}",
                descriptor_path, e
            )
        })?;
        let fds = prost_reflect::prost_types::FileDescriptorSet::decode(&descriptor_bytes[..])
            .map_err(|e| format!("Failed to decode FileDescriptorSet: {}", e))?;
        let pool = DescriptorPool::from_file_descriptor_set(fds)
            .map_err(|e| format!("Failed to build descriptor pool: {}", e))?;
        let service_desc = pool
            .get_service_by_name(&service_name)
            .ok_or_else(|| format!("Service '{}' not found in descriptor", service_name))?;
        let method = service_desc
            .methods()
            .find(|m| m.name() == method_name)
            .ok_or_else(|| format!("Method '{}' not found in service", method_name))?;

        let client = build_grpc_client(config.tls.as_ref())?;
        let endpoint: Uri = endpoint_str.parse().map_err(|e| {
            format!(
                "Invalid bricklens_config_endpoint '{}': {}",
                endpoint_str, e
            )
        })?;

        Ok(Self {
            client,
            endpoint,
            method,
            request_timeout: Duration::from_secs(config.request_timeout_secs.unwrap_or(5)),
        })
    }
}

/// A `ConfigResolver` decorator that retries a failed inner resolve with exponential backoff.
///
/// Layered over the live `GrpcConfigResolver` (and under `CachedConfigResolver`) so retries are
/// composable and unit-testable independently of the gRPC transport. Retries help only transient
/// failures; a deterministic error just costs `sum(backoff)` before the same error is returned. We
/// retry on ANY error rather than classifying transient-vs-terminal from the stringly error type —
/// the extra attempts are cheap and bounded, and misclassifying a transient failure as terminal
/// (dropping data a retry would have saved) is worse than a few wasted tries. Because the transform
/// stream is serial, the backoff delay is head-of-line for later events, so keep `max_retries` and
/// the base backoff small.
pub struct RetryResolver {
    inner: Arc<dyn ConfigResolver>,
    max_retries: u32,
    backoff_base: Duration,
}

impl RetryResolver {
    pub fn new(inner: Arc<dyn ConfigResolver>, max_retries: u32) -> Self {
        Self {
            inner,
            max_retries,
            backoff_base: RETRY_BACKOFF_BASE,
        }
    }

    /// Test-only constructor allowing a tiny backoff so retry tests don't sleep for real.
    #[cfg(test)]
    fn with_backoff(inner: Arc<dyn ConfigResolver>, max_retries: u32, backoff_base: Duration) -> Self {
        Self {
            inner,
            max_retries,
            backoff_base,
        }
    }
}

/// Capped exponential backoff: `base * 2^attempt`, clamped to `MAX_RETRY_BACKOFF`. Overflow-safe —
/// `saturating_pow` + `checked_mul` keep a large (operator-set) `max_retries` from panicking on
/// `u32::pow` or the `Duration` multiply; a would-be overflow just saturates to the cap.
fn backoff_for_attempt(base: Duration, attempt: u32) -> Duration {
    let factor = 2u32.saturating_pow(attempt);
    base.checked_mul(factor)
        .unwrap_or(MAX_RETRY_BACKOFF)
        .min(MAX_RETRY_BACKOFF)
}

#[async_trait::async_trait]
impl ConfigResolver for RetryResolver {
    async fn resolve(&self, key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>> {
        let mut attempt = 0;
        loop {
            match self.inner.resolve(key).await {
                Ok(value) => return Ok(value),
                Err(e) if attempt >= self.max_retries => return Err(e),
                Err(e) => {
                    let backoff = backoff_for_attempt(self.backoff_base, attempt);
                    tracing::warn!(
                        error = %e,
                        attempt = attempt + 1,
                        max_retries = self.max_retries,
                        backoff_ms = backoff.as_millis() as u64,
                        "bricklens_config_enricher: GetTelemetryConfig failed; retrying after backoff"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl ConfigResolver for GrpcConfigResolver {
    /// One GetTelemetryConfig attempt. Retries are layered on top by `RetryResolver` (composed in
    /// `build()`), so this stays a single request/response.
    async fn resolve(&self, key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>> {
        // Build GetTelemetryConfigRequest from the per-event lookup key. Use try_set_field_by_name
        // (not set_field_by_name, which unwraps internally): if the shipped bricklens-config
        // descriptor's field types don't match these value kinds, return an error rather than
        // panicking the transform's async task.
        let mut req_msg = DynamicMessage::new(self.method.input());
        req_msg
            .try_set_field_by_name("workload_type", ProtoValue::EnumNumber(key.workload_type))
            .map_err(|e| format!("set workload_type: {}", e))?;
        req_msg
            .try_set_field_by_name("config_id", ProtoValue::String(key.config_id.clone()))
            .map_err(|e| format!("set config_id: {}", e))?;
        req_msg
            .try_set_field_by_name("workspace_id", ProtoValue::I64(key.workspace_id))
            .map_err(|e| format!("set workspace_id: {}", e))?;
        let payload = req_msg.encode_to_vec();

        tracing::info!(
            workload_type = key.workload_type,
            config_id = %key.config_id,
            workspace_id = key.workspace_id,
            endpoint = %self.endpoint,
            "bricklens_config_enricher: calling GetTelemetryConfig"
        );

        let path = format!(
            "/{}/{}",
            self.method.parent_service().full_name(),
            self.method.name()
        );
        let mut uri_parts = self.endpoint.clone().into_parts();
        uri_parts.path_and_query = Some(
            path.parse()
                .map_err(|e| format!("Bad request path: {}", e))?,
        );
        let uri = Uri::from_parts(uri_parts).map_err(|e| format!("Bad request URI: {}", e))?;

        let grpc_timeout = format!("{}S", self.request_timeout.as_secs());
        let http_req = Request::builder()
            .uri(uri)
            .method("POST")
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .header("grpc-encoding", "identity")
            .header("grpc-timeout", grpc_timeout)
            .body(Body::from(frame_grpc_message(payload)))
            .map_err(|e| format!("Failed to build request: {}", e))?;

        // Enforce the deadline CLIENT-side. `grpc-timeout` above is only a hint the server may
        // ignore, and a TaskTransform has no Tower Timeout layer (unlike sinks); without this a
        // hung/black-holed bricklens-config would make this future never resolve and — because
        // route() awaits it in the serial per-event stream loop — head-of-line-block the whole
        // transform indefinitely.
        //
        // Use ONE absolute deadline spanning both phases (request + body/trailer read). Two separate
        // `timeout(request_timeout, ...)` windows would allow up to 2x request_timeout of head-of-line
        // blocking; `timeout_at(deadline, ...)` bounds the whole resolve() to a single request_timeout.
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let response = tokio::time::timeout_at(deadline, self.client.request(http_req))
            .await
            .map_err(|_| {
                format!(
                    "bricklens-config request timed out after {}s",
                    self.request_timeout.as_secs()
                )
            })?
            .map_err(|e| format!("bricklens-config request failed: {}", e))?;

        // The gRPC status can arrive in the initial HEADERS (a "Trailers-Only" response, typical for
        // fast errors) OR in the HTTP/2 trailers after the body. The s2s-proxy/Envoy hop this call
        // traverses can inject a non-OK status in the trailers; reading headers alone would miss it
        // and then mis-decode the (empty/error) body. Drain data frames then trailers explicitly
        // (to_bytes() discards trailers), and reuse the sink's resolve_grpc_status (header-wins),
        // under the same client deadline.
        let response_headers = response.headers().clone();
        let mut response_body = response.into_body();
        let mut body = bytes::BytesMut::new();
        let read = async {
            while let Some(chunk) =
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut response_body).poll_data(cx))
                    .await
            {
                body.extend_from_slice(
                    &chunk
                        .map_err(|e| format!("Failed to read bricklens-config response: {}", e))?,
                );
            }
            let trailers =
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut response_body).poll_trailers(cx))
                    .await
                    .map_err(|e| format!("Failed to read bricklens-config trailers: {}", e))?;
            Ok::<_, String>(trailers)
        };
        // Same absolute deadline as the request phase above: the request + read together get one
        // request_timeout, not one each.
        let trailers = tokio::time::timeout_at(deadline, read)
            .await
            .map_err(|_| {
                format!(
                    "bricklens-config response timed out after {}s",
                    self.request_timeout.as_secs()
                )
            })??;

        let (status, message) = resolve_grpc_status(&response_headers, trailers.as_ref());
        if status != 0 {
            return Err(format!(
                "bricklens-config grpc-status {}: {}",
                status,
                message.unwrap_or_else(|| "Unknown error".to_string())
            )
            .into());
        }

        let msg_bytes = unframe_grpc_message(body.as_ref())?;
        let resp_msg = DynamicMessage::decode(self.method.output(), msg_bytes)
            .map_err(|e| format!("Failed to decode TelemetryConfig: {}", e))?;

        let action = route_info_from_telemetry_config(&resp_msg);
        tracing::info!(
            found = action.is_some(),
            enabled = action.as_ref().map(|a| a.enabled).unwrap_or(false),
            uc_resource_target = action
                .as_ref()
                .map(|a| a.uc_resource_target.as_str())
                .unwrap_or(""),
            "bricklens_config_enricher: GetTelemetryConfig returned"
        );
        Ok(action)
    }
}

/// Build the same hyper + hyper_openssl mTLS client the `bricklens_ingest` sink uses, so DBNS /
/// s2s-proxy routing + the client cert are identical. (Copied from
/// `src/sinks/bricklens_ingest/sink.rs`; factor into a shared helper in a follow-up.)
fn build_grpc_client(
    tls: Option<&crate::tls::TlsEnableableConfig>,
) -> crate::Result<hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>> {
    let mut http_connector = hyper::client::HttpConnector::new();
    http_connector.enforce_http(false);

    let verify_cert = tls
        .and_then(|t| t.options.verify_certificate)
        .unwrap_or(true);
    let verify_hostname = tls.and_then(|t| t.options.verify_hostname).unwrap_or(true);
    let server_name = tls.and_then(|t| t.options.server_name.clone());
    let key_file = tls.and_then(|t| t.options.key_file.clone());
    let crt_file = tls.and_then(|t| t.options.crt_file.clone());

    let mut ssl_builder = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
        .map_err(|e| format!("Failed to create SSL builder: {}", e))?;
    // Require TLS 1.3. The s2s-proxy N/S ingress (pop-proxy) only trusts the serverless-DP client
    // certificate CA on TLS 1.3; on TLS 1.2 it rejects the same client cert with `unknown_ca`
    // (alert 48). Verified at the wire on staging against three routes (bricklens-config-trusted-
    // daemon, bricklens-ingest-external, bricklens-ingest-internal): all three handshake cleanly on
    // 1.3 and all three fail identically on forced 1.2. Bare `SslMethod::tls()` lets OpenSSL pick
    // either, and this transform was landing on 1.2 -> `unknown_ca` -> every GetTelemetryConfig
    // dropped (fail-closed). Pin the floor to 1.3 so we always negotiate the version the ingress
    // trusts (the bricklens_ingest sink happens to negotiate 1.3 already, which is why it works).
    ssl_builder
        .set_min_proto_version(Some(openssl::ssl::SslVersion::TLS1_3))
        .map_err(|e| format!("Failed to set minimum TLS version to 1.3: {}", e))?;
    if !verify_cert {
        ssl_builder.set_verify(openssl::ssl::SslVerifyMode::NONE);
    }
    if let (Some(crt), Some(key)) = (crt_file, key_file) {
        // Use set_certificate_CHAIN_file, not set_certificate_file: the former sends every cert in
        // the PEM (leaf + intermediates), the latter sends ONLY the leaf. bricklens-config's s2s-proxy
        // N/S ingress trusts the UWI cert's ROOT (Data Plane Misc Root) but must be handed the
        // intermediate (Staging DP Services UWI CA) to build leaf->root; with a leaf-only presentation
        // it cannot complete the path and rejects the handshake with `unknown_ca` (TLS alert 48).
        // Verified at the wire on staging: leaf-only dbts2 cert -> unknown_ca; full-chain dbts2 cert
        // (creds.cert.pem, 3 blocks) -> handshake accepted. (openssl `SSL_CTX_use_certificate_file` vs
        // `SSL_CTX_use_certificate_chain_file` gotcha.)
        ssl_builder
            .set_certificate_chain_file(&crt)
            .map_err(|e| format!("Failed to load client certificate chain {:?}: {}", crt, e))?;
        ssl_builder
            .set_private_key_file(&key, openssl::ssl::SslFiletype::PEM)
            .map_err(|e| format!("Failed to load client key {:?}: {}", key, e))?;
    }
    let mut https_connector =
        hyper_openssl::HttpsConnector::with_connector(http_connector, ssl_builder)
            .map_err(|e| format!("Failed to create HTTPS connector: {}", e))?;
    let sni_override = server_name.clone();
    https_connector.set_callback(move |connection, _uri| {
        connection.set_verify_hostname(verify_hostname);
        if let Some(ref name) = sni_override {
            connection.set_use_server_name_indication(false);
            connection.set_hostname(name)?;
        }
        Ok(())
    });
    Ok(hyper::Client::builder()
        .http2_only(true)
        .build(https_connector))
}

/// Wrap a proto message in the 5-byte gRPC length-prefix frame (1-byte uncompressed flag + 4-byte
/// big-endian length).
fn frame_grpc_message(payload: Vec<u8>) -> Vec<u8> {
    let mut framed = Vec::with_capacity(payload.len() + 5);
    framed.push(0);
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(&payload);
    framed
}

/// Strip the 5-byte gRPC length-prefix frame, returning the message bytes.
fn unframe_grpc_message(body: &[u8]) -> crate::Result<&[u8]> {
    if body.len() < 5 {
        return Err(format!("gRPC response too short: {} bytes", body.len()).into());
    }
    if body[0] != 0 {
        return Err("compressed gRPC responses are not supported"
            .to_string()
            .into());
    }
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    let end = 5 + len;
    if body.len() < end {
        return Err(format!("gRPC response truncated: need {}, have {}", end, body.len()).into());
    }
    Ok(&body[5..end])
}

/// Navigate a decoded `TelemetryConfig` to the first logging sink writing to a Unity Catalog table
/// and turn it into a `RouteInfo`. Returns `None` when there is no such sink (treated as logging
/// disabled for this tenant/config).
///
/// TODO(EOQ2 universe port): this hard-codes the bricklens-config `TelemetryConfig` navigation
/// (mappings -> sinks -> logging_sink -> destination -> unity_catalog / auth_config) and picks the
/// FIRST unity_catalog logging sink. That shape is assumed for testing; it should be reconciled with
/// the finalized bricklens-config schema and made configurable rather than embedded in code. Like
/// `lookup_key_from_event`, the extraction strategy varies per product/telemetry type, so this
/// belongs in Vector configuration (or a per-product remap) once Vector Search's payload + config
/// contract is settled — not compiled in here. Kept minimal now to reduce throwaway work when Vector
/// is ported into universe by EOQ2.
fn route_info_from_telemetry_config(config: &DynamicMessage) -> Option<RouteInfo> {
    let mappings_field = config.get_field_by_name("mappings");
    let mappings = mappings_field.as_ref().and_then(|f| f.as_list())?;
    // Scan every unity_catalog logging sink and keep the first that is actually enabled (non-empty
    // table). Do NOT return on the first UC sink unconditionally: a sink with an empty
    // table_full_name yields a disabled action, and returning it early would drop the event even
    // though a later sink in the same or a subsequent mapping has a valid table. Fall back to the
    // first disabled sink only if no enabled one exists (so "config present but logging off" is
    // still represented as an intentional-drop action rather than None / missing-config).
    let mut fallback: Option<RouteInfo> = None;
    for mapping in mappings {
        let Some(mapping) = mapping.as_message() else {
            continue;
        };
        let sinks_field = mapping.get_field_by_name("sinks");
        let Some(sinks) = sinks_field.as_ref().and_then(|f| f.as_list()) else {
            continue;
        };
        for sink in sinks {
            let Some(sink) = sink.as_message() else {
                continue;
            };
            let logging_field = sink.get_field_by_name("logging_sink");
            let Some(logging) = logging_field.as_ref().and_then(|f| f.as_message()) else {
                continue;
            };
            let destination_field = logging.get_field_by_name("destination");
            let Some(destination) = destination_field.as_ref().and_then(|f| f.as_message()) else {
                continue;
            };
            let uc_field = destination.get_field_by_name("unity_catalog");
            let Some(uc) = uc_field.as_ref().and_then(|f| f.as_message()) else {
                continue;
            };
            let table_field = uc.get_field_by_name("table_full_name");
            let table = table_field
                .as_ref()
                .and_then(|f| f.as_str())
                .unwrap_or_default()
                .to_string();

            let auth = destination
                .get_field_by_name("auth_config")
                .and_then(|f| f.as_message().cloned());
            let service_principal_resource = auth
                .as_ref()
                .and_then(|auth| {
                    auth.get_field_by_name("service_principal_resource")
                        .as_ref()
                        .and_then(|f| f.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_default();

            // Pull the SP type from the config's AuthConfig when it carries one. The enum can be
            // delivered either as its name (string) or its number (proto enum wire form); accept
            // both. When the field is absent, fall back to a System SP (SSP).
            //
            // TODO: the current bricklens-config AuthConfig has only `service_principal_resource`,
            // not `service_principal_type` (pending — Hima is adding it to the TelemetryConfig proto;
            // cf. bricklens-ingest-external's UnityCatalogTableConfig.service_principal_type, which
            // the downstream sink needs to distinguish ASP vs SSP). Until that field ships, this read
            // always misses and we default to SSP. Remove the fallback once the proto provides it.
            let service_principal_type = auth
                .as_ref()
                .and_then(read_service_principal_type)
                .unwrap_or_else(|| DEFAULT_SERVICE_PRINCIPAL_TYPE.to_string());

            let action = RouteInfo {
                enabled: !table.is_empty(),
                uc_resource_target: table,
                service_principal_resource,
                service_principal_type,
            };
            if action.enabled {
                return Some(action);
            }
            fallback.get_or_insert(action);
        }
    }
    fallback
}

/// Default max distinct `(workload_type, config_id, workspace_id)` entries held in the resolver
/// cache. `config_id` is a per-VS-instance id, so key cardinality can be large; a bounded LRU caps
/// memory (least-recently-used entries evict when full) while TTL still gates staleness on read.
const DEFAULT_CACHE_MAX_ENTRIES: usize = 10_000;

/// Bounded TTL cache around a `ConfigResolver`. A cold miss awaits the inner (async) resolve and
/// caches it, so a brand-new tenant resolves on its first event — no pre-population, no drop.
/// Bounded by an LRU capacity so a long-running transform can't leak memory under high
/// `config_id` cardinality (unbounded growth was the prior hazard); freshness is enforced by TTL.
pub struct CachedConfigResolver {
    inner: Arc<dyn ConfigResolver>,
    ttl: Duration,
    cache: Mutex<lru::LruCache<ConfigLookupKey, CacheEntry>>,
    /// Single-flight: per-key fetch locks so concurrent cold misses for the SAME key issue one
    /// GetTelemetryConfig, not N. The `TaskTransform` stream is serial per instance today (so this
    /// can't trigger on one instance), but the resolver is `Arc`-shared and could be driven
    /// concurrently in future; without this, a burst of first-sight events for a hot new key would
    /// stampede bricklens-config. A tokio async `Mutex` (held across the `.await`) serializes
    /// same-key fetchers; the loser re-reads the cache the winner just populated. Keyed entries are
    /// removed once the fetch settles so the map does not grow unbounded.
    in_flight: Mutex<HashMap<ConfigLookupKey, Arc<tokio::sync::Mutex<()>>>>,
}

struct CacheEntry {
    fetched_at: Instant,
    value: Option<RouteInfo>,
}

impl CachedConfigResolver {
    pub fn new(inner: Arc<dyn ConfigResolver>, ttl: Duration) -> Self {
        Self::with_capacity(inner, ttl, DEFAULT_CACHE_MAX_ENTRIES)
    }

    pub fn with_capacity(
        inner: Arc<dyn ConfigResolver>,
        ttl: Duration,
        max_entries: usize,
    ) -> Self {
        let cap = std::num::NonZeroUsize::new(max_entries.max(1))
            .expect("max_entries.max(1) is always >= 1");
        Self {
            inner,
            ttl,
            cache: Mutex::new(lru::LruCache::new(cap)),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Read a fresh (non-expired) cache entry, if present. Separated so both the fast path and the
    /// post-fetch-lock re-check share identical freshness logic.
    fn fresh_cached(&self, key: &ConfigLookupKey) -> Option<Option<RouteInfo>> {
        let mut cache = self.cache.lock().expect("cache mutex poisoned");
        cache.get(key).and_then(|entry| {
            (entry.fetched_at.elapsed() < self.ttl).then(|| entry.value.clone())
        })
    }

    pub async fn resolve(&self, key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>> {
        // Fast path: a fresh cache hit. `LruCache::get` marks the entry most-recently-used and needs
        // `&mut`, hence `lock()`; the std guard is dropped before any `.await`, so it is never held
        // across a fetch. A stale/absent entry falls through to the single-flight fetch below.
        if let Some(value) = self.fresh_cached(key) {
            return Ok(value);
        }

        // Single-flight: take (or create) the per-key async fetch lock and hold it across the fetch,
        // so concurrent cold misses for the same key collapse into ONE GetTelemetryConfig. Different
        // keys use different locks and still fetch in parallel.
        let fetch_lock = self
            .in_flight
            .lock()
            .expect("in_flight mutex poisoned")
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = fetch_lock.lock().await;

        // Re-check under the fetch lock: a caller we queued behind may have just populated the cache,
        // in which case we serve their result instead of issuing a duplicate RPC.
        if let Some(value) = self.fresh_cached(key) {
            self.release_in_flight(key, &fetch_lock);
            return Ok(value);
        }

        // Cold or stale and we are the elected fetcher: resolve live, then cache (negative results
        // are cached too, bounded by TTL and by the LRU capacity). `put` evicts the LRU entry when
        // at capacity. On error we still release the in-flight slot so the key is retried, not wedged.
        let result = self.inner.resolve(key).await;
        if let Ok(value) = &result {
            self.cache.lock().expect("cache mutex poisoned").put(
                key.clone(),
                CacheEntry {
                    fetched_at: Instant::now(),
                    value: value.clone(),
                },
            );
        }
        self.release_in_flight(key, &fetch_lock);
        result
    }

    /// Drop the per-key in-flight entry once this fetcher is done, but only when no other task still
    /// holds a clone of the lock. `strong_count == 2` means just the map entry + our local
    /// `fetch_lock`, so removing it can't strand a queued waiter; a higher count means another task
    /// is mid-fetch/queued, so we leave the entry for it to clean up.
    fn release_in_flight(&self, key: &ConfigLookupKey, fetch_lock: &Arc<tokio::sync::Mutex<()>>) {
        let mut in_flight = self.in_flight.lock().expect("in_flight mutex poisoned");
        if Arc::strong_count(fetch_lock) <= 2 {
            in_flight.remove(key);
        }
    }
}

// ---------------------------------------------------------------------------
// Transform
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct BricklensConfigEnricher {
    configs: Arc<CachedConfigResolver>,
}

impl BricklensConfigEnricher {
    /// Resolve + tag one event. EVERY event flows through a `GetTelemetryConfig` lookup keyed by the
    /// `(workload_type, config_id, workspace_id)` a preceding remap annotates onto it (the trusted
    /// MT identity model). On an enabled result the event's `.destination` is set and the event is
    /// forwarded to the downstream `bricklens_ingest` sink; **every other outcome fails closed
    /// (drops the event)** so no event ever reaches the sink without a resolved destination:
    ///
    /// - enabled config      -> set `.destination`, forward.
    /// - disabled / no config -> intentional drop (this tenant/config should not export).
    /// - missing annotation   -> unintentional drop (the identity remap did not run / renamed a
    ///   field; forwarding would send an un-routable event).
    /// - lookup error         -> unintentional drop, fail-closed (a bricklens-config outage must not
    ///   leak un-routed data to `-external`; the finalizer is NOT acked as delivered).
    ///
    /// Dropping via `None` + `ComponentEventsDropped` lets the source's acknowledgement finalize the
    /// event as *dropped* (not delivered), so at-least-once producers can retry rather than silently
    /// lose data — the correct fail-closed contract.
    ///
    /// Batch-granularity trade-off: the finalizer status set below is per *event*, but finalizers are
    /// shared at coarser grain upstream. The OTLP source attaches one `BatchNotifier` per gRPC request
    /// across all its events (`sources/opentelemetry/grpc.rs`), and `reduce` merges every input
    /// event's finalizers onto the single reduced output (`transforms/reduce/transform.rs`
    /// `metadata.merge`). Because `BatchStatus` takes the max priority (Rejected > Errored >
    /// Delivered), one failed lookup marks the whole shared batch Errored/Rejected — so the source
    /// returns a non-OK status and an at-least-once producer retries the ENTIRE batch, re-delivering
    /// siblings that already succeeded. That duplicate delivery is deliberate and acceptable here:
    /// this is a fail-closed export where at-least-once (possible duplicates) is the contract, and
    /// silently dropping un-routed data (the alternative — leaving the finalizer Delivered) is not.
    async fn route(&self, mut event: Event) -> Option<Event> {
        let Some(key) = lookup_key_from_event(&event) else {
            // Rejected (permanent), not the default Dropped: a Dropped finalizer is a no-op on the
            // source batch (BatchNotifier::update_status ignores Dropped/Delivered), so the source
            // would ack the event as *delivered* and the producer would never retry. A missing
            // identity annotation is unfixable by retry, so mark it Rejected to fail closed without
            // requesting a redelivery.
            event.metadata().update_status(EventStatus::Rejected);
            emit!(ComponentEventsDropped::<UNINTENTIONAL> {
                count: 1,
                reason: "bricklens_config_enricher: event missing workload_type/config_id/workspace_id \
                         annotation (identity remap not run or field renamed)",
            });
            return None;
        };
        match self.configs.resolve(&key).await {
            Ok(Some(action)) if action.enabled => {
                apply_destination(&mut event, &key, &action);
                Some(event)
            }
            Ok(Some(_disabled)) => {
                emit!(ComponentEventsDropped::<INTENTIONAL> {
                    count: 1,
                    reason: "bricklens_config_enricher: logging disabled for this tenant/config",
                });
                None
            }
            Ok(None) => {
                emit!(ComponentEventsDropped::<INTENTIONAL> {
                    count: 1,
                    reason: "bricklens_config_enricher: no telemetry config for this tenant/config",
                });
                None
            }
            Err(err) => {
                // Fail closed: a lookup failure (bricklens-config unreachable / error / timeout) must
                // not forward an un-routed event. Mark Errored (retriable) BEFORE dropping: the
                // finalizer defaults to Dropped, which BatchNotifier::update_status treats as a no-op,
                // so without this the source would finalize the batch as *delivered* and an
                // at-least-once producer would never retry -> silent data loss on a bricklens-config
                // outage. Errored propagates a retriable status so the source can request redelivery.
                event.metadata().update_status(EventStatus::Errored);
                tracing::warn!(error = %err, "bricklens_config_enricher: GetTelemetryConfig failed; dropping event (fail-closed)");
                emit!(ComponentEventsDropped::<UNINTENTIONAL> {
                    count: 1,
                    reason: "bricklens_config_enricher: GetTelemetryConfig lookup failed",
                });
                None
            }
        }
    }
}

impl TaskTransform<Event> for BricklensConfigEnricher {
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        let this = *self;
        Box::pin(stream! {
            while let Some(event) = input_rx.next().await {
                if let Some(out) = this.route(event).await {
                    yield out;
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Event glue
// ---------------------------------------------------------------------------

/// Build the `GetTelemetryConfig` lookup key from the identity fields a preceding remap annotates
/// onto every event: `.workload_type` (int enum number), `.config_id` (string), `.workspace_id`
/// (int). Returns `None` if any is missing so `route()` forwards without a garbage lookup.
///
/// TODO(reconcile with Hima Sheth): this assumes the event already carries a specific top-level
/// structure (`.workload_type` / `.config_id` / `.workspace_id`). That structure does not exist on
/// real payloads yet — how the config-id key is inferred from the log body / `attributes` /
/// `resources` is unsettled because Vector Search has not finalized its payload schema, so we can't
/// extrapolate the extraction here either. This ALSO must not stay hard-coded to one shape: different
/// product types + lookup strategies need different extraction, so the key-derivation should be
/// configurable — bubbled up into Vector configuration, or expressed as an explicit per-product
/// remap transform that parses the product's payload and populates the fields this function reads.
/// For now the pipeline's `bricklens_external_otlp_identity` remap hard-codes them for testing; keep
/// the input contract obvious and minimal to reduce throwaway work when Vector ports to universe by
/// EOQ2.
fn lookup_key_from_event(event: &Event) -> Option<ConfigLookupKey> {
    let log = event.as_log();
    // workload_type is a proto enum number (i32). `as_integer()` yields i64; use a checked
    // conversion rather than `as i32`, which would silently wrap an out-of-range value into a
    // different (valid-looking) enum number and route to the wrong config. An out-of-range value
    // is a malformed event, so return None -> route() drops it (fail closed).
    let workload_type = i32::try_from(log.get("workload_type").and_then(|v| v.as_integer())?).ok()?;
    let config_id = log.get("config_id").and_then(|v| v.as_str())?.into_owned();
    let workspace_id = log.get("workspace_id").and_then(|v| v.as_integer())?;
    Some(ConfigLookupKey {
        workload_type,
        config_id,
        workspace_id,
    })
}

/// Set the `.destination` sub-fields the `bricklens_ingest` sink encodes into the
/// bricklens-ingest-external `Destination`. `workspace_id` comes from the lookup key (int64), the
/// rest from the resolved `RouteInfo`.
///
/// TODO(EOQ2 universe port): the `.destination.*` field names written here assume the
/// bricklens-ingest-external `Destination`/`UnityCatalogTableConfig` message structure (the sink
/// encodes this event shape straight into that proto). That coupling is assumed for testing; it
/// should be validated against the finalized bricklens-ingest-external schema. Kept minimal now to
/// reduce throwaway work when Vector is ported into universe by EOQ2 (where the two can share the
/// proto directly instead of matching field names by convention).
fn apply_destination(event: &mut Event, key: &ConfigLookupKey, action: &RouteInfo) {
    let log = event.as_mut_log();
    // bricklens-ingest-external's `Destination` message is a `oneof destination_config`, so the UC-table
    // fields must nest under the `unity_catalog_table` arm — writing them flat on `.destination` makes
    // the bricklens_ingest sink encode scalars into the `unity_catalog_table` (message) field, which the
    // server rejects with "invalid wire type: LengthDelimited (expected Varint)". Build the nested shape.
    log.insert(
        "destination.unity_catalog_table.workspace_id",
        key.workspace_id,
    );
    log.insert(
        "destination.unity_catalog_table.uc_resource_target",
        action.uc_resource_target.clone(),
    );
    log.insert(
        "destination.unity_catalog_table.service_principal_resource",
        action.service_principal_resource.clone(),
    );
    log.insert(
        "destination.unity_catalog_table.service_principal_type",
        action.service_principal_type.clone(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vector_lib::event::{BatchNotifier, BatchStatus, LogEvent};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<BricklensConfigEnricherConfig>();
    }

    #[tokio::test]
    async fn build_rejects_partial_resolver_config() {
        let ctx = TransformContext::default();

        // Neither endpoint nor descriptor: there is no no-op fallback resolver, so this must be a
        // build error rather than a transform that silently drops every event in production.
        assert!(
            BricklensConfigEnricherConfig::default()
                .build(&ctx)
                .await
                .is_err(),
            "neither field set must be a build error (no stub/no-op fallback)"
        );

        // Endpoint without descriptor: partial config would silently drop all events — must fail.
        let endpoint_only = BricklensConfigEnricherConfig {
            bricklens_config_endpoint: Some("https://bricklens-config:443".to_string()),
            ..Default::default()
        };
        assert!(
            endpoint_only.build(&ctx).await.is_err(),
            "endpoint without descriptor must be a build error, not a silent drop-all"
        );

        // Descriptor without endpoint: same — must fail.
        let descriptor_only = BricklensConfigEnricherConfig {
            proto_descriptor_path: Some(PathBuf::from("/etc/proto/bricklens-config.pb")),
            ..Default::default()
        };
        assert!(
            descriptor_only.build(&ctx).await.is_err(),
            "descriptor without endpoint must be a build error, not a silent drop-all"
        );
    }

    #[test]
    fn untrusted_pod_is_pinned_to_its_label_tenant() {
        let id = PodIdentity {
            tenant: "ws-1".to_string(),
            trust_tier: TrustTier::UntrustedSingleTenant,
        };
        // Even if the payload claims another tenant, an untrusted pod is pinned to its label.
        assert_eq!(resolve_tenant(&id, Some("ws-evil")), "ws-1".to_string());
    }

    #[test]
    fn trusted_multitenant_pod_is_believed() {
        let id = PodIdentity {
            tenant: "host-pod".to_string(),
            trust_tier: TrustTier::TrustedMultiTenant,
        };
        assert_eq!(resolve_tenant(&id, Some("ws-42")), "ws-42".to_string());
        // Falls back to the pod's own tenant when the payload omits one.
        assert_eq!(resolve_tenant(&id, None), "host-pod".to_string());
    }

    /// A resolver that counts underlying calls, to prove the cache collapses repeats while still
    /// serving a brand-new key on first sight (cold-miss await).
    struct CountingResolver {
        calls: AtomicUsize,
        map: HashMap<ConfigLookupKey, RouteInfo>,
    }

    #[async_trait::async_trait]
    impl ConfigResolver for CountingResolver {
        async fn resolve(&self, key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.map.get(key).cloned())
        }
    }

    #[tokio::test]
    async fn cache_serves_cold_miss_then_hits() {
        let action = RouteInfo {
            enabled: true,
            uc_resource_target: "system.observability.otel_logs".to_string(),
            service_principal_resource: "accounts/acct/bricklens".to_string(),
            service_principal_type: "SERVICE_PRINCIPAL_TYPE_SYSTEM".to_string(),
        };
        let key = ConfigLookupKey {
            workload_type: 5,
            config_id: "vs-a".to_string(),
            workspace_id: 1,
        };
        let mut map = HashMap::new();
        map.insert(key.clone(), action.clone());
        let inner = Arc::new(CountingResolver {
            calls: AtomicUsize::new(0),
            map,
        });
        let counter = Arc::clone(&inner);
        let cached = CachedConfigResolver::new(inner, Duration::from_secs(60));

        // Cold miss resolves the new key on first sight...
        assert_eq!(cached.resolve(&key).await.unwrap(), Some(action));
        // ...and a repeat is served from cache (still exactly one underlying call).
        assert!(cached.resolve(&key).await.unwrap().is_some());
        assert_eq!(counter.calls.load(Ordering::SeqCst), 1);
    }

    /// A resolver that blocks on a barrier before returning, so concurrent callers genuinely
    /// overlap inside `resolve()` and we can prove single-flight collapses them to one call.
    struct BlockingResolver {
        calls: AtomicUsize,
        release: Arc<tokio::sync::Notify>,
        action: RouteInfo,
    }

    #[async_trait::async_trait]
    impl ConfigResolver for BlockingResolver {
        async fn resolve(&self, _key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.release.notified().await;
            Ok(Some(self.action.clone()))
        }
    }

    #[tokio::test]
    async fn single_flight_collapses_concurrent_cold_misses() {
        let action = enabled_action();
        let key = ConfigLookupKey {
            workload_type: 5,
            config_id: "vs-hot".to_string(),
            workspace_id: 1,
        };
        let release = Arc::new(tokio::sync::Notify::new());
        let inner = Arc::new(BlockingResolver {
            calls: AtomicUsize::new(0),
            release: Arc::clone(&release),
            action: action.clone(),
        });
        let counter = Arc::clone(&inner);
        let cached = Arc::new(CachedConfigResolver::new(inner, Duration::from_secs(60)));

        // Fire 10 concurrent resolves for the SAME cold key; they all block in the resolver.
        let mut handles = Vec::new();
        for _ in 0..10 {
            let c = Arc::clone(&cached);
            let k = key.clone();
            handles.push(tokio::spawn(async move { c.resolve(&k).await }));
        }
        // Let them queue on the single per-key fetch lock, then unblock the elected fetcher.
        tokio::task::yield_now().await;
        release.notify_waiters();

        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), Some(action.clone()));
        }
        // Exactly one underlying GetTelemetryConfig despite 10 concurrent cold misses.
        assert_eq!(counter.calls.load(Ordering::SeqCst), 1);
        // The in-flight slot is cleaned up after the fetch settles (no unbounded growth).
        assert!(cached.in_flight.lock().unwrap().is_empty());
    }

    /// A resolver that fails its first `fail_times` calls, then succeeds — to exercise retry.
    struct FlakyResolver {
        calls: AtomicUsize,
        fail_times: usize,
        action: RouteInfo,
    }

    #[async_trait::async_trait]
    impl ConfigResolver for FlakyResolver {
        async fn resolve(&self, _key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_times {
                Err(format!("transient failure {}", n).into())
            } else {
                Ok(Some(self.action.clone()))
            }
        }
    }

    fn test_key() -> ConfigLookupKey {
        ConfigLookupKey {
            workload_type: 5,
            config_id: "vs-a".to_string(),
            workspace_id: 1,
        }
    }

    #[tokio::test]
    async fn retry_succeeds_within_budget() {
        // Fails twice, succeeds on the 3rd attempt; max_retries=2 (3 attempts) => success.
        let inner = Arc::new(FlakyResolver {
            calls: AtomicUsize::new(0),
            fail_times: 2,
            action: enabled_action(),
        });
        let counter = Arc::clone(&inner);
        let retry = RetryResolver::with_backoff(inner, 2, Duration::from_millis(1));
        assert_eq!(retry.resolve(&test_key()).await.unwrap(), Some(enabled_action()));
        assert_eq!(counter.calls.load(Ordering::SeqCst), 3, "1 initial + 2 retries");
    }

    #[tokio::test]
    async fn retry_gives_up_after_budget_and_propagates_error() {
        // Fails more times than the budget allows => the final error propagates (caller fails closed).
        let inner = Arc::new(FlakyResolver {
            calls: AtomicUsize::new(0),
            fail_times: 99,
            action: enabled_action(),
        });
        let counter = Arc::clone(&inner);
        let retry = RetryResolver::with_backoff(inner, 2, Duration::from_millis(1));
        assert!(retry.resolve(&test_key()).await.is_err());
        assert_eq!(counter.calls.load(Ordering::SeqCst), 3, "1 initial + 2 retries, then give up");
    }

    #[tokio::test]
    async fn retry_zero_is_single_attempt() {
        let inner = Arc::new(FlakyResolver {
            calls: AtomicUsize::new(0),
            fail_times: 99,
            action: enabled_action(),
        });
        let counter = Arc::clone(&inner);
        let retry = RetryResolver::with_backoff(inner, 0, Duration::from_millis(1));
        assert!(retry.resolve(&test_key()).await.is_err());
        assert_eq!(counter.calls.load(Ordering::SeqCst), 1, "max_retries=0 => no retry");
    }

    #[test]
    fn backoff_is_capped_and_overflow_safe() {
        let base = Duration::from_millis(100);
        // Normal doubling for small attempts.
        assert_eq!(backoff_for_attempt(base, 0), Duration::from_millis(100));
        assert_eq!(backoff_for_attempt(base, 1), Duration::from_millis(200));
        assert_eq!(backoff_for_attempt(base, 2), Duration::from_millis(400));
        // Clamped once the doubling would exceed the cap.
        assert_eq!(backoff_for_attempt(base, 10), MAX_RETRY_BACKOFF); // 100ms * 1024 = 102.4s -> cap
        // Overflow-safe: attempts past the u32::pow / Duration-multiply overflow points must not
        // panic — they saturate to the cap. (Uncapped `base * 2u32.pow(40)` would panic in debug.)
        assert_eq!(backoff_for_attempt(base, 40), MAX_RETRY_BACKOFF);
        assert_eq!(backoff_for_attempt(base, u32::MAX), MAX_RETRY_BACKOFF);
    }

    // -----------------------------------------------------------------------
    // route_info_from_telemetry_config: TelemetryConfig proto navigation
    // -----------------------------------------------------------------------

    /// Hand-build a descriptor pool for the slice of bricklens-config's `TelemetryConfig` that
    /// `route_info_from_telemetry_config` navigates: TelemetryConfig.mappings[] ->
    /// TelemetryMapping.sinks[] -> TelemetrySink.logging_sink -> LoggingSink.destination ->
    /// DestinationConfig.{unity_catalog, auth_config} -> UnityCatalogDestination.table_full_name /
    /// AuthConfig.service_principal_resource. Field numbers mirror telemetry_config.proto so a
    /// message encoded against the real schema decodes the same way here.
    fn telemetry_config_message_desc() -> prost_reflect::MessageDescriptor {
        fn msg_field(
            name: &str,
            number: i32,
            type_name: &str,
            repeated: bool,
        ) -> FieldDescriptorProto {
            FieldDescriptorProto {
                name: Some(name.to_string()),
                number: Some(number),
                label: Some(if repeated {
                    field_descriptor_proto::Label::Repeated as i32
                } else {
                    field_descriptor_proto::Label::Optional as i32
                }),
                r#type: Some(field_descriptor_proto::Type::Message as i32),
                type_name: Some(type_name.to_string()),
                json_name: Some(name.to_string()),
                ..Default::default()
            }
        }
        fn string_field(name: &str, number: i32) -> FieldDescriptorProto {
            FieldDescriptorProto {
                name: Some(name.to_string()),
                number: Some(number),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some(name.to_string()),
                ..Default::default()
            }
        }
        fn message(name: &str, fields: Vec<FieldDescriptorProto>) -> DescriptorProto {
            DescriptorProto {
                name: Some(name.to_string()),
                field: fields,
                ..Default::default()
            }
        }

        let file = FileDescriptorProto {
            name: Some("telemetry_config.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                message(
                    "TelemetryConfig",
                    vec![msg_field("mappings", 1, ".test.TelemetryMapping", true)],
                ),
                message(
                    "TelemetryMapping",
                    vec![msg_field("sinks", 2, ".test.TelemetrySink", true)],
                ),
                // Real TelemetrySink puts logging_sink in a `oneof sink_type`; on the wire a oneof
                // arm is just its field, so a plain optional message field decodes identically.
                message(
                    "TelemetrySink",
                    vec![msg_field("logging_sink", 1, ".test.LoggingSink", false)],
                ),
                message(
                    "LoggingSink",
                    vec![msg_field(
                        "destination",
                        2,
                        ".test.DestinationConfig",
                        false,
                    )],
                ),
                message(
                    "DestinationConfig",
                    vec![
                        msg_field("unity_catalog", 1, ".test.UnityCatalogDestination", false),
                        msg_field("auth_config", 3, ".test.AuthConfig", false),
                    ],
                ),
                message(
                    "UnityCatalogDestination",
                    vec![string_field("table_full_name", 1)],
                ),
                DescriptorProto {
                    name: Some("AuthConfig".to_string()),
                    field: vec![
                        string_field("service_principal_resource", 1),
                        // Enum field mirroring the (pending) AuthConfig.service_principal_type.
                        FieldDescriptorProto {
                            name: Some("service_principal_type".to_string()),
                            number: Some(2),
                            label: Some(field_descriptor_proto::Label::Optional as i32),
                            r#type: Some(field_descriptor_proto::Type::Enum as i32),
                            type_name: Some(".test.ServicePrincipalType".to_string()),
                            json_name: Some("servicePrincipalType".to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            enum_type: vec![prost_reflect::prost_types::EnumDescriptorProto {
                name: Some("ServicePrincipalType".to_string()),
                value: vec![
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("SERVICE_PRINCIPAL_TYPE_UNSPECIFIED".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("SERVICE_PRINCIPAL_TYPE_APPLICATION".to_string()),
                        number: Some(2),
                        ..Default::default()
                    },
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("SERVICE_PRINCIPAL_TYPE_SYSTEM".to_string()),
                        number: Some(3),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        prost_reflect::DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![file],
        })
        .expect("test descriptor pool")
        .get_message_by_name("test.TelemetryConfig")
        .expect("TelemetryConfig message")
    }

    /// proto3 wire-encode a length-delimited (wire type 2) sub-message for `field_number`.
    fn encode_len_delimited(field_number: u32, body: &[u8]) -> Vec<u8> {
        fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
            while value >= 0x80 {
                out.push((value as u8) | 0x80);
                value >>= 7;
            }
            out.push(value as u8);
        }
        let mut out = Vec::new();
        encode_varint(((field_number as u64) << 3) | 2, &mut out); // tag: wire type 2
        encode_varint(body.len() as u64, &mut out);
        out.extend_from_slice(body);
        out
    }

    /// proto3 varint-encode a field (wire type 0), for the enum `service_principal_type`.
    fn encode_varint_field(field_number: u32, value: u64) -> Vec<u8> {
        fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
            while value >= 0x80 {
                out.push((value as u8) | 0x80);
                value >>= 7;
            }
            out.push(value as u8);
        }
        let mut out = Vec::new();
        encode_varint((field_number as u64) << 3, &mut out); // tag: wire type 0
        encode_varint(value, &mut out);
        out
    }

    /// Hand-encode a `TelemetryConfig` with a single logging sink whose UC table + SSP are the
    /// given values (empty strings simply produce empty proto3 fields). `sp_type` optionally sets
    /// AuthConfig.service_principal_type (the enum number); `None` omits it (proto3 default).
    /// Field numbers per telemetry_config.proto; decoded via the hand-built descriptor above.
    fn encode_telemetry_config(table: &str, ssp: &str, sp_type: Option<u64>) -> DynamicMessage {
        let uc = encode_len_delimited(1, table.as_bytes()); // UnityCatalogDestination.table_full_name
        let mut auth = encode_len_delimited(1, ssp.as_bytes()); // AuthConfig.service_principal_resource
        if let Some(t) = sp_type {
            auth.extend_from_slice(&encode_varint_field(2, t)); // AuthConfig.service_principal_type
        }
        let mut destination = Vec::new();
        destination.extend_from_slice(&encode_len_delimited(1, &uc)); // DestinationConfig.unity_catalog
        destination.extend_from_slice(&encode_len_delimited(3, &auth)); // DestinationConfig.auth_config
        let logging = encode_len_delimited(2, &destination); // LoggingSink.destination
        let sink = encode_len_delimited(1, &logging); // TelemetrySink.logging_sink
        let mapping = encode_len_delimited(2, &sink); // TelemetryMapping.sinks
        let config_bytes = encode_len_delimited(1, &mapping); // TelemetryConfig.mappings
        DynamicMessage::decode(telemetry_config_message_desc(), config_bytes.as_slice())
            .expect("decode TelemetryConfig")
    }

    #[test]
    fn route_action_extracts_uc_table_and_ssp_from_logging_sink() {
        let config = encode_telemetry_config(
            "main.observability.otel_logs",
            "accounts/acct-1/bricklens",
            None,
        );
        let action = route_info_from_telemetry_config(&config).expect("some route action");
        assert!(action.enabled);
        assert_eq!(action.uc_resource_target, "main.observability.otel_logs");
        assert_eq!(
            action.service_principal_resource,
            "accounts/acct-1/bricklens"
        );
        // No service_principal_type set in the config, so it falls back to the System-SP default.
        assert_eq!(
            action.service_principal_type,
            "SERVICE_PRINCIPAL_TYPE_SYSTEM"
        );
    }

    #[test]
    fn route_action_reads_service_principal_type_from_config() {
        // When the config carries an explicit service_principal_type enum, it is surfaced (by name)
        // instead of the System-SP fallback. 2 = SERVICE_PRINCIPAL_TYPE_APPLICATION.
        let config = encode_telemetry_config(
            "main.observability.otel_logs",
            "accounts/acct-1/bricklens",
            Some(2),
        );
        let action = route_info_from_telemetry_config(&config).expect("some route action");
        assert_eq!(
            action.service_principal_type,
            "SERVICE_PRINCIPAL_TYPE_APPLICATION"
        );
    }

    #[test]
    fn route_action_empty_table_is_disabled() {
        // A logging sink whose UC destination has an empty table_full_name is treated as disabled
        // (enabled = !table.is_empty()), so route() will intentionally drop rather than forward an
        // event with no resolvable destination.
        let config = encode_telemetry_config("", "accounts/acct-1/bricklens", None);
        let action = route_info_from_telemetry_config(&config).expect("some route action");
        assert!(!action.enabled);
        assert_eq!(action.uc_resource_target, "");
    }

    #[test]
    fn route_action_none_when_no_mappings() {
        // An empty TelemetryConfig (no mappings) yields None => "no telemetry config" => drop.
        let empty = DynamicMessage::decode(telemetry_config_message_desc(), [].as_slice())
            .expect("decode empty TelemetryConfig");
        assert!(route_info_from_telemetry_config(&empty).is_none());
    }

    /// A single mapping carrying two UC logging sinks: `first` then `second` (each a table string).
    fn encode_telemetry_config_two_sinks(first: &str, second: &str) -> DynamicMessage {
        let encode_sink = |table: &str| {
            let uc = encode_len_delimited(1, table.as_bytes());
            let auth = encode_len_delimited(1, b"accounts/acct-1/bricklens");
            let mut destination = Vec::new();
            destination.extend_from_slice(&encode_len_delimited(1, &uc));
            destination.extend_from_slice(&encode_len_delimited(3, &auth));
            let logging = encode_len_delimited(2, &destination);
            encode_len_delimited(1, &logging) // TelemetrySink.logging_sink
        };
        // Two TelemetryMapping.sinks entries (field 2, repeated) in one mapping.
        let mut mapping = Vec::new();
        mapping.extend_from_slice(&encode_len_delimited(2, &encode_sink(first)));
        mapping.extend_from_slice(&encode_len_delimited(2, &encode_sink(second)));
        let config_bytes = encode_len_delimited(1, &mapping); // TelemetryConfig.mappings
        DynamicMessage::decode(telemetry_config_message_desc(), config_bytes.as_slice())
            .expect("decode TelemetryConfig")
    }

    #[test]
    fn route_action_prefers_enabled_sink_over_earlier_empty_table() {
        // First sink has an empty table (disabled); a later sink has a valid table. The scan must
        // NOT stop at the first UC sink -- it must return the enabled one, or the event would be
        // wrongly dropped despite a resolvable destination existing.
        let config = encode_telemetry_config_two_sinks("", "main.obs.otel_logs");
        let action = route_info_from_telemetry_config(&config).expect("some route action");
        assert!(action.enabled, "must resolve to the enabled (non-empty-table) sink");
        assert_eq!(action.uc_resource_target, "main.obs.otel_logs");
    }

    #[test]
    fn route_action_falls_back_to_disabled_when_no_enabled_sink() {
        // Every sink has an empty table: no enabled sink exists, so fall back to a disabled action
        // (represents "config present, logging off" as an intentional drop rather than None).
        let config = encode_telemetry_config_two_sinks("", "");
        let action = route_info_from_telemetry_config(&config).expect("some route action");
        assert!(!action.enabled);
    }

    // -----------------------------------------------------------------------
    // lookup_key_from_event: identity-annotation extraction
    // -----------------------------------------------------------------------

    fn annotated_event(workload_type: i64, config_id: &str, workspace_id: i64) -> Event {
        let mut log = LogEvent::default();
        log.insert("workload_type", workload_type);
        log.insert("config_id", config_id);
        log.insert("workspace_id", workspace_id);
        Event::Log(log)
    }

    #[test]
    fn lookup_key_reads_all_three_identity_fields() {
        let event = annotated_event(5, "vs-instance-a", 1426760070658584);
        let key = lookup_key_from_event(&event).expect("key present");
        assert_eq!(key.workload_type, 5);
        assert_eq!(key.config_id, "vs-instance-a");
        assert_eq!(key.workspace_id, 1426760070658584);
    }

    #[test]
    fn lookup_key_missing_field_yields_none() {
        // Each of the three identity fields is required; dropping any one makes the key unresolvable
        // so route() fails closed (unintentional drop) rather than issuing a garbage lookup.
        for missing in ["workload_type", "config_id", "workspace_id"] {
            let mut log = LogEvent::default();
            log.insert("workload_type", 5_i64);
            log.insert("config_id", "vs-a");
            log.insert("workspace_id", 1_i64);
            log.remove(missing);
            assert!(
                lookup_key_from_event(&Event::Log(log)).is_none(),
                "expected None when {missing} is absent"
            );
        }
    }

    // -----------------------------------------------------------------------
    // route(): fail-closed outcomes (every non-enabled path drops)
    // -----------------------------------------------------------------------

    /// A resolver that always returns the same canned outcome, to drive route()'s branches.
    struct FixedResolver(crate::Result<Option<RouteInfo>>);

    #[async_trait::async_trait]
    impl ConfigResolver for FixedResolver {
        async fn resolve(&self, _key: &ConfigLookupKey) -> crate::Result<Option<RouteInfo>> {
            match &self.0 {
                Ok(v) => Ok(v.clone()),
                Err(e) => Err(e.to_string().into()),
            }
        }
    }

    fn router_with(outcome: crate::Result<Option<RouteInfo>>) -> BricklensConfigEnricher {
        let inner: Arc<dyn ConfigResolver> = Arc::new(FixedResolver(outcome));
        BricklensConfigEnricher {
            configs: Arc::new(CachedConfigResolver::new(inner, Duration::from_secs(60))),
        }
    }

    fn enabled_action() -> RouteInfo {
        RouteInfo {
            enabled: true,
            uc_resource_target: "main.obs.otel_logs".to_string(),
            service_principal_resource: "accounts/acct/bricklens".to_string(),
            service_principal_type: "SERVICE_PRINCIPAL_TYPE_SYSTEM".to_string(),
        }
    }

    #[tokio::test]
    async fn route_enabled_forwards_and_sets_destination() {
        let router = router_with(Ok(Some(enabled_action())));
        let out = router
            .route(annotated_event(5, "vs-a", 42))
            .await
            .expect("enabled config forwards the event");
        let log = out.as_log();
        // The sink encodes these .destination sub-fields into the -external Destination message.
        assert_eq!(
            log.get("destination.uc_resource_target")
                .unwrap()
                .as_str()
                .unwrap(),
            "main.obs.otel_logs"
        );
        assert_eq!(
            log.get("destination.service_principal_resource")
                .unwrap()
                .as_str()
                .unwrap(),
            "accounts/acct/bricklens"
        );
        assert_eq!(
            log.get("destination.workspace_id")
                .unwrap()
                .as_integer()
                .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn route_disabled_config_drops() {
        let mut disabled = enabled_action();
        disabled.enabled = false;
        let router = router_with(Ok(Some(disabled)));
        assert!(
            router.route(annotated_event(5, "vs-a", 42)).await.is_none(),
            "a disabled config must drop, not forward"
        );
    }

    #[tokio::test]
    async fn route_no_config_drops() {
        let router = router_with(Ok(None));
        assert!(
            router.route(annotated_event(5, "vs-a", 42)).await.is_none(),
            "no telemetry config must drop, not forward"
        );
    }

    #[tokio::test]
    async fn route_lookup_error_fails_closed() {
        // A bricklens-config outage / timeout must NOT forward an un-routed event to -external, AND
        // must finalize the source batch as Errored (retriable) rather than the default Delivered --
        // otherwise an at-least-once producer would never retry and the event is silently lost.
        let router = router_with(Err("bricklens-config unreachable".to_string().into()));
        let (batch, mut receiver) = BatchNotifier::new_with_receiver();
        let event = annotated_event(5, "vs-a", 42).with_batch_notifier(&batch);
        drop(batch); // only the event's finalizer keeps the batch alive now

        assert!(
            router.route(event).await.is_none(),
            "a lookup error must fail closed (drop), not forward"
        );
        // route() dropped the event -> its finalizer resolved the batch. Errored, not Delivered,
        // is the whole point of the fix: a default-Dropped finalizer would leave the batch Delivered.
        assert_eq!(
            receiver.try_recv(),
            Ok(BatchStatus::Errored),
            "a lookup-error drop must mark the batch Errored (retriable), not Delivered"
        );
    }

    #[tokio::test]
    async fn route_missing_annotation_drops() {
        // No identity fields => no lookup key => fail closed. A missing annotation is unfixable by
        // retry, so the batch must finalize as Rejected (permanent), not the default Delivered.
        let router = router_with(Ok(Some(enabled_action())));
        let (batch, mut receiver) = BatchNotifier::new_with_receiver();
        let bare = Event::Log(LogEvent::from("just a message")).with_batch_notifier(&batch);
        drop(batch);

        assert!(
            router.route(bare).await.is_none(),
            "an event missing the identity annotation must drop"
        );
        assert_eq!(
            receiver.try_recv(),
            Ok(BatchStatus::Rejected),
            "a missing-annotation drop must mark the batch Rejected (permanent), not Delivered"
        );
    }

    // -----------------------------------------------------------------------
    // gRPC framing helpers
    // -----------------------------------------------------------------------

    #[test]
    fn frame_then_unframe_roundtrips() {
        let payload = b"telemetry-config-bytes".to_vec();
        let framed = frame_grpc_message(payload.clone());
        // 5-byte header (1 compression flag + 4 big-endian length) + payload.
        assert_eq!(framed.len(), payload.len() + 5);
        assert_eq!(framed[0], 0, "compression flag must be 0 (uncompressed)");
        assert_eq!(unframe_grpc_message(&framed).unwrap(), payload.as_slice());
    }

    #[test]
    fn unframe_rejects_short_frame() {
        assert!(unframe_grpc_message(&[0, 0, 0]).is_err());
    }

    #[test]
    fn unframe_rejects_compressed_frame() {
        // Compression flag = 1: we don't support compressed gRPC responses.
        let framed = [1u8, 0, 0, 0, 1, 0xAB];
        assert!(unframe_grpc_message(&framed).is_err());
    }

    #[test]
    fn unframe_rejects_truncated_body() {
        // Header claims 4 bytes but only 1 follows.
        let framed = [0u8, 0, 0, 0, 4, 0xAB];
        assert!(unframe_grpc_message(&framed).is_err());
    }
}
