//! Kubernetes endpoint provider for Prometheus scraping.
//!
//! This module provides functionality to discover Prometheus scrape endpoints
//! from Kubernetes pods via two independent paths:
//!
//! 1. **Named-port discovery** — pods that expose a container port with a
//!    configured name (e.g. `"user-metrics"`) are scraped on that port.
//! 2. **Annotation-based discovery** — pods that carry a configured annotation
//!    (e.g. `"system_metrics_enabled"`) are scraped on the port number(s) given
//!    as the annotation value. The value may be a single port (`"9091"`) or a
//!    comma-separated list (`"9091,9092"`), in which case the pod yields one
//!    endpoint per valid port. Unparseable entries are silently skipped.
//!
//! Either path can be disabled by passing `None` for the corresponding
//! argument. Endpoints discovered by both paths for the same pod are
//! deduplicated by URL.

use std::{collections::HashSet, sync::Arc};

use k8s_openapi::api::core::v1::Pod;
use kube::runtime::reflector::store::Store;
use tracing::{trace, warn};

/// Represents a Kubernetes pod endpoint for Prometheus scraping
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The full URL to scrape metrics from (e.g., "http://10.0.0.1:9090/metrics")
    pub url: String,
    /// The name of the pod
    pub name: String,
    /// The namespace of the pod
    pub namespace: String,
    /// Optional metrics-namespace value read from a configurable pod
    /// annotation. `None` when metrics-namespace discovery is disabled or the
    /// pod does not carry the configured annotation. Emitted on each scraped
    /// metric as the `tenant` tag (see `METRICS_NAMESPACE_TAG`).
    pub metrics_namespace: Option<String>,
}

/// Trait for providing endpoints for Prometheus scraping
pub trait EndpointProvider {
    /// The iterator type returned by `endpoints()`
    type IntoIter;

    /// Returns a collection of endpoints to scrape
    fn endpoints(&self) -> Self::IntoIter;
}

/// A Kubernetes endpoint provider that discovers Prometheus scrape endpoints
/// from pods in the reflector store.
///
/// Two discovery paths are supported and can be enabled independently:
///
/// - **Named-port**: pods whose container spec includes a port with `named_port`
///   as its name are scraped on that port.
/// - **Annotation**: pods that carry `annotation_name` as a pod annotation are
///   scraped on the port number given as the annotation value.
///
/// Results from both paths are merged and deduplicated by URL before being
/// returned.
pub struct K8sEndpointProvider {
    pod_state: Store<Pod>,
    /// The container port name used for named-port discovery, or `None` to
    /// disable that discovery path entirely.
    named_port: Option<String>,
    /// The pod annotation key used for annotation-based discovery, or `None` to
    /// disable that discovery path entirely. The annotation value must be a
    /// valid TCP port number, or a comma-separated list of port numbers.
    annotation_name: Option<String>,
    /// The maximum number of endpoints contributed per pod by the annotation
    /// path. Ports parsed beyond this cap are dropped.
    max_endpoints_per_pod: usize,
    /// Pod annotation key whose value is the metrics-namespace identifier to
    /// attach to each endpoint discovered for that pod, or `None` to disable
    /// metrics-namespace discovery. Pods that lack the annotation produce
    /// endpoints with `metrics_namespace = None`.
    metrics_ns_annotation_name: Option<String>,
}

impl K8sEndpointProvider {
    /// Create a new [`K8sEndpointProvider`].
    ///
    /// # Arguments
    ///
    /// * `pod_state` - A read-only view of the Kubernetes pod state from the reflector
    /// * `named_port` - The name of the container port to look for when discovering pods,
    ///   or `None` to disable named-port-based discovery entirely
    /// * `annotation_name` - The pod annotation key whose value is the port number to
    ///   scrape, or `None` to disable annotation-based discovery entirely
    /// * `max_endpoints_per_pod` - Caps the number of endpoints a single pod
    ///   can contribute via the annotation path; additional parsed ports are
    ///   silently dropped
    /// * `metrics_ns_annotation_name` - Pod annotation key whose value is recorded as
    ///   the endpoint's `metrics_namespace`; `None` disables metrics-namespace discovery
    pub fn new(
        pod_state: Store<Pod>,
        named_port: Option<String>,
        annotation_name: Option<String>,
        max_endpoints_per_pod: usize,
        metrics_ns_annotation_name: Option<String>,
    ) -> Self {
        Self {
            pod_state,
            named_port,
            annotation_name,
            max_endpoints_per_pod,
            metrics_ns_annotation_name,
        }
    }
}

impl EndpointProvider for K8sEndpointProvider {
    type IntoIter = Vec<Endpoint>;

    fn endpoints(&self) -> Vec<Endpoint> {
        let state = self.pod_state.state();
        compute_endpoints(
            &state,
            self.named_port.as_deref(),
            self.annotation_name.as_deref(),
            self.max_endpoints_per_pod,
            self.metrics_ns_annotation_name.as_deref(),
        )
    }
}

/// Compute the full endpoint list from a pod state snapshot.
///
/// Merges results from the named-port path and the annotation path, deduplicating
/// by URL so a pod matched by both only appears once.
///
/// # Arguments
///
/// * `state` - Current snapshot of all pods from the reflector store
/// * `named_port` - Container port name used for the named-port discovery path, or
///   `None` to disable named-port-based discovery entirely
/// * `annotation_name` - Pod annotation key used for the annotation-based discovery path;
///   its value is expected to be a TCP port number or a comma-separated list of port
///   numbers. Pass `None` to disable annotation-based discovery entirely.
/// * `max_endpoints_per_pod` - Caps the number of endpoints contributed per pod
///   by the annotation path; additional parsed ports are silently dropped.
/// * `metrics_ns_annotation_name` - Pod annotation key whose value is recorded as the
///   endpoint's `metrics_namespace`; `None` disables metrics-namespace discovery so
///   all endpoints have `metrics_namespace = None`.
fn compute_endpoints(
    state: &[Arc<Pod>],
    named_port: Option<&str>,
    annotation_name: Option<&str>,
    max_endpoints_per_pod: usize,
    metrics_ns_annotation_name: Option<&str>,
) -> Vec<Endpoint> {
    let named_port_endpoints: Vec<Endpoint> = match named_port {
        Some(port) => state
            .iter()
            .filter(|pod| pod_has_named_port(pod.as_ref(), port))
            .filter_map(|pod| extract_metrics_endpoint(pod.as_ref(), port, metrics_ns_annotation_name))
            .collect(),
        None => vec![],
    };

    let annotation_endpoints: Vec<Endpoint> = match annotation_name {
        Some(name) => state
            .iter()
            .flat_map(|pod| {
                extract_endpoints_from_annotation(
                    pod.as_ref(),
                    name,
                    max_endpoints_per_pod,
                    metrics_ns_annotation_name,
                )
            })
            .collect(),
        None => vec![],
    };

    let mut seen = HashSet::new();
    named_port_endpoints
        .into_iter()
        .chain(annotation_endpoints)
        .filter(|endpoint| seen.insert(endpoint.url.clone()))
        .collect()
}

/// Check if a pod has a container port with the specified name
///
/// # Arguments
///
/// * `pod` - The Kubernetes pod to check
/// * `port_name` - The name of the port to look for
///
/// # Returns
///
/// `true` if any container in the pod has a port with the specified name, `false` otherwise
fn pod_has_named_port(pod: &Pod, port_name: &str) -> bool {
    pod.spec
        .as_ref()
        .map(|spec| {
            spec.containers.iter().any(|container| {
                container
                    .ports
                    .as_ref()
                    .map(|ports| {
                        ports
                            .iter()
                            .any(|port| port.name.as_ref().is_some_and(|name| name == port_name))
                    })
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// Read the metrics-namespace value for a pod from the configured annotation.
///
/// The returned value is emitted on each scraped metric as the `tenant` tag
/// (see [`super::k8s_scrape::METRICS_NAMESPACE_TAG`]) — the user-visible label
/// is named `tenant` because that is the label name the downstream metrics
/// system expects, even though everything in code refers to it as the metrics
/// namespace.
///
/// Returns `None` when metrics-namespace discovery is disabled
/// (`metrics_ns_annotation_name` is `None`), when the pod carries no
/// annotations, or when the configured annotation key is absent. The
/// annotation value is returned verbatim with no parsing — empty strings are
/// returned as `Some("".to_string())` since the caller may want to treat that
/// as "explicitly empty".
fn extract_metrics_namespace(pod: &Pod, metrics_ns_annotation_name: Option<&str>) -> Option<String> {
    let key = metrics_ns_annotation_name?;
    pod.metadata
        .annotations
        .as_ref()?
        .get(key)
        .cloned()
}

/// Extract the endpoint (pod_ip:port) for pods with the specified named port
///
/// # Arguments
///
/// * `pod` - The Kubernetes pod to extract the endpoint from
/// * `port_name` - The name of the port to look for
/// * `metrics_ns_annotation_name` - Pod annotation key whose value is recorded as the
///   endpoint's `metrics_namespace`; `None` disables metrics-namespace discovery
///
/// # Returns
///
/// An `Option<Endpoint>` containing the HTTP endpoint URL if the pod has an IP
/// and a matching port, or `None` otherwise
fn extract_metrics_endpoint(
    pod: &Pod,
    port_name: &str,
    metrics_ns_annotation_name: Option<&str>,
) -> Option<Endpoint> {
    let pod_ip = pod.status.as_ref()?.pod_ip.as_ref()?;
    let name = pod.metadata.name.clone().unwrap_or_default();
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();

    let port_number = pod.spec.as_ref()?.containers.iter().find_map(|container| {
        container.ports.as_ref()?.iter().find_map(|port| {
            if port.name.as_ref()? == port_name {
                Some(port.container_port)
            } else {
                None
            }
        })
    })?;

    let url = format!("http://{}:{}/metrics", pod_ip, port_number);
    let metrics_ns = extract_metrics_namespace(pod, metrics_ns_annotation_name);

    trace!(
        message = "Created endpoint for pod with named port.",
        pod = %name,
        namespace = %namespace,
        port_name,
        endpoint = %url,
        metrics_namespace = ?metrics_ns,
    );

    Some(Endpoint {
        url,
        name,
        namespace,
        metrics_namespace: metrics_ns,
    })
}

/// Extract endpoints for pods that carry the specified annotation.
///
/// The annotation value may be a single TCP port (e.g. `"9091"`) or a
/// comma-separated list (e.g. `"9091,9092"`); whitespace around each entry is
/// ignored. Duplicate port numbers in the list are collapsed (first-seen wins,
/// preserving declared order), so duplicates do not consume cap slots. One
/// endpoint is produced per unique successfully-parsed port, capped at
/// `max_ports`; ports beyond that are dropped and a warning is logged.
/// Entries that fail to parse as `i32` are silently skipped. Returns an empty
/// `Vec` if the annotation is absent, the pod has no IP address yet, no entry
/// parses, or `max_ports` is zero.
fn extract_endpoints_from_annotation(
    pod: &Pod,
    annotation_name: &str,
    max_ports: usize,
    metrics_ns_annotation_name: Option<&str>,
) -> Vec<Endpoint> {
    let Some(annotations) = pod.metadata.annotations.as_ref() else {
        return vec![];
    };
    let Some(port_str) = annotations.get(annotation_name) else {
        return vec![];
    };
    let Some(pod_ip) = pod.status.as_ref().and_then(|s| s.pod_ip.as_ref()) else {
        return vec![];
    };
    let name = pod.metadata.name.clone().unwrap_or_default();
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let metrics_ns = extract_metrics_namespace(pod, metrics_ns_annotation_name);

    // Parse, drop invalid entries, then dedupe (first-seen wins). Deduplication
    // runs before the cap so accidental duplicates in the annotation don't
    // burn cap slots and silently squeeze out distinct ports.
    let mut seen_ports = HashSet::new();
    let parsed: Vec<i32> = port_str
        .split(',')
        .filter_map(|entry| entry.trim().parse::<i32>().ok())
        .filter(|port| seen_ports.insert(*port))
        .collect();

    if parsed.len() > max_ports {
        warn!(
            message = "Annotation specifies more ports than allowed; truncating.",
            annotation = annotation_name,
            annotation_value = %port_str,
            pod = %name,
            namespace = %namespace,
            parsed_ports = parsed.len(),
            limit = max_ports,
            dropped = parsed.len() - max_ports,
            internal_log_rate_secs = 60
        );
    }

    parsed
        .into_iter()
        .take(max_ports)
        .map(|port_number| {
            let url = format!("http://{}:{}/metrics", pod_ip, port_number);
            trace!(
                message = "Created endpoint for pod with annotation.",
                annotation = annotation_name,
                pod = %name,
                namespace = %namespace,
                port = port_number,
                endpoint = %url,
                metrics_namespace = ?metrics_ns,
            );
            Endpoint {
                url,
                name: name.clone(),
                namespace: namespace.clone(),
                metrics_namespace: metrics_ns.clone(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use k8s_openapi::{
        api::core::v1::{Container, ContainerPort, PodSpec, PodStatus},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };
    use rstest::rstest;

    /// Effectively-unlimited cap used in tests whose intent is unrelated to the
    /// `max_endpoints_per_pod` enforcement.
    const UNLIMITED: usize = usize::MAX;

    #[test]
    fn test_pod_has_named_port_with_matching_port() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test-container".to_string(),
                    ports: Some(vec![
                        ContainerPort {
                            name: Some("http".to_string()),
                            container_port: 8080,
                            ..Default::default()
                        },
                        ContainerPort {
                            name: Some("user-metrics".to_string()),
                            container_port: 9090,
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(pod_has_named_port(&pod, "user-metrics"));
        assert!(pod_has_named_port(&pod, "http"));
        assert!(!pod_has_named_port(&pod, "nonexistent"));
    }

    #[test]
    fn test_extract_metrics_endpoint() {
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("test-pod".to_string()),
                namespace: Some("test-namespace".to_string()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test-container".to_string(),
                    ports: Some(vec![ContainerPort {
                        name: Some("user-metrics".to_string()),
                        container_port: 9090,
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                pod_ip: Some("10.244.1.5".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoint = extract_metrics_endpoint(&pod, "user-metrics", None);
        assert_eq!(
            endpoint,
            Some(Endpoint {
                url: "http://10.244.1.5:9090/metrics".to_string(),
                name: "test-pod".to_string(),
                namespace: "test-namespace".to_string(),
                metrics_namespace: None,
            })
        );
    }

    #[test]
    fn test_extract_metrics_endpoint_without_ip() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test-container".to_string(),
                    ports: Some(vec![ContainerPort {
                        name: Some("user-metrics".to_string()),
                        container_port: 9090,
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
            ..Default::default()
        };

        let endpoint = extract_metrics_endpoint(&pod, "user-metrics", None);
        assert_eq!(endpoint, None);
    }

    #[test]
    fn test_extract_endpoint_from_annotation_valid() {
        let mut annotations = BTreeMap::new();
        annotations.insert("system_metrics_enabled".to_string(), "9091".to_string());

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("annotated-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.3".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints = extract_endpoints_from_annotation(&pod, "system_metrics_enabled", UNLIMITED, None);
        assert_eq!(
            endpoints,
            vec![Endpoint {
                url: "http://10.244.2.3:9091/metrics".to_string(),
                name: "annotated-pod".to_string(),
                namespace: "default".to_string(),
                metrics_namespace: None,
            }]
        );
    }

    #[test]
    fn test_extract_endpoints_from_annotation_multiple_ports() {
        let mut annotations = BTreeMap::new();
        annotations.insert("system_metrics_enabled".to_string(), "9091,9092".to_string());

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("multi-port-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.4".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints = extract_endpoints_from_annotation(&pod, "system_metrics_enabled", UNLIMITED, None);
        assert_eq!(endpoints.len(), 2);
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert!(urls.contains("http://10.244.2.4:9091/metrics"));
        assert!(urls.contains("http://10.244.2.4:9092/metrics"));
        for ep in &endpoints {
            assert_eq!(ep.name, "multi-port-pod");
            assert_eq!(ep.namespace, "default");
        }
    }

    #[test]
    fn test_extract_endpoints_from_annotation_whitespace_tolerated() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            " 9091 , 9092 ,9093".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("ws-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.5".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints = extract_endpoints_from_annotation(&pod, "system_metrics_enabled", UNLIMITED, None);
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert_eq!(urls.len(), 3);
        assert!(urls.contains("http://10.244.2.5:9091/metrics"));
        assert!(urls.contains("http://10.244.2.5:9092/metrics"));
        assert!(urls.contains("http://10.244.2.5:9093/metrics"));
    }

    #[test]
    fn test_extract_endpoints_from_annotation_mixed_valid_and_invalid() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,not-a-port,9092,".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("mixed-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.6".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints = extract_endpoints_from_annotation(&pod, "system_metrics_enabled", UNLIMITED, None);
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert_eq!(urls.len(), 2);
        assert!(urls.contains("http://10.244.2.6:9091/metrics"));
        assert!(urls.contains("http://10.244.2.6:9092/metrics"));
    }

    #[test]
    fn test_extract_endpoints_from_annotation_all_invalid() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "foo,bar".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("bad-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.7".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints = extract_endpoints_from_annotation(&pod, "system_metrics_enabled", UNLIMITED, None);
        assert!(endpoints.is_empty());
    }

    #[test]
    fn test_extract_endpoints_from_annotation_without_ip() {
        let mut annotations = BTreeMap::new();
        annotations.insert("system_metrics_enabled".to_string(), "9091,9092".to_string());

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("no-ip-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: None,
            ..Default::default()
        };

        let endpoints = extract_endpoints_from_annotation(&pod, "system_metrics_enabled", UNLIMITED, None);
        assert!(endpoints.is_empty());
    }

    /// `max_ports` truncates a multi-port annotation to the first N parsed
    /// ports (preserving original order).
    #[test]
    fn test_extract_endpoints_from_annotation_respects_max_ports() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,9092,9093,9094".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("capped-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.8".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, None);
        assert_eq!(endpoints.len(), 2);
        // Order preserved: first two declared ports survive.
        assert_eq!(endpoints[0].url, "http://10.244.2.8:9091/metrics");
        assert_eq!(endpoints[1].url, "http://10.244.2.8:9092/metrics");
    }

    /// `max_ports = 0` rejects all ports (defensive: no endpoints emitted).
    #[test]
    fn test_extract_endpoints_from_annotation_max_ports_zero() {
        let mut annotations = BTreeMap::new();
        annotations.insert("system_metrics_enabled".to_string(), "9091,9092".to_string());

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("zero-cap-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.9".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 0, None);
        assert!(endpoints.is_empty());
    }

    /// `max_ports` is a cap, not a target: an annotation with fewer ports than
    /// the cap returns all of them.
    #[test]
    fn test_extract_endpoints_from_annotation_cap_above_supplied() {
        let mut annotations = BTreeMap::new();
        annotations.insert("system_metrics_enabled".to_string(), "9091".to_string());

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("under-cap-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.10".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 5, None);
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.244.2.10:9091/metrics");
    }

    /// The cap counts successfully-parsed ports, not raw entries: invalid
    /// entries don't consume cap slots.
    #[test]
    fn test_extract_endpoints_from_annotation_cap_ignores_invalid_entries() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "bogus,9091,bogus,9092,bogus".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("filter-cap-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.2.11".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, None);
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].url, "http://10.244.2.11:9091/metrics");
        assert_eq!(endpoints[1].url, "http://10.244.2.11:9092/metrics");
    }

    // -----------------------------------------------------------------------
    // Duplicate-port handling: dedup runs before the cap, so duplicates do
    // not consume cap slots and never squeeze out distinct ports.
    // -----------------------------------------------------------------------

    /// Duplicate within the would-be cap window: `"9091,9091,9092"` cap=2 used
    /// to yield only `:9091` because the duplicate consumed the second cap
    /// slot. With dedup-before-cap, both `:9091` and `:9092` survive.
    #[test]
    fn test_extract_endpoints_from_annotation_dedupes_within_cap() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,9091,9092".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("dup-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.3.1".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, None);
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].url, "http://10.244.3.1:9091/metrics");
        assert_eq!(endpoints[1].url, "http://10.244.3.1:9092/metrics");
    }

    /// Duplicate beyond the cap: `"9091,9092,9091,9093"` cap=2. After dedup
    /// the list is `[9091, 9092, 9093]`; cap=2 keeps the first two.
    #[test]
    fn test_extract_endpoints_from_annotation_dedupes_beyond_cap() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,9092,9091,9093".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("dup-beyond-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.3.2".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, None);
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].url, "http://10.244.3.2:9091/metrics");
        assert_eq!(endpoints[1].url, "http://10.244.3.2:9092/metrics");
    }

    /// All entries are the same port: `"9091,9091,9091"` cap=2 → one endpoint.
    #[test]
    fn test_extract_endpoints_from_annotation_all_duplicates() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,9091,9091".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("all-dup-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.3.3".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, None);
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.244.3.3:9091/metrics");
    }

    /// Dedup preserves first-seen declared order: `"9092,9091,9092"` →
    /// `[9092, 9091]`, not `[9091, 9092]`.
    #[test]
    fn test_extract_endpoints_from_annotation_dedup_preserves_first_seen_order() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9092,9091,9092".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("order-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.3.4".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", UNLIMITED, None);
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].url, "http://10.244.3.4:9092/metrics");
        assert_eq!(endpoints[1].url, "http://10.244.3.4:9091/metrics");
    }

    /// Dedup and invalid-entry skipping compose correctly:
    /// `"9091,bogus,9091,9092"` cap=2 → `[9091, 9092]` (invalid skipped, dup
    /// merged, neither consumed a cap slot).
    #[test]
    fn test_extract_endpoints_from_annotation_dedup_with_invalid_entries() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,bogus,9091,9092".to_string(),
        );

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("dup-invalid-pod".to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            status: Some(PodStatus {
                pod_ip: Some("10.244.3.5".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoints =
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, None);
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].url, "http://10.244.3.5:9091/metrics");
        assert_eq!(endpoints[1].url, "http://10.244.3.5:9092/metrics");
    }

    // -----------------------------------------------------------------------
    // compute_endpoints – four combined cases
    // -----------------------------------------------------------------------

    fn make_pod(
        name: &str,
        ip: &str,
        port_name: Option<&str>,
        port_number: Option<i32>,
        annotation_port: Option<&str>,
    ) -> Arc<Pod> {
        let mut annotations = BTreeMap::new();
        if let Some(v) = annotation_port {
            annotations.insert("system_metrics_enabled".to_string(), v.to_string());
        }

        Arc::new(Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                annotations: if annotations.is_empty() {
                    None
                } else {
                    Some(annotations)
                },
                ..Default::default()
            },
            spec: match (port_name, port_number) {
                (Some(pn), Some(num)) => Some(PodSpec {
                    containers: vec![Container {
                        name: "app".to_string(),
                        ports: Some(vec![ContainerPort {
                            name: Some(pn.to_string()),
                            container_port: num,
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                _ => None,
            },
            status: Some(PodStatus {
                pod_ip: Some(ip.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    /// Case 1: pod has both a named port and a valid annotation pointing to the
    /// same port → one endpoint (deduplication).
    #[test]
    fn test_compute_endpoints_named_port_and_annotation_deduped() {
        let pod = make_pod(
            "pod-a",
            "10.0.0.1",
            Some("user-metrics"),
            Some(9090),
            Some("9090"),
        );
        let state = vec![pod];

        let endpoints =
            compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None,
        );

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.0.0.1:9090/metrics");
    }

    /// Case 2: pod has a named port but no valid annotation → one endpoint from
    /// the named-port path only.
    #[test]
    fn test_compute_endpoints_named_port_only() {
        let pod = make_pod("pod-b", "10.0.0.2", Some("user-metrics"), Some(9090), None);
        let state = vec![pod];

        let endpoints =
            compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None,
        );

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.0.0.2:9090/metrics");
    }

    /// Case 3: pod has no named port but has a valid annotation → one endpoint
    /// from the annotation path only.
    #[test]
    fn test_compute_endpoints_annotation_only() {
        let pod = make_pod("pod-c", "10.0.0.3", None, None, Some("9091"));
        let state = vec![pod];

        let endpoints =
            compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None,
        );

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.0.0.3:9091/metrics");
    }

    /// Case 4: pod has neither a named port nor a valid annotation → skipped
    /// entirely, no endpoints produced.
    #[test]
    fn test_compute_endpoints_no_named_port_no_annotation() {
        let pod = make_pod("pod-d", "10.0.0.4", None, None, None);
        let state = vec![pod];

        let endpoints =
            compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None,
        );

        assert!(endpoints.is_empty());
    }

    /// Case 1 variant: pod has both a named port and a valid annotation pointing
    /// to a *different* port → two distinct endpoints, one per path.
    #[test]
    fn test_compute_endpoints_named_port_and_annotation_different_ports() {
        let pod = make_pod(
            "pod-f",
            "10.0.0.6",
            Some("user-metrics"),
            Some(9090),
            Some("9091"),
        );
        let state = vec![pod];

        let endpoints =
            compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None,
        );

        assert_eq!(endpoints.len(), 2);
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert!(urls.contains("http://10.0.0.6:9090/metrics"));
        assert!(urls.contains("http://10.0.0.6:9091/metrics"));
    }

    /// Passing `None` as annotation_name disables annotation-based discovery;
    /// only the named-port path contributes endpoints.
    #[test]
    fn test_compute_endpoints_annotation_disabled() {
        // Pod has both a named port and a valid annotation on a different port.
        // With annotation_name = None, only the named-port endpoint should appear.
        let pod = make_pod(
            "pod-g",
            "10.0.0.7",
            Some("user-metrics"),
            Some(9090),
            Some("9091"),
        );
        let state = vec![pod];

        let endpoints = compute_endpoints(&state, Some("user-metrics"), None, UNLIMITED, None);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.0.0.7:9090/metrics");
    }

    /// Passing `None` as named_port disables named-port-based discovery;
    /// only the annotation path contributes endpoints.
    #[test]
    fn test_compute_endpoints_named_port_disabled() {
        // Pod has both a named port and a valid annotation on a different port.
        // With named_port = None, only the annotation endpoint should appear.
        let pod = make_pod(
            "pod-h",
            "10.0.0.8",
            Some("user-metrics"),
            Some(9090),
            Some("9091"),
        );
        let state = vec![pod];

        let endpoints = compute_endpoints(&state, None, Some("system_metrics_enabled"), UNLIMITED, None);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.0.0.8:9091/metrics");
    }

    /// Passing `None` for both disables all discovery; no endpoints produced.
    #[test]
    fn test_compute_endpoints_both_disabled() {
        let pod = make_pod(
            "pod-i",
            "10.0.0.9",
            Some("user-metrics"),
            Some(9090),
            Some("9091"),
        );
        let state = vec![pod];

        let endpoints = compute_endpoints(&state, None, None, UNLIMITED, None);

        assert!(endpoints.is_empty());
    }

    /// Multi-port annotation overlapping with named-port: pod has a `user-metrics`
    /// named port on 9090 and an annotation `"9090,9091"`. The shared URL
    /// (`:9090`) is deduplicated, so the result is two distinct endpoints.
    #[test]
    fn test_compute_endpoints_annotation_multiple_ports_deduped_with_named() {
        let pod = make_pod(
            "pod-multi-dedup",
            "10.0.0.11",
            Some("user-metrics"),
            Some(9090),
            Some("9090,9091"),
        );
        let state = vec![pod];

        let endpoints =
            compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None,
        );

        assert_eq!(endpoints.len(), 2);
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert!(urls.contains("http://10.0.0.11:9090/metrics"));
        assert!(urls.contains("http://10.0.0.11:9091/metrics"));
    }

    /// End-to-end cap: even with multiple pods each declaring 4 annotation
    /// ports, a cap of 2 truncates each pod's annotation list independently
    /// (the cap applies per-pod, not globally).
    #[test]
    fn test_compute_endpoints_max_endpoints_per_pod_applied_independently() {
        let pod_a = make_pod("pod-a", "10.0.0.20", None, None, Some("9091,9092,9093,9094"));
        let pod_b = make_pod("pod-b", "10.0.0.21", None, None, Some("9091,9092,9093,9094"));
        let state = vec![pod_a, pod_b];

        let endpoints = compute_endpoints(&state, None, Some("system_metrics_enabled"), 2, None);

        // 2 ports * 2 pods = 4 endpoints total.
        assert_eq!(endpoints.len(), 4);
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert!(urls.contains("http://10.0.0.20:9091/metrics"));
        assert!(urls.contains("http://10.0.0.20:9092/metrics"));
        assert!(urls.contains("http://10.0.0.21:9091/metrics"));
        assert!(urls.contains("http://10.0.0.21:9092/metrics"));
    }

    /// Case 4 variant: annotation is present but contains an invalid port value.
    #[test]
    fn test_compute_endpoints_invalid_annotation_no_named_port() {
        let pod = make_pod("pod-e", "10.0.0.5", None, None, Some("not-a-port"));
        let state = vec![pod];

        let endpoints =
            compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None,
        );

        assert!(endpoints.is_empty());
    }

    // -----------------------------------------------------------------------
    // Tenant-annotation discovery
    // -----------------------------------------------------------------------

    /// Build a pod that carries an additional annotation `key=value` alongside
    /// whatever the standard helper already configures.
    fn make_pod_with_extra_annotation(
        name: &str,
        ip: &str,
        port_name: Option<&str>,
        port_number: Option<i32>,
        annotation_port: Option<&str>,
        extra_key: &str,
        extra_value: &str,
    ) -> Arc<Pod> {
        let mut annotations = BTreeMap::new();
        if let Some(v) = annotation_port {
            annotations.insert("system_metrics_enabled".to_string(), v.to_string());
        }
        annotations.insert(extra_key.to_string(), extra_value.to_string());

        Arc::new(Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            spec: match (port_name, port_number) {
                (Some(pn), Some(num)) => Some(PodSpec {
                    containers: vec![Container {
                        name: "app".to_string(),
                        ports: Some(vec![ContainerPort {
                            name: Some(pn.to_string()),
                            container_port: num,
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                _ => None,
            },
            status: Some(PodStatus {
                pod_ip: Some(ip.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    /// Build a pod carrying at most one annotation; `None` means no
    /// annotations map is set on the pod at all (distinct from "empty map").
    fn pod_with_optional_annotation(kv: Option<(&str, &str)>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                annotations: kv.map(|(k, v)| {
                    let mut m = BTreeMap::new();
                    m.insert(k.to_string(), v.to_string());
                    m
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Exercises every branch of `extract_metrics_namespace`:
    /// 1. discovery disabled (caller passes `None`) — pod content irrelevant
    /// 2. pod has no annotations map at all
    /// 3. pod has annotations but the configured key is absent
    /// 4. pod has the configured key — value returned verbatim
    #[rstest]
    #[case::disabled(Some(("databricks_tenant", "alpha")), None,                      None)]
    #[case::no_annotations_map(None,                       Some("databricks_tenant"), None)]
    #[case::key_absent(Some(("other_key", "ignored")),     Some("databricks_tenant"), None)]
    #[case::present(Some(("databricks_tenant", "alpha")),  Some("databricks_tenant"), Some("alpha".to_string()))]
    fn test_extract_metrics_namespace(
        #[case] annotation: Option<(&str, &str)>,
        #[case] metrics_ns_annotation_name: Option<&str>,
        #[case] expected: Option<String>,
    ) {
        let pod = pod_with_optional_annotation(annotation);
        assert_eq!(extract_metrics_namespace(&pod, metrics_ns_annotation_name), expected);
    }

    /// Named-port path attaches the metrics-namespace value when the pod
    /// carries the configured annotation.
    #[test]
    fn test_extract_metrics_endpoint_with_metrics_namespace() {
        let pod = make_pod_with_extra_annotation(
            "tenant-pod",
            "10.244.1.5",
            Some("user-metrics"),
            Some(9090),
            None,
            "databricks_tenant",
            "alpha",
        );

        let endpoint =
            extract_metrics_endpoint(pod.as_ref(), "user-metrics", Some("databricks_tenant"));
        assert_eq!(
            endpoint,
            Some(Endpoint {
                url: "http://10.244.1.5:9090/metrics".to_string(),
                name: "tenant-pod".to_string(),
                namespace: "default".to_string(),
                metrics_namespace: Some("alpha".to_string()),
            })
        );
    }

    /// Named-port path: pod missing the annotation yields `metrics_namespace = None`.
    #[test]
    fn test_extract_metrics_endpoint_without_metrics_ns_annotation_name() {
        let pod = make_pod(
            "no-tenant-pod",
            "10.244.1.6",
            Some("user-metrics"),
            Some(9090),
            None,
        );

        let endpoint =
            extract_metrics_endpoint(pod.as_ref(), "user-metrics", Some("databricks_tenant"));
        assert_eq!(endpoint.unwrap().metrics_namespace, None);
    }

    /// Annotation-discovery path: every endpoint produced from one pod shares
    /// the same metrics-namespace value.
    #[test]
    fn test_extract_endpoints_from_annotation_propagates_metrics_namespace() {
        let pod = make_pod_with_extra_annotation(
            "tenant-multi-pod",
            "10.244.2.4",
            None,
            None,
            Some("9091,9092"),
            "databricks_tenant",
            "beta",
        );

        let endpoints = extract_endpoints_from_annotation(
            pod.as_ref(),
            "system_metrics_enabled",
            UNLIMITED,
            Some("databricks_tenant"),
        );
        assert_eq!(endpoints.len(), 2);
        for ep in &endpoints {
            assert_eq!(ep.metrics_namespace.as_deref(), Some("beta"));
        }
    }

    /// `compute_endpoints` propagates the metrics-namespace value through both
    /// discovery paths. Pod has a named port (9090) and a different-port
    /// annotation (9091); both resulting endpoints inherit the value.
    #[test]
    fn test_compute_endpoints_metrics_namespace_propagates_through_both_paths() {
        let pod = make_pod_with_extra_annotation(
            "tenant-both-pod",
            "10.244.5.5",
            Some("user-metrics"),
            Some(9090),
            Some("9091"),
            "databricks_tenant",
            "gamma",
        );
        let state = vec![pod];

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            Some("databricks_tenant"),
        );

        assert_eq!(endpoints.len(), 2);
        for ep in &endpoints {
            assert_eq!(ep.metrics_namespace.as_deref(), Some("gamma"));
        }
    }

    /// `compute_endpoints` produces endpoints with `metrics_namespace = None`
    /// when metrics-namespace discovery is disabled, even if the pod happens
    /// to carry the annotation that would otherwise match.
    #[test]
    fn test_compute_endpoints_metrics_namespace_disabled_ignores_annotation() {
        let pod = make_pod_with_extra_annotation(
            "would-be-tenant-pod",
            "10.244.5.6",
            Some("user-metrics"),
            Some(9090),
            None,
            "databricks_tenant",
            "delta",
        );
        let state = vec![pod];

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            Some("system_metrics_enabled"),
            UNLIMITED,
            None, // metrics-namespace discovery disabled
        );

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].metrics_namespace, None);
    }

    /// Per-pod metrics-namespace isolation: two pods with different values
    /// produce endpoints carrying their own value.
    #[test]
    fn test_compute_endpoints_per_pod_metrics_namespace_isolation() {
        let pod_a = make_pod_with_extra_annotation(
            "pod-a",
            "10.0.0.30",
            None,
            None,
            Some("9091"),
            "databricks_tenant",
            "alpha",
        );
        let pod_b = make_pod_with_extra_annotation(
            "pod-b",
            "10.0.0.31",
            None,
            None,
            Some("9091"),
            "databricks_tenant",
            "beta",
        );
        let state = vec![pod_a, pod_b];

        let endpoints = compute_endpoints(
            &state,
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            Some("databricks_tenant"),
        );

        assert_eq!(endpoints.len(), 2);
        let metrics_ns_for = |name: &str| -> Option<String> {
            endpoints
                .iter()
                .find(|e| e.name == name)
                .and_then(|e| e.metrics_namespace.clone())
        };
        assert_eq!(metrics_ns_for("pod-a").as_deref(), Some("alpha"));
        assert_eq!(metrics_ns_for("pod-b").as_deref(), Some("beta"));
    }
}
