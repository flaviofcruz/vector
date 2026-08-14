//! Kubernetes endpoint provider for Prometheus scraping.
//!
//! This module provides functionality to discover Prometheus scrape endpoints
//! from Kubernetes pods via two independent paths:
//!
//! 1. **Named-port discovery** — pods that expose a container port with a
//!    configured name (e.g. `"user-metrics"`) are scraped on that port. This
//!    path yields at most one endpoint per pod (first matching port).
//! 2. **Regex named-port discovery** — pods that expose one or more container
//!    ports whose names match a configured regex (e.g. `"metrics.*"`) are
//!    scraped on every matching port. Unlike the exact named-port path, this
//!    yields one endpoint per matching port, so a single pod exposing
//!    `metrics0` and `metrics1` produces two endpoints (capped by
//!    `max_endpoints_per_pod`).
//! 3. **Annotation-based discovery** — pods that carry a configured annotation
//!    (e.g. `"system_metrics_enabled"`) are scraped on the port number(s) given
//!    as the annotation value. The value may be a single port (`"9091"`) or a
//!    comma-separated list (`"9091,9092"`), in which case the pod yields one
//!    endpoint per valid port. Unparseable entries are silently skipped.
//!
//! Any path can be disabled by passing `None` for the corresponding argument.
//! Endpoints discovered by multiple paths for the same pod are deduplicated by
//! URL.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use k8s_openapi::api::core::v1::Pod;
use kube::runtime::reflector::store::Store;
use regex::Regex;
use tracing::{trace, warn};

/// Per-namespace mapping of pod annotation key → label name. Outer key is the
/// pod's namespace (matched exactly); inner key is the annotation key to read
/// off the pod; inner value is the label name to emit on each scraped metric.
/// An empty map disables this feature entirely.
pub type NamespaceAnnotationLabels = HashMap<String, HashMap<String, String>>;

/// Represents a Kubernetes pod endpoint for Prometheus scraping
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The full URL to scrape metrics from (e.g., "http://10.0.0.1:9090/metrics")
    pub url: String,
    /// The name of the pod
    pub name: String,
    /// The namespace of the pod
    pub namespace: String,
    /// Labels sourced from pod annotations per the configured
    /// `namespace_annotation_labels` map. Keys are label names; values are the
    /// raw annotation values. Empty when discovery is disabled, the pod's
    /// namespace has no entry in the map, or none of the configured
    /// annotations are present on the pod. `BTreeMap` for deterministic
    /// ordering in logs and tests.
    pub extra_labels: BTreeMap<String, String>,
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
    /// The container port name used for exact named-port discovery, or `None`
    /// to disable that discovery path entirely.
    named_port: Option<String>,
    /// A regex matched against container port names for regex named-port
    /// discovery, or `None` to disable that path. Every port whose name matches
    /// yields an endpoint (capped by `max_endpoints_per_pod`).
    named_port_regex: Option<Regex>,
    /// The pod annotation key used for annotation-based discovery, or `None` to
    /// disable that discovery path entirely. The annotation value must be a
    /// valid TCP port number, or a comma-separated list of port numbers.
    annotation_name: Option<String>,
    /// The maximum number of endpoints contributed per pod by the annotation
    /// and regex named-port paths. Ports beyond this cap are dropped.
    max_endpoints_per_pod: usize,
    /// Per-namespace mapping of pod annotation key → label name (see
    /// [`NamespaceAnnotationLabels`]). When the map is empty the feature is a
    /// no-op and every endpoint carries `extra_labels = {}`.
    namespace_annotation_labels: NamespaceAnnotationLabels,
}

impl K8sEndpointProvider {
    /// Create a new [`K8sEndpointProvider`].
    ///
    /// # Arguments
    ///
    /// * `pod_state` - A read-only view of the Kubernetes pod state from the reflector
    /// * `named_port` - The name of the container port to look for when discovering pods,
    ///   or `None` to disable exact named-port-based discovery entirely
    /// * `named_port_regex` - A regex matched against container port names; every
    ///   matching port yields an endpoint, or `None` to disable regex named-port
    ///   discovery entirely
    /// * `annotation_name` - The pod annotation key whose value is the port number to
    ///   scrape, or `None` to disable annotation-based discovery entirely
    /// * `max_endpoints_per_pod` - Caps the number of endpoints a single pod
    ///   can contribute via the annotation and regex named-port paths;
    ///   additional ports are silently dropped
    /// * `namespace_annotation_labels` - Per-namespace map of annotation key
    ///   → label name; empty disables the feature
    pub fn new(
        pod_state: Store<Pod>,
        named_port: Option<String>,
        named_port_regex: Option<Regex>,
        annotation_name: Option<String>,
        max_endpoints_per_pod: usize,
        namespace_annotation_labels: NamespaceAnnotationLabels,
    ) -> Self {
        Self {
            pod_state,
            named_port,
            named_port_regex,
            annotation_name,
            max_endpoints_per_pod,
            namespace_annotation_labels,
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
            self.named_port_regex.as_ref(),
            self.annotation_name.as_deref(),
            self.max_endpoints_per_pod,
            &self.namespace_annotation_labels,
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
/// * `named_port` - Container port name used for the exact named-port discovery
///   path, or `None` to disable it entirely
/// * `named_port_regex` - Regex matched against container port names for the
///   regex named-port discovery path, or `None` to disable it entirely
/// * `annotation_name` - Pod annotation key used for the annotation-based discovery path;
///   its value is expected to be a TCP port number or a comma-separated list of port
///   numbers. Pass `None` to disable annotation-based discovery entirely.
/// * `max_endpoints_per_pod` - Caps the number of endpoints contributed per pod
///   by the annotation and regex named-port paths; additional ports are silently
///   dropped.
/// * `namespace_annotation_labels` - Per-namespace map of annotation key → label
///   name. When empty, every endpoint carries `extra_labels = {}`.
fn compute_endpoints(
    state: &[Arc<Pod>],
    named_port: Option<&str>,
    named_port_regex: Option<&Regex>,
    annotation_name: Option<&str>,
    max_endpoints_per_pod: usize,
    namespace_annotation_labels: &NamespaceAnnotationLabels,
) -> Vec<Endpoint> {
    let named_port_endpoints: Vec<Endpoint> = match named_port {
        Some(port) => state
            .iter()
            .filter(|pod| pod_has_named_port(pod.as_ref(), port))
            .filter_map(|pod| {
                extract_metrics_endpoint(pod.as_ref(), port, namespace_annotation_labels)
            })
            .collect(),
        None => vec![],
    };

    let regex_endpoints: Vec<Endpoint> = match named_port_regex {
        Some(re) => state
            .iter()
            .flat_map(|pod| {
                extract_endpoints_by_port_regex(
                    pod.as_ref(),
                    re,
                    max_endpoints_per_pod,
                    namespace_annotation_labels,
                )
            })
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
                    namespace_annotation_labels,
                )
            })
            .collect(),
        None => vec![],
    };

    let mut seen = HashSet::new();
    named_port_endpoints
        .into_iter()
        .chain(regex_endpoints)
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

/// Build the per-pod `extra_labels` map from the configured
/// `namespace_annotation_labels`.
///
/// Looks up the pod's namespace (exact match) in the outer map; for each
/// (annotation_key → label_name) entry, reads the annotation off the pod and,
/// if present, records `label_name → annotation_value` on the returned map.
/// Annotations that are absent contribute no label.
///
/// Returns an empty map when the outer config is empty, when the pod's
/// namespace has no entry, when the pod carries no annotations, or when none
/// of the configured annotations are present. Annotation values are taken
/// verbatim (no parsing); an empty annotation value produces an empty label
/// value.
fn extract_extra_labels(
    pod: &Pod,
    namespace_annotation_labels: &NamespaceAnnotationLabels,
) -> BTreeMap<String, String> {
    if namespace_annotation_labels.is_empty() {
        return BTreeMap::new();
    }
    let Some(ns) = pod.metadata.namespace.as_deref() else {
        return BTreeMap::new();
    };
    let Some(rules) = namespace_annotation_labels.get(ns) else {
        return BTreeMap::new();
    };
    let Some(annotations) = pod.metadata.annotations.as_ref() else {
        return BTreeMap::new();
    };

    let mut out = BTreeMap::new();
    for (annotation_key, label_name) in rules {
        if let Some(value) = annotations.get(annotation_key) {
            out.insert(label_name.clone(), value.clone());
        }
    }
    out
}

/// Extract the endpoint (pod_ip:port) for pods with the specified named port
///
/// # Arguments
///
/// * `pod` - The Kubernetes pod to extract the endpoint from
/// * `port_name` - The name of the port to look for
/// * `namespace_annotation_labels` - Per-namespace map driving `extra_labels`
///
/// # Returns
///
/// An `Option<Endpoint>` containing the HTTP endpoint URL if the pod has an IP
/// and a matching port, or `None` otherwise
fn extract_metrics_endpoint(
    pod: &Pod,
    port_name: &str,
    namespace_annotation_labels: &NamespaceAnnotationLabels,
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
    let extra_labels = extract_extra_labels(pod, namespace_annotation_labels);

    trace!(
        message = "Created endpoint for pod with named port.",
        pod = %name,
        namespace = %namespace,
        port_name,
        endpoint = %url,
        extra_labels = ?extra_labels,
    );

    Some(Endpoint {
        url,
        name,
        namespace,
        extra_labels,
    })
}

/// Extract endpoints for a pod by matching container port names against a regex.
///
/// Every container port (across all containers) whose name matches `port_re`
/// yields one endpoint, so a pod exposing several matching ports (e.g.
/// `metrics0` and `metrics1`) produces several endpoints — unlike
/// [`extract_metrics_endpoint`], which is exact-match and single-port. Port
/// numbers are deduplicated (first-seen wins) so a port declared more than once
/// does not consume extra cap slots, and the result is capped at `max_ports`
/// (ports beyond that are dropped and a warning is logged). Returns an empty
/// `Vec` if the pod has no IP address yet, no port name matches, or `max_ports`
/// is zero.
fn extract_endpoints_by_port_regex(
    pod: &Pod,
    port_re: &Regex,
    max_ports: usize,
    namespace_annotation_labels: &NamespaceAnnotationLabels,
) -> Vec<Endpoint> {
    let Some(pod_ip) = pod.status.as_ref().and_then(|s| s.pod_ip.as_ref()) else {
        return vec![];
    };
    let name = pod.metadata.name.clone().unwrap_or_default();
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let extra_labels = extract_extra_labels(pod, namespace_annotation_labels);

    // Collect the container_port of every port whose name matches, deduping by
    // port number (first-seen wins) before the cap so duplicate declarations
    // don't squeeze out distinct ports.
    let mut seen_ports = HashSet::new();
    let matched: Vec<i32> = pod
        .spec
        .as_ref()
        .into_iter()
        .flat_map(|spec| spec.containers.iter())
        .flat_map(|container| container.ports.iter().flatten())
        .filter(|port| {
            port.name
                .as_deref()
                .is_some_and(|name| port_re.is_match(name))
        })
        .map(|port| port.container_port)
        .filter(|port_number| seen_ports.insert(*port_number))
        .collect();

    if matched.len() > max_ports {
        warn!(
            message = "Pod exposes more matching ports than allowed; truncating.",
            regex = %port_re,
            pod = %name,
            namespace = %namespace,
            matched_ports = matched.len(),
            limit = max_ports,
            dropped = matched.len() - max_ports,
            internal_log_rate_secs = 60
        );
    }

    matched
        .into_iter()
        .take(max_ports)
        .map(|port_number| {
            let url = format!("http://{}:{}/metrics", pod_ip, port_number);
            trace!(
                message = "Created endpoint for pod with matching port name.",
                regex = %port_re,
                pod = %name,
                namespace = %namespace,
                port = port_number,
                endpoint = %url,
                extra_labels = ?extra_labels,
            );
            Endpoint {
                url,
                name: name.clone(),
                namespace: namespace.clone(),
                extra_labels: extra_labels.clone(),
            }
        })
        .collect()
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
    namespace_annotation_labels: &NamespaceAnnotationLabels,
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
    let extra_labels = extract_extra_labels(pod, namespace_annotation_labels);

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
                extra_labels = ?extra_labels,
            );
            Endpoint {
                url,
                name: name.clone(),
                namespace: namespace.clone(),
                extra_labels: extra_labels.clone(),
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

    /// Effectively-unlimited cap used in tests whose intent is unrelated to the
    /// `max_endpoints_per_pod` enforcement.
    const UNLIMITED: usize = usize::MAX;

    /// Empty per-namespace label config, for tests whose intent is unrelated
    /// to `namespace_annotation_labels`.
    fn no_labels() -> NamespaceAnnotationLabels {
        HashMap::new()
    }

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

        let endpoint = extract_metrics_endpoint(&pod, "user-metrics", &no_labels());
        assert_eq!(
            endpoint,
            Some(Endpoint {
                url: "http://10.244.1.5:9090/metrics".to_string(),
                name: "test-pod".to_string(),
                namespace: "test-namespace".to_string(),
                extra_labels: BTreeMap::new(),
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

        let endpoint = extract_metrics_endpoint(&pod, "user-metrics", &no_labels());
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

        let endpoints = extract_endpoints_from_annotation(
            &pod,
            "system_metrics_enabled",
            UNLIMITED,
            &no_labels(),
        );
        assert_eq!(
            endpoints,
            vec![Endpoint {
                url: "http://10.244.2.3:9091/metrics".to_string(),
                name: "annotated-pod".to_string(),
                namespace: "default".to_string(),
                extra_labels: BTreeMap::new(),
            }]
        );
    }

    #[test]
    fn test_extract_endpoints_from_annotation_multiple_ports() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,9092".to_string(),
        );

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

        let endpoints = extract_endpoints_from_annotation(
            &pod,
            "system_metrics_enabled",
            UNLIMITED,
            &no_labels(),
        );
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

        let endpoints = extract_endpoints_from_annotation(
            &pod,
            "system_metrics_enabled",
            UNLIMITED,
            &no_labels(),
        );
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

        let endpoints = extract_endpoints_from_annotation(
            &pod,
            "system_metrics_enabled",
            UNLIMITED,
            &no_labels(),
        );
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert_eq!(urls.len(), 2);
        assert!(urls.contains("http://10.244.2.6:9091/metrics"));
        assert!(urls.contains("http://10.244.2.6:9092/metrics"));
    }

    #[test]
    fn test_extract_endpoints_from_annotation_all_invalid() {
        let mut annotations = BTreeMap::new();
        annotations.insert("system_metrics_enabled".to_string(), "foo,bar".to_string());

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

        let endpoints = extract_endpoints_from_annotation(
            &pod,
            "system_metrics_enabled",
            UNLIMITED,
            &no_labels(),
        );
        assert!(endpoints.is_empty());
    }

    #[test]
    fn test_extract_endpoints_from_annotation_without_ip() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,9092".to_string(),
        );

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

        let endpoints = extract_endpoints_from_annotation(
            &pod,
            "system_metrics_enabled",
            UNLIMITED,
            &no_labels(),
        );
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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, &no_labels());
        assert_eq!(endpoints.len(), 2);
        // Order preserved: first two declared ports survive.
        assert_eq!(endpoints[0].url, "http://10.244.2.8:9091/metrics");
        assert_eq!(endpoints[1].url, "http://10.244.2.8:9092/metrics");
    }

    /// `max_ports = 0` rejects all ports (defensive: no endpoints emitted).
    #[test]
    fn test_extract_endpoints_from_annotation_max_ports_zero() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "system_metrics_enabled".to_string(),
            "9091,9092".to_string(),
        );

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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 0, &no_labels());
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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 5, &no_labels());
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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, &no_labels());
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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, &no_labels());
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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, &no_labels());
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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, &no_labels());
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

        let endpoints = extract_endpoints_from_annotation(
            &pod,
            "system_metrics_enabled",
            UNLIMITED,
            &no_labels(),
        );
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
            extract_endpoints_from_annotation(&pod, "system_metrics_enabled", 2, &no_labels());
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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            None,
            UNLIMITED,
            &no_labels(),
        );

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

        let endpoints = compute_endpoints(
            &state,
            None,
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
        );

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

        let endpoints = compute_endpoints(&state, None, None, None, UNLIMITED, &no_labels());

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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
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
        let pod_a = make_pod(
            "pod-a",
            "10.0.0.20",
            None,
            None,
            Some("9091,9092,9093,9094"),
        );
        let pod_b = make_pod(
            "pod-b",
            "10.0.0.21",
            None,
            None,
            Some("9091,9092,9093,9094"),
        );
        let state = vec![pod_a, pod_b];

        let endpoints = compute_endpoints(
            &state,
            None,
            None,
            Some("system_metrics_enabled"),
            2,
            &no_labels(),
        );

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

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &no_labels(),
        );

        assert!(endpoints.is_empty());
    }

    // -----------------------------------------------------------------------
    // namespace_annotation_labels: per-namespace annotation → label mapping
    // -----------------------------------------------------------------------

    /// Build a pod with an arbitrary namespace, annotations, and IP. Spec is
    /// optional so tests can exercise both discovery paths.
    fn make_pod_in_ns(
        name: &str,
        ns: &str,
        ip: &str,
        port_name: Option<&str>,
        port_number: Option<i32>,
        annotations: BTreeMap<String, String>,
    ) -> Arc<Pod> {
        Arc::new(Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(ns.to_string()),
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

    fn ann(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn labels_map(entries: &[(&str, &[(&str, &str)])]) -> NamespaceAnnotationLabels {
        entries
            .iter()
            .map(|(ns, rules)| {
                (
                    ns.to_string(),
                    rules
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            })
            .collect()
    }

    /// Empty config → no extra labels regardless of pod annotations.
    #[test]
    fn test_extract_extra_labels_empty_config() {
        let pod = make_pod_in_ns(
            "p",
            "team-a",
            "10.0.0.1",
            None,
            None,
            ann(&[("databricks_tenant", "alpha")]),
        );
        assert!(extract_extra_labels(pod.as_ref(), &HashMap::new()).is_empty());
    }

    /// Pod namespace does not appear in the outer map → no extra labels.
    #[test]
    fn test_extract_extra_labels_unmatched_namespace() {
        let pod = make_pod_in_ns(
            "p",
            "team-c",
            "10.0.0.1",
            None,
            None,
            ann(&[("databricks_tenant", "alpha")]),
        );
        let cfg = labels_map(&[("team-a", &[("databricks_tenant", "tenant")])]);
        assert!(extract_extra_labels(pod.as_ref(), &cfg).is_empty());
    }

    /// Namespace match is exact — `team-a-prod` does not match a rule for `team-a`.
    #[test]
    fn test_extract_extra_labels_namespace_exact_match() {
        let pod = make_pod_in_ns(
            "p",
            "team-a-prod",
            "10.0.0.1",
            None,
            None,
            ann(&[("databricks_tenant", "alpha")]),
        );
        let cfg = labels_map(&[("team-a", &[("databricks_tenant", "tenant")])]);
        assert!(extract_extra_labels(pod.as_ref(), &cfg).is_empty());
    }

    /// Annotation listed in the rule is missing on the pod → that label is
    /// not added. Annotations present on the pod produce labels; absent ones
    /// are skipped.
    #[test]
    fn test_extract_extra_labels_missing_annotation_skipped() {
        let pod = make_pod_in_ns(
            "p",
            "team-a",
            "10.0.0.1",
            None,
            None,
            ann(&[("service", "checkout")]),
        );
        let cfg = labels_map(&[("team-a", &[("service", "service"), ("version", "version")])]);
        let got = extract_extra_labels(pod.as_ref(), &cfg);
        assert_eq!(got.len(), 1);
        assert_eq!(got.get("service").map(String::as_str), Some("checkout"));
    }

    /// Multiple matching annotations → multiple labels emitted with the
    /// configured names.
    #[test]
    fn test_extract_extra_labels_multiple_present() {
        let pod = make_pod_in_ns(
            "p",
            "team-a",
            "10.0.0.1",
            None,
            None,
            ann(&[("service", "checkout"), ("version", "1.2.3")]),
        );
        let cfg = labels_map(&[("team-a", &[("service", "svc"), ("version", "ver")])]);
        let got = extract_extra_labels(pod.as_ref(), &cfg);
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("svc").map(String::as_str), Some("checkout"));
        assert_eq!(got.get("ver").map(String::as_str), Some("1.2.3"));
    }

    /// Pod has no annotations map at all → no extra labels even when the
    /// namespace matches a rule.
    #[test]
    fn test_extract_extra_labels_no_annotations_map() {
        let pod = make_pod_in_ns("p", "team-a", "10.0.0.1", None, None, BTreeMap::new());
        let cfg = labels_map(&[("team-a", &[("databricks_tenant", "tenant")])]);
        assert!(extract_extra_labels(pod.as_ref(), &cfg).is_empty());
    }

    /// Named-port path: `extra_labels` populated from the configured rule.
    #[test]
    fn test_extract_metrics_endpoint_attaches_extra_labels() {
        let pod = make_pod_in_ns(
            "tenant-pod",
            "team-a",
            "10.244.1.5",
            Some("user-metrics"),
            Some(9090),
            ann(&[("databricks_tenant", "alpha")]),
        );
        let cfg = labels_map(&[("team-a", &[("databricks_tenant", "tenant")])]);

        let endpoint = extract_metrics_endpoint(pod.as_ref(), "user-metrics", &cfg).unwrap();
        assert_eq!(
            endpoint.extra_labels.get("tenant").map(String::as_str),
            Some("alpha")
        );
    }

    /// Annotation-discovery path: every endpoint produced from one pod
    /// carries the same `extra_labels` map.
    #[test]
    fn test_extract_endpoints_from_annotation_propagates_extra_labels() {
        let pod = make_pod_in_ns(
            "tenant-multi-pod",
            "team-a",
            "10.244.2.4",
            None,
            None,
            ann(&[
                ("system_metrics_enabled", "9091,9092"),
                ("databricks_tenant", "beta"),
            ]),
        );
        let cfg = labels_map(&[("team-a", &[("databricks_tenant", "tenant")])]);

        let endpoints = extract_endpoints_from_annotation(
            pod.as_ref(),
            "system_metrics_enabled",
            UNLIMITED,
            &cfg,
        );
        assert_eq!(endpoints.len(), 2);
        for ep in &endpoints {
            assert_eq!(
                ep.extra_labels.get("tenant").map(String::as_str),
                Some("beta")
            );
        }
    }

    /// `compute_endpoints` propagates `extra_labels` through both discovery
    /// paths. Pod has a named port (9090) and a different-port annotation
    /// (9091); both resulting endpoints carry the configured label.
    #[test]
    fn test_compute_endpoints_extra_labels_propagate_through_both_paths() {
        let pod = make_pod_in_ns(
            "tenant-both-pod",
            "team-a",
            "10.244.5.5",
            Some("user-metrics"),
            Some(9090),
            ann(&[
                ("system_metrics_enabled", "9091"),
                ("databricks_tenant", "gamma"),
            ]),
        );
        let state = vec![pod];
        let cfg = labels_map(&[("team-a", &[("databricks_tenant", "tenant")])]);

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &cfg,
        );

        assert_eq!(endpoints.len(), 2);
        for ep in &endpoints {
            assert_eq!(
                ep.extra_labels.get("tenant").map(String::as_str),
                Some("gamma")
            );
        }
    }

    /// Empty `namespace_annotation_labels` is a no-op: endpoints are produced
    /// normally but carry no extra labels.
    #[test]
    fn test_compute_endpoints_empty_map_is_noop() {
        let pod = make_pod_in_ns(
            "p",
            "team-a",
            "10.244.5.6",
            Some("user-metrics"),
            Some(9090),
            ann(&[("databricks_tenant", "delta")]),
        );
        let state = vec![pod];

        let endpoints = compute_endpoints(
            &state,
            Some("user-metrics"),
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &HashMap::new(),
        );

        assert_eq!(endpoints.len(), 1);
        assert!(endpoints[0].extra_labels.is_empty());
    }

    /// Per-pod isolation: pods in different namespaces resolve to different
    /// rule sets and carry their own labels.
    #[test]
    fn test_compute_endpoints_per_namespace_isolation() {
        let pod_a = make_pod_in_ns(
            "pod-a",
            "team-a",
            "10.0.0.30",
            None,
            None,
            ann(&[("system_metrics_enabled", "9091"), ("service", "checkout")]),
        );
        let pod_b = make_pod_in_ns(
            "pod-b",
            "team-b",
            "10.0.0.31",
            None,
            None,
            ann(&[("system_metrics_enabled", "9091"), ("app", "billing")]),
        );
        let state = vec![pod_a, pod_b];
        let cfg = labels_map(&[
            ("team-a", &[("service", "service_label")]),
            ("team-b", &[("app", "app_label")]),
        ]);

        let endpoints = compute_endpoints(
            &state,
            None,
            None,
            Some("system_metrics_enabled"),
            UNLIMITED,
            &cfg,
        );

        assert_eq!(endpoints.len(), 2);
        let by_name = |n: &str| endpoints.iter().find(|e| e.name == n).unwrap();
        assert_eq!(
            by_name("pod-a")
                .extra_labels
                .get("service_label")
                .map(String::as_str),
            Some("checkout"),
        );
        assert_eq!(
            by_name("pod-b")
                .extra_labels
                .get("app_label")
                .map(String::as_str),
            Some("billing"),
        );
    }

    /// End-to-end through `compute_endpoints`: a pod that carries several
    /// annotations all listed in its namespace's rule yields an endpoint
    /// whose `extra_labels` map contains every matching label — and no
    /// labels for annotations the pod carries that aren't in the rule, nor
    /// for annotations the rule lists that the pod doesn't carry.
    #[test]
    fn test_compute_endpoints_multiple_matching_annotations() {
        let pod = make_pod_in_ns(
            "multi-ann-pod",
            "team-a",
            "10.0.0.40",
            Some("user-metrics"),
            Some(9090),
            ann(&[
                ("service", "checkout"),
                ("version", "1.2.3"),
                ("owner", "team-a"),
                ("unrelated", "ignored"), // not in the rule
            ]),
        );
        let state = vec![pod];
        let cfg = labels_map(&[(
            "team-a",
            &[
                ("service", "svc"),
                ("version", "ver"),
                ("owner", "owner_label"),
                ("missing", "missing_label"), // not on the pod
            ],
        )]);

        let endpoints =
            compute_endpoints(&state, Some("user-metrics"), None, None, UNLIMITED, &cfg);

        assert_eq!(endpoints.len(), 1);
        let labels = &endpoints[0].extra_labels;
        assert_eq!(
            labels.len(),
            3,
            "exactly the three matching annotations should produce labels"
        );
        assert_eq!(labels.get("svc").map(String::as_str), Some("checkout"));
        assert_eq!(labels.get("ver").map(String::as_str), Some("1.2.3"));
        assert_eq!(
            labels.get("owner_label").map(String::as_str),
            Some("team-a")
        );
        assert!(
            labels.get("missing_label").is_none(),
            "rule entry with no matching annotation must not emit a label"
        );
        assert!(
            labels.get("unrelated").is_none(),
            "pod annotation not in the rule must not become a label"
        );
    }

    // -----------------------------------------------------------------------
    // Regex named-port discovery
    // -----------------------------------------------------------------------

    /// Build the anchored regex the way `build()` does, so tests exercise the
    /// same full-match semantics as production.
    fn port_re(pattern: &str) -> Regex {
        Regex::new(&format!("^(?:{pattern})$")).unwrap()
    }

    /// Build a pod with an arbitrary set of named container ports.
    fn make_pod_with_named_ports(name: &str, ip: &str, ports: &[(&str, i32)]) -> Arc<Pod> {
        Arc::new(Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "app".to_string(),
                    ports: Some(
                        ports
                            .iter()
                            .map(|(n, num)| ContainerPort {
                                name: Some(n.to_string()),
                                container_port: *num,
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                pod_ip: Some(ip.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    /// A pod exposing `metrics0` and `metrics1` yields one endpoint per matching
    /// port (multi-endpoint per pod), and non-matching ports are excluded.
    #[test]
    fn test_regex_named_port_multiple_ports() {
        let pod = make_pod_with_named_ports(
            "multi",
            "10.1.0.1",
            &[("metrics0", 7788), ("metrics1", 7789), ("http", 8080)],
        );
        let endpoints =
            extract_endpoints_by_port_regex(&pod, &port_re("metrics.*"), UNLIMITED, &no_labels());
        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert_eq!(urls.len(), 2);
        assert!(urls.contains("http://10.1.0.1:7788/metrics"));
        assert!(urls.contains("http://10.1.0.1:7789/metrics"));
        assert!(!urls.contains("http://10.1.0.1:8080/metrics"));
    }

    /// Anchored match: `metrics.*` must match the whole name, so a port named
    /// `xmetrics` is not scraped.
    #[test]
    fn test_regex_named_port_is_anchored() {
        let pod = make_pod_with_named_ports(
            "anchor",
            "10.1.0.2",
            &[("metrics0", 7788), ("xmetrics", 9999)],
        );
        let endpoints =
            extract_endpoints_by_port_regex(&pod, &port_re("metrics.*"), UNLIMITED, &no_labels());
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.1.0.2:7788/metrics");
    }

    /// The cap applies to the regex path; ports beyond `max_ports` are dropped.
    #[test]
    fn test_regex_named_port_respects_cap() {
        let pod = make_pod_with_named_ports(
            "capped",
            "10.1.0.3",
            &[("metrics0", 1), ("metrics1", 2), ("metrics2", 3)],
        );
        let endpoints =
            extract_endpoints_by_port_regex(&pod, &port_re("metrics.*"), 2, &no_labels());
        assert_eq!(endpoints.len(), 2);
    }

    /// Duplicate port numbers are collapsed (first-seen wins) before the cap.
    #[test]
    fn test_regex_named_port_dedupes_port_numbers() {
        let pod = make_pod_with_named_ports(
            "dupe",
            "10.1.0.4",
            &[("metrics0", 7788), ("metrics-again", 7788)],
        );
        let endpoints =
            extract_endpoints_by_port_regex(&pod, &port_re("metrics.*"), UNLIMITED, &no_labels());
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].url, "http://10.1.0.4:7788/metrics");
    }

    /// A pod without an IP yields nothing on the regex path.
    #[test]
    fn test_regex_named_port_no_ip() {
        let pod = Arc::new(Pod {
            metadata: ObjectMeta {
                name: Some("no-ip".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "app".to_string(),
                    ports: Some(vec![ContainerPort {
                        name: Some("metrics0".to_string()),
                        container_port: 7788,
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
            ..Default::default()
        });
        let endpoints =
            extract_endpoints_by_port_regex(&pod, &port_re("metrics.*"), UNLIMITED, &no_labels());
        assert!(endpoints.is_empty());
    }

    /// Through `compute_endpoints`: the regex path emits both ports, and an
    /// overlapping exact `named_port` on the same port number dedupes by URL.
    #[test]
    fn test_compute_endpoints_regex_path_and_dedup_with_exact() {
        let pod = make_pod_with_named_ports(
            "combo",
            "10.1.0.5",
            &[("metrics0", 7788), ("metrics1", 7789)],
        );
        let state = vec![pod];

        // Exact named_port "metrics0" (7788) overlaps the regex match on 7788.
        let endpoints = compute_endpoints(
            &state,
            Some("metrics0"),
            Some(&port_re("metrics.*")),
            None,
            UNLIMITED,
            &no_labels(),
        );

        let urls: HashSet<&str> = endpoints.iter().map(|e| e.url.as_str()).collect();
        assert_eq!(urls.len(), 2, "7788 from both paths must dedupe to one");
        assert!(urls.contains("http://10.1.0.5:7788/metrics"));
        assert!(urls.contains("http://10.1.0.5:7789/metrics"));
    }
}
