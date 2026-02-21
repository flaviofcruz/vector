//! Kubernetes endpoint provider for Prometheus scraping.
//!
//! This module provides functionality to discover Prometheus scrape endpoints
//! from Kubernetes pods via two independent paths:
//!
//! 1. **Named-port discovery** — pods that expose a container port with a
//!    configured name (e.g. `"user-metrics"`) are scraped on that port.
//! 2. **Annotation-based discovery** — pods that carry a configured annotation
//!    (e.g. `"system_metrics_enabled"`) are scraped on the port number given
//!    as the annotation value.
//!
//! Either path can be disabled by passing `None` for the corresponding
//! argument. Endpoints discovered by both paths for the same pod are
//! deduplicated by URL.

use std::{collections::HashSet, sync::Arc};

use k8s_openapi::api::core::v1::Pod;
use kube::runtime::reflector::store::Store;
use tracing::trace;

/// Represents a Kubernetes pod endpoint for Prometheus scraping
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The full URL to scrape metrics from (e.g., "http://10.0.0.1:9090/metrics")
    pub url: String,
    /// The name of the pod
    pub name: String,
    /// The namespace of the pod
    pub namespace: String,
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
    /// valid TCP port number.
    annotation_name: Option<String>,
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
    pub fn new(
        pod_state: Store<Pod>,
        named_port: Option<String>,
        annotation_name: Option<String>,
    ) -> Self {
        Self {
            pod_state,
            named_port,
            annotation_name,
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
///   its value is expected to be a TCP port number. Pass `None` to disable
///   annotation-based discovery entirely.
fn compute_endpoints(
    state: &[Arc<Pod>],
    named_port: Option<&str>,
    annotation_name: Option<&str>,
) -> Vec<Endpoint> {
    let named_port_endpoints: Vec<Endpoint> = match named_port {
        Some(port) => state
            .iter()
            .filter(|pod| pod_has_named_port(pod.as_ref(), port))
            .filter_map(|pod| extract_metrics_endpoint(pod.as_ref(), port))
            .collect(),
        None => vec![],
    };

    let annotation_endpoints: Vec<Endpoint> = match annotation_name {
        Some(name) => state
            .iter()
            .filter_map(|pod| extract_endpoint_from_annotation(pod.as_ref(), name))
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

/// Extract the endpoint (pod_ip:port) for pods with the specified named port
///
/// # Arguments
///
/// * `pod` - The Kubernetes pod to extract the endpoint from
/// * `port_name` - The name of the port to look for
///
/// # Returns
///
/// An `Option<Endpoint>` containing the HTTP endpoint URL if the pod has an IP
/// and a matching port, or `None` otherwise
fn extract_metrics_endpoint(pod: &Pod, port_name: &str) -> Option<Endpoint> {
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

    trace!(
        message = "Created endpoint for pod with named port.",
        pod = %name,
        namespace = %namespace,
        port_name,
        endpoint = %url
    );

    Some(Endpoint {
        url,
        name,
        namespace,
    })
}

/// Extract the endpoint for pods that carry the specified annotation.
///
/// The annotation value is expected to be a valid TCP port number (e.g. `"9091"`).
/// Returns `None` if the annotation is absent, the port cannot be parsed, or the
/// pod has no IP address assigned yet.
fn extract_endpoint_from_annotation(pod: &Pod, annotation_name: &str) -> Option<Endpoint> {
    let annotations = pod.metadata.annotations.as_ref()?;
    let port_str = annotations.get(annotation_name)?;
    let port_number: i32 = port_str.parse().ok()?;

    let pod_ip = pod.status.as_ref()?.pod_ip.as_ref()?;
    let name = pod.metadata.name.clone().unwrap_or_default();
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();

    let url = format!("http://{}:{}/metrics", pod_ip, port_number);

    trace!(
        message = "Created endpoint for pod with annotation.",
        annotation = annotation_name,
        pod = %name,
        namespace = %namespace,
        port = port_number,
        endpoint = %url
    );

    Some(Endpoint {
        url,
        name,
        namespace,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use k8s_openapi::{
        api::core::v1::{Container, ContainerPort, PodSpec, PodStatus},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

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

        let endpoint = extract_metrics_endpoint(&pod, "user-metrics");
        assert_eq!(
            endpoint,
            Some(Endpoint {
                url: "http://10.244.1.5:9090/metrics".to_string(),
                name: "test-pod".to_string(),
                namespace: "test-namespace".to_string(),
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

        let endpoint = extract_metrics_endpoint(&pod, "user-metrics");
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

        let endpoint = extract_endpoint_from_annotation(&pod, "system_metrics_enabled");
        assert_eq!(
            endpoint,
            Some(Endpoint {
                url: "http://10.244.2.3:9091/metrics".to_string(),
                name: "annotated-pod".to_string(),
                namespace: "default".to_string(),
            })
        );
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
            compute_endpoints(&state, Some("user-metrics"), Some("system_metrics_enabled"));

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
            compute_endpoints(&state, Some("user-metrics"), Some("system_metrics_enabled"));

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
            compute_endpoints(&state, Some("user-metrics"), Some("system_metrics_enabled"));

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
            compute_endpoints(&state, Some("user-metrics"), Some("system_metrics_enabled"));

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
            compute_endpoints(&state, Some("user-metrics"), Some("system_metrics_enabled"));

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

        let endpoints = compute_endpoints(&state, Some("user-metrics"), None);

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

        let endpoints = compute_endpoints(&state, None, Some("system_metrics_enabled"));

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

        let endpoints = compute_endpoints(&state, None, None);

        assert!(endpoints.is_empty());
    }

    /// Case 4 variant: annotation is present but contains an invalid port value.
    #[test]
    fn test_compute_endpoints_invalid_annotation_no_named_port() {
        let pod = make_pod("pod-e", "10.0.0.5", None, None, Some("not-a-port"));
        let state = vec![pod];

        let endpoints =
            compute_endpoints(&state, Some("user-metrics"), Some("system_metrics_enabled"));

        assert!(endpoints.is_empty());
    }
}
