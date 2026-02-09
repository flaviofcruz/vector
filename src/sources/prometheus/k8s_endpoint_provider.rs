//! Kubernetes endpoint provider for Prometheus scraping.
//!
//! This module provides functionality to discover Prometheus scrape endpoints
//! from Kubernetes pods based on named ports.

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

/// A Kubernetes endpoint provider that discovers pods with specific named ports
/// and generates Prometheus scrape endpoints from them.
///
/// This provider watches Kubernetes pods and filters for those with a container
/// port matching the specified name, then constructs HTTP endpoints using the pod's IP
/// and the port number.
pub struct K8sEndpointProvider {
    pod_state: Store<Pod>,
    named_port: String,
}

impl K8sEndpointProvider {
    /// Create a new [`K8sEndpointProvider`].
    ///
    /// # Arguments
    ///
    /// * `pod_state` - A read-only view of the Kubernetes pod state from the reflector
    /// * `named_port` - The name of the container port to look for when discovering pods
    pub fn new(pod_state: Store<Pod>, named_port: String) -> Self {
        Self {
            pod_state,
            named_port,
        }
    }
}

impl EndpointProvider for K8sEndpointProvider {
    type IntoIter = Vec<Endpoint>;

    fn endpoints(&self) -> Vec<Endpoint> {
        let state = self.pod_state.state();

        state
            .into_iter()
            // Filter for pods that have a container port with the specified name
            .filter(|pod| pod_has_named_port(pod.as_ref(), &self.named_port))
            .filter_map(|pod| extract_metrics_endpoint(pod.as_ref(), &self.named_port))
            .collect()
    }
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
    // Get the pod IP from status
    let pod_ip = pod.status.as_ref()?.pod_ip.as_ref()?;

    // Get pod name and namespace from metadata
    let name = pod.metadata.name.clone().unwrap_or_default();
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();

    // Find the port number with the specified name
    let port_number = pod.spec.as_ref()?.containers.iter().find_map(|container| {
        container.ports.as_ref()?.iter().find_map(|port| {
            if port.name.as_ref()? == port_name {
                Some(port.container_port)
            } else {
                None
            }
        })
    })?;

    // Construct the endpoint URL
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

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_pod_has_named_port_without_matching_port() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test-container".to_string(),
                    ports: Some(vec![ContainerPort {
                        name: Some("http".to_string()),
                        container_port: 8080,
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(!pod_has_named_port(&pod, "user-metrics"));
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
    fn test_extract_metrics_endpoint_without_matching_port() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test-container".to_string(),
                    ports: Some(vec![ContainerPort {
                        name: Some("http".to_string()),
                        container_port: 8080,
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
        assert_eq!(endpoint, None);
    }
}
