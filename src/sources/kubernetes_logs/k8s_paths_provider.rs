//! A paths provider for k8s logs.

#![deny(missing_docs)]

use std::path::PathBuf;

/// Default container name used when container name extraction is disabled.
/// Choosing something obvious to make debugging easier when container name is invalid
const DEFAULT_CONTAINER_NAME: &str = "DEFAULT_CONTAINER_NAME";

use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::runtime::reflector::{ObjectRef, store::Store};
use vector_lib::file_source::paths_provider::{LogFileInfo, PathsProvider};

use super::path_helpers::{build_databricks_k8s_pod_logs_directory, build_pod_logs_directory};
use crate::kubernetes::pod_manager_logic::extract_static_pod_config_hashsum;

/// A paths provider implementation that uses the state obtained from the
/// the k8s API.
pub struct K8sPathsProvider {
    pod_state: Store<Pod>,
    namespace_state: Store<Namespace>,
    pod_logs_glob_patterns: Vec<String>,
    include_paths: Vec<glob::Pattern>,
    exclude_paths: Vec<glob::Pattern>,
    insert_namespace_fields: bool,
    extract_databricks_logs: bool,
    /// When set, the annotation key to read for hostPath-based log directory discovery.
    /// None = use emptyDir-based discovery only.
    hostpath_logging_annotation_key: Option<String>,
}

/// Extracts container name from a log file path.
/// Only extracts if `extract_databricks_logs` is false (for normal k8s logs).
/// Otherwise returns `DEFAULT_CONTAINER_NAME`.
fn extract_container_name_from_path(
    path: &std::path::Path,
    extract_databricks_logs: bool,
) -> String {
    if !extract_databricks_logs {
        // Only do this for normal kubernetes logs, as the databricks logs paths may not follow the exact pattern
        path.parent() // Get directory containing the log file
            .and_then(|p| p.file_name()) // Get container directory name
            .and_then(|name| name.to_str())
            .unwrap_or(DEFAULT_CONTAINER_NAME)
            .to_string()
    } else {
        DEFAULT_CONTAINER_NAME.to_string()
    }
}

impl K8sPathsProvider {
    /// Create a new [`K8sPathsProvider`].
    pub fn new(
        pod_state: Store<Pod>,
        namespace_state: Store<Namespace>,
        pod_logs_glob_patterns: Vec<String>,
        include_paths: Vec<glob::Pattern>,
        exclude_paths: Vec<glob::Pattern>,
        insert_namespace_fields: bool,
        extract_databricks_logs: bool,
        hostpath_logging_annotation_key: Option<String>,
    ) -> Self {
        Self {
            pod_state,
            namespace_state,
            pod_logs_glob_patterns,
            include_paths,
            exclude_paths,
            insert_namespace_fields,
            extract_databricks_logs,
            hostpath_logging_annotation_key,
        }
    }
}

impl PathsProvider for K8sPathsProvider {
    type IntoIter = Vec<(Option<LogFileInfo>, PathBuf)>;

    fn paths(&self) -> Self::IntoIter {
        let state = self.pod_state.state();

        state
            .into_iter()
            // filter out pods where we haven't fetched the namespace metadata yet
            // they will be picked up on a later run
            // Only check namespace metadata if insert_namespace_fields is enabled
            .filter(|pod| {
                if !self.insert_namespace_fields {
                    // Skip namespace metadata check when namespace fields are disabled
                    return true;
                }
                trace!(message = "Verifying Namespace metadata for pod.", pod = ?pod.metadata.name);
                if let Some(namespace) = pod.metadata.namespace.as_ref() {
                    self.namespace_state
                        .get(&ObjectRef::<Namespace>::new(namespace))
                        .is_some()
                } else {
                    false
                }
            })
            .flat_map(|pod| {
                trace!(message = "Providing log paths for pod.", pod = ?pod.metadata.name);
                let paths_iter = list_pod_log_paths(
                    real_glob,
                    self.pod_logs_glob_patterns.as_slice(),
                    pod.as_ref(),
                    self.extract_databricks_logs,
                    self.hostpath_logging_annotation_key.as_deref(),
                );
                filter_paths(
                    filter_paths(paths_iter, &self.include_paths, true),
                    &self.exclude_paths,
                    false,
                )
                // Add the pod metadata associated with the paths.
                .map(|path| {
                    let container_name =
                        extract_container_name_from_path(&path, self.extract_databricks_logs);

                    (
                        Some(LogFileInfo {
                            pod_namespace: pod
                                .metadata
                                .namespace
                                .clone()
                                .unwrap_or_default()
                                .to_string(),
                            pod_name: pod.metadata.name.clone().unwrap_or_default().to_string(),
                            pod_uid: pod.metadata.uid.clone().unwrap_or_default().to_string(),
                            container_name,
                        }),
                        path,
                    )
                })
                .collect::<Vec<_>>()
            })
            .collect()
    }
}

/// This function takes a `Pod` resource and returns the path to where the logs
/// for the said `Pod` are expected to be found.
///
/// In the common case, the effective path is built using the `namespace`,
/// `name` and `uid` of the Pod. However, there's a special case for
/// `Static Pod`s: they keep their logs at the path that consists of config
/// hashsum instead of the `Pod` `uid`. The reason for this is `kubelet` is
/// locally authoritative over those `Pod`s, and the API only has
/// `Monitor Pod`s - the "dummy" entries useful for discovery and association.
/// Their UIDs are generated at the Kubernetes API side, and do not represent
/// the actual config hashsum as one would expect.
///
/// To work around this, we use the mirror pod annotations (if any) to obtain
/// the effective config hashsum, see the `extract_static_pod_config_hashsum`
/// function that does this.
///
/// See <https://github.com/vectordotdev/vector/issues/6001>
/// See <https://github.com/kubernetes/kubernetes/blob/ef3337a443b402756c9f0bfb1f844b1b45ce289d/pkg/kubelet/pod/pod_manager.go#L30-L44>
/// See <https://github.com/kubernetes/kubernetes/blob/cea1d4e20b4a7886d8ff65f34c6d4f95efcb4742/pkg/kubelet/pod/mirror_client.go#L80-L81>
fn extract_pod_logs_directory(pod: &Pod) -> Option<PathBuf> {
    let metadata = &pod.metadata;
    let pod_name = metadata.name.as_deref().unwrap_or("<unknown>");

    let namespace = match metadata.namespace.as_ref() {
        Some(ns) => ns,
        None => {
            trace!(
                message = "Skipping pod: missing namespace metadata.",
                %pod_name,
            );
            return None;
        }
    };

    let name = match metadata.name.as_ref() {
        Some(n) => n,
        None => {
            trace!(message = "Skipping pod: missing name metadata.",);
            return None;
        }
    };

    let uid = if let Some(static_pod_config_hashsum) = extract_static_pod_config_hashsum(metadata) {
        // If there's a static pod config hashsum - use it instead of uid.
        static_pod_config_hashsum
    } else {
        // In the common case - just fallback to the real pod uid.
        match metadata.uid.as_ref() {
            Some(u) => u,
            None => {
                trace!(
                    message = "Skipping pod: missing uid metadata.",
                    %pod_name,
                    %namespace,
                );
                return None;
            }
        }
    };

    // Pods running inside microVMs (brickvisor runtime) have their kubelet pod
    // log tree rooted at /var/log/microvms instead of /var/log/pods. The dblet
    // runtime mode is surfaced via a pod label; default to /var/log/pods
    // when it is unset or set to anything other than "brickvisor".
    let use_microvms_path = metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(RUNTIME_MODE_LABEL_KEY))
        .is_some_and(|mode| mode == RUNTIME_MODE_BRICKVISOR);

    Some(build_pod_logs_directory(
        namespace,
        name,
        uid,
        use_microvms_path,
    ))
}

/// The annotation name for the Databricks hostPath logging override (internal logs).
const DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY: &str = "logging.databricks.com/dblet-logs-path";
/// The annotation name for the Databricks hostPath customer logs directory.
const DATABRICKS_HOSTPATH_CUSTOMER_LOGGING_ANNOTATION_KEY: &str =
    "logging.databricks.com/dblet-customer-logs-path";
const DATABRICKS_HOSTPATH_LOG_DIRECTORY_PREFIX: &str = "/databricks/host-root";

// Given a pod spec, produce the Databricks-specific logs directory.
// There are two modes by which the root logging directory for Databricks services is determined:
// 1. The hostPath logging annotation override is used in place of the kubelet log directory.
//    For pods that log to a hostPath volume, the hostPath logging annotation override is used in
//    place of the kubelet log directory. This is an explicit per-pod directory under which all
//    containers of the pod write their logs, each to a container-specific subdirectory.
// 2. The kubelet log directory is used.
//    For pods that log to a kubelet-managed volume, the emptyDir volume under the pod's UID is
//    used. This is the default behavior for pods.
/// The annotation key for the pod name (used when metadata.name includes node suffix).
const POD_NAME_ANNOTATION_KEY: &str = "dblet.dev/pod-name";

/// The label key describing the dblet runtime mode of the pod.
const RUNTIME_MODE_LABEL_KEY: &str = "dblet.dev/runtime-mode";
/// The runtime-mode label value indicating the pod runs inside a microVM,
/// whose kubelet pod log tree is rooted at `/var/log/microvms`.
const RUNTIME_MODE_BRICKVISOR: &str = "brickvisor";

fn extract_databricks_pod_logs_directory(
    pod: &Pod,
    use_hostpath_logging_annotation_override: bool,
) -> Option<PathBuf> {
    extract_databricks_pod_logs_directory_with_annotation(
        pod,
        use_hostpath_logging_annotation_override,
        DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY,
    )
}

/// Core implementation: resolves a Databricks pod logs directory from either the kubelet emptyDir
/// path or a hostPath annotation. The `annotation_key` parameter controls which annotation is read
/// when `use_hostpath_logging_annotation_override` is true.
fn extract_databricks_pod_logs_directory_with_annotation(
    pod: &Pod,
    use_hostpath_logging_annotation_override: bool,
    annotation_key: &str,
) -> Option<PathBuf> {
    // Allow the hostPath logging annotation override to be used in place of the kubelet log directory.
    let metadata = &pod.metadata;
    // Prefer the dblet.dev/pod-name annotation over metadata.name, as metadata.name may include
    // a node IP suffix in some environments (e.g., pod pools).
    let pod_name_from_annotation = metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(POD_NAME_ANNOTATION_KEY))
        .map(|s| s.as_str());
    let pod_name_from_metadata = metadata.name.as_deref();
    trace!(
        message = "Extracting pod name for Databricks logs.",
        pod_name_from_annotation = ?pod_name_from_annotation,
        pod_name_from_metadata = ?pod_name_from_metadata,
    );
    let pod_name = pod_name_from_annotation.or(pod_name_from_metadata);

    let uid = if let Some(static_pod_config_hashsum) = extract_static_pod_config_hashsum(metadata) {
        // If there's a static pod config hashsum - use it instead of uid.
        static_pod_config_hashsum
    } else {
        // In the common case - just fallback to the real pod uid.
        match metadata.uid.as_ref() {
            Some(u) => u,
            None => {
                trace!(
                    message = "Skipping pod: missing uid metadata for Databricks logs.",
                    pod_name = ?pod_name,
                );
                return None;
            }
        }
    };

    if use_hostpath_logging_annotation_override {
        // Use the hostPath logging annotation override to determine the Databricks logs directory.
        // If the annotation is not present, return None.
        let hostpath_logging_annotation: Option<&str> = metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(annotation_key).map(|value| value.as_str()));
        match hostpath_logging_annotation {
            Some(value) => {
                // If the annotation contains $POD_NAME but we don't have a pod name, skip this pod.
                let resolved_value = if value.contains("$POD_NAME") {
                    match pod_name {
                        Some(name) => value.replace("$POD_NAME", name),
                        None => {
                            trace!(
                                message = "Skipping pod: annotation contains $POD_NAME but pod name is unavailable.",
                                annotation_value = %value,
                            );
                            return None;
                        }
                    }
                } else {
                    value.to_string()
                };
                let resolved_path = PathBuf::from(format!(
                    "{}/{}",
                    DATABRICKS_HOSTPATH_LOG_DIRECTORY_PREFIX,
                    resolved_value.trim_start_matches('/')
                ));
                trace!(
                    message = "Resolved hostpath logging annotation for pod.",
                    pod_name = ?pod_name,
                    annotation_key = %annotation_key,
                    annotation_value = %value,
                    resolved_path = %resolved_path.display(),
                );
                Some(resolved_path)
            }
            None => {
                trace!(
                    message = "Skipping pod: missing hostpath logging annotation.",
                    pod_name = ?pod_name,
                    annotation_key = %annotation_key,
                );
                None
            }
        }
    } else {
        // Use the kubelet log directory to determine the Databricks logs directory.
        Some(build_databricks_k8s_pod_logs_directory(uid))
    }
}

const CONTAINER_EXCLUSION_ANNOTATION_KEY: &str = "vector.dev/exclude-containers";

fn extract_excluded_containers_for_pod(pod: &Pod) -> impl Iterator<Item = &str> {
    let metadata = &pod.metadata;
    metadata.annotations.iter().flat_map(|annotations| {
        annotations
            .iter()
            .filter_map(|(key, value)| {
                if key != CONTAINER_EXCLUSION_ANNOTATION_KEY {
                    return None;
                }
                Some(value)
            })
            .flat_map(|containers| containers.split(','))
            .map(|container| container.trim())
    })
}

fn build_container_exclusion_patterns<'a>(
    pod_logs_dir: &'a str,
    containers: impl Iterator<Item = &'a str> + 'a,
) -> impl Iterator<Item = glob::Pattern> + 'a {
    containers.filter_map(move |container| {
        let escaped_container_name = glob::Pattern::escape(container);
        glob::Pattern::new(&[pod_logs_dir, &escaped_container_name, "**"].join("/")).ok()
    })
}

const VALID_LOG_VOLUME_NAMES: &[&str] = &["logs", "data", "container-build", "event-logs"];
fn get_databricks_pod_logs_directories(
    pod: &Pod,
    empty_dir_pod_logs_directory: Option<PathBuf>,
    hostpath_logging_annotation_key: Option<&str>,
) -> Vec<PathBuf> {
    let mut log_dirs = Vec::new();
    if let Some(empty_dir_pod_logs_directory) = empty_dir_pod_logs_directory {
        // First, include the original pod logs directory (with no subdirectories) in the list of paths.
        log_dirs.push(empty_dir_pod_logs_directory.clone());
        // Then, include the direct subdirectories in the list of paths.
        let subdirectories = std::fs::read_dir(&empty_dir_pod_logs_directory);
        if let Ok(subdirectories) = subdirectories {
            log_dirs.extend(subdirectories.filter_map(|entry| {
                entry
                    .ok()
                    .and_then(|entry| {
                        VALID_LOG_VOLUME_NAMES
                            .contains(
                                &entry
                                    .path()
                                    .file_name()
                                    .unwrap_or_default()
                                    .to_str()
                                    .unwrap_or_default(),
                            )
                            .then_some(entry.path())
                    })
                    .and_then(|entry| entry.is_dir().then_some(entry))
            }));
        } else {
            trace!(
                message = "Failed to read subdirectories of emptyDir pod logs directory.",
                pod = ?pod.metadata.name,
                log_directory = ?empty_dir_pod_logs_directory.to_str(),
                error = subdirectories.err().map(|e| e.to_string()),
            );
        }
    }
    // If a hostpath annotation key is configured, resolve the annotation and include its directory.
    if let Some(annotation_key) = hostpath_logging_annotation_key {
        if let Some(hostpath_logs_directory) =
            extract_databricks_pod_logs_directory_with_annotation(pod, true, annotation_key)
        {
            log_dirs.push(hostpath_logs_directory);
        }
    }
    log_dirs
}

fn list_pod_log_paths<'a, G, GI>(
    mut glob_impl: G,
    pod_logs_glob_patterns: &'a [String],
    pod: &'a Pod,
    extract_databricks_logs: bool,
    hostpath_logging_annotation_key: Option<&str>,
) -> impl Iterator<Item = PathBuf> + 'a
where
    G: FnMut(&str) -> GI + 'a,
    GI: Iterator<Item = PathBuf> + 'a,
{
    // Extract log file paths from the pod logs directory of the logging empty-dir volume associated
    // with the pod.
    // If hostpath_logging_annotation_key is set, also extract log file paths from
    // the hostPath logging annotation and merge the two sets of paths.
    let log_dirs = if extract_databricks_logs {
        let empty_dir_pod_logs_directory = extract_databricks_pod_logs_directory(
            pod, /*use_hostpath_logging_annotation_override=*/ false,
        );
        get_databricks_pod_logs_directories(
            pod,
            empty_dir_pod_logs_directory,
            hostpath_logging_annotation_key,
        )
    } else {
        extract_pod_logs_directory(pod)
            .into_iter()
            .collect::<Vec<_>>()
    };
    log_dirs.into_iter().flat_map(move |dir| {
        let dir = dir
            .to_str()
            .expect("non-utf8 path to pod logs dir is not supported");

        let pod_name = pod.metadata.name.as_deref().unwrap_or("<unknown>");
        trace!(
            message = "Resolved pod logs directory.",
            %pod_name,
            pod_logs_directory = %dir,
        );

        // Build the full glob patterns for logging.
        let full_glob_patterns: Vec<String> = pod_logs_glob_patterns
            .iter()
            .map(|pattern| [dir, pattern].join("/"))
            .collect();

        trace!(
            message = "Applying glob patterns to pod logs directory.",
            %pod_name,
            glob_patterns = ?full_glob_patterns,
        );

        // Run the glob to get a list of unfiltered paths.
        let pod_logs_glob_patterns_globs = full_glob_patterns
            .iter()
            .map(|pattern| glob_impl(pattern))
            .collect::<Vec<_>>();

        // Combine the paths for the user-specified glob patterns.
        // Collect to a Vec so we can log the found paths.
        let found_paths: Vec<PathBuf> =
            pod_logs_glob_patterns_globs.into_iter().flatten().collect();

        trace!(
            message = "Files found by glob patterns.",
            %pod_name,
            pod_logs_directory = %dir,
            files_found = ?found_paths,
            count = found_paths.len(),
        );

        let path_iter = found_paths.into_iter();

        // Extract the containers to exclude, then build patterns from them
        // and cache the results into a Vec.
        let excluded_containers = extract_excluded_containers_for_pod(pod);
        let exclusion_patterns: Vec<_> =
            build_container_exclusion_patterns(dir, excluded_containers).collect();

        // Return paths filtered with container exclusion.
        filter_paths(path_iter, exclusion_patterns, false)
    })
}

fn real_glob(pattern: &str) -> impl Iterator<Item = PathBuf> + use<> {
    glob::glob_with(
        pattern,
        glob::MatchOptions {
            require_literal_separator: true,
            ..Default::default()
        },
    )
    .expect("the pattern is supposed to always be correct")
    .flat_map(|paths| paths.into_iter())
}

fn filter_paths<'a>(
    iter: impl Iterator<Item = PathBuf> + 'a,
    patterns: impl AsRef<[glob::Pattern]> + 'a,
    include: bool,
) -> impl Iterator<Item = PathBuf> + 'a {
    iter.filter(move |path| {
        let m = patterns.as_ref().iter().any(|pattern| {
            pattern.matches_path_with(
                path,
                glob::MatchOptions {
                    require_literal_separator: true,
                    ..Default::default()
                },
            )
        });
        if include { m } else { !m }
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use tempfile::TempDir;

    use k8s_openapi::{api::core::v1::Pod, apimachinery::pkg::apis::meta::v1::ObjectMeta};

    use super::{
        DATABRICKS_HOSTPATH_CUSTOMER_LOGGING_ANNOTATION_KEY,
        DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY, build_container_exclusion_patterns,
        extract_databricks_pod_logs_directory,
        extract_databricks_pod_logs_directory_with_annotation, extract_excluded_containers_for_pod,
        extract_pod_logs_directory, filter_paths, get_databricks_pod_logs_directories,
        list_pod_log_paths,
    };

    #[test]
    fn test_extract_pod_logs_directory() {
        let cases = vec![
            // Empty pod.
            (Pod::default(), None),
            // Happy path.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                Some("/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid"),
            ),
            // No uid.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                None,
            ),
            // No name.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                None,
            ),
            // No namespace.
            (
                Pod {
                    metadata: ObjectMeta {
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                None,
            ),
            // Static pod config hashsum as uid.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        annotations: Some(
                            vec![(
                                "kubernetes.io/config.mirror".to_owned(),
                                "sandbox0-config-hashsum".to_owned(),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                Some("/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-config-hashsum"),
            ),
            // brickvisor runtime mode -> /var/log/microvms.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        labels: Some(
                            vec![("dblet.dev/runtime-mode".to_owned(), "brickvisor".to_owned())]
                                .into_iter()
                                .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                Some("/var/log/microvms/sandbox0-ns_sandbox0-name_sandbox0-uid"),
            ),
            // Non-brickvisor runtime mode -> status quo /var/log/pods.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        labels: Some(
                            vec![("dblet.dev/runtime-mode".to_owned(), "default".to_owned())]
                                .into_iter()
                                .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                Some("/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid"),
            ),
        ];

        for (pod, expected) in cases {
            assert_eq!(
                extract_pod_logs_directory(&pod),
                expected.map(PathBuf::from)
            );
        }
    }

    #[test]
    fn test_extract_databricks_pod_logs_directory() {
        let cases = vec![
            // Empty pod.
            (Pod::default(), false, None),
            // Happy path.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                false,
                Some("/var/lib/kubelet/pods/sandbox0-uid/volumes/kubernetes.io~empty-dir"),
            ),
            // No uid.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                false,
                None,
            ),
            // Attempt to use the hostPath logging annotation override, but the annotation is not
            // present.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                true,
                None,
            ),
            // Pod annotation overrides uid-based emptyDir path..
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        annotations: Some(
                            vec![(
                                "logging.databricks.com/dblet-logs-path".to_owned(),
                                "/local_disk0/sandbox0-custom-logs-path/$POD_NAME".to_owned(),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                true,
                Some("/databricks/host-root/local_disk0/sandbox0-custom-logs-path/sandbox0-name"),
            ),
        ];

        for (pod, use_hostpath_logging_annotation, expected) in cases {
            assert_eq!(
                extract_databricks_pod_logs_directory(&pod, use_hostpath_logging_annotation),
                expected.map(PathBuf::from)
            );
        }
    }

    #[test]
    fn test_extract_databricks_pod_logs_directory_customer_annotation() {
        // No customer-logs annotation present -> None
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("test-pod".to_owned()),
                uid: Some("test-uid".to_owned()),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        };
        assert_eq!(
            extract_databricks_pod_logs_directory_with_annotation(
                &pod,
                true,
                DATABRICKS_HOSTPATH_CUSTOMER_LOGGING_ANNOTATION_KEY,
            ),
            None,
        );

        // Customer-logs annotation present -> resolved path
        let pod_with_annotation = Pod {
            metadata: ObjectMeta {
                name: Some("test-pod".to_owned()),
                uid: Some("test-uid".to_owned()),
                annotations: Some(
                    vec![(
                        "logging.databricks.com/dblet-customer-logs-path".to_owned(),
                        "/local_disk0/serverless-logs/customer/mosaic_test-pod".to_owned(),
                    )]
                    .into_iter()
                    .collect(),
                ),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        };
        assert_eq!(
            extract_databricks_pod_logs_directory_with_annotation(
                &pod_with_annotation,
                true,
                DATABRICKS_HOSTPATH_CUSTOMER_LOGGING_ANNOTATION_KEY,
            ),
            Some(PathBuf::from(
                "/databricks/host-root/local_disk0/serverless-logs/customer/mosaic_test-pod"
            )),
        );
    }

    #[test]
    fn test_extract_excluded_containers_for_pod() {
        let cases = vec![
            // No annotations.
            (Pod::default(), vec![]),
            // Empty annotations.
            (
                Pod {
                    metadata: ObjectMeta {
                        annotations: Some(vec![].into_iter().collect()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                vec![],
            ),
            // Irrelevant annotations.
            (
                Pod {
                    metadata: ObjectMeta {
                        annotations: Some(
                            vec![("some-other-annotation".to_owned(), "some value".to_owned())]
                                .into_iter()
                                .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                vec![],
            ),
            // Proper annotation without spaces.
            (
                Pod {
                    metadata: ObjectMeta {
                        annotations: Some(
                            vec![(
                                super::CONTAINER_EXCLUSION_ANNOTATION_KEY.to_owned(),
                                "container1,container4".to_owned(),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                vec!["container1", "container4"],
            ),
            // Proper annotation with spaces.
            (
                Pod {
                    metadata: ObjectMeta {
                        annotations: Some(
                            vec![(
                                super::CONTAINER_EXCLUSION_ANNOTATION_KEY.to_owned(),
                                "container1, container4".to_owned(),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                vec!["container1", "container4"],
            ),
        ];

        for (pod, expected) in cases {
            let actual: Vec<&str> = extract_excluded_containers_for_pod(&pod).collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_list_pod_log_paths() {
        let cases = vec![
            // Pod exists and has some containers that write logs, and some of
            // the containers are excluded.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        annotations: Some(
                            vec![(
                                super::CONTAINER_EXCLUSION_ANNOTATION_KEY.to_owned(),
                                "excluded1,excluded2".to_owned(),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                // Calls to the glob mock.
                vec![
                    (
                        // The pattern to expect at the mock.
                        "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/*/*.log*",
                        // The paths to return from the mock.
                        vec![
                            "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container1/qwe.log",
                            "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container2/qwe.log",
                            "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/excluded1/qwe.log",
                            "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container3/qwe.log",
                            "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/excluded2/qwe.log",
                        ],
                    ),
                    (
                        // The pattern to expect at the mock.
                        "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/*/*.json*",
                        // The paths to return from the mock.
                        vec![
                            "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container1/qwe.json",
                            "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/excluded1/qwe.json",
                        ],
                    ),
                    (
                        // The pattern to expect at the mock.
                        "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/*/*.pb.base64*",
                        // The paths to return from the mock.
                        vec![],
                    ),
                ],
                // Expected result.
                vec![
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container1/qwe.log",
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container2/qwe.log",
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container3/qwe.log",
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container1/qwe.json",
                ],
            ),
            // Pod doesn't have the metadata set.
            (Pod::default(), vec![], vec![]),
            // Pod has proper metadata, but doesn't have log files.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                vec![
                    (
                        "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/*/*.log*",
                        vec![],
                    ),
                    (
                        "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/*/*.json*",
                        vec![],
                    ),
                    (
                        "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/*/*.pb.base64*",
                        vec![],
                    ),
                ],
                vec![],
            ),
        ];

        for (pod, expected_calls, expected_paths) in cases {
            // Prepare the mock fn.
            let mut expected_calls = expected_calls.into_iter();
            let mock_glob = move |pattern: &str| {
                let (expected_pattern, paths_to_return) = expected_calls
                    .next()
                    .expect("implementation did a call that wasn't expected");

                assert_eq!(pattern, expected_pattern);
                paths_to_return.into_iter().map(PathBuf::from)
            };

            let pod_logs_glob_patterns: Vec<String> = vec![
                "*/*.log*".to_string(),
                "*/*.json*".to_string(),
                "*/*.pb.base64*".to_string(),
            ];
            let actual_paths: Vec<_> = list_pod_log_paths(
                mock_glob,
                pod_logs_glob_patterns.as_slice(),
                &pod,
                false,
                None,
            )
            .collect();
            let expected_paths: Vec<_> = expected_paths.into_iter().map(PathBuf::from).collect();
            assert_eq!(actual_paths, expected_paths)
        }
    }

    #[test]
    fn test_get_databricks_pod_logs_directories() {
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_path = temp_dir.path();
        let temp_logs_volume_path = temp_dir_path.join("logs");
        std::fs::create_dir_all(&temp_logs_volume_path).unwrap();
        let temp_data_volume_path = temp_dir_path.join("data");
        std::fs::create_dir_all(&temp_data_volume_path).unwrap();
        let temp_invalid_volume_path = temp_dir_path.join("invalid_test_volume");
        std::fs::create_dir_all(&temp_invalid_volume_path).unwrap();
        // Confirm that the function returns the correct directories for a pod with or without a hostpath logging annotation override.
        let cases = vec![
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        annotations: Some(
                            vec![(
                                super::DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY.to_owned(),
                                "/local_disk0/sandbox0-custom-logs-path".to_owned(),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                vec![PathBuf::from(
                    "/databricks/host-root/local_disk0/sandbox0-custom-logs-path",
                )],
                vec![
                    PathBuf::from("/databricks/host-root/local_disk0/sandbox0-custom-logs-path"),
                    PathBuf::from(temp_dir_path),
                    temp_data_volume_path.clone(),
                    temp_logs_volume_path.clone(),
                ],
            ),
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                vec![],
                vec![
                    PathBuf::from(temp_dir_path),
                    temp_data_volume_path.clone(),
                    temp_logs_volume_path.clone(),
                ],
            ),
        ];

        for (pod, expected_directories_no_empty_dir, expected_directories_with_empty_dir) in cases {
            let mut actual_directories_no_empty_dir = get_databricks_pod_logs_directories(
                &pod,
                None,
                Some(DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY),
            );
            actual_directories_no_empty_dir.sort();
            let mut expected_directories_no_empty_dir = expected_directories_no_empty_dir;
            expected_directories_no_empty_dir.sort();
            assert_eq!(
                actual_directories_no_empty_dir,
                expected_directories_no_empty_dir
            );
            let mut actual_directories_with_empty_dir = get_databricks_pod_logs_directories(
                &pod,
                Some(PathBuf::from(temp_dir_path)),
                Some(DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY),
            );
            actual_directories_with_empty_dir.sort();
            let mut expected_directories_with_empty_dir = expected_directories_with_empty_dir;
            expected_directories_with_empty_dir.sort();
            assert_eq!(
                actual_directories_with_empty_dir,
                expected_directories_with_empty_dir
            );
        }
    }

    #[test]
    fn test_list_databricks_pod_log_paths() {
        let cases = vec![
            // Pod exists and has some containers that write logs, and some of
            // the containers are excluded.
            (
                Pod {
                    metadata: ObjectMeta {
                        namespace: Some("sandbox0-ns".to_owned()),
                        name: Some("sandbox0-name".to_owned()),
                        uid: Some("sandbox0-uid".to_owned()),
                        annotations: Some(
                            vec![
                                (
                                    super::DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY.to_owned(),
                                    "/local_disk0/sandbox0-custom-logs-path".to_owned(),
                                ),
                                (
                                    super::CONTAINER_EXCLUSION_ANNOTATION_KEY.to_owned(),
                                    "excluded1,excluded2".to_owned(),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                        ..ObjectMeta::default()
                    },
                    ..Pod::default()
                },
                // Calls to the glob mock.
                vec![
                    // The first calls are to the base emptyDir directory.
                    (
                        // The pattern to expect at the mock.
                        "/var/lib/kubelet/pods/sandbox0-uid/volumes/kubernetes.io~empty-dir/*/*.log*",
                        // The paths to return from the mock. No paths returned as this test case
                        // simulates an unused emptyDir directory.
                        vec![],
                    ),
                    (
                        "/var/lib/kubelet/pods/sandbox0-uid/volumes/kubernetes.io~empty-dir/*/*.json*",
                        vec![],
                    ),
                    (
                        "/var/lib/kubelet/pods/sandbox0-uid/volumes/kubernetes.io~empty-dir/*/*.pb.base64*",
                        vec![],
                    ),
                    (
                        // The pattern to expect at the mock.
                        "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/*/*.log*",
                        // The paths to return from the mock.
                        vec![
                            "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container1/qwe.log",
                            "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container2/qwe.log",
                            "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/excluded1/qwe.log",
                            "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container3/qwe.log",
                            "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/excluded2/qwe.log",
                        ],
                    ),
                    (
                        // The pattern to expect at the mock.
                        "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/*/*.json*",
                        // The paths to return from the mock.
                        vec![
                            "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container1/qwe.json",
                            "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/excluded1/qwe.json",
                        ],
                    ),
                    (
                        // The pattern to expect at the mock.
                        "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/*/*.pb.base64*",
                        // The paths to return from the mock.
                        vec![],
                    ),
                ],
                // Expected result.
                vec![
                    "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container1/qwe.log",
                    "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container2/qwe.log",
                    "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container3/qwe.log",
                    "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/container1/qwe.json",
                ],
            ),
        ];

        for (pod, expected_calls, expected_paths) in cases {
            // Prepare the mock fn.
            let mut expected_calls = expected_calls.into_iter();
            let mock_glob = move |pattern: &str| {
                let (expected_pattern, paths_to_return) = expected_calls
                    .next()
                    .expect("implementation did a call that wasn't expected");

                assert_eq!(pattern, expected_pattern);
                paths_to_return.into_iter().map(PathBuf::from)
            };

            let pod_logs_glob_patterns: Vec<String> = vec![
                "*/*.log*".to_string(),
                "*/*.json*".to_string(),
                "*/*.pb.base64*".to_string(),
            ];
            let actual_paths: Vec<_> = list_pod_log_paths(
                mock_glob,
                pod_logs_glob_patterns.as_slice(),
                &pod,
                true,
                Some(DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY),
            )
            .collect();
            let expected_paths: Vec<_> = expected_paths.into_iter().map(PathBuf::from).collect();
            assert_eq!(actual_paths, expected_paths)
        }
    }

    #[test]
    fn test_exclude_paths() {
        let cases = vec![
            // No exclusion pattern allows everything.
            (
                vec![
                    "/var/log/pods/a.log",
                    "/var/log/pods/b.log",
                    "/var/log/pods/c.log.foo",
                    "/var/log/pods/d.logbar",
                ],
                vec![],
                vec![
                    "/var/log/pods/a.log",
                    "/var/log/pods/b.log",
                    "/var/log/pods/c.log.foo",
                    "/var/log/pods/d.logbar",
                ],
            ),
            // Test a filter that doesn't apply to anything.
            (
                vec!["/var/log/pods/a.log", "/var/log/pods/b.log"],
                vec!["notmatched"],
                vec!["/var/log/pods/a.log", "/var/log/pods/b.log"],
            ),
            // Multiple filters.
            (
                vec![
                    "/var/log/pods/a.log",
                    "/var/log/pods/b.log",
                    "/var/log/pods/c.log",
                ],
                vec!["notmatched", "**/b.log", "**/c.log"],
                vec!["/var/log/pods/a.log"],
            ),
            // Requires literal path separator (`*` does not include dirs).
            (
                vec![
                    "/var/log/pods/a.log",
                    "/var/log/pods/b.log",
                    "/var/log/pods/c.log",
                ],
                vec!["*/b.log", "**/c.log"],
                vec!["/var/log/pods/a.log", "/var/log/pods/b.log"],
            ),
            // Filtering by container name with a real-life-like file path.
            (
                vec![
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container1/1.log",
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container1/2.log",
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container2/1.log",
                ],
                vec!["**/container1/**"],
                vec!["/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container2/1.log"],
            ),
        ];

        for (input_paths, str_patterns, expected_paths) in cases {
            let patterns: Vec<_> = str_patterns
                .iter()
                .map(|pattern| glob::Pattern::new(pattern).unwrap())
                .collect();
            let actual_paths: Vec<_> =
                filter_paths(input_paths.into_iter().map(Into::into), &patterns, false).collect();
            let expected_paths: Vec<_> = expected_paths.into_iter().map(PathBuf::from).collect();
            assert_eq!(
                actual_paths, expected_paths,
                "failed for patterns {:?}",
                &str_patterns
            )
        }
    }

    #[test]
    fn test_include_paths() {
        let cases = vec![
            (
                vec![
                    "/var/log/pods/a.log",
                    "/var/log/pods/b.log",
                    "/var/log/pods/c.log.foo",
                    "/var/log/pods/d.logbar",
                    "/tmp/foo",
                ],
                vec!["/var/log/pods/*"],
                vec![
                    "/var/log/pods/a.log",
                    "/var/log/pods/b.log",
                    "/var/log/pods/c.log.foo",
                    "/var/log/pods/d.logbar",
                ],
            ),
            (
                vec![
                    "/var/log/pods/a.log",
                    "/var/log/pods/b.log",
                    "/var/log/pods/c.log.foo",
                    "/var/log/pods/d.logbar",
                ],
                vec!["/tmp/*"],
                vec![],
            ),
            (
                vec!["/var/log/pods/a.log", "/tmp/foo"],
                vec!["**/*"],
                vec!["/var/log/pods/a.log", "/tmp/foo"],
            ),
        ];

        for (input_paths, str_patterns, expected_paths) in cases {
            let patterns: Vec<_> = str_patterns
                .iter()
                .map(|pattern| glob::Pattern::new(pattern).unwrap())
                .collect();
            let actual_paths: Vec<_> =
                filter_paths(input_paths.into_iter().map(Into::into), &patterns, true).collect();
            let expected_paths: Vec<_> = expected_paths.into_iter().map(PathBuf::from).collect();
            assert_eq!(
                actual_paths, expected_paths,
                "failed for patterns {:?}",
                &str_patterns
            )
        }
    }

    #[test]
    fn test_build_container_exclusion_patterns() {
        let cases = vec![
            // No excluded containers - no exclusion patterns.
            (
                "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid",
                vec![],
                vec![],
            ),
            // Ensure the paths are concatenated correctly and look good.
            (
                "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid",
                vec!["container1", "container2"],
                vec![
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container1/**",
                    "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/container2/**",
                ],
            ),
            // Ensure control characters are escaped properly.
            (
                "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid",
                vec!["*[]"],
                vec!["/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid/[*][[][]]/**"],
            ),
        ];

        for (pod_logs_dir, containers, expected_patterns) in cases {
            let actual_patterns: Vec<_> =
                build_container_exclusion_patterns(pod_logs_dir, containers.clone().into_iter())
                    .collect();
            let expected_patterns: Vec<_> = expected_patterns
                .into_iter()
                .map(|pattern| glob::Pattern::new(pattern).unwrap())
                .collect();
            assert_eq!(
                actual_patterns, expected_patterns,
                "failed for dir {:?} and containers {:?}",
                &pod_logs_dir, &containers,
            )
        }
    }

    #[test]
    fn test_container_name_extraction_from_path() {
        // Verify that container_name is correctly extracted from the path
        // Path format: .../container-name/0.log
        let test_cases = vec![
            (
                "/var/log/pods/ns_name_uid/my-container/0.log",
                "my-container",
            ),
            (
                "/var/log/pods/ns_name_uid/another-container/1.log.gz",
                "another-container",
            ),
            (
                "/databricks/host-root/local_disk0/logs/service-container/app.log",
                "service-container",
            ),
        ];

        for (path_str, expected_container) in test_cases {
            let path = PathBuf::from(path_str);
            let container_name = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|name| name.to_str())
                .unwrap_or("");
            assert_eq!(
                container_name, expected_container,
                "Failed for path: {}",
                path_str
            );
        }
    }

    #[test]
    fn test_container_name_conditional_extraction() {
        use super::{DEFAULT_CONTAINER_NAME, extract_container_name_from_path};

        let path = PathBuf::from("/var/log/pods/ns_name_uid/my-container/0.log");

        // When extract_databricks_logs is false, extract from path
        let container_name = extract_container_name_from_path(&path, false);
        assert_eq!(container_name, "my-container");

        // When extract_databricks_logs is true, use default
        let container_name = extract_container_name_from_path(&path, true);
        assert_eq!(container_name, DEFAULT_CONTAINER_NAME);
    }
}
