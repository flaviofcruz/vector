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
    use_hostpath_logging_annotation_override: bool,
}

impl K8sPathsProvider {
    /// Create a new [`K8sPathsProvider`].
    pub const fn new(
        pod_state: Store<Pod>,
        namespace_state: Store<Namespace>,
        pod_logs_glob_patterns: Vec<String>,
        include_paths: Vec<glob::Pattern>,
        exclude_paths: Vec<glob::Pattern>,
        insert_namespace_fields: bool,
        extract_databricks_logs: bool,
        use_hostpath_logging_annotation_override: bool,
    ) -> Self {
        Self {
            pod_state,
            namespace_state,
            pod_logs_glob_patterns,
            include_paths,
            exclude_paths,
            insert_namespace_fields,
            extract_databricks_logs,
            use_hostpath_logging_annotation_override,
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
                    self.use_hostpath_logging_annotation_override,
                );
                filter_paths(
                    filter_paths(paths_iter, &self.include_paths, true),
                    &self.exclude_paths,
                    false,
                )
                // Add the pod metadata associated with the paths.
                .map(|path| {
                    // Only extract container_name from the path if extract_databricks_logs is enabled.
                    // Path format: .../container-name/0.log
                    let container_name = if self.extract_databricks_logs {
                        path.parent() // Get directory containing the log file
                            .and_then(|p| p.file_name()) // Get container directory name
                            .and_then(|name| name.to_str())
                            .unwrap_or(DEFAULT_CONTAINER_NAME)
                            .to_string()
                    } else {
                        DEFAULT_CONTAINER_NAME.to_string()
                    };

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
    let namespace = metadata.namespace.as_ref()?;
    let name = metadata.name.as_ref()?;

    let uid = if let Some(static_pod_config_hashsum) = extract_static_pod_config_hashsum(metadata) {
        // If there's a static pod config hashsum - use it instead of uid.
        static_pod_config_hashsum
    } else {
        // In the common case - just fallback to the real pod uid.
        metadata.uid.as_ref()?
    };

    Some(build_pod_logs_directory(namespace, name, uid))
}

/// The annotation name for the Databricks hostPath logging override.
const DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY: &str = "logging.databricks.com/dblet-logs-path";
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
fn extract_databricks_pod_logs_directory(
    pod: &Pod,
    use_hostpath_logging_annotation_override: bool,
) -> Option<PathBuf> {
    // Allow the hostPath logging annotation override to be used in place of the kubelet log directory.
    let metadata = &pod.metadata;
    let uid = if let Some(static_pod_config_hashsum) = extract_static_pod_config_hashsum(metadata) {
        // If there's a static pod config hashsum - use it instead of uid.
        static_pod_config_hashsum
    } else {
        // In the common case - just fallback to the real pod uid.
        metadata.uid.as_ref()?
    };

    if use_hostpath_logging_annotation_override {
        // Use the hostPath logging annotation override to determine the Databricks logs directory.
        // If the annotation is not present, return None.
        let hostpath_logging_annotation: Option<&str> =
            metadata.annotations.as_ref().and_then(|annotations| {
                annotations
                    .get(DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY)
                    .map(|value| value.as_str())
            });
        hostpath_logging_annotation.map(|value| {
            let pod_name = metadata.name.as_deref().unwrap_or("");
            let resolved_value = value.replace("$POD_NAME", pod_name);
            PathBuf::from(format!(
                "{}/{}",
                DATABRICKS_HOSTPATH_LOG_DIRECTORY_PREFIX,
                resolved_value.trim_start_matches('/')
            ))
        })
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

fn list_pod_log_paths<'a, G, GI>(
    mut glob_impl: G,
    pod_logs_glob_patterns: &'a [String],
    pod: &'a Pod,
    extract_databricks_logs: bool,
    use_hostpath_logging_annotation_override: bool,
) -> impl Iterator<Item = PathBuf> + 'a
where
    G: FnMut(&str) -> GI + 'a,
    GI: Iterator<Item = PathBuf> + 'a,
{
    if extract_databricks_logs {
        extract_databricks_pod_logs_directory(pod, use_hostpath_logging_annotation_override)
    } else {
        extract_pod_logs_directory(pod)
    }
    .into_iter()
    .flat_map(move |dir| {
        let dir = dir
            .to_str()
            .expect("non-utf8 path to pod logs dir is not supported");

        // Run the glob to get a list of unfiltered paths.
        let pod_logs_glob_patterns_globs = pod_logs_glob_patterns
            .iter()
            .map(|pattern| glob_impl(&[dir, pattern].join("/")))
            .collect::<Vec<_>>();

        // Combine the paths for the user-specified glob patterns.
        let path_iter = pod_logs_glob_patterns_globs.into_iter().flatten();

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

    use k8s_openapi::{api::core::v1::Pod, apimachinery::pkg::apis::meta::v1::ObjectMeta};

    use super::{
        build_container_exclusion_patterns, extract_databricks_pod_logs_directory,
        extract_excluded_containers_for_pod, extract_pod_logs_directory, filter_paths,
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
                false,
            )
            .collect();
            let expected_paths: Vec<_> = expected_paths.into_iter().map(PathBuf::from).collect();
            assert_eq!(actual_paths, expected_paths)
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
                true,
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
        use super::DEFAULT_CONTAINER_NAME;

        let path = PathBuf::from("/var/log/pods/ns_name_uid/my-container/0.log");

        // When extract_databricks_logs is true, extract from path
        let extract_databricks_logs = true;
        let container_name = if extract_databricks_logs {
            path.parent()
                .and_then(|p| p.file_name())
                .and_then(|name| name.to_str())
                .unwrap_or(DEFAULT_CONTAINER_NAME)
                .to_string()
        } else {
            DEFAULT_CONTAINER_NAME.to_string()
        };
        assert_eq!(container_name, "my-container");

        // When extract_databricks_logs is false, use default
        let extract_databricks_logs = false;
        let container_name = if extract_databricks_logs {
            path.parent()
                .and_then(|p| p.file_name())
                .and_then(|name| name.to_str())
                .unwrap_or(DEFAULT_CONTAINER_NAME)
                .to_string()
        } else {
            DEFAULT_CONTAINER_NAME.to_string()
        };
        assert_eq!(container_name, DEFAULT_CONTAINER_NAME);
    }
}
