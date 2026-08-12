//! A paths provider for k8s logs.

#![deny(missing_docs)]

use std::{collections::BTreeMap, path::PathBuf};

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
    annotation_selector: AnnotationSelector,
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

fn service_system_from_pod(pod: &Pod) -> Option<String> {
    if let Some(system) = pod
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("system"))
    {
        return (!system.is_empty()).then(|| system.clone());
    }

    pod.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("databricks/system_uri"))
        .and_then(|system_uri| system_uri.strip_prefix("system:"))
        .filter(|system| !system.is_empty())
        .map(str::to_string)
}

impl K8sPathsProvider {
    /// Create a new [`K8sPathsProvider`].
    pub fn new(
        pod_state: Store<Pod>,
        namespace_state: Store<Namespace>,
        annotation_selector: AnnotationSelector,
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
            annotation_selector,
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
            .filter(|pod| self.annotation_selector.matches(pod.metadata.annotations.as_ref()))
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
                            service_system: service_system_from_pod(&pod),
                        }),
                        path,
                    )
                })
                .collect::<Vec<_>>()
            })
            .collect()
    }
}

/// A local selector for matching Pod annotations.
///
/// Kubernetes does not support server-side annotation selectors. This mirrors the useful subset of
/// Kubernetes label selector syntax and is applied client-side while deriving log paths from watched
/// Pods.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AnnotationSelector {
    requirements: Vec<AnnotationSelectorRequirement>,
}

impl AnnotationSelector {
    /// Parse a comma-separated annotation selector.
    pub fn parse(selector: &str) -> crate::Result<Self> {
        let selector = selector.trim();
        if selector.is_empty() {
            return Ok(Self::default());
        }

        let requirements = split_selector_requirements(selector)
            .into_iter()
            .map(AnnotationSelectorRequirement::parse)
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self { requirements })
    }

    fn matches(&self, annotations: Option<&BTreeMap<String, String>>) -> bool {
        self.requirements
            .iter()
            .all(|requirement| requirement.matches(annotations))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AnnotationSelectorRequirement {
    Exists(String),
    DoesNotExist(String),
    Equals(String, String),
    NotEquals(String, String),
    In(String, Vec<String>),
    NotIn(String, Vec<String>),
}

impl AnnotationSelectorRequirement {
    fn parse(requirement: &str) -> crate::Result<Self> {
        let requirement = requirement.trim();
        if requirement.is_empty() {
            return Err("annotation selector contains an empty requirement".into());
        }

        if let Some((key, values)) = parse_set_requirement(requirement, " notin ")? {
            return Ok(Self::NotIn(key, values));
        }
        if let Some((key, values)) = parse_set_requirement(requirement, " in ")? {
            return Ok(Self::In(key, values));
        }
        if let Some((key, value)) = requirement.split_once("!=") {
            return Ok(Self::NotEquals(
                parse_selector_key(key)?,
                parse_selector_value(value)?,
            ));
        }
        if let Some((key, value)) = requirement.split_once("==") {
            return Ok(Self::Equals(
                parse_selector_key(key)?,
                parse_selector_value(value)?,
            ));
        }
        if let Some((key, value)) = requirement.split_once('=') {
            return Ok(Self::Equals(
                parse_selector_key(key)?,
                parse_selector_value(value)?,
            ));
        }
        if let Some(key) = requirement.strip_prefix('!') {
            return Ok(Self::DoesNotExist(parse_selector_key(key)?));
        }

        Ok(Self::Exists(parse_selector_key(requirement)?))
    }

    fn matches(&self, annotations: Option<&BTreeMap<String, String>>) -> bool {
        let annotation_value = |key: &str| annotations.and_then(|annotations| annotations.get(key));

        match self {
            Self::Exists(key) => annotation_value(key).is_some(),
            Self::DoesNotExist(key) => annotation_value(key).is_none(),
            Self::Equals(key, expected) => annotation_value(key) == Some(expected),
            Self::NotEquals(key, expected) => annotation_value(key) != Some(expected),
            Self::In(key, expected_values) => annotation_value(key)
                .is_some_and(|actual| expected_values.iter().any(|expected| expected == actual)),
            Self::NotIn(key, expected_values) => match annotation_value(key) {
                Some(actual) => expected_values.iter().all(|expected| expected != actual),
                None => true,
            },
        }
    }
}

fn split_selector_requirements(selector: &str) -> Vec<&str> {
    let mut requirements = Vec::new();
    let mut start = 0;
    let mut paren_depth = 0_usize;

    for (idx, ch) in selector.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            ',' if paren_depth == 0 => {
                requirements.push(&selector[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    requirements.push(&selector[start..]);
    requirements
}

fn parse_set_requirement(
    requirement: &str,
    operator: &'static str,
) -> crate::Result<Option<(String, Vec<String>)>> {
    let Some((key, values)) = requirement.split_once(operator) else {
        return Ok(None);
    };

    let values = values.trim();
    if !values.starts_with('(') || !values.ends_with(')') {
        return Err(
            format!("annotation selector set requirement must use parentheses: {requirement}")
                .into(),
        );
    }

    let values = values[1..values.len() - 1]
        .split(',')
        .map(parse_selector_value)
        .collect::<crate::Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(
            format!("annotation selector set requirement has no values: {requirement}").into(),
        );
    }

    Ok(Some((parse_selector_key(key)?, values)))
}

fn parse_selector_key(key: &str) -> crate::Result<String> {
    let key = key.trim();
    if key.is_empty() {
        return Err("annotation selector requirement has an empty key".into());
    }
    Ok(key.to_string())
}

fn parse_selector_value(value: &str) -> crate::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("annotation selector requirement has an empty value".into());
    }
    Ok(value.to_string())
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

/// Resolves the pod name, preferring the `dblet.dev/pod-name` annotation over `metadata.name`,
/// which may carry a node IP suffix in some environments (e.g. pod pools).
fn resolve_pod_name(pod: &Pod) -> Option<&str> {
    let metadata = &pod.metadata;
    metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(POD_NAME_ANNOTATION_KEY))
        .map(|s| s.as_str())
        .or(metadata.name.as_deref())
}

/// Resolves the always-scraped kubelet (emptyDir) pod logs directory from the pod's
/// static-pod-config hashsum, or its uid. `None` if the pod has neither.
fn extract_databricks_pod_logs_directory(pod: &Pod) -> Option<PathBuf> {
    let metadata = &pod.metadata;
    let pod_name = resolve_pod_name(pod);

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

    Some(build_databricks_k8s_pod_logs_directory(uid))
}

/// Resolves a hostPath pod logs directory from `annotation_key`. `None` if the annotation is
/// absent (the silent opt-out for pods that don't log via hostPath). A `$POD_NAME` token in the
/// value is substituted with the pod name (preferring `dblet.dev/pod-name` over `metadata.name`).
fn extract_hostpath_logging_annotation_directory(
    pod: &Pod,
    annotation_key: &str,
) -> Option<PathBuf> {
    let metadata = &pod.metadata;
    let pod_name = resolve_pod_name(pod);

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

/// The emptyDir volume names a pod may write logs to. Scan roots are built by joining these
/// onto the pod's emptyDir base rather than by enumerating the base, so a volume the pod does
/// not mount simply has no scan root.
const VALID_LOG_VOLUME_NAMES: &[&str] = &["logs", "data", "container-build", "event-logs"];
fn get_databricks_pod_logs_directories(
    pod: &Pod,
    empty_dir_pod_logs_directory: Option<PathBuf>,
    hostpath_logging_annotation_key: Option<&str>,
) -> Vec<PathBuf> {
    let mut log_dirs = Vec::new();
    if let Some(empty_dir_pod_logs_directory) = empty_dir_pod_logs_directory {
        // Only the named volume directories are scan roots; the emptyDir base itself is not.
        // Configured glob patterns are volume-relative (e.g. `*/access.log*` for the file
        // `logs/<dir>/access.log`), so globbing the base too would resolve each pattern one
        // segment short of its intended volume and match unintended files.
        log_dirs.extend(
            VALID_LOG_VOLUME_NAMES
                .iter()
                .map(|volume| empty_dir_pod_logs_directory.join(volume))
                .filter(|dir| dir.is_dir()),
        );
    }
    // If a hostpath annotation key is configured, resolve the annotation and include its directory.
    if let Some(annotation_key) = hostpath_logging_annotation_key {
        if let Some(hostpath_logs_directory) =
            extract_hostpath_logging_annotation_directory(pod, annotation_key)
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
        let empty_dir_pod_logs_directory = extract_databricks_pod_logs_directory(pod);
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
    use std::{collections::BTreeMap, path::PathBuf};
    use tempfile::TempDir;

    use k8s_openapi::{api::core::v1::Pod, apimachinery::pkg::apis::meta::v1::ObjectMeta};

    use super::{
        AnnotationSelector,
        DATABRICKS_HOSTPATH_CUSTOMER_LOGGING_ANNOTATION_KEY,
        DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY, build_container_exclusion_patterns,
        extract_databricks_pod_logs_directory, extract_excluded_containers_for_pod,
        extract_hostpath_logging_annotation_directory, extract_pod_logs_directory, filter_paths,
        get_databricks_pod_logs_directories, list_pod_log_paths, service_system_from_pod,
    };

    fn annotations(entries: Vec<(&str, &str)>) -> BTreeMap<String, String> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn service_system_prefers_pod_label() {
        let pod = Pod {
            metadata: ObjectMeta {
                labels: Some(annotations(vec![("system", "label-system")])),
                annotations: Some(annotations(vec![(
                    "databricks/system_uri",
                    "system:annotation-system",
                )])),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        };

        assert_eq!(
            service_system_from_pod(&pod).as_deref(),
            Some("label-system")
        );
    }

    #[test]
    fn service_system_falls_back_to_system_uri_annotation() {
        let pod = Pod {
            metadata: ObjectMeta {
                annotations: Some(annotations(vec![(
                    "databricks/system_uri",
                    "system:annotation-system",
                )])),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        };

        assert_eq!(
            service_system_from_pod(&pod).as_deref(),
            Some("annotation-system")
        );
    }

    #[test]
    fn test_annotation_selector_matches_required_annotation() {
        let selector =
            AnnotationSelector::parse("logDaemonDockerLoggingGroup=docker-common-log-group")
                .unwrap();

        assert!(selector.matches(Some(&annotations(vec![(
            "logDaemonDockerLoggingGroup",
            "docker-common-log-group",
        )]))));
        assert!(!selector.matches(Some(&annotations(vec![(
            "logDaemonDockerLoggingGroup",
            "other-log-group",
        )]))));
        assert!(!selector.matches(None));
    }

    #[test]
    fn test_annotation_selector_supports_label_selector_operators() {
        let selector = AnnotationSelector::parse(
            "group in (docker-common-log-group,system),env!=dev,present,!missing",
        )
        .unwrap();

        assert!(selector.matches(Some(&annotations(vec![
            ("group", "docker-common-log-group"),
            ("env", "prod"),
            ("present", ""),
        ]))));
        assert!(!selector.matches(Some(&annotations(vec![
            ("group", "debug"),
            ("env", "prod"),
            ("present", ""),
        ]))));
        assert!(!selector.matches(Some(&annotations(vec![
            ("group", "system"),
            ("env", "dev"),
            ("present", ""),
        ]))));
    }

    #[test]
    fn test_annotation_selector_rejects_invalid_requirements() {
        assert!(AnnotationSelector::parse("=value").is_err());
        assert!(AnnotationSelector::parse("key=").is_err());
        assert!(AnnotationSelector::parse("key in value").is_err());
        assert!(AnnotationSelector::parse("key,").is_err());
    }

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
            // Empty pod: no uid -> None.
            (Pod::default(), None),
            // Happy path: uid present -> kubelet emptyDir path.
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
                None,
            ),
        ];

        for (pod, expected) in cases {
            assert_eq!(
                extract_databricks_pod_logs_directory(&pod),
                expected.map(PathBuf::from)
            );
        }
    }

    #[test]
    fn test_extract_hostpath_logging_annotation_directory() {
        // Annotation missing -> None (silent opt-out).
        let pod_without_annotation = Pod {
            metadata: ObjectMeta {
                namespace: Some("sandbox0-ns".to_owned()),
                name: Some("sandbox0-name".to_owned()),
                uid: Some("sandbox0-uid".to_owned()),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        };
        assert_eq!(
            extract_hostpath_logging_annotation_directory(
                &pod_without_annotation,
                DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY,
            ),
            None,
        );

        // Happy path: annotation with $POD_NAME substitution.
        let pod_with_annotation = Pod {
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
        };
        assert_eq!(
            extract_hostpath_logging_annotation_directory(
                &pod_with_annotation,
                DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY,
            ),
            Some(PathBuf::from(
                "/databricks/host-root/local_disk0/sandbox0-custom-logs-path/sandbox0-name"
            )),
        );
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
            extract_hostpath_logging_annotation_directory(
                &pod,
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
            extract_hostpath_logging_annotation_directory(
                &pod_with_annotation,
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
        // `container-build` and `event-logs` are deliberately not created: a whitelisted volume the
        // pod does not mount must not become a scan root.
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
                vec![temp_data_volume_path.clone(), temp_logs_volume_path.clone()],
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
                // Calls to the glob mock. The pod's emptyDir volume directories do not exist on
                // this test host, so no emptyDir scan root is produced and the only globs issued
                // are for the hostPath annotation directory.
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
