//! Simple helpers for building and parsing k8s paths.
//!
//! Loosely based on <https://github.com/kubernetes/kubernetes/blob/31305966789525fca49ec26c289e565467d1f1c4/pkg/kubelet/kuberuntime/helpers.go>.

#![deny(missing_docs)]

use std::path::PathBuf;
use vector_lib::file_source::paths_provider::LogFileInfo;

/// The root directory for pod logs.
const K8S_LOGS_DIR: &str = "/var/log/pods";
/// The root directory for pod logs on instances that run pods inside microVMs
/// (brickvisor runtime). The directory tree under it mirrors [`K8S_LOGS_DIR`].
const MICROVMS_LOGS_DIR: &str = "/var/log/microvms";
const DATABRICKS_K8S_LOGS_DIR: &str = "/var/lib/kubelet/pods";
const DATABRICKS_K8S_LOGS_DIR_SUFFIX: &str = "volumes/kubernetes.io~empty-dir";

/// The delimiter used in the log path.
const LOG_PATH_DELIMITER: &str = "_";

/// Builds absolute log directory path for a pod sandbox.
///
/// When `use_microvms_path` is true the path is rooted at [`MICROVMS_LOGS_DIR`]
/// instead of [`K8S_LOGS_DIR`]; the rest of the path layout is identical.
///
/// Based on <https://github.com/kubernetes/kubernetes/blob/31305966789525fca49ec26c289e565467d1f1c4/pkg/kubelet/kuberuntime/helpers.go#L178>
pub(super) fn build_pod_logs_directory(
    pod_namespace: &str,
    pod_name: &str,
    pod_uid: &str,
    use_microvms_path: bool,
) -> PathBuf {
    let root = if use_microvms_path {
        MICROVMS_LOGS_DIR
    } else {
        K8S_LOGS_DIR
    };
    [
        root,
        &[pod_namespace, pod_name, pod_uid].join(LOG_PATH_DELIMITER),
    ]
    .join("/")
    .into()
}

pub(super) fn build_databricks_k8s_pod_logs_directory(pod_uid: &str) -> PathBuf {
    [
        DATABRICKS_K8S_LOGS_DIR,
        pod_uid,
        DATABRICKS_K8S_LOGS_DIR_SUFFIX,
    ]
    .join("/")
    .into()
}

/// Parses pod log file path and returns the log file info.
///
/// Assumes the input is a valid pod log file name.
///
/// Inspired by <https://github.com/kubernetes/kubernetes/blob/31305966789525fca49ec26c289e565467d1f1c4/pkg/kubelet/kuberuntime/helpers.go#L186>
pub(super) fn parse_log_file_path(path: &str) -> Option<LogFileInfo> {
    let mut components = path.rsplit(std::path::MAIN_SEPARATOR);

    let _log_file_name = components.next()?;
    let container_name = components.next()?;
    let pod_dir = components.next()?;

    let mut pod_dir_components = pod_dir.rsplit(LOG_PATH_DELIMITER);

    let pod_uid = pod_dir_components.next()?;
    let pod_name = pod_dir_components.next()?;
    let pod_namespace = pod_dir_components.next()?;

    Some(LogFileInfo {
        pod_namespace: pod_namespace.to_string(),
        pod_name: pod_name.to_string(),
        pod_uid: pod_uid.to_string(),
        container_name: container_name.to_string(),
        service_system: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_pod_logs_directory() {
        let cases = vec![
            // Valid inputs, default (/var/log/pods) root.
            (
                ("sandbox0-ns", "sandbox0-name", "sandbox0-uid", false),
                "/var/log/pods/sandbox0-ns_sandbox0-name_sandbox0-uid",
            ),
            // Valid inputs, microVM (/var/log/microvms) root.
            (
                ("sandbox0-ns", "sandbox0-name", "sandbox0-uid", true),
                "/var/log/microvms/sandbox0-ns_sandbox0-name_sandbox0-uid",
            ),
            // Invalid inputs.
            (("", "", "", false), "/var/log/pods/__"),
            (("", "", "", true), "/var/log/microvms/__"),
        ];

        for ((in_namespace, in_name, in_uid, use_microvms_path), expected) in cases.into_iter() {
            assert_eq!(
                build_pod_logs_directory(in_namespace, in_name, in_uid, use_microvms_path),
                PathBuf::from(expected)
            );
        }
    }

    #[test]
    fn test_build_databricks_k8s_pod_logs_directory() {
        let cases = vec![
            // Valid inputs.
            (
                "uid-1",
                "/var/lib/kubelet/pods/uid-1/volumes/kubernetes.io~empty-dir",
            ),
            // Invalid inputs.
            (
                "uid-2",
                "/var/lib/kubelet/pods/uid-2/volumes/kubernetes.io~empty-dir",
            ),
        ];

        for (in_uid, expected) in cases.into_iter() {
            assert_eq!(
                build_databricks_k8s_pod_logs_directory(in_uid),
                PathBuf::from(expected)
            );
        }
    }

    #[test]
    fn test_parse_log_file_path() {
        let path = format!(
            "{}{}",
            std::path::MAIN_SEPARATOR,
            [
                "var",
                "log",
                "pods",
                "sandbox0-ns_sandbox0-name_sandbox0-uid",
                "sandbox0-container0-name",
                "1.log",
            ]
            .iter()
            .collect::<PathBuf>()
            .into_os_string()
            .into_string()
            .unwrap()
        );
        let s_path = path.as_str();
        let cases = vec![
            // Valid inputs.
            (
                s_path,
                Some(LogFileInfo {
                    pod_namespace: "sandbox0-ns".to_string(),
                    pod_name: "sandbox0-name".to_string(),
                    pod_uid: "sandbox0-uid".to_string(),
                    container_name: "sandbox0-container0-name".to_string(),
                    service_system: None,
                }),
            ),
            // Invalid inputs.
            ("/var/log/pods/other", None),
            ("qwe", None),
            ("", None),
        ];

        for (input, expected) in cases.into_iter() {
            assert_eq!(parse_log_file_path(input), expected);
        }
    }
}
