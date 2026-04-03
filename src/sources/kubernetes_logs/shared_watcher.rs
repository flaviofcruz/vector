//! Shared Kubernetes API watcher registry.
//!
//! Deduplicates Kubernetes API watch connections across multiple `kubernetes_logs`
//! source instances. Sources with identical watcher parameters share a single set
//! of reflector tasks and in-memory stores.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{LazyLock, Mutex},
    time::Duration,
};

use k8s_openapi::api::core::v1::{Namespace, Node, Pod};
use kube::runtime::reflector::store::Store;
use tokio::task::JoinHandle;

/// Global registry of shared Kubernetes API watchers.
static SHARED_WATCHERS: LazyLock<Mutex<HashMap<WatcherKey, SharedWatcherEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Key that identifies a unique watcher configuration.
/// Two sources with the same `WatcherKey` share a single set of reflector tasks.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct WatcherKey {
    pub kube_config_file: Option<PathBuf>,
    pub field_selector: String,
    pub label_selector: String,
    pub namespace_label_selector: String,
    pub node_selector: String,
    pub use_apiserver_cache: bool,
    pub delay_deletion: Duration,
    pub insert_namespace_fields: bool,
}

/// Stores shared across all sources with the same `WatcherKey`.
pub(super) struct SharedWatcherStores {
    pub pod_state: Store<Pod>,
    pub ns_state: Store<Namespace>,
    pub node_state: Store<Node>,
}

/// Internal registry entry holding stores, reflector handles, and consumer count.
struct SharedWatcherEntry {
    pod_state: Store<Pod>,
    ns_state: Store<Namespace>,
    node_state: Store<Node>,
    reflector_handles: Vec<JoinHandle<()>>,
    consumer_count: usize,
}

/// RAII guard that decrements the consumer count on drop.
/// When the last consumer drops, reflector tasks are aborted and the entry is removed.
pub(super) struct SharedWatcherGuard {
    key: WatcherKey,
}

impl Drop for SharedWatcherGuard {
    fn drop(&mut self) {
        let mut registry = SHARED_WATCHERS
            .lock()
            .expect("shared watcher registry poisoned");
        if let Some(entry) = registry.get_mut(&self.key) {
            entry.consumer_count -= 1;
            if entry.consumer_count == 0 {
                let entry = registry
                    .remove(&self.key)
                    .expect("entry existed in get_mut");
                for handle in entry.reflector_handles {
                    handle.abort();
                }
                info!(
                    message = "Shared Kubernetes watcher stopped (last consumer dropped).",
                    key = ?self.key,
                );
            }
        }
    }
}

/// Result of acquiring a shared watcher entry.
pub(super) struct AcquireResult {
    pub stores: SharedWatcherStores,
    pub guard: SharedWatcherGuard,
}

/// Output from the watcher creation closure passed to `acquire()`.
pub(super) struct CreatedWatcher {
    pub pod_state: Store<Pod>,
    pub ns_state: Store<Namespace>,
    pub node_state: Store<Node>,
    pub reflector_handles: Vec<JoinHandle<()>>,
}

/// Acquire shared stores for the given key.
///
/// If an entry already exists, clones the store readers and increments the consumer count.
/// If no entry exists, calls `create_fn` to create the client, reflector tasks, and stores,
/// then inserts the entry. Uses a double-check pattern to avoid holding the lock across
/// the async `create_fn`.
pub(super) async fn acquire<F, Fut>(key: WatcherKey, create_fn: F) -> crate::Result<AcquireResult>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::Result<CreatedWatcher>>,
{
    // First check: is there already an entry?
    {
        let mut registry = SHARED_WATCHERS
            .lock()
            .expect("shared watcher registry poisoned");
        if let Some(entry) = registry.get_mut(&key) {
            entry.consumer_count += 1;
            info!(
                message = "Reusing shared Kubernetes watcher.",
                key = ?key,
                consumer_count = entry.consumer_count,
            );
            return Ok(AcquireResult {
                stores: SharedWatcherStores {
                    pod_state: entry.pod_state.clone(),
                    ns_state: entry.ns_state.clone(),
                    node_state: entry.node_state.clone(),
                },
                guard: SharedWatcherGuard { key },
            });
        }
    }
    // Lock released here — create_fn can do async work without holding it.

    let created = create_fn().await?;

    // Second check: another source may have raced and inserted while we were creating.
    let mut registry = SHARED_WATCHERS
        .lock()
        .expect("shared watcher registry poisoned");
    if let Some(entry) = registry.get_mut(&key) {
        // Race: another source already inserted. Discard what we created.
        entry.consumer_count += 1;
        for handle in created.reflector_handles {
            handle.abort();
        }
        info!(
            message = "Reusing shared Kubernetes watcher (lost race).",
            key = ?key,
            consumer_count = entry.consumer_count,
        );
        Ok(AcquireResult {
            stores: SharedWatcherStores {
                pod_state: entry.pod_state.clone(),
                ns_state: entry.ns_state.clone(),
                node_state: entry.node_state.clone(),
            },
            guard: SharedWatcherGuard { key },
        })
    } else {
        let stores = SharedWatcherStores {
            pod_state: created.pod_state.clone(),
            ns_state: created.ns_state.clone(),
            node_state: created.node_state.clone(),
        };
        registry.insert(
            key.clone(),
            SharedWatcherEntry {
                pod_state: created.pod_state,
                ns_state: created.ns_state,
                node_state: created.node_state,
                reflector_handles: created.reflector_handles,
                consumer_count: 1,
            },
        );
        info!(
            message = "Created new shared Kubernetes watcher.",
            key = ?key,
        );
        Ok(AcquireResult {
            stores,
            guard: SharedWatcherGuard { key },
        })
    }
}

/// Query the consumer count for a given key. Returns `None` if the key is not in the registry.
#[cfg(test)]
pub(super) fn consumer_count(key: &WatcherKey) -> Option<usize> {
    let registry = SHARED_WATCHERS
        .lock()
        .expect("shared watcher registry poisoned");
    registry.get(key).map(|entry| entry.consumer_count)
}

/// Remove all entries from the shared watcher registry. For testing only.
#[cfg(test)]
pub(super) fn reset_registry() {
    let mut registry = SHARED_WATCHERS
        .lock()
        .expect("shared watcher registry poisoned");
    for (_, entry) in registry.drain() {
        for handle in entry.reflector_handles {
            handle.abort();
        }
    }
}
