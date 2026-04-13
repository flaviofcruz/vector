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

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use futures::channel::mpsc;
    use futures_util::SinkExt;
    use k8s_openapi::api::core::v1::{Namespace, Node, Pod};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::runtime::reflector;
    use kube::runtime::reflector::ObjectRef;
    use kube::runtime::watcher as kube_watcher;
    use serial_test::serial;

    use super::*;
    use crate::kubernetes::{custom_reflector, meta_cache::MetaCache};

    fn default_key() -> WatcherKey {
        WatcherKey {
            kube_config_file: None,
            field_selector: "spec.nodeName=node1".to_string(),
            label_selector: "vector.dev/exclude!=true".to_string(),
            namespace_label_selector: "vector.dev/exclude!=true".to_string(),
            node_selector: "metadata.name=node1".to_string(),
            use_apiserver_cache: false,
            delay_deletion: Duration::from_secs(60),
            insert_namespace_fields: true,
        }
    }

    fn mock_created_watcher() -> CreatedWatcher {
        let pod_store_w = reflector::store::Writer::<Pod>::default();
        let pod_state = pod_store_w.as_reader();
        let ns_store_w = reflector::store::Writer::<Namespace>::default();
        let ns_state = ns_store_w.as_reader();
        let node_store_w = reflector::store::Writer::<Node>::default();
        let node_state = node_store_w.as_reader();

        let reflector_handles = vec![
            tokio::spawn(futures::future::pending::<()>()),
            tokio::spawn(futures::future::pending::<()>()),
            tokio::spawn(futures::future::pending::<()>()),
        ];

        CreatedWatcher {
            pod_state,
            ns_state,
            node_state,
            reflector_handles,
        }
    }

    fn hash_of(key: &WatcherKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    // -----------------------------------------------------------------------
    // WatcherKey equality / hashing tests
    // -----------------------------------------------------------------------

    #[test]
    fn identical_keys_are_equal_and_hash_the_same() {
        let a = default_key();
        let b = default_key();
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_kube_config_file_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.kube_config_file = Some(PathBuf::from("/other/config"));
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_field_selector_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.field_selector = "spec.nodeName=node2".to_string();
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_label_selector_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.label_selector = "app=nginx".to_string();
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_namespace_label_selector_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.namespace_label_selector = "team=backend".to_string();
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_node_selector_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.node_selector = "metadata.name=node2".to_string();
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_use_apiserver_cache_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.use_apiserver_cache = true;
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_delay_deletion_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.delay_deletion = Duration::from_secs(120);
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differing_insert_namespace_fields_produces_different_key() {
        let a = default_key();
        let mut b = default_key();
        b.insert_namespace_fields = false;
        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    // -----------------------------------------------------------------------
    // Registry lifecycle tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn acquire_creates_new_entry_and_cleanup_on_drop() {
        reset_registry();
        let key = default_key();

        let result = acquire(key.clone(), || async { Ok(mock_created_watcher()) })
            .await
            .expect("acquire should succeed");

        assert_eq!(consumer_count(&key), Some(1));

        // Drop the guard — entry should be removed.
        drop(result.guard);
        assert_eq!(consumer_count(&key), None);
    }

    #[tokio::test]
    #[serial]
    async fn acquire_same_key_reuses_entry_and_does_not_call_create_fn() {
        reset_registry();
        let key = default_key();
        let call_count = AtomicUsize::new(0);

        let r1 = acquire(key.clone(), || {
            call_count.fetch_add(1, Ordering::SeqCst);
            async { Ok(mock_created_watcher()) }
        })
        .await
        .expect("first acquire should succeed");

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert_eq!(consumer_count(&key), Some(1));

        // Second acquire with the same key should reuse, NOT call create_fn.
        let r2 = acquire(key.clone(), || {
            call_count.fetch_add(1, Ordering::SeqCst);
            async { Ok(mock_created_watcher()) }
        })
        .await
        .expect("second acquire should succeed");

        assert_eq!(call_count.load(Ordering::SeqCst), 1, "create_fn should NOT have been called a second time");
        assert_eq!(consumer_count(&key), Some(2));

        // Drop first guard — count goes to 1.
        drop(r1.guard);
        assert_eq!(consumer_count(&key), Some(1));

        // Drop second guard — entry removed.
        drop(r2.guard);
        assert_eq!(consumer_count(&key), None);
    }

    #[tokio::test]
    #[serial]
    async fn different_keys_get_separate_entries() {
        reset_registry();
        let key1 = default_key();
        let mut key2 = default_key();
        key2.field_selector = "spec.nodeName=node2".to_string();

        let r1 = acquire(key1.clone(), || async { Ok(mock_created_watcher()) })
            .await
            .expect("acquire key1");
        let r2 = acquire(key2.clone(), || async { Ok(mock_created_watcher()) })
            .await
            .expect("acquire key2");

        assert_eq!(consumer_count(&key1), Some(1));
        assert_eq!(consumer_count(&key2), Some(1));

        drop(r1.guard);
        assert_eq!(consumer_count(&key1), None);
        assert_eq!(consumer_count(&key2), Some(1));

        drop(r2.guard);
        assert_eq!(consumer_count(&key2), None);
    }

    #[tokio::test]
    #[serial]
    async fn reflector_handles_are_aborted_when_last_consumer_drops() {
        reset_registry();
        let key = default_key();

        // Create a watcher with handles we can observe.
        let h1 = tokio::spawn(futures::future::pending::<()>());
        let h2 = tokio::spawn(futures::future::pending::<()>());
        // Keep clones of abort handles so we can check them later.
        let abort1 = h1.abort_handle();
        let abort2 = h2.abort_handle();

        let pod_store_w = reflector::store::Writer::<Pod>::default();
        let pod_state = pod_store_w.as_reader();
        let ns_store_w = reflector::store::Writer::<Namespace>::default();
        let ns_state = ns_store_w.as_reader();
        let node_store_w = reflector::store::Writer::<Node>::default();
        let node_state = node_store_w.as_reader();

        let created = CreatedWatcher {
            pod_state,
            ns_state,
            node_state,
            reflector_handles: vec![h1, h2],
        };

        let result = acquire(key.clone(), || async { Ok(created) })
            .await
            .expect("acquire should succeed");

        assert!(!abort1.is_finished());
        assert!(!abort2.is_finished());

        // Drop guard — last consumer, handles should be aborted.
        drop(result.guard);
        // Yield to let the runtime process the aborts.
        tokio::task::yield_now().await;
        assert!(abort1.is_finished());
        assert!(abort2.is_finished());
    }

    #[tokio::test]
    #[serial]
    async fn guard_drop_on_error_path_still_cleans_up() {
        reset_registry();
        let key = default_key();

        let result = acquire(key.clone(), || async { Ok(mock_created_watcher()) })
            .await
            .expect("acquire should succeed");

        assert_eq!(consumer_count(&key), Some(1));

        // Simulate an error path: drop everything (including guard) via drop.
        drop(result);
        assert_eq!(consumer_count(&key), None);
    }

    // -----------------------------------------------------------------------
    // Store data visibility test
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn data_written_to_shared_reflector_is_visible_to_all_consumers() {
        reset_registry();
        let key = default_key();

        // Create a pod store with a writer we control.
        let pod_store_w = reflector::store::Writer::<Pod>::default();
        let pod_state = pod_store_w.as_reader();

        // Namespace and node stores — not under test, just placeholders.
        let ns_store_w = reflector::store::Writer::<Namespace>::default();
        let ns_state = ns_store_w.as_reader();
        let node_store_w = reflector::store::Writer::<Node>::default();
        let node_state = node_store_w.as_reader();

        // Create a mock watcher stream using an mpsc channel.
        let (mut tx, rx) = mpsc::channel::<kube_watcher::Result<kube_watcher::Event<Pod>>>(10);

        // Spawn a custom_reflector to process events from the channel.
        let meta_cache = MetaCache::new();
        let reflector_handle = tokio::spawn(custom_reflector(
            pod_store_w,
            meta_cache,
            rx,
            Duration::from_secs(60),
        ));

        // Build the CreatedWatcher and insert it via acquire.
        let created = CreatedWatcher {
            pod_state: pod_state.clone(),
            ns_state,
            node_state,
            reflector_handles: vec![reflector_handle],
        };

        // First consumer acquires.
        let r1 = acquire(key.clone(), || async { Ok(created) })
            .await
            .expect("first acquire should succeed");

        // Second consumer acquires (reuses).
        let r2 = acquire(key.clone(), || async {
            panic!("create_fn should not be called for second consumer");
        })
        .await
        .expect("second acquire should succeed");

        assert_eq!(consumer_count(&key), Some(2));

        // Create a test pod and send it through the channel.
        let test_pod = Pod {
            metadata: ObjectMeta {
                name: Some("test-pod".to_string()),
                namespace: Some("default".to_string()),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        };

        tx.send(Ok(kube_watcher::Event::Apply(test_pod.clone())))
            .await
            .expect("send event");

        // Give the reflector time to process.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Both consumers should see the pod in their stores.
        let pod_ref = ObjectRef::from_obj(&test_pod);
        let pod_from_r1 = r1.stores.pod_state.get(&pod_ref);
        let pod_from_r2 = r2.stores.pod_state.get(&pod_ref);

        assert_eq!(
            pod_from_r1.as_deref(),
            Some(&test_pod),
            "consumer 1 should see the pod"
        );
        assert_eq!(
            pod_from_r2.as_deref(),
            Some(&test_pod),
            "consumer 2 should see the pod"
        );

        // Cleanup.
        drop(r1.guard);
        drop(r2.guard);
    }

    #[tokio::test]
    #[serial]
    async fn double_check_race_discards_loser_and_reuses_winner() {
        reset_registry();
        let key = default_key();

        // The create_fn simulates a race: before returning, it manually
        // inserts a competing entry (as if another source won the race).
        let result = acquire(key.clone(), || {
            let race_key = key.clone();
            async move {
                // Simulate another source winning the race by inserting directly.
                let winner = mock_created_watcher();
                {
                    let mut registry =
                        SHARED_WATCHERS.lock().expect("shared watcher registry poisoned");
                    registry.insert(
                        race_key,
                        SharedWatcherEntry {
                            pod_state: winner.pod_state.clone(),
                            ns_state: winner.ns_state.clone(),
                            node_state: winner.node_state.clone(),
                            reflector_handles: winner.reflector_handles,
                            consumer_count: 1,
                        },
                    );
                }
                // Return our "loser" watcher — acquire() should discard it.
                Ok(mock_created_watcher())
            }
        })
        .await
        .expect("acquire should succeed");

        // Consumer count should be 2 (winner's 1 + our increment).
        assert_eq!(consumer_count(&key), Some(2));
        drop(result.guard);
        // Winner's original count remains.
        assert_eq!(consumer_count(&key), Some(1));
    }
}
