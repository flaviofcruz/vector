//! Kubernetes watchers that periodically start over with a fresh LIST.
//!
//! `kube-runtime` only re-lists after HTTP 410 Gone; otherwise a dropped watch resumes from the
//! last `resourceVersion` and relies on the server to replay deletions. The dblet node discovery
//! API doesn't: every object stays at `resourceVersion: "1"`, so objects it never reports as
//! `DELETED` stay in the reflector store for the life of the process (ES-2271421). A new watcher
//! always LISTs, and its `InitDone` replaces the store with exactly the listed objects.

use std::{fmt::Debug, time::Duration};

use futures::{Stream, StreamExt, stream};
use kube::{
    Api, Resource,
    runtime::{WatchStreamExt, watcher},
};
use serde::de::DeserializeOwned;

/// `watcher(api, config)` with the default backoff, replaced by a new watcher every
/// `relist_interval` if set. The interval should comfortably exceed the time a LIST takes, since a
/// watcher replaced before `InitDone` leaves the store unchanged.
pub fn relisting_watcher<K>(
    api: Api<K>,
    config: watcher::Config,
    relist_interval: Option<Duration>,
) -> impl Stream<Item = watcher::Result<watcher::Event<K>>> + Send
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + 'static,
{
    let new_watcher =
        move || watcher(api.clone(), config.clone()).backoff(watcher::DefaultBackoff::default());
    match relist_interval {
        None => new_watcher().left_stream(),
        Some(interval) => {
            stream::repeat_with(move || new_watcher().take_until(tokio::time::sleep(interval)))
                .flatten()
                .right_stream()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use http_1 as http;
    use k8s_openapi::{api::core::v1::Pod, apimachinery::pkg::apis::meta::v1::ObjectMeta};
    use kube::{
        Api, Client,
        client::Body,
        runtime::{
            reflector::{ObjectRef, store},
            watcher,
        },
    };
    use tower_test::mock;

    use super::relisting_watcher;
    use crate::kubernetes::{custom_reflector, meta_cache::MetaCache};

    fn pod(name: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some("default".to_owned()),
                uid: Some(format!("{name}-uid")),
                resource_version: Some("1".to_owned()),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        }
    }

    /// Serves `pods` the way the dblet node discovery API does: every object (and the list) is at
    /// `resourceVersion: "1"`, a watch replays the current objects as `ADDED` and then ends, and
    /// removals are never sent as `DELETED`.
    fn serve_discovery_api(pods: Arc<Mutex<BTreeMap<String, Pod>>>) -> Client {
        let (service, mut handle) = mock::pair::<http::Request<Body>, http::Response<Body>>();
        tokio::spawn(async move {
            while let Some((request, send)) = handle.next_request().await {
                let current: Vec<Pod> = pods.lock().unwrap().values().cloned().collect();
                let watch = request
                    .uri()
                    .query()
                    .unwrap_or_default()
                    .contains("watch=true");
                let body = if watch {
                    // A watch stream that ends right away would make kube-runtime re-watch in a
                    // tight loop; real streams stay open for a while.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    current
                        .into_iter()
                        .map(|pod| {
                            format!("{}\n", serde_json::json!({"type": "ADDED", "object": pod}))
                        })
                        .collect::<String>()
                } else {
                    serde_json::json!({"metadata": {"resourceVersion": "1"}, "items": current})
                        .to_string()
                };
                send.send_response(http::Response::new(Body::from(body.into_bytes())));
            }
        });
        Client::new(service, "default")
    }

    async fn store_after_deleting_b(relist_interval: Option<Duration>) -> store::Store<Pod> {
        let pods = Arc::new(Mutex::new(BTreeMap::from([
            ("a".to_owned(), pod("a")),
            ("b".to_owned(), pod("b")),
        ])));
        let api = Api::<Pod>::all(serve_discovery_api(Arc::clone(&pods)));
        let store_w = store::Writer::default();
        let store = store_w.as_reader();
        tokio::spawn(custom_reflector(
            store_w,
            MetaCache::new(),
            relisting_watcher(api, watcher::Config::default(), relist_interval),
            Duration::from_millis(100),
        ));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(store.get(&ObjectRef::from_obj(&pod("b"))).is_some());

        pods.lock().unwrap().remove("b");
        // Many watch streams, and the re-list interval, elapse in this window.
        tokio::time::sleep(Duration::from_secs(3)).await;
        store
    }

    /// Documents the ES-2271421 leak: without re-listing, an object removed between watch streams
    /// stays in the store forever.
    #[tokio::test]
    async fn without_relist_object_removed_between_watches_is_never_evicted() {
        let store = store_after_deleting_b(None).await;
        assert!(store.get(&ObjectRef::from_obj(&pod("a"))).is_some());
        assert!(store.get(&ObjectRef::from_obj(&pod("b"))).is_some());
    }

    #[tokio::test]
    async fn relist_evicts_object_removed_between_watches() {
        let store = store_after_deleting_b(Some(Duration::from_secs(1))).await;
        assert!(store.get(&ObjectRef::from_obj(&pod("a"))).is_some());
        assert_eq!(store.get(&ObjectRef::from_obj(&pod("b"))), None);
        assert_eq!(store.state().len(), 1);
    }
}
