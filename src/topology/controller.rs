use std::{future::Future, pin::pin, sync::Arc, time::Duration};

use futures_util::FutureExt as _;
use metrics::gauge;
use tokio::sync::{Mutex, MutexGuard};

#[cfg(feature = "api")]
use crate::api;
use crate::{
    config,
    extra_context::ExtraContext,
    internal_events::{VectorRecoveryError, VectorReloadError, VectorReloaded},
    signal::ShutdownError,
    topology::{ReloadError, RunningTopology},
};

#[derive(Clone, Debug)]
pub struct SharedTopologyController(Arc<Mutex<TopologyController>>);

impl SharedTopologyController {
    pub fn new(inner: TopologyController) -> Self {
        Self(Arc::new(Mutex::new(inner)))
    }

    pub fn blocking_lock(&self) -> MutexGuard<'_, TopologyController> {
        self.0.blocking_lock()
    }

    pub async fn lock(&self) -> MutexGuard<'_, TopologyController> {
        self.0.lock().await
    }

    pub fn try_into_inner(self) -> Result<Mutex<TopologyController>, Self> {
        Arc::try_unwrap(self.0).map_err(Self)
    }
}

pub struct TopologyController {
    pub topology: RunningTopology,
    pub config_paths: Vec<config::ConfigPath>,
    pub require_healthy: Option<bool>,
    #[cfg(feature = "api")]
    pub api_server: Option<api::Server>,
    pub extra_context: ExtraContext,
}

impl std::fmt::Debug for TopologyController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopologyController")
            .field("config_paths", &self.config_paths)
            .field("require_healthy", &self.require_healthy)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum ReloadOutcome {
    MissingApiKey,
    Success,
    RolledBack,
    FatalError(ShutdownError),
}

/// How often an in-flight reload re-reports `config_reload_in_progress`. Idle metrics expire
/// (300s by default), so a reload that hangs must keep refreshing the gauge to stay visible.
const RELOAD_IN_PROGRESS_REFRESH: Duration = Duration::from_secs(30);

/// Reports `config_reload_in_progress` = 1 while `reload` is pending and 0 once it finishes or is
/// dropped. Completion-only reload metrics never record a reload that hangs (ES-2267297).
async fn with_reload_in_progress_gauge<F: Future>(reload: F, refresh: Duration) -> F::Output {
    struct ResetOnDrop;
    impl Drop for ResetOnDrop {
        fn drop(&mut self) {
            gauge!("config_reload_in_progress").set(0.0);
        }
    }
    let _reset = ResetOnDrop;

    let mut reload = pin!(reload);
    let mut refresh = tokio::time::interval(refresh);
    loop {
        tokio::select! {
            output = &mut reload => return output,
            _ = refresh.tick() => gauge!("config_reload_in_progress").set(1.0),
        }
    }
}

impl TopologyController {
    pub async fn reload(&mut self, mut new_config: config::Config) -> ReloadOutcome {
        new_config
            .healthchecks
            .set_require_healthy(self.require_healthy);

        // Start the api server or disable it, if necessary
        #[cfg(feature = "api")]
        if !new_config.api.enabled {
            if let Some(server) = self.api_server.take() {
                debug!("Dropping api server.");
                drop(server)
            }
        } else if self.api_server.is_none() {
            use std::sync::atomic::AtomicBool;

            use tokio::runtime::Handle;

            use crate::internal_events::ApiStarted;

            debug!("Starting api server.");

            self.api_server = match api::Server::start(
                self.topology.config(),
                self.topology.watch(),
                Arc::<AtomicBool>::clone(&self.topology.running),
                &Handle::current(),
            ) {
                Ok(api_server) => {
                    emit!(ApiStarted {
                        addr: new_config.api.address.unwrap(),
                        playground: new_config.api.playground,
                        graphql: new_config.api.graphql,
                    });

                    Some(api_server)
                }
                Err(error) => {
                    let error = error.to_string();
                    error!("An error occurred that Vector couldn't handle: {}.", error);
                    return ReloadOutcome::FatalError(ShutdownError::ApiFailed { error });
                }
            }
        }

        match with_reload_in_progress_gauge(
            self.topology
                .reload_config_and_respawn(new_config, self.extra_context.clone()),
            RELOAD_IN_PROGRESS_REFRESH,
        )
        .await
        {
            Ok(()) => {
                #[cfg(feature = "api")]
                // Pass the new config to the API server.
                if let Some(ref api_server) = self.api_server {
                    api_server.update_config(self.topology.config());
                }

                emit!(VectorReloaded {
                    config_paths: &self.config_paths
                });
                ReloadOutcome::Success
            }
            Err(ReloadError::GlobalOptionsChanged { changed_fields }) => {
                error!(
                    message = "Config reload rejected due to non-reloadable global options.",
                    changed_fields = %changed_fields.join(", "),
                    internal_log_rate_limit = false,
                );
                emit!(VectorReloadError {
                    reason: "global_options_changed",
                });
                ReloadOutcome::RolledBack
            }
            Err(ReloadError::GlobalDiffFailed { source }) => {
                error!(
                    message = "Config reload rejected because computing global diff failed.",
                    error = %source,
                    internal_log_rate_limit = false,
                );
                emit!(VectorReloadError {
                    reason: "global_diff_failed",
                });
                ReloadOutcome::RolledBack
            }
            Err(ReloadError::TopologyBuildFailed) => {
                emit!(VectorReloadError {
                    reason: "topology_build_failed",
                });
                ReloadOutcome::RolledBack
            }
            Err(ReloadError::FailedToRestore) => {
                emit!(VectorReloadError {
                    reason: "restore_failed",
                });
                emit!(VectorRecoveryError);
                ReloadOutcome::FatalError(ShutdownError::ReloadFailedToRestore)
            }
        }
    }

    pub async fn stop(self) {
        self.topology.stop().await;
    }

    // The `sources_finished` method on `RunningTopology` only considers sources that are currently
    // running at the time the method is called. This presents a problem when the set of running
    // sources can change while we are waiting on the resulting future to resolve.
    //
    // This function resolves that issue by waiting in two stages. The first is the usual asynchronous
    // wait for the future to complete. When it does, we know that all of the sources that existed when
    // the future was built have finished, but we don't know if that's because they were replaced as
    // part of a reload (in which case we don't want to return yet). To differentiate, we acquire the
    // lock on the topology, create a new future, and check whether it resolves immediately or not. If
    // it does resolve, we know all sources are truly finished because we held the lock during the
    // check, preventing anyone else from adding new sources. If it does not resolve, that indicates
    // that new sources have been added since our original call and we should start the process over to
    // continue waiting.
    pub async fn sources_finished(mutex: SharedTopologyController) {
        loop {
            // Do an initial async wait while the topology is running, making sure not the hold the
            // mutex lock while we wait on sources to finish.
            let initial = {
                let tc = mutex.lock().await;
                tc.topology.sources_finished()
            };
            initial.await;

            // Once the initial signal is tripped, hold lock on the topology while checking again. This
            // ensures that no other task is adding new sources.
            let top = mutex.lock().await;
            if top.topology.sources_finished().now_or_never().is_some() {
                return;
            } else {
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use vector_lib::{
        event::{Metric, MetricValue},
        metrics::Controller,
    };

    use super::with_reload_in_progress_gauge;

    fn reload_in_progress() -> Option<f64> {
        Controller::get()
            .unwrap()
            .capture_metrics()
            .into_iter()
            .find(|metric: &Metric| metric.name() == "config_reload_in_progress")
            .map(|metric| match metric.value() {
                MetricValue::Gauge { value } => *value,
                other => panic!("unexpected metric value {other:?}"),
            })
    }

    #[tokio::test]
    async fn dropped_reload_resets_gauge() {
        vector_lib::metrics::init_test();
        let controller = Controller::get().unwrap();
        controller.reset();
        controller.set_expiry(Some(0.2), Vec::new()).unwrap();

        let hung_reload =
            with_reload_in_progress_gauge(std::future::pending::<()>(), Duration::from_millis(50));
        assert!(
            tokio::time::timeout(Duration::from_millis(600), hung_reload)
                .await
                .is_err()
        );
        // Dropping the reload (the timeout above did) resets the gauge.
        assert_eq!(reload_in_progress(), Some(0.0));
    }

    #[tokio::test]
    async fn stalled_reload_reports_in_progress_until_it_finishes() {
        vector_lib::metrics::init_test();
        let controller = Controller::get().unwrap();
        controller.reset();
        controller.set_expiry(Some(0.2), Vec::new()).unwrap();

        let (finish, finished) = tokio::sync::oneshot::channel::<()>();
        let reload = tokio::spawn(async move {
            with_reload_in_progress_gauge(finished, Duration::from_millis(50)).await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(reload_in_progress(), Some(1.0));
        // Expiry counts from the last capture that saw a change; several periods pass while the
        // reload is stuck.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(reload_in_progress(), Some(1.0));

        finish.send(()).unwrap();
        reload.await.unwrap().unwrap();
        assert_eq!(reload_in_progress(), Some(0.0));
    }
}
