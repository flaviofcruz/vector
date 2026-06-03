use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::{Future, FutureExt, future};
use itertools::Itertools;
use snafu::Snafu;
use stream_cancel::Trigger;
use tokio::{
    sync::{mpsc, watch},
    time::{Duration, Instant, interval, sleep_until},
};
use tracing::Instrument;
use vector_lib::{
    buffers::topology::channel::BufferSender,
    shutdown::ShutdownSignal,
    tap::topology::{TapOutput, TapResource, WatchRx, WatchTx},
    trigger::DisabledTrigger,
};

use super::{
    BuiltBuffer, TaskHandle,
    builder::{self, TopologyPieces, TopologyPiecesBuilder, reload_enrichment_tables},
    fanout::{ControlChannel, ControlMessage},
    handle_errors, retain, take_healthchecks,
    task::{Task, TaskOutput},
};
use crate::{
    config::{ComponentKey, Config, ConfigDiff, HealthcheckOptions, Inputs, OutputId, Resource},
    event::EventArray,
    extra_context::ExtraContext,
    shutdown::SourceShutdownCoordinator,
    signal::ShutdownError,
    spawn_named,
    utilization::UtilizationRegistry,
};

pub type ShutdownErrorReceiver = mpsc::UnboundedReceiver<ShutdownError>;

/// Role a component plays in the topology, used to bucket still-running components in the
/// shutdown logs. `Config` exposes sources/transforms/sinks separately rather than a single
/// key->role lookup, so we precompute this map once before the reporting closures take over.
#[derive(Clone, Copy)]
enum ComponentRole {
    Source,
    Transform,
    Sink,
}

/// Partitions still-running component keys by role so shutdown logs can report sources,
/// transforms, and sinks separately instead of as one undifferentiated list. Each returned
/// string is the matching keys, sorted and comma-joined.
fn partition_remaining_by_role<'a>(
    keys: impl Iterator<Item = &'a ComponentKey>,
    roles: &HashMap<ComponentKey, ComponentRole>,
) -> (String, String, String) {
    let (mut sources, mut transforms, mut sinks) = (Vec::new(), Vec::new(), Vec::new());
    for key in keys {
        match roles.get(key) {
            Some(ComponentRole::Source) => sources.push(key),
            Some(ComponentRole::Transform) => transforms.push(key),
            Some(ComponentRole::Sink) => sinks.push(key),
            None => {}
        }
    }
    (
        sources.into_iter().sorted().join(", "),
        transforms.into_iter().sorted().join(", "),
        sinks.into_iter().sorted().join(", "),
    )
}

#[derive(Debug, Snafu)]
pub enum ReloadError {
    #[snafu(display("global options changed: {}", changed_fields.join(", ")))]
    GlobalOptionsChanged { changed_fields: Vec<String> },
    #[snafu(display("failed to compute global diff: {}", source))]
    GlobalDiffFailed { source: serde_json::Error },
    #[snafu(display("topology build failed"))]
    TopologyBuildFailed,
    #[snafu(display("failed to restore previous config"))]
    FailedToRestore,
}

#[allow(dead_code)]
pub struct RunningTopology {
    inputs: HashMap<ComponentKey, BufferSender<EventArray>>,
    inputs_tap_metadata: HashMap<ComponentKey, Inputs<OutputId>>,
    outputs: HashMap<OutputId, ControlChannel>,
    outputs_tap_metadata: HashMap<ComponentKey, (&'static str, String)>,
    source_tasks: HashMap<ComponentKey, TaskHandle>,
    tasks: HashMap<ComponentKey, TaskHandle>,
    shutdown_coordinator: SourceShutdownCoordinator,
    detach_triggers: HashMap<ComponentKey, DisabledTrigger>,
    pub(crate) config: Config,
    pub(crate) abort_tx: mpsc::UnboundedSender<ShutdownError>,
    watch: (WatchTx, WatchRx),
    pub(crate) running: Arc<AtomicBool>,
    graceful_shutdown_duration: Option<Duration>,
    graceful_data_source_shutdown_duration: Option<Duration>,
    utilization_registry: Option<UtilizationRegistry>,
    utilization_task: Option<TaskHandle>,
    utilization_task_shutdown_trigger: Option<Trigger>,
    metrics_task: Option<TaskHandle>,
    metrics_task_shutdown_trigger: Option<Trigger>,
    pending_reload: Option<HashSet<ComponentKey>>,
}

impl RunningTopology {
    pub fn new(config: Config, abort_tx: mpsc::UnboundedSender<ShutdownError>) -> Self {
        Self {
            inputs: HashMap::new(),
            inputs_tap_metadata: HashMap::new(),
            outputs: HashMap::new(),
            outputs_tap_metadata: HashMap::new(),
            shutdown_coordinator: SourceShutdownCoordinator::default(),
            detach_triggers: HashMap::new(),
            source_tasks: HashMap::new(),
            tasks: HashMap::new(),
            abort_tx,
            watch: watch::channel(TapResource::default()),
            running: Arc::new(AtomicBool::new(true)),
            graceful_shutdown_duration: config.graceful_shutdown_duration,
            graceful_data_source_shutdown_duration: config.graceful_data_source_shutdown_duration,
            config,
            utilization_registry: None,
            utilization_task: None,
            utilization_task_shutdown_trigger: None,
            metrics_task: None,
            metrics_task_shutdown_trigger: None,
            pending_reload: None,
        }
    }

    /// Gets the configuration that represents this running topology.
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// Adds a set of component keys to the pending reload set if one exists. Otherwise, it
    /// initializes the pending reload set.
    pub fn extend_reload_set(&mut self, new_set: HashSet<ComponentKey>) {
        match &mut self.pending_reload {
            None => self.pending_reload = Some(new_set.clone()),
            Some(existing) => existing.extend(new_set),
        }
    }

    /// Creates a subscription to topology changes.
    ///
    /// This is used by the tap API to observe configuration changes, and re-wire tap sinks.
    pub fn watch(&self) -> watch::Receiver<TapResource> {
        self.watch.1.clone()
    }

    /// Signal that all sources in this topology are ended.
    ///
    /// The future returned by this function will finish once all the sources in
    /// this topology have finished. This allows the caller to wait for or
    /// detect that the sources in the topology are no longer
    /// producing. [`Application`][crate::app::Application], as an example, uses this as a
    /// shutdown signal.
    pub fn sources_finished(&self) -> future::BoxFuture<'static, ()> {
        self.shutdown_coordinator.shutdown_tripwire()
    }

    /// Shut down all topology components.
    ///
    /// This function sends the shutdown signal to all sources in this topology
    /// and returns a future that resolves once all components (sources,
    /// transforms, and sinks) have finished shutting down. Transforms and sinks
    /// will shut down automatically once their input tasks finish.
    ///
    /// This function takes ownership of `self`, so once it returns everything
    /// in the [`RunningTopology`] instance has been dropped except for the
    /// `tasks` map. This map gets moved into the returned future and is used to
    /// poll for when the tasks have completed. Once the returned future is
    /// dropped then everything from this RunningTopology instance is fully
    /// dropped.
    pub fn stop(self) -> impl Future<Output = ()> {
        // Update the API's health endpoint to signal shutdown
        self.running.store(false, Ordering::Relaxed);

        let map_closure = |_result| ();

        // If we reach this, we will forcefully shutdown the sources. If None, we will never force shutdown.
        let deadline = self
            .graceful_shutdown_duration
            .map(|grace_period| Instant::now() + grace_period);

        let data_source_deadline = self
            .graceful_data_source_shutdown_duration
            .map(|grace_period| Instant::now() + grace_period);

        // Cancel utilization and metrics tasks.
        if let Some(trigger) = self.utilization_task_shutdown_trigger {
            trigger.cancel();
        }
        if let Some(trigger) = self.metrics_task_shutdown_trigger {
            trigger.cancel();
        }

        // Split the shutdown into two waves if there are deferred sources and a data source
        // deadline is configured. Otherwise, use the existing single-pass shutdown to maintain
        // backward compatibility.
        let (wave1_complete, deferred_shutdowns) = self
            .shutdown_coordinator
            .shutdown_non_deferred(data_source_deadline.or(deadline));

        let two_wave_enabled = self.config.global.two_wave_shutdown.enabled();
        let use_two_wave = two_wave_enabled
            && deferred_shutdowns.has_deferred_sources()
            && data_source_deadline.is_some();

        // In two-wave mode, compute which components are exclusively downstream of
        // non-deferred sources. Only those components should be waited on in wave 1.
        // Components with ANY deferred source in their ancestry stay alive until wave 2.
        let exclusively_non_deferred_keys = if use_two_wave {
            let deferred_source_keys = deferred_shutdowns.deferred_keys();
            Self::compute_exclusively_non_deferred_components(&self.config, &deferred_source_keys)
        } else {
            HashSet::new()
        };

        // Create handy handles collections of all tasks for the subsequent operations.
        let mut wait_handles = Vec::new();
        let mut wave1_wait_handles = Vec::new();
        let mut check_handles = HashMap::<ComponentKey, Vec<_>>::new();

        // Source components have two tasks: pump in self.tasks, and source in self.source_tasks.
        for (key, task) in self.tasks.into_iter().chain(self.source_tasks.into_iter()) {
            let task = task.map(map_closure).shared();

            wait_handles.push(task.clone());
            if use_two_wave && exclusively_non_deferred_keys.contains(&key) {
                wave1_wait_handles.push(task.clone());
            }
            check_handles.entry(key).or_default().push(task);
        }

        if let Some(utilization_task) = self.utilization_task {
            wait_handles.push(utilization_task.map(map_closure).shared());
        }

        if let Some(metrics_task) = self.metrics_task {
            wait_handles.push(metrics_task.map(map_closure).shared());
        }

        // Classify component keys by role up front so the shutdown logs below can report
        // still-running sources, transforms, and sinks separately. `self.config` is borrowed
        // here before the reporting closures take ownership of the handle maps. Wrapped in an
        // `Arc` so each closure shares one copy via a cheap pointer clone.
        let component_roles: Arc<HashMap<ComponentKey, ComponentRole>> = Arc::new(
            self.config
                .sources()
                .map(|(key, _)| (key.clone(), ComponentRole::Source))
                .chain(
                    self.config
                        .transforms()
                        .map(|(key, _)| (key.clone(), ComponentRole::Transform)),
                )
                .chain(
                    self.config
                        .sinks()
                        .map(|(key, _)| (key.clone(), ComponentRole::Sink)),
                )
                .collect(),
        );

        let timeout = if let Some(deadline) = deadline {
            // If we reach the deadline, this future will print out which components
            // won't gracefully shutdown since we will start to forcefully shutdown
            // the sources.
            let mut check_handles2 = check_handles.clone();
            let component_roles = Arc::clone(&component_roles);
            Box::pin(async move {
                sleep_until(deadline).await;
                // Remove all tasks that have shutdown.
                check_handles2.retain(|_key, handles| {
                    retain(handles, |handle| handle.peek().is_none());
                    !handles.is_empty()
                });
                let (remaining_sources, remaining_transforms, remaining_sinks) =
                    partition_remaining_by_role(check_handles2.keys(), &component_roles);

                error!(
                    remaining_sources = ?remaining_sources,
                    remaining_transforms = ?remaining_transforms,
                    remaining_sinks = ?remaining_sinks,
                    message = "Failed to gracefully shut down in time. Killing components.",
                    internal_log_rate_limit = false
                );
            }) as future::BoxFuture<'static, ()>
        } else {
            Box::pin(future::pending()) as future::BoxFuture<'static, ()>
        };

        // Flag to suppress the shutdown reporter before wave 2. This breaks the feedback
        // loop where the reporter generates log events → internal_logs captures them →
        // internal_logs never shuts down.
        let suppress_reporter = Arc::new(AtomicBool::new(false));
        let suppress_reporter_check = Arc::clone(&suppress_reporter);

        // Snapshot of check_handles for logging still-active components at wave 2 start.
        // The reporter closure below takes ownership of the original.
        let mut wave2_start_check_handles = check_handles.clone();

        // Separate snapshot of check_handles used only if the wave 1 drain exceeds
        // data_source_deadline, to name the still-active exclusively-non-deferred
        // components in a warn log. Must be cloned before `reporter` takes ownership of
        // the original `check_handles`.
        let wave1_straggler_check_handles = check_handles.clone();

        // Reports in intervals which components are still running.
        let mut interval = interval(Duration::from_secs(5));
        let reporter_component_roles = Arc::clone(&component_roles);
        let reporter = async move {
            loop {
                interval.tick().await;

                // Stop reporting if wave 2 is about to begin. Continued reporting
                // would feed events into internal_logs, preventing it from shutting
                // down cleanly.
                if suppress_reporter_check.load(Ordering::Relaxed) {
                    break;
                }

                // Remove all tasks that have shutdown.
                check_handles.retain(|_key, handles| {
                    retain(handles, |handle| handle.peek().is_none());
                    !handles.is_empty()
                });
                let (remaining_sources, remaining_transforms, remaining_sinks) =
                    partition_remaining_by_role(check_handles.keys(), &reporter_component_roles);

                let (deadline_passed, time_remaining) = match deadline {
                    Some(d) => match d.checked_duration_since(Instant::now()) {
                        Some(remaining) => (false, format!("{} seconds left", remaining.as_secs())),
                        None => (true, "overdue".to_string()),
                    },
                    None => (false, "no time limit".to_string()),
                };

                info!(
                    remaining_sources = ?remaining_sources,
                    remaining_transforms = ?remaining_transforms,
                    remaining_sinks = ?remaining_sinks,
                    time_remaining = ?time_remaining,
                    "Shutting down... Waiting on running components."
                );

                let all_done = check_handles.is_empty();

                if all_done {
                    info!("Shutdown reporter exiting: all components shut down.");
                    break;
                } else if deadline_passed {
                    error!(
                        remaining_sources = ?remaining_sources,
                        remaining_transforms = ?remaining_transforms,
                        remaining_sinks = ?remaining_sinks,
                        "Shutdown reporter: deadline exceeded."
                    );
                    break;
                }
            }
        };

        // Finishes once all tasks have shutdown.
        let success = futures::future::join_all(wait_handles).map(|_| ());

        // Aggregate future that ends once anything detects that all tasks have shutdown.
        let shutdown_complete_future = future::select_all(vec![
            Box::pin(timeout) as future::BoxFuture<'static, ()>,
            Box::pin(reporter) as future::BoxFuture<'static, ()>,
            Box::pin(success) as future::BoxFuture<'static, ()>,
        ]);

        if use_two_wave {
            // Two-wave shutdown.
            //
            // Wave 1: Non-deferred (data) sources shut down, then we wait for all
            // exclusively-non-deferred transforms/sinks to drain. Components with ANY
            // deferred source in their ancestry stay alive — their deferred source input
            // keeps the channel open so they naturally continue running.
            //
            // Wave 2: Deferred (internal) sources shut down. Their downstream components
            // (including any with mixed inputs) close naturally as the remaining input
            // channels drop.

            // Snapshot the component classifications now so they're available for logging
            // inside source_shutdown_complete after deferred_shutdowns is consumed. Sort so
            // log output is stable across runs (HashSet iteration order otherwise shuffles).
            let non_deferred_components_str =
                exclusively_non_deferred_keys.iter().sorted().join(", ");
            let deferred_components_str = deferred_shutdowns
                .deferred_keys()
                .iter()
                .sorted()
                .join(", ");

            // exclusively_non_deferred_keys is also consumed by source_shutdown_complete
            // (for filtering the wave 1 straggler snapshot below), so clone it here — the
            // snapshot above already borrowed it for non_deferred_components_str.
            let wave1_straggler_keys = exclusively_non_deferred_keys.clone();

            let source_shutdown_complete = async move {
                info!(
                    non_deferred_components = ?non_deferred_components_str,
                    deferred_components = ?deferred_components_str,
                    message = "Wave 1: Shutting down data sources.",
                    internal_log_rate_limit = false,
                );
                wave1_complete.await;

                // Wait for all exclusively-non-deferred components to finish draining.
                info!(
                    message = "Wave 1 complete. Waiting for exclusively-non-deferred transforms and sinks to drain.",
                    internal_log_rate_limit = false,
                );
                // Bound the wait by data_source_deadline. wave1_complete is already bounded
                // by this deadline inside the shutdown coordinator, but without a matching
                // bound here a stuck downstream sink (e.g., a Kafka producer that can't
                // flush) would block wave 2 indefinitely. On timeout, proceed to wave 2
                // anyway: the straggling components will be cancelled naturally when their
                // upstream channels close as wave 2 shuts down deferred sources.
                //
                // `gracefully_closed` records whether wave 1 drained inside the deadline so
                // the COMPONENTS_CLOSED VEL below can carry it. The defensive no-deadline
                // arm is also considered graceful because the unbounded join can't time
                // out; the only non-graceful path is the timeout arm here.
                let mut gracefully_closed = false;
                if let Some(data_deadline) = data_source_deadline {
                    let mut wave1_straggler_check_handles = wave1_straggler_check_handles;
                    match tokio::time::timeout_at(
                        data_deadline,
                        futures::future::join_all(wave1_wait_handles),
                    )
                    .await
                    {
                        Ok(_) => {
                            gracefully_closed = true;
                        }
                        Err(_) => {
                            // Compute the straggler list using the same peek-based filter
                            // as the reporter, restricted to exclusively-non-deferred keys
                            // so the warn log only names components that were actually
                            // blocking wave 2.
                            wave1_straggler_check_handles.retain(|key, handles| {
                                if !wave1_straggler_keys.contains(key) {
                                    return false;
                                }
                                retain(handles, |handle| handle.peek().is_none());
                                !handles.is_empty()
                            });
                            let stragglers =
                                wave1_straggler_check_handles.keys().sorted().join(", ");
                            warn!(
                                components = ?stragglers,
                                message = "Wave 1 drain deadline exceeded; proceeding to wave 2.",
                                internal_log_rate_limit = false,
                            );
                        }
                    }
                } else {
                    // Defensive: use_two_wave implies data_source_deadline.is_some(), but
                    // fall back to the original unbounded wait if that invariant changes.
                    futures::future::join_all(wave1_wait_handles).await;
                    gracefully_closed = true;
                }

                // Emit a VEL event indicating all data components have been closed.
                // This must happen before wave 2 shuts down internal sources (including
                // internal_logs), so the event can still be delivered through the pipeline.
                info!(
                    message = "All Vector data components have been closed.",
                    // VECTOR_SERVICE_EVENT
                    vector_event_type = 2,
                    // VECTOR_PROCESS_COMPONENTS_CLOSED
                    service_event = 5,
                    gracefully_closed = gracefully_closed,
                    internal_log_rate_limit = false,
                );

                // Suppress the shutdown reporter before wave 2. The reporter generates
                // log events that feed into internal_logs, creating a feedback loop that
                // prevents internal_logs from shutting down. Stopping the reporter breaks
                // this cycle and allows a clean wave 2 shutdown.
                suppress_reporter.store(true, Ordering::Relaxed);

                // Snapshot still-active components as wave 2 begins. Mirrors the periodic
                // reporter's format so the wave-boundary state shows up in the same log
                // stream readers already parse.
                wave2_start_check_handles.retain(|_key, handles| {
                    retain(handles, |handle| handle.peek().is_none());
                    !handles.is_empty()
                });
                let (remaining_sources, remaining_transforms, remaining_sinks) =
                    partition_remaining_by_role(wave2_start_check_handles.keys(), &component_roles);
                let time_remaining = match deadline {
                    Some(d) => match d.checked_duration_since(Instant::now()) {
                        Some(remaining) => format!("{} seconds left", remaining.as_secs()),
                        None => "overdue".to_string(),
                    },
                    None => "no time limit".to_string(),
                };
                info!(
                    remaining_sources = ?remaining_sources,
                    remaining_transforms = ?remaining_transforms,
                    remaining_sinks = ?remaining_sinks,
                    time_remaining = ?time_remaining,
                    "Wave 2 starting. Components still active."
                );

                // Wave 2: Shut down deferred (internal) sources with remaining main deadline.
                info!(
                    message = "Wave 2: Shutting down deferred (internal) sources.",
                    internal_log_rate_limit = false,
                );
                // Give `internal_logs` (and its downstream transforms/sinks) a
                // scheduler window to consume the two tracing events we just
                // emitted before we cancel the source. Paired with the drain-
                // on-shutdown behavior in `internal_logs`; either alone closes
                // the common case, but together they tolerate scheduler jitter.
                tokio::time::sleep(Duration::from_millis(50)).await;
                deferred_shutdowns.shutdown_all(deadline).await;
            };

            futures::future::join(source_shutdown_complete, shutdown_complete_future)
                .map(|_| ())
                .boxed()
        } else {
            // No deferred sources or no data source deadline: use original single-pass behavior.
            let source_shutdown_complete = async move {
                wave1_complete.await;

                // Emit a VEL event indicating data components have been closed.
                // Emitted before deferred source shutdown so that internal_logs
                // (if present as a deferred source) can still deliver the event.
                // `gracefully_closed` is always false on this branch: two-wave shutdown
                // is not active, so there is no wave 1 deadline to meet.
                info!(
                    message = "All Vector data components have been closed.",
                    // VECTOR_SERVICE_EVENT
                    vector_event_type = 2,
                    // VECTOR_PROCESS_COMPONENTS_CLOSED
                    service_event = 5,
                    gracefully_closed = false,
                    internal_log_rate_limit = false,
                );

                // See the matching comment in the two-wave branch above.
                tokio::time::sleep(Duration::from_millis(50)).await;
                deferred_shutdowns.shutdown_all(deadline).await;
            };

            futures::future::join(source_shutdown_complete, shutdown_complete_future)
                .map(|_| ())
                .boxed()
        }
    }

    /// Computes the set of component keys that are exclusively downstream of non-deferred
    /// sources. A component is "exclusively non-deferred" if every path from it back to a
    /// source ends at a non-deferred source — i.e., it has NO deferred source anywhere in
    /// its transitive input ancestry.
    ///
    /// These components can be safely drained in wave 1 since their only inputs come from
    /// non-deferred sources that are being shut down. Components with any deferred ancestor
    /// stay alive until wave 2.
    fn compute_exclusively_non_deferred_components(
        config: &Config,
        deferred_source_keys: &HashSet<ComponentKey>,
    ) -> HashSet<ComponentKey> {
        // Start with all non-deferred sources.
        let mut non_deferred: HashSet<ComponentKey> = config
            .sources()
            .map(|(k, _)| k.clone())
            .filter(|k| !deferred_source_keys.contains(k))
            .collect();

        // Iteratively add transforms/sinks whose inputs are ALL in the non-deferred set.
        loop {
            let mut changed = false;

            for (key, transform) in config.transforms() {
                if non_deferred.contains(key) {
                    continue;
                }
                if !transform.inputs.is_empty()
                    && transform
                        .inputs
                        .iter()
                        .all(|input| non_deferred.contains(&input.component))
                {
                    non_deferred.insert(key.clone());
                    changed = true;
                }
            }

            for (key, sink) in config.sinks() {
                if non_deferred.contains(key) {
                    continue;
                }
                if !sink.inputs.is_empty()
                    && sink
                        .inputs
                        .iter()
                        .all(|input| non_deferred.contains(&input.component))
                {
                    non_deferred.insert(key.clone());
                    changed = true;
                }
            }

            if !changed {
                break;
            }
        }

        non_deferred
    }

    /// Attempts to load a new configuration and update this running topology.
    ///
    /// If the new configuration was valid, and all changes were able to be made -- removing of
    /// old components, changing of existing components, adding of new components -- then
    /// `Ok(())` is returned.
    ///
    /// If the new configuration is not valid, or not all of the changes in the new configuration
    /// were able to be made, then this method will attempt to undo the changes made and bring the
    /// topology back to its previous state, returning the appropriate error.
    ///
    /// If the restore also fails, `ReloadError::FailedToRestore` is returned.
    pub async fn reload_config_and_respawn(
        &mut self,
        new_config: Config,
        extra_context: ExtraContext,
    ) -> Result<(), ReloadError> {
        info!("Reloading running topology with new configuration.");

        if self.config.global != new_config.global {
            return match self.config.global.diff(&new_config.global) {
                Ok(changed_fields) => Err(ReloadError::GlobalOptionsChanged { changed_fields }),
                Err(source) => Err(ReloadError::GlobalDiffFailed { source }),
            };
        }

        // Calculate the change between the current configuration and the new configuration, and
        // shutdown any components that are changing so that we can reclaim their buffers before
        // spawning the new version of the component.
        //
        // We also shutdown any component that is simply being removed entirely.
        let diff = if let Some(components) = &self.pending_reload {
            ConfigDiff::new(&self.config, &new_config, components.clone())
        } else {
            ConfigDiff::new(&self.config, &new_config, HashSet::new())
        };
        let buffers = self.shutdown_diff(&diff, &new_config).await;

        // Gives windows some time to make available any port
        // released by shutdown components.
        // Issue: https://github.com/vectordotdev/vector/issues/3035
        if cfg!(windows) {
            // This value is guess work.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Try to build all of the new components coming from the new configuration.  If we can
        // successfully build them, we'll attempt to connect them up to the topology and spawn their
        // respective component tasks.
        if let Some(mut new_pieces) = TopologyPiecesBuilder::new(&new_config, &diff)
            .with_buffers(buffers.clone())
            .with_extra_context(extra_context.clone())
            .with_utilization_registry(self.utilization_registry.clone())
            .build_or_log_errors()
            .await
        {
            // If healthchecks are configured for any of the changing/new components, try running
            // them before moving forward with connecting and spawning.  In some cases, healthchecks
            // failing may be configured as a non-blocking issue and so we'll still continue on.
            if self
                .run_healthchecks(&diff, &mut new_pieces, new_config.healthchecks)
                .await
            {
                self.connect_diff(&diff, &mut new_pieces).await;
                self.spawn_diff(&diff, new_pieces);
                self.config = new_config;

                info!("New configuration loaded successfully.");

                return Ok(());
            }
        }

        // We failed to build, connect, and spawn all of the changed/new components, so we flip
        // around the configuration differential to generate all the components that we need to
        // bring back to restore the current configuration.
        warn!("Failed to completely load new configuration. Restoring old configuration.");

        let diff = diff.flip();
        if let Some(mut new_pieces) = TopologyPiecesBuilder::new(&self.config, &diff)
            .with_buffers(buffers)
            .with_extra_context(extra_context.clone())
            .with_utilization_registry(self.utilization_registry.clone())
            .build_or_log_errors()
            .await
            && self
                .run_healthchecks(&diff, &mut new_pieces, self.config.healthchecks)
                .await
        {
            self.connect_diff(&diff, &mut new_pieces).await;
            self.spawn_diff(&diff, new_pieces);

            info!("Old configuration restored successfully.");

            return Err(ReloadError::TopologyBuildFailed);
        }

        error!(
            message = "Failed to restore old configuration.",
            internal_log_rate_limit = false
        );

        Err(ReloadError::FailedToRestore)
    }

    /// Attempts to reload enrichment tables.
    pub(crate) async fn reload_enrichment_tables(&self) {
        reload_enrichment_tables(&self.config).await;
    }

    pub(crate) async fn run_healthchecks(
        &mut self,
        diff: &ConfigDiff,
        pieces: &mut TopologyPieces,
        options: HealthcheckOptions,
    ) -> bool {
        if options.enabled {
            let healthchecks = take_healthchecks(diff, pieces)
                .into_iter()
                .map(|(_, task)| task);
            let healthchecks = future::try_join_all(healthchecks);

            info!("Running healthchecks.");
            if options.require_healthy {
                let success = healthchecks.await;

                if success.is_ok() {
                    info!("All healthchecks passed.");
                    true
                } else {
                    error!(
                        message = "Sinks unhealthy.",
                        internal_log_rate_limit = false
                    );
                    false
                }
            } else {
                tokio::spawn(healthchecks);
                true
            }
        } else {
            true
        }
    }

    /// Shuts down any changed/removed component in the given configuration diff.
    ///
    /// If buffers for any of the changed/removed components can be recovered, they'll be returned.
    async fn shutdown_diff(
        &mut self,
        diff: &ConfigDiff,
        new_config: &Config,
    ) -> HashMap<ComponentKey, BuiltBuffer> {
        // First, we shutdown any changed/removed sources. This ensures that we can allow downstream
        // components to terminate naturally by virtue of the flow of events stopping.
        if diff.sources.any_changed_or_removed() {
            let timeout = Duration::from_secs(30);
            let mut source_shutdown_handles = Vec::new();

            let deadline = Instant::now() + timeout;
            for key in &diff.sources.to_remove {
                debug!(component_id = %key, "Removing source.");

                let previous = self.tasks.remove(key).unwrap();
                drop(previous); // detach and forget

                self.remove_outputs(key);
                source_shutdown_handles
                    .push(self.shutdown_coordinator.shutdown_source(key, deadline));
            }

            for key in &diff.sources.to_change {
                debug!(component_id = %key, "Changing source.");

                self.remove_outputs(key);
                source_shutdown_handles
                    .push(self.shutdown_coordinator.shutdown_source(key, deadline));
            }

            debug!(
                "Waiting for up to {} seconds for source(s) to finish shutting down.",
                timeout.as_secs()
            );
            futures::future::join_all(source_shutdown_handles).await;

            // Final cleanup pass now that all changed/removed sources have signalled as having shutdown.
            for key in diff.sources.removed_and_changed() {
                if let Some(task) = self.source_tasks.remove(key) {
                    task.await.unwrap().unwrap();
                }
            }
        }

        // Next, we shutdown any changed/removed transforms.  Same as before: we want allow
        // downstream components to terminate naturally by virtue of the flow of events stopping.
        //
        // Since transforms are entirely driven by the flow of events into them from upstream
        // components, the shutdown of sources they depend on, or the shutdown of transforms they
        // depend on, and thus the closing of their buffer, will naturally cause them to shutdown,
        // which is why we don't do any manual triggering of shutdown here.
        for key in &diff.transforms.to_remove {
            debug!(component_id = %key, "Removing transform.");

            let previous = self.tasks.remove(key).unwrap();
            drop(previous); // detach and forget

            self.remove_inputs(key, diff, new_config).await;
            self.remove_outputs(key);

            if let Some(registry) = self.utilization_registry.as_ref() {
                registry.remove_component(key);
            }
        }

        for key in &diff.transforms.to_change {
            debug!(component_id = %key, "Changing transform.");

            self.remove_inputs(key, diff, new_config).await;
            self.remove_outputs(key);
        }

        // Now we'll process any changed/removed sinks.
        //
        // At this point both the old and the new config don't have conflicts in their resource
        // usage. So if we combine their resources, all found conflicts are between to be removed
        // and to be added components.
        let removed_table_sinks = diff
            .enrichment_tables
            .removed_and_changed()
            .filter_map(|key| {
                self.config
                    .enrichment_table(key)
                    .and_then(|t| t.as_sink(key))
                    .map(|(key, s)| (key.clone(), s.resources(&key)))
            })
            .collect::<Vec<_>>();
        let remove_sink = diff
            .sinks
            .removed_and_changed()
            .map(|key| {
                (
                    key,
                    self.config
                        .sink(key)
                        .map(|s| s.resources(key))
                        .unwrap_or_default(),
                )
            })
            .chain(removed_table_sinks.iter().map(|(k, s)| (k, s.clone())));
        let add_source = diff
            .sources
            .changed_and_added()
            .map(|key| (key, new_config.source(key).unwrap().inner.resources()));
        let added_table_sinks = diff
            .enrichment_tables
            .changed_and_added()
            .filter_map(|key| {
                self.config
                    .enrichment_table(key)
                    .and_then(|t| t.as_sink(key))
                    .map(|(key, s)| (key.clone(), s.resources(&key)))
            })
            .collect::<Vec<_>>();
        let add_sink = diff
            .sinks
            .changed_and_added()
            .map(|key| {
                (
                    key,
                    new_config
                        .sink(key)
                        .map(|s| s.resources(key))
                        .unwrap_or_default(),
                )
            })
            .chain(added_table_sinks.iter().map(|(k, s)| (k, s.clone())));
        let conflicts = Resource::conflicts(
            remove_sink.map(|(key, value)| ((true, key), value)).chain(
                add_sink
                    .chain(add_source)
                    .map(|(key, value)| ((false, key), value)),
            ),
        )
        .into_iter()
        .flat_map(|(_, components)| components)
        .collect::<HashSet<_>>();
        // Existing conflicting sinks
        let conflicting_sinks = conflicts
            .into_iter()
            .filter(|&(existing_sink, _)| existing_sink)
            .map(|(_, key)| key.clone());

        // For any sink whose buffer configuration didn't change, we can reuse their buffer.
        let reuse_buffers = diff
            .sinks
            .to_change
            .iter()
            .filter(|&key| {
                if diff.components_to_reload.contains(key) {
                    return false;
                }
                self.config.sink(key).map(|s| s.buffer.clone()).or_else(|| {
                    self.config
                        .enrichment_table(key)
                        .and_then(|t| t.as_sink(key))
                        .map(|(_, s)| s.buffer)
                }) == new_config.sink(key).map(|s| s.buffer.clone()).or_else(|| {
                    self.config
                        .enrichment_table(key)
                        .and_then(|t| t.as_sink(key))
                        .map(|(_, s)| s.buffer)
                })
            })
            .cloned()
            .collect::<HashSet<_>>();

        // For any existing sink that has a conflicting resource dependency with a changed/added
        // sink, or for any sink that we want to reuse their buffer, we need to explicit wait for
        // them to finish processing so we can reclaim ownership of those resources/buffers.
        let wait_for_sinks = conflicting_sinks
            .chain(reuse_buffers.iter().cloned())
            .collect::<HashSet<_>>();

        // First, we remove any inputs to removed sinks so they can naturally shut down.
        let removed_sinks = diff
            .sinks
            .to_remove
            .iter()
            .chain(diff.enrichment_tables.to_remove.iter().filter(|key| {
                self.config
                    .enrichment_table(key)
                    .and_then(|t| t.as_sink(key))
                    .is_some()
            }))
            .collect::<Vec<_>>();
        for key in &removed_sinks {
            debug!(component_id = %key, "Removing sink.");
            self.remove_inputs(key, diff, new_config).await;

            if let Some(registry) = self.utilization_registry.as_ref() {
                registry.remove_component(key);
            }
        }

        // After that, for any changed sinks, we temporarily detach their inputs (not remove) so
        // they can naturally shutdown and allow us to recover their buffers if possible.
        let mut buffer_tx = HashMap::new();

        let sinks_to_change = diff
            .sinks
            .to_change
            .iter()
            .chain(diff.enrichment_tables.to_change.iter().filter(|key| {
                self.config
                    .enrichment_table(key)
                    .and_then(|t| t.as_sink(key))
                    .is_some()
            }))
            .collect::<Vec<_>>();

        for key in &sinks_to_change {
            debug!(component_id = %key, "Changing sink.");
            if reuse_buffers.contains(key) {
                self.detach_triggers
                    .remove(key)
                    .unwrap()
                    .into_inner()
                    .cancel();

                // We explicitly clone the input side of the buffer and store it so we don't lose
                // it when we remove the inputs below.
                //
                // We clone instead of removing here because otherwise the input will be missing for
                // the rest of the reload process, which violates the assumption that all previous
                // inputs for components not being removed are still available. It's simpler to
                // allow the "old" input to stick around and be replaced (even though that's
                // basically a no-op since we're reusing the same buffer) than it is to pass around
                // info about which sinks are having their buffers reused and treat them differently
                // at other stages.
                buffer_tx.insert((*key).clone(), self.inputs.get(key).unwrap().clone());
            }
            self.remove_inputs(key, diff, new_config).await;
        }

        // Now that we've disconnected or temporarily detached the inputs to all changed/removed
        // sinks, we can actually wait for them to shutdown before collecting any buffers that are
        // marked for reuse.
        //
        // If a sink we're removing isn't tying up any resource that a changed/added sink depends
        // on, we don't bother waiting for it to shutdown.
        for key in &removed_sinks {
            let previous = self.tasks.remove(key).unwrap();
            if wait_for_sinks.contains(key) {
                debug!(message = "Waiting for sink to shutdown.", component_id = %key);
                previous.await.unwrap().unwrap();
            } else {
                drop(previous); // detach and forget
            }
        }

        let mut buffers = HashMap::<ComponentKey, BuiltBuffer>::new();
        for key in &sinks_to_change {
            if wait_for_sinks.contains(key) {
                let previous = self.tasks.remove(key).unwrap();
                debug!(message = "Waiting for sink to shutdown.", component_id = %key);
                let buffer = previous.await.unwrap().unwrap();

                if reuse_buffers.contains(key) {
                    // We clone instead of removing here because otherwise the input will be
                    // missing for the rest of the reload process, which violates the assumption
                    // that all previous inputs for components not being removed are still
                    // available. It's simpler to allow the "old" input to stick around and be
                    // replaced (even though that's basically a no-op since we're reusing the same
                    // buffer) than it is to pass around info about which sinks are having their
                    // buffers reused and treat them differently at other stages.
                    let tx = buffer_tx.remove(key).unwrap();
                    let rx = match buffer {
                        TaskOutput::Sink(rx) => rx.into_inner(),
                        _ => unreachable!(),
                    };

                    buffers.insert((*key).clone(), (tx, Arc::new(Mutex::new(Some(rx)))));
                }
            }
        }

        buffers
    }

    /// Connects all changed/added components in the given configuration diff.
    pub(crate) async fn connect_diff(
        &mut self,
        diff: &ConfigDiff,
        new_pieces: &mut TopologyPieces,
    ) {
        debug!("Connecting changed/added component(s).");

        // Update tap metadata
        if !self.watch.0.is_closed() {
            for key in &diff.sources.to_remove {
                // Sources only have outputs
                self.outputs_tap_metadata.remove(key);
            }

            for key in &diff.transforms.to_remove {
                // Transforms can have both inputs and outputs
                self.outputs_tap_metadata.remove(key);
                self.inputs_tap_metadata.remove(key);
            }

            for key in &diff.sinks.to_remove {
                // Sinks only have inputs
                self.inputs_tap_metadata.remove(key);
            }

            let removed_sinks = diff.enrichment_tables.to_remove.iter().filter(|key| {
                self.config
                    .enrichment_table(key)
                    .and_then(|t| t.as_sink(key))
                    .is_some()
            });
            for key in removed_sinks {
                // Sinks only have inputs
                self.inputs_tap_metadata.remove(key);
            }

            let removed_sources = diff.enrichment_tables.to_remove.iter().filter_map(|key| {
                self.config
                    .enrichment_table(key)
                    .and_then(|t| t.as_source(key).map(|(key, _)| key))
            });
            for key in removed_sources {
                // Sources only have outputs
                self.outputs_tap_metadata.remove(&key);
            }

            for key in diff.sources.changed_and_added() {
                if let Some(task) = new_pieces.tasks.get(key) {
                    self.outputs_tap_metadata
                        .insert(key.clone(), ("source", task.typetag().to_string()));
                }
            }

            for key in diff
                .enrichment_tables
                .changed_and_added()
                .filter_map(|key| {
                    self.config
                        .enrichment_table(key)
                        .and_then(|t| t.as_source(key).map(|(key, _)| key))
                })
            {
                if let Some(task) = new_pieces.tasks.get(&key) {
                    self.outputs_tap_metadata
                        .insert(key.clone(), ("source", task.typetag().to_string()));
                }
            }

            for key in diff.transforms.changed_and_added() {
                if let Some(task) = new_pieces.tasks.get(key) {
                    self.outputs_tap_metadata
                        .insert(key.clone(), ("transform", task.typetag().to_string()));
                }
            }

            for (key, input) in &new_pieces.inputs {
                self.inputs_tap_metadata
                    .insert(key.clone(), input.1.clone());
            }
        }

        // We configure the outputs of any changed/added sources first, so they're available to any
        // transforms and sinks that come afterwards.
        for key in diff.sources.changed_and_added() {
            debug!(component_id = %key, "Configuring outputs for source.");
            self.setup_outputs(key, new_pieces).await;
        }

        let added_changed_table_sources: Vec<&ComponentKey> = diff
            .enrichment_tables
            .changed_and_added()
            .filter(|k| new_pieces.source_tasks.contains_key(k))
            .collect();
        for key in added_changed_table_sources.iter() {
            debug!(component_id = %key, "Connecting outputs for enrichment table source.");
            self.setup_outputs(key, new_pieces).await;
        }

        // We configure the outputs of any changed/added transforms next, for the same reason: we
        // need them to be available to any transforms and sinks that come afterwards.
        for key in diff.transforms.changed_and_added() {
            debug!(component_id = %key, "Configuring outputs for transform.");
            self.setup_outputs(key, new_pieces).await;
        }

        // Now that all possible outputs are configured, we can start wiring up inputs, starting
        // with transforms.
        for key in diff.transforms.changed_and_added() {
            debug!(component_id = %key, "Connecting inputs for transform.");
            self.setup_inputs(key, diff, new_pieces).await;
        }

        // Now that all sources and transforms are fully configured, we can wire up sinks.
        for key in diff.sinks.changed_and_added() {
            debug!(component_id = %key, "Connecting inputs for sink.");
            self.setup_inputs(key, diff, new_pieces).await;
        }
        let added_changed_tables: Vec<&ComponentKey> = diff
            .enrichment_tables
            .changed_and_added()
            .filter(|k| new_pieces.inputs.contains_key(k))
            .collect();
        for key in added_changed_tables.iter() {
            debug!(component_id = %key, "Connecting inputs for enrichment table sink.");
            self.setup_inputs(key, diff, new_pieces).await;
        }

        // We do a final pass here to reconnect unchanged components.
        //
        // Why would we reconnect unchanged components?  Well, as sources and transforms will
        // recreate their fanouts every time they're changed, we can run into a situation where a
        // transform/sink, which we'll call B, is pointed at a source/transform that was changed, which
        // we'll call A, but because B itself didn't change at all, we haven't yet reconnected it.
        //
        // Instead of propagating connections forward -- B reconnecting A forcefully -- we only
        // connect components backwards i.e. transforms to sources/transforms, and sinks to
        // sources/transforms, to ensure we're connecting components in order.
        self.reattach_severed_inputs(diff);

        // Broadcast any topology changes to subscribers.
        if !self.watch.0.is_closed() {
            let outputs = self
                .outputs
                .clone()
                .into_iter()
                .flat_map(|(output_id, control_tx)| {
                    self.outputs_tap_metadata.get(&output_id.component).map(
                        |(component_kind, component_type)| {
                            (
                                TapOutput {
                                    output_id,
                                    component_kind,
                                    component_type: component_type.clone(),
                                },
                                control_tx,
                            )
                        },
                    )
                })
                .collect::<HashMap<_, _>>();

            let mut removals = diff.sources.to_remove.clone();
            removals.extend(diff.transforms.to_remove.iter().cloned());
            self.watch
                .0
                .send(TapResource {
                    outputs,
                    inputs: self.inputs_tap_metadata.clone(),
                    source_keys: diff
                        .sources
                        .changed_and_added()
                        .map(|key| key.to_string())
                        .chain(
                            added_changed_table_sources
                                .iter()
                                .map(|key| key.to_string()),
                        )
                        .collect(),
                    sink_keys: diff
                        .sinks
                        .changed_and_added()
                        .map(|key| key.to_string())
                        .chain(added_changed_tables.iter().map(|key| key.to_string()))
                        .collect(),
                    // Note, only sources and transforms are relevant. Sinks do
                    // not have outputs to tap.
                    removals,
                })
                .expect("Couldn't broadcast config changes.");
        }
    }

    async fn setup_outputs(
        &mut self,
        key: &ComponentKey,
        new_pieces: &mut builder::TopologyPieces,
    ) {
        let outputs = new_pieces.outputs.remove(key).unwrap();
        for (port, output) in outputs {
            debug!(component_id = %key, output_id = ?port, "Configuring output for component.");

            let id = OutputId {
                component: key.clone(),
                port,
            };

            self.outputs.insert(id, output);
        }
    }

    async fn setup_inputs(
        &mut self,
        key: &ComponentKey,
        diff: &ConfigDiff,
        new_pieces: &mut builder::TopologyPieces,
    ) {
        let (tx, inputs) = new_pieces.inputs.remove(key).unwrap();

        let old_inputs = self
            .config
            .inputs_for_node(key)
            .into_iter()
            .flatten()
            .cloned()
            .collect::<HashSet<_>>();

        let new_inputs = inputs.iter().cloned().collect::<HashSet<_>>();
        let inputs_to_add = &new_inputs - &old_inputs;

        for input in inputs {
            let output = self.outputs.get_mut(&input).expect("unknown output");

            if diff.contains(&input.component) || inputs_to_add.contains(&input) {
                // If the input we're connecting to is changing, that means its outputs will have been
                // recreated, so instead of replacing a paused sink, we have to add it to this new
                // output for the first time, since there's nothing to actually replace at this point.
                debug!(component_id = %key, fanout_id = %input, "Adding component input to fanout.");

                _ = output.send(ControlMessage::Add(key.clone(), tx.clone()));
            } else {
                // We know that if this component is connected to a given input, and neither
                // components were changed, then the output must still exist, which means we paused
                // this component's connection to its output, so we have to replace that connection
                // now:
                debug!(component_id = %key, fanout_id = %input, "Replacing component input in fanout.");

                _ = output.send(ControlMessage::Replace(key.clone(), tx.clone()));
            }
        }

        self.inputs.insert(key.clone(), tx);
        new_pieces
            .detach_triggers
            .remove(key)
            .map(|trigger| self.detach_triggers.insert(key.clone(), trigger.into()));
    }

    fn remove_outputs(&mut self, key: &ComponentKey) {
        self.outputs.retain(|id, _output| &id.component != key);
    }

    async fn remove_inputs(&mut self, key: &ComponentKey, diff: &ConfigDiff, new_config: &Config) {
        self.inputs.remove(key);
        self.detach_triggers.remove(key);

        let old_inputs = self.config.inputs_for_node(key).expect("node exists");
        let new_inputs = new_config
            .inputs_for_node(key)
            .unwrap_or_default()
            .iter()
            .collect::<HashSet<_>>();

        for input in old_inputs {
            if let Some(output) = self.outputs.get_mut(input) {
                if diff.contains(&input.component)
                    || diff.is_removed(key)
                    || !new_inputs.contains(input)
                {
                    // 3 cases to remove the input:
                    //
                    // Case 1: If the input we're removing ourselves from is changing, that means its
                    // outputs will be recreated, so instead of pausing the sink, we just delete it
                    // outright to ensure things are clean.
                    //
                    // Case 2: If this component itself is being removed, then pausing makes no sense
                    // because it isn't coming back.
                    //
                    // Case 3: This component is no longer connected to the input from new config.
                    debug!(component_id = %key, fanout_id = %input, "Removing component input from fanout.");

                    _ = output.send(ControlMessage::Remove(key.clone()));
                } else {
                    // We know that if this component is connected to a given input, and it isn't being
                    // changed, then it will exist when we reconnect inputs, so we should pause it
                    // now to pause further sends through that component until we reconnect:
                    debug!(component_id = %key, fanout_id = %input, "Pausing component input in fanout.");

                    _ = output.send(ControlMessage::Pause(key.clone()));
                }
            }
        }
    }

    fn reattach_severed_inputs(&mut self, diff: &ConfigDiff) {
        let unchanged_transforms = self
            .config
            .transforms()
            .filter(|(key, _)| !diff.transforms.contains(key));
        for (transform_key, transform) in unchanged_transforms {
            let changed_outputs = get_changed_outputs(diff, transform.inputs.clone());
            for output_id in changed_outputs {
                debug!(component_id = %transform_key, fanout_id = %output_id.component, "Reattaching component input to fanout.");

                let input = self.inputs.get(transform_key).cloned().unwrap();
                let output = self.outputs.get_mut(&output_id).unwrap();
                _ = output.send(ControlMessage::Add(transform_key.clone(), input));
            }
        }

        let unchanged_sinks = self
            .config
            .sinks()
            .filter(|(key, _)| !diff.sinks.contains(key));
        for (sink_key, sink) in unchanged_sinks {
            let changed_outputs = get_changed_outputs(diff, sink.inputs.clone());
            for output_id in changed_outputs {
                debug!(component_id = %sink_key, fanout_id = %output_id.component, "Reattaching component input to fanout.");

                let input = self.inputs.get(sink_key).cloned().unwrap();
                let output = self.outputs.get_mut(&output_id).unwrap();
                _ = output.send(ControlMessage::Add(sink_key.clone(), input));
            }
        }
    }

    /// Starts any new or changed components in the given configuration diff.
    pub(crate) fn spawn_diff(&mut self, diff: &ConfigDiff, mut new_pieces: TopologyPieces) {
        for key in &diff.sources.to_change {
            debug!(message = "Spawning changed source.", component_id = %key);
            self.spawn_source(key, &mut new_pieces);
        }

        for key in &diff.sources.to_add {
            debug!(message = "Spawning new source.", component_id = %key);
            self.spawn_source(key, &mut new_pieces);
        }

        let changed_table_sources: Vec<&ComponentKey> = diff
            .enrichment_tables
            .to_change
            .iter()
            .filter(|k| new_pieces.source_tasks.contains_key(k))
            .collect();

        let added_table_sources: Vec<&ComponentKey> = diff
            .enrichment_tables
            .to_add
            .iter()
            .filter(|k| new_pieces.source_tasks.contains_key(k))
            .collect();

        for key in changed_table_sources {
            debug!(message = "Spawning changed enrichment table source.", component_id = %key);
            self.spawn_source(key, &mut new_pieces);
        }

        for key in added_table_sources {
            debug!(message = "Spawning new enrichment table source.", component_id = %key);
            self.spawn_source(key, &mut new_pieces);
        }

        for key in &diff.transforms.to_change {
            debug!(message = "Spawning changed transform.", component_id = %key);
            self.spawn_transform(key, &mut new_pieces);
        }

        for key in &diff.transforms.to_add {
            debug!(message = "Spawning new transform.", component_id = %key);
            self.spawn_transform(key, &mut new_pieces);
        }

        for key in &diff.sinks.to_change {
            debug!(message = "Spawning changed sink.", component_id = %key);
            self.spawn_sink(key, &mut new_pieces);
        }

        for key in &diff.sinks.to_add {
            trace!(message = "Spawning new sink.", component_id = %key);
            self.spawn_sink(key, &mut new_pieces);
        }

        let changed_tables: Vec<&ComponentKey> = diff
            .enrichment_tables
            .to_change
            .iter()
            .filter(|k| {
                new_pieces.tasks.contains_key(k) && !new_pieces.source_tasks.contains_key(k)
            })
            .collect();

        let added_tables: Vec<&ComponentKey> = diff
            .enrichment_tables
            .to_add
            .iter()
            .filter(|k| {
                new_pieces.tasks.contains_key(k) && !new_pieces.source_tasks.contains_key(k)
            })
            .collect();

        for key in changed_tables {
            debug!(message = "Spawning changed enrichment table sink.", component_id = %key);
            self.spawn_sink(key, &mut new_pieces);
        }

        for key in added_tables {
            debug!(message = "Spawning enrichment table new sink.", component_id = %key);
            self.spawn_sink(key, &mut new_pieces);
        }
    }

    fn spawn_sink(&mut self, key: &ComponentKey, new_pieces: &mut builder::TopologyPieces) {
        let task = new_pieces.tasks.remove(key).unwrap();
        let span = error_span!(
            "sink",
            component_kind = "sink",
            component_id = %task.id(),
            component_type = %task.typetag(),
        );

        let task_span = span.or_current();
        #[cfg(feature = "allocation-tracing")]
        if crate::internal_telemetry::allocations::is_allocation_tracing_enabled() {
            let group_id = crate::internal_telemetry::allocations::acquire_allocation_group_id(
                task.id().to_string(),
                "sink".to_string(),
                task.typetag().to_string(),
            );
            debug!(
                component_kind = "sink",
                component_type = task.typetag(),
                component_id = task.id(),
                group_id = group_id.as_raw().to_string(),
                "Registered new allocation group."
            );
            group_id.attach_to_span(&task_span);
        }

        let task_name = format!(">> {} ({})", task.typetag(), task.id());
        let task = {
            let key = key.clone();
            handle_errors(task, self.abort_tx.clone(), |error| {
                ShutdownError::SinkAborted { key, error }
            })
        }
        .instrument(task_span);
        let spawned = spawn_named(task, task_name.as_ref());
        if let Some(previous) = self.tasks.insert(key.clone(), spawned) {
            drop(previous); // detach and forget
        }
    }

    fn spawn_transform(&mut self, key: &ComponentKey, new_pieces: &mut builder::TopologyPieces) {
        let task = new_pieces.tasks.remove(key).unwrap();
        let span = error_span!(
            "transform",
            component_kind = "transform",
            component_id = %task.id(),
            component_type = %task.typetag(),
        );

        let task_span = span.or_current();
        #[cfg(feature = "allocation-tracing")]
        if crate::internal_telemetry::allocations::is_allocation_tracing_enabled() {
            let group_id = crate::internal_telemetry::allocations::acquire_allocation_group_id(
                task.id().to_string(),
                "transform".to_string(),
                task.typetag().to_string(),
            );
            debug!(
                component_kind = "transform",
                component_type = task.typetag(),
                component_id = task.id(),
                group_id = group_id.as_raw().to_string(),
                "Registered new allocation group."
            );
            group_id.attach_to_span(&task_span);
        }

        let task_name = format!(">> {} ({}) >>", task.typetag(), task.id());
        let task = {
            let key = key.clone();
            handle_errors(task, self.abort_tx.clone(), |error| {
                ShutdownError::TransformAborted { key, error }
            })
        }
        .instrument(task_span);
        let spawned = spawn_named(task, task_name.as_ref());
        if let Some(previous) = self.tasks.insert(key.clone(), spawned) {
            drop(previous); // detach and forget
        }
    }

    fn spawn_source(&mut self, key: &ComponentKey, new_pieces: &mut builder::TopologyPieces) {
        let task = new_pieces.tasks.remove(key).unwrap();
        let span = error_span!(
            "source",
            component_kind = "source",
            component_id = %task.id(),
            component_type = %task.typetag(),
        );

        let task_span = span.or_current();
        #[cfg(feature = "allocation-tracing")]
        if crate::internal_telemetry::allocations::is_allocation_tracing_enabled() {
            let group_id = crate::internal_telemetry::allocations::acquire_allocation_group_id(
                task.id().to_string(),
                "source".to_string(),
                task.typetag().to_string(),
            );

            debug!(
                component_kind = "source",
                component_type = task.typetag(),
                component_id = task.id(),
                group_id = group_id.as_raw().to_string(),
                "Registered new allocation group."
            );
            group_id.attach_to_span(&task_span);
        }

        let task_name = format!("{} ({}) >>", task.typetag(), task.id());
        let task = {
            let key = key.clone();
            handle_errors(task, self.abort_tx.clone(), |error| {
                ShutdownError::SourceAborted { key, error }
            })
        }
        .instrument(task_span.clone());
        let spawned = spawn_named(task, task_name.as_ref());
        if let Some(previous) = self.tasks.insert(key.clone(), spawned) {
            drop(previous); // detach and forget
        }

        self.shutdown_coordinator
            .takeover_source(key, &mut new_pieces.shutdown_coordinator);

        // Now spawn the actual source task.
        let source_task = new_pieces.source_tasks.remove(key).unwrap();
        let source_task = {
            let key = key.clone();
            handle_errors(source_task, self.abort_tx.clone(), |error| {
                ShutdownError::SourceAborted { key, error }
            })
        }
        .instrument(task_span);
        self.source_tasks
            .insert(key.clone(), spawn_named(source_task, task_name.as_ref()));
    }

    pub async fn start_init_validated(
        config: Config,
        extra_context: ExtraContext,
    ) -> Option<(Self, ShutdownErrorReceiver)> {
        let diff = ConfigDiff::initial(&config);
        let pieces = TopologyPiecesBuilder::new(&config, &diff)
            .with_extra_context(extra_context)
            .build_or_log_errors()
            .await?;
        Self::start_validated(config, diff, pieces).await
    }

    pub async fn start_validated(
        config: Config,
        diff: ConfigDiff,
        mut pieces: TopologyPieces,
    ) -> Option<(Self, ShutdownErrorReceiver)> {
        let (abort_tx, abort_rx) = mpsc::unbounded_channel();

        let expire_metrics = match (
            config.global.expire_metrics,
            config.global.expire_metrics_secs,
        ) {
            (Some(e), None) => {
                warn!(
                    "DEPRECATED: `expire_metrics` setting is deprecated and will be removed in a future version. Use `expire_metrics_secs` instead."
                );
                if e < Duration::from_secs(0) {
                    None
                } else {
                    Some(e.as_secs_f64())
                }
            }
            (Some(_), Some(_)) => {
                error!(
                    message = "Cannot set both `expire_metrics` and `expire_metrics_secs`.",
                    internal_log_rate_limit = false
                );
                return None;
            }
            (None, Some(e)) => {
                if e < 0f64 {
                    None
                } else {
                    Some(e)
                }
            }
            (None, None) => Some(300f64),
        };

        if let Err(error) = crate::metrics::Controller::get()
            .expect("Metrics must be initialized")
            .set_expiry(
                expire_metrics,
                config
                    .global
                    .expire_metrics_per_metric_set
                    .clone()
                    .unwrap_or_default(),
            )
        {
            error!(message = "Invalid metrics expiry.", %error, internal_log_rate_limit = false);
            return None;
        }

        let (utilization_emitter, utilization_registry) = pieces
            .utilization
            .take()
            .expect("Topology is missing the utilization metric emitter!");
        let metrics_storage = pieces.metrics_storage.clone();
        let metrics_refresh_period = config
            .global
            .metrics_storage_refresh_period
            .map(Duration::from_secs_f64);
        let mut running_topology = Self::new(config, abort_tx);

        if !running_topology
            .run_healthchecks(&diff, &mut pieces, running_topology.config.healthchecks)
            .await
        {
            return None;
        }
        running_topology.connect_diff(&diff, &mut pieces).await;
        running_topology.spawn_diff(&diff, pieces);

        let (utilization_task_shutdown_trigger, utilization_shutdown_signal, _) =
            ShutdownSignal::new_wired();
        running_topology.utilization_registry = Some(utilization_registry.clone());
        running_topology.utilization_task_shutdown_trigger =
            Some(utilization_task_shutdown_trigger);
        running_topology.utilization_task = Some(tokio::spawn(Task::new(
            "utilization_heartbeat".into(),
            "",
            async move {
                utilization_emitter
                    .run_utilization(utilization_shutdown_signal)
                    .await;
                Ok(TaskOutput::Healthcheck)
            },
        )));
        if let Some(metrics_refresh_period) = metrics_refresh_period {
            let (metrics_task_shutdown_trigger, metrics_shutdown_signal, _) =
                ShutdownSignal::new_wired();
            running_topology.metrics_task_shutdown_trigger = Some(metrics_task_shutdown_trigger);
            running_topology.metrics_task = Some(tokio::spawn(Task::new(
                "metrics_heartbeat".into(),
                "",
                async move {
                    metrics_storage
                        .run_periodic_refresh(metrics_refresh_period, metrics_shutdown_signal)
                        .await;
                    Ok(TaskOutput::Healthcheck)
                },
            )));
        }

        Some((running_topology, abort_rx))
    }
}

fn get_changed_outputs(diff: &ConfigDiff, output_ids: Inputs<OutputId>) -> Vec<OutputId> {
    let mut changed_outputs = Vec::new();

    for source_key in &diff.sources.to_change {
        changed_outputs.extend(
            output_ids
                .iter()
                .filter(|id| &id.component == source_key)
                .cloned(),
        );
    }

    for transform_key in &diff.transforms.to_change {
        changed_outputs.extend(
            output_ids
                .iter()
                .filter(|id| &id.component == transform_key)
                .cloned(),
        );
    }

    changed_outputs
}
