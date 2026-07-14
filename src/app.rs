#![allow(missing_docs)]
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(windows)]
use std::os::windows::process::ExitStatusExt;
use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    process::ExitStatus,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use exitcode::ExitCode;
use futures::StreamExt;
use tokio::{
    runtime::{self, Handle, Runtime},
    sync::{MutexGuard, broadcast::error::RecvError},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[cfg(feature = "api")]
use crate::{api, internal_events::ApiStarted};
use crate::{
    cli::{LogFormat, Opts, RootOpts, WatchConfigMethod, handle_config_errors},
    config::{self, ComponentConfig, ComponentType, Config, ConfigPath},
    extra_context::ExtraContext,
    heartbeat,
    internal_events::{VectorConfigLoadError, VectorQuit, VectorStarted, VectorStopped},
    signal::{SignalHandler, SignalPair, SignalRx, SignalTo},
    topology::{
        ReloadOutcome, RunningTopology, SharedTopologyController, ShutdownErrorReceiver,
        TopologyController,
    },
    trace,
};

static WORKER_THREADS: AtomicUsize = AtomicUsize::new(0);

pub fn worker_threads() -> Option<NonZeroUsize> {
    NonZeroUsize::new(WORKER_THREADS.load(Ordering::Relaxed))
}

pub struct ApplicationConfig {
    pub config_paths: Vec<config::ConfigPath>,
    pub topology: RunningTopology,
    pub graceful_crash_receiver: ShutdownErrorReceiver,
    pub internal_topologies: Vec<RunningTopology>,
    #[cfg(feature = "api")]
    pub api: config::api::Options,
    pub extra_context: ExtraContext,
}

pub struct Application {
    pub root_opts: RootOpts,
    pub config: ApplicationConfig,
    pub signals: SignalPair,
}

impl ApplicationConfig {
    pub async fn from_opts(
        opts: &RootOpts,
        signal_handler: &mut SignalHandler,
        extra_context: ExtraContext,
    ) -> Result<Self, ExitCode> {
        let config_paths = opts.config_paths_with_formats();

        let graceful_shutdown_duration = (!opts.no_graceful_shutdown_limit)
            .then(|| Duration::from_secs(u64::from(opts.graceful_shutdown_limit_secs)));

        // Derive the three staged sub-deadlines (data-source < data-sink < internal-source <
        // overall), silently clamping to preserve strict ordering. `None` for all three means
        // two-wave shutdown is disabled and the legacy single-pass path is used (which happens
        // when there is no overall limit, or the data-source limit is not below the overall
        // limit). See `staged_shutdown_secs`.
        let (
            graceful_data_source_shutdown_duration,
            graceful_data_sink_shutdown_duration,
            graceful_internal_source_shutdown_duration,
        ) = match graceful_shutdown_duration.and_then(|main| {
            staged_shutdown_secs(
                main.as_secs(),
                u64::from(opts.graceful_data_source_shutdown_limit_secs),
                u64::from(opts.graceful_data_sink_shutdown_limit_secs),
                u64::from(opts.graceful_internal_source_shutdown_limit_secs),
            )
        }) {
            Some((data_source, data_sink, internal_source)) => (
                Some(Duration::from_secs(data_source)),
                Some(Duration::from_secs(data_sink)),
                Some(Duration::from_secs(internal_source)),
            ),
            None => (None, None, None),
        };

        let watcher_conf = if opts.watch_config {
            Some(watcher_config(
                opts.watch_config_method,
                opts.watch_config_poll_interval_seconds,
            ))
        } else {
            None
        };

        let config = load_configs(
            &config_paths,
            watcher_conf,
            opts.require_healthy,
            opts.allow_empty_config,
            !opts.disable_env_var_interpolation,
            graceful_shutdown_duration,
            graceful_data_source_shutdown_duration,
            graceful_data_sink_shutdown_duration,
            graceful_internal_source_shutdown_duration,
            signal_handler,
        )
        .await?;

        Self::from_config(config_paths, config, extra_context).await
    }

    pub async fn from_config(
        config_paths: Vec<ConfigPath>,
        config: Config,
        extra_context: ExtraContext,
    ) -> Result<Self, ExitCode> {
        #[cfg(feature = "api")]
        let api = config.api;

        let (topology, graceful_crash_receiver) =
            RunningTopology::start_init_validated(config, extra_context.clone())
                .await
                .ok_or(exitcode::CONFIG)?;

        Ok(Self {
            config_paths,
            topology,
            graceful_crash_receiver,
            internal_topologies: Vec::new(),
            #[cfg(feature = "api")]
            api,
            extra_context,
        })
    }

    pub async fn add_internal_config(
        &mut self,
        config: Config,
        extra_context: ExtraContext,
    ) -> Result<(), ExitCode> {
        let Some((topology, _)) =
            RunningTopology::start_init_validated(config, extra_context).await
        else {
            return Err(exitcode::CONFIG);
        };
        self.internal_topologies.push(topology);
        Ok(())
    }

    /// Configure the API server, if applicable
    #[cfg(feature = "api")]
    pub fn setup_api(&self, handle: &Handle) -> Option<api::Server> {
        if self.api.enabled {
            match api::Server::start(
                self.topology.config(),
                self.topology.watch(),
                std::sync::Arc::clone(&self.topology.running),
                handle,
            ) {
                Ok(api_server) => {
                    emit!(ApiStarted {
                        addr: self.api.address.unwrap(),
                        playground: self.api.playground,
                        graphql: self.api.graphql
                    });

                    Some(api_server)
                }
                Err(error) => {
                    let error = error.to_string();
                    error!(message = "An error occurred that Vector couldn't handle.", %error, internal_log_rate_limit = false);
                    _ = self
                        .topology
                        .abort_tx
                        .send(crate::signal::ShutdownError::ApiFailed { error });
                    None
                }
            }
        } else {
            info!(
                message = "API is disabled, enable by setting `api.enabled` to `true` and use commands like `vector top`."
            );
            None
        }
    }
}

impl Application {
    pub fn run(extra_context: ExtraContext) -> ExitStatus {
        let (runtime, app) =
            Self::prepare_start(extra_context).unwrap_or_else(|code| std::process::exit(code));

        info!(
            message = "Started Vector instance.",
            // VECTOR_SERVICE_EVENT
            vector_event_type = 2,
            // VECTOR_PROCESS_START
            service_event = 1,
            internal_log_rate_limit = false,
        );
        runtime.block_on(app.run())
    }

    pub fn prepare_start(
        extra_context: ExtraContext,
    ) -> Result<(Runtime, StartedApplication), ExitCode> {
        Self::prepare(extra_context)
            .and_then(|(runtime, app)| app.start(runtime.handle()).map(|app| (runtime, app)))
    }

    pub fn prepare(extra_context: ExtraContext) -> Result<(Runtime, Self), ExitCode> {
        let opts = Opts::get_matches().map_err(|error| {
            // Printing to stdout/err can itself fail; ignore it.
            _ = error.print();
            exitcode::USAGE
        })?;

        Self::prepare_from_opts(opts, extra_context)
    }

    pub fn prepare_from_opts(
        opts: Opts,
        extra_context: ExtraContext,
    ) -> Result<(Runtime, Self), ExitCode> {
        opts.root.init_global();

        let color = opts.root.color.use_color();

        init_logging(
            color,
            opts.root.log_format,
            opts.log_level(),
            opts.root.internal_log_rate_limit,
        );

        // Set global color preference for downstream modules
        crate::set_global_color(color);

        // Can only log this after initializing the logging subsystem
        if opts.root.openssl_no_probe {
            debug!(
                message = "Disabled probing and configuration of root certificate locations on the system for OpenSSL."
            );
        }

        let runtime = build_runtime(opts.root.threads, "vector-worker")?;

        // Signal handler for OS and provider messages.
        let mut signals = SignalPair::new(&runtime);

        if let Some(sub_command) = &opts.sub_command {
            return Err(runtime.block_on(sub_command.execute(signals, color)));
        }

        let config = runtime.block_on(ApplicationConfig::from_opts(
            &opts.root,
            &mut signals.handler,
            extra_context,
        ))?;

        Ok((
            runtime,
            Self {
                root_opts: opts.root,
                config,
                signals,
            },
        ))
    }

    pub fn start(self, handle: &Handle) -> Result<StartedApplication, ExitCode> {
        // Any internal_logs sources will have grabbed a copy of the
        // early buffer by this point and set up a subscriber.
        crate::trace::stop_early_buffering();

        emit!(VectorStarted);
        handle.spawn(heartbeat::heartbeat());
        #[cfg(feature = "tikv-jemallocator")]
        handle.spawn(crate::jemalloc_stats::report_jemalloc_stats());

        let Self {
            root_opts,
            config,
            signals,
        } = self;

        let topology_controller = SharedTopologyController::new(TopologyController {
            #[cfg(feature = "api")]
            api_server: config.setup_api(handle),
            topology: config.topology,
            config_paths: config.config_paths.clone(),
            require_healthy: root_opts.require_healthy,
            extra_context: config.extra_context,
        });

        Ok(StartedApplication {
            config_paths: config.config_paths,
            internal_topologies: config.internal_topologies,
            graceful_crash_receiver: config.graceful_crash_receiver,
            signals,
            topology_controller,
            allow_empty_config: root_opts.allow_empty_config,
            interpolate_env: !root_opts.disable_env_var_interpolation,
        })
    }
}

pub struct StartedApplication {
    pub config_paths: Vec<ConfigPath>,
    pub internal_topologies: Vec<RunningTopology>,
    pub graceful_crash_receiver: ShutdownErrorReceiver,
    pub signals: SignalPair,
    pub topology_controller: SharedTopologyController,
    pub allow_empty_config: bool,
    pub interpolate_env: bool,
}

impl StartedApplication {
    pub async fn run(self) -> ExitStatus {
        self.main().await.shutdown().await
    }

    pub async fn main(self) -> FinishedApplication {
        let Self {
            config_paths,
            graceful_crash_receiver,
            signals,
            topology_controller,
            internal_topologies,
            allow_empty_config,
            interpolate_env,
        } = self;

        let mut graceful_crash = UnboundedReceiverStream::new(graceful_crash_receiver);

        let mut signal_handler = signals.handler;
        let mut signal_rx = signals.receiver;

        let signal = loop {
            let has_sources = !topology_controller.lock().await.topology.config.is_empty();
            tokio::select! {
                signal = signal_rx.recv() => if let Some(signal) = handle_signal(
                    signal,
                    &topology_controller,
                    &config_paths,
                    &mut signal_handler,
                    allow_empty_config,
                    interpolate_env,
                ).await {
                    break signal;
                },
                // Trigger graceful shutdown if a component crashed, or all sources have ended.
                error = graceful_crash.next() => break SignalTo::Shutdown(error),
                _ = TopologyController::sources_finished(topology_controller.clone()), if has_sources => {
                    info!("All sources have finished.");
                    break SignalTo::Shutdown(None)
                } ,
                else => unreachable!("Signal streams never end"),
            }
        };

        FinishedApplication {
            signal,
            signal_rx,
            topology_controller,
            internal_topologies,
        }
    }
}

async fn handle_signal(
    signal: Result<SignalTo, RecvError>,
    topology_controller: &SharedTopologyController,
    config_paths: &[ConfigPath],
    signal_handler: &mut SignalHandler,
    allow_empty_config: bool,
    interpolate_env: bool,
) -> Option<SignalTo> {
    match signal {
        Ok(SignalTo::ReloadComponents(components_to_reload)) => {
            let mut topology_controller = topology_controller.lock().await;
            topology_controller
                .topology
                .extend_reload_set(components_to_reload);

            // Reload paths
            if let Some(paths) = config::process_paths(config_paths) {
                topology_controller.config_paths = paths;
            }

            // Reload config
            let new_config = config::load_from_paths_with_provider_and_secrets(
                &topology_controller.config_paths,
                signal_handler,
                allow_empty_config,
                interpolate_env,
            )
            .await;

            reload_config_from_result(topology_controller, new_config).await
        }
        Ok(SignalTo::ReloadFromConfigBuilder(config_builder)) => {
            let topology_controller = topology_controller.lock().await;
            reload_config_from_result(topology_controller, config_builder.build()).await
        }
        Ok(SignalTo::ReloadFromDisk) => {
            let mut topology_controller = topology_controller.lock().await;

            // Reload paths
            if let Some(paths) = config::process_paths(config_paths) {
                topology_controller.config_paths = paths;
            }

            // Reload config
            let new_config = config::load_from_paths_with_provider_and_secrets(
                &topology_controller.config_paths,
                signal_handler,
                allow_empty_config,
                interpolate_env,
            )
            .await;

            if let Ok(ref config) = new_config {
                // Find all transforms that have external files to watch
                let transform_keys_to_reload = config.transform_keys_with_external_files();

                // Add these transforms to reload set
                if !transform_keys_to_reload.is_empty() {
                    info!(
                        message = "Reloading transforms with external files.",
                        count = transform_keys_to_reload.len()
                    );
                    topology_controller
                        .topology
                        .extend_reload_set(transform_keys_to_reload);
                }
            }

            reload_config_from_result(topology_controller, new_config).await
        }
        Ok(SignalTo::ReloadEnrichmentTables) => {
            let topology_controller = topology_controller.lock().await;

            topology_controller
                .topology
                .reload_enrichment_tables()
                .await;
            None
        }
        Err(RecvError::Lagged(amt)) => {
            warn!("Overflow, dropped {} signals.", amt);
            None
        }
        Err(RecvError::Closed) => Some(SignalTo::Shutdown(None)),
        Ok(signal) => Some(signal),
    }
}

async fn reload_config_from_result(
    mut topology_controller: MutexGuard<'_, TopologyController>,
    config: Result<Config, Vec<String>>,
) -> Option<SignalTo> {
    match config {
        Ok(new_config) => match topology_controller.reload(new_config).await {
            ReloadOutcome::FatalError(error) => Some(SignalTo::Shutdown(Some(error))),
            _ => None,
        },
        Err(errors) => {
            handle_config_errors(errors);
            emit!(VectorConfigLoadError);
            None
        }
    }
}

pub struct FinishedApplication {
    pub signal: SignalTo,
    pub signal_rx: SignalRx,
    pub topology_controller: SharedTopologyController,
    pub internal_topologies: Vec<RunningTopology>,
}

impl FinishedApplication {
    pub async fn shutdown(self) -> ExitStatus {
        let FinishedApplication {
            signal,
            signal_rx,
            topology_controller,
            internal_topologies,
        } = self;

        // Emit a vector event indicating the shutdown.
        info!(
            message = "Shutting down Vector instance.",
            // VECTOR_SERVICE_EVENT
            vector_event_type = 2,
            // VECTOR_PROCESS_TERMINATION_SIGNAL_RECEIVED
            service_event = 4,
            internal_log_rate_limit = false,
        );
        // Prometheus mirror of the TERMINATION_SIGNAL_RECEIVED VEL. Fires once per shutdown here (both
        // the graceful `stop` and the `quit` paths flow through this method), so it is the
        // denominator for the close-complete rate: `vector_shutdown_components_closed_total` (emitted
        // when a shutdown reaches COMPONENTS_CLOSED) / `vector_shutdown_termination_signal_total`.
        metrics::counter!("shutdown_termination_signal_total").increment(1);

        // At this point, we'll have the only reference to the shared topology controller and can
        // safely remove it from the wrapper to shut down the topology.
        let topology_controller = topology_controller
            .try_into_inner()
            .expect("fail to unwrap topology controller")
            .into_inner();

        let status = match signal {
            SignalTo::Shutdown(_) => Self::stop(topology_controller, signal_rx).await,
            SignalTo::Quit => Self::quit(),
            _ => unreachable!(),
        };

        for topology in internal_topologies {
            topology.stop().await;
        }

        status
    }

    async fn stop(topology_controller: TopologyController, mut signal_rx: SignalRx) -> ExitStatus {
        emit!(VectorStopped);
        tokio::select! {
            _ = topology_controller.stop() => ExitStatus::from_raw({
                #[cfg(windows)]
                {
                    exitcode::OK as u32
                }
                #[cfg(unix)]
                exitcode::OK
            }), // Graceful shutdown finished
            _ = signal_rx.recv() => Self::quit(),
        }
    }

    fn quit() -> ExitStatus {
        // It is highly unlikely that this event will exit from topology.
        emit!(VectorQuit);
        ExitStatus::from_raw({
            #[cfg(windows)]
            {
                exitcode::UNAVAILABLE as u32
            }
            #[cfg(unix)]
            exitcode::OK
        })
    }
}

fn get_log_levels(default: &str) -> String {
    std::env::var("VECTOR_LOG")
        .or_else(|_| {
            std::env::var("LOG").inspect(|_log| {
                warn!(
                    message =
                        "DEPRECATED: Use of $LOG is deprecated. Please use $VECTOR_LOG instead."
                );
            })
        })
        .unwrap_or_else(|_| default.into())
}

pub fn build_runtime(threads: Option<usize>, thread_name: &str) -> Result<Runtime, ExitCode> {
    let mut rt_builder = runtime::Builder::new_multi_thread();
    let max_blocking_threads = std::env::var("VECTOR_MAX_BLOCKING_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(20_000);
    rt_builder.max_blocking_threads(max_blocking_threads);

    // Apply a reduced thread stack size when VECTOR_THREAD_STACK_SIZE is set.
    // This matters because tokio's default Rust stack is 2 MB per thread, and
    // vector pins one blocking thread per file/kubernetes_logs source for its
    // lifetime (via spawn_blocking). With ~250 such sources in production that
    // amounts to ~500 MB of committed stack space at startup. Setting a smaller
    // value (e.g. 512 KB) recovers ~375 MB with no behavioral change for
    // sources that do not recurse deeply. The setting applies to both worker
    // threads and blocking-pool threads, so choose a value safe for both
    // (≥256 KB is the practical minimum; anything smaller risks stack overflow
    // in deeply recursive VRL scripts or large-config reload paths).
    //
    // When unset, the Rust/tokio default (2 MB) is preserved exactly — this env
    // var is intentionally absent from the deploy config by default (G4: zero-diff
    // default behavior).
    if let Some(stack_size) = std::env::var("VECTOR_THREAD_STACK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
    {
        rt_builder.thread_stack_size(stack_size);
    }

    rt_builder.enable_all();

    let threads = threads.unwrap_or_else(crate::num_threads);
    if threads == 0 {
        error!("The `threads` argument must be greater or equal to 1.");
        return Err(exitcode::CONFIG);
    }
    WORKER_THREADS
        .compare_exchange(0, threads, Ordering::Acquire, Ordering::Relaxed)
        .unwrap_or_else(|_| panic!("double thread initialization"));
    rt_builder.worker_threads(threads);

    // Name worker vs blocking-pool threads distinctly. tokio applies a single
    // thread name to BOTH the (eagerly-spawned) worker pool and the (on-demand)
    // blocking pool, so we differentiate by spawn order: the first `threads`
    // threads are the workers, the rest are blocking threads. The names differ
    // early so they remain distinguishable after the kernel truncates
    // /proc/<pid>/comm to 15 chars (e.g. "vector-worker" vs "vector-blocking").
    let base = thread_name.trim_end_matches("-worker").to_string();
    let worker_name = format!("{base}-worker");
    let blocking_name = format!("{base}-blocking-worker");
    let worker_count = threads;
    let spawned = std::sync::Arc::new(AtomicUsize::new(0));
    rt_builder.thread_name_fn(move || {
        if spawned.fetch_add(1, Ordering::SeqCst) < worker_count {
            worker_name.clone()
        } else {
            blocking_name.clone()
        }
    });

    debug!(message = "Building runtime.", worker_threads = threads);
    Ok(rt_builder.build().expect("Unable to create async runtime"))
}

pub async fn load_configs(
    config_paths: &[ConfigPath],
    watcher_conf: Option<config::watcher::WatcherConfig>,
    require_healthy: Option<bool>,
    allow_empty_config: bool,
    interpolate_env: bool,
    graceful_shutdown_duration: Option<Duration>,
    graceful_data_source_shutdown_duration: Option<Duration>,
    graceful_data_sink_shutdown_duration: Option<Duration>,
    graceful_internal_source_shutdown_duration: Option<Duration>,
    signal_handler: &mut SignalHandler,
) -> Result<Config, ExitCode> {
    let config_paths = config::process_paths(config_paths).ok_or(exitcode::CONFIG)?;

    let watched_paths = config_paths
        .iter()
        .map(<&PathBuf>::from)
        .collect::<Vec<_>>();

    info!(
        message = "Loading configs.",
        paths = ?watched_paths
    );

    let mut config = config::load_from_paths_with_provider_and_secrets(
        &config_paths,
        signal_handler,
        allow_empty_config,
        interpolate_env,
    )
    .await
    .map_err(handle_config_errors)?;

    let mut watched_component_paths = Vec::new();

    if let Some(watcher_conf) = watcher_conf {
        for (name, transform) in config.transforms() {
            let files = transform.inner.files_to_watch();
            let component_config = ComponentConfig::new(
                files.into_iter().cloned().collect(),
                name.clone(),
                ComponentType::Transform,
            );
            watched_component_paths.push(component_config);
        }

        for (name, sink) in config.sinks() {
            let files = sink.inner.files_to_watch();
            let mut config_paths: Vec<PathBuf> = files.into_iter().cloned().collect();
            config_paths.append(&mut sink.files_to_watch.clone());

            let component_config =
                ComponentConfig::new(config_paths, name.clone(), ComponentType::Sink);
            watched_component_paths.push(component_config);
        }

        for (name, table) in config.enrichment_tables() {
            let files = table.inner.files_to_watch();
            let component_config = ComponentConfig::new(
                files.into_iter().cloned().collect(),
                name.clone(),
                ComponentType::EnrichmentTable,
            );
            watched_component_paths.push(component_config);
        }

        info!(
            message = "Starting watcher.",
            paths = ?watched_paths
        );
        info!(
            message = "Components to watch.",
            paths = ?watched_component_paths
        );

        // Start listening for config changes.
        config::watcher::spawn_thread(
            watcher_conf,
            signal_handler.clone_tx(),
            watched_paths,
            watched_component_paths,
            None,
        )
        .map_err(|error| {
            error!(message = "Unable to start config watcher.", %error);
            exitcode::CONFIG
        })?;
    }

    config::init_log_schema(config.global.log_schema.clone(), true);
    config::init_telemetry(config.global.telemetry.clone(), true);

    if !config.healthchecks.enabled {
        info!("Health checks are disabled.");
    }
    config.healthchecks.set_require_healthy(require_healthy);
    config.graceful_shutdown_duration = graceful_shutdown_duration;
    config.graceful_data_source_shutdown_duration = graceful_data_source_shutdown_duration;
    config.graceful_data_sink_shutdown_duration = graceful_data_sink_shutdown_duration;
    config.graceful_internal_source_shutdown_duration = graceful_internal_source_shutdown_duration;

    Ok(config)
}

/// Given the overall graceful-shutdown limit and the three staged sub-limits (all in seconds),
/// return `(data_source, data_sink, internal_source)` clamped to satisfy the strict ordering
/// `data_source < data_sink < internal_source < overall`. Adjustment is silent.
///
/// Returns `None` — disabling two-wave shutdown so the legacy single-pass path runs — only when
/// the data-source limit is not below the overall limit, matching the historical fallback where
/// `graceful_data_source_shutdown_duration` was dropped if it was `>= graceful_shutdown_duration`.
///
/// The clamp walks each point above its predecessor, then pulls any that reach the overall limit
/// back below it. With realistic inputs (the deploy config derives all four as fractions of
/// `terminationGracePeriodSeconds`) the inputs are already well-ordered and pass through
/// unchanged; the clamp only matters for hand-set or pathological values.
fn staged_shutdown_secs(
    overall: u64,
    data_source: u64,
    data_sink: u64,
    internal_source: u64,
) -> Option<(u64, u64, u64)> {
    if data_source >= overall {
        return None;
    }

    // Each staged point must be strictly greater than the previous one. `data_source` is never
    // adjusted — it is the contractual wave-1 source deadline and is only range-checked above.
    let mut data_sink = data_sink.max(data_source + 1);
    let mut internal_source = internal_source.max(data_sink + 1);

    // internal_source must stay strictly below the overall limit. If it overshoots, pull it (and
    // then data_sink, if needed) back down while keeping everything above data_source.
    let ceiling = overall.saturating_sub(1);
    if internal_source > ceiling {
        internal_source = ceiling;
    }
    if data_sink >= internal_source {
        data_sink = internal_source.saturating_sub(1);
    }
    // Last-resort guard for a window too tight to hold two strict intermediate points
    // (overall - data_source < 3): keep data_sink above data_source without exceeding
    // internal_source. The deadlines may coincide in this degenerate case, which only makes
    // the corresponding stage's budget zero — aggressive, but still correct.
    if data_sink <= data_source {
        data_sink = (data_source + 1).min(internal_source);
    }

    Some((data_source, data_sink, internal_source))
}

pub fn init_logging(color: bool, format: LogFormat, log_level: &str, rate: u64) {
    let level = get_log_levels(log_level);
    let json = match format {
        LogFormat::Text => false,
        LogFormat::Json => true,
    };

    trace::init(color, json, &level, rate);
    debug!(
        message = "Internal log rate limit configured.",
        internal_log_rate_secs = rate,
    );
    info!(message = "Log level is enabled.", level = ?level);
}

pub fn watcher_config(
    method: WatchConfigMethod,
    interval: NonZeroU64,
) -> config::watcher::WatcherConfig {
    match method {
        WatchConfigMethod::Recommended => config::watcher::WatcherConfig::RecommendedWatcher,
        WatchConfigMethod::Poll => config::watcher::WatcherConfig::PollWatcher(interval.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::staged_shutdown_secs;

    #[test]
    fn staged_shutdown_secs_passes_through_well_ordered_inputs() {
        // The deploy config derives all four as fractions of terminationGracePeriodSeconds, so
        // realistic inputs are already strictly ordered and must be left untouched.
        assert_eq!(staged_shutdown_secs(54, 30, 36, 48), Some((30, 36, 48)));
        assert_eq!(staged_shutdown_secs(60, 20, 30, 50), Some((20, 30, 50)));
    }

    #[test]
    fn staged_shutdown_secs_disables_two_wave_when_data_source_not_below_overall() {
        // Matches the historical fallback: data-source >= overall drops the staged deadlines and
        // the single-pass path runs.
        assert_eq!(staged_shutdown_secs(20, 20, 25, 30), None);
        assert_eq!(staged_shutdown_secs(20, 25, 26, 27), None);
    }

    #[test]
    fn staged_shutdown_secs_bumps_each_point_above_its_predecessor() {
        // data_sink and internal_source below data_source get pulled up to keep strict ordering.
        assert_eq!(staged_shutdown_secs(60, 30, 20, 25), Some((30, 31, 32)));
        // Equal inputs are nudged apart.
        assert_eq!(staged_shutdown_secs(60, 30, 30, 30), Some((30, 31, 32)));
    }

    #[test]
    fn staged_shutdown_secs_pulls_overshooting_points_below_overall() {
        // internal_source above the overall limit is pulled to overall-1, and data_sink follows.
        assert_eq!(staged_shutdown_secs(40, 30, 50, 60), Some((30, 38, 39)));
    }

    #[test]
    fn staged_shutdown_secs_preserves_strict_ordering() {
        // Property: for any inputs where it returns Some, the result is strictly increasing and
        // strictly below the overall limit (except in the degenerate too-tight window, which is
        // exercised separately).
        for overall in [4u64, 25, 54, 60, 120] {
            for data_source in [1u64, 5, 20, 30] {
                for data_sink in [1u64, 10, 36, 100] {
                    for internal_source in [1u64, 25, 48, 100] {
                        if let Some((ds, dk, is)) =
                            staged_shutdown_secs(overall, data_source, data_sink, internal_source)
                        {
                            assert_eq!(ds, data_source, "data_source is never adjusted");
                            // Strict ordering holds whenever the window can fit it.
                            if overall - data_source >= 3 {
                                assert!(
                                    ds < dk && dk < is && is < overall,
                                    "expected {ds} < {dk} < {is} < {overall}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
