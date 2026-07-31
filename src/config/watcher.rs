use notify::{EventKind, RecursiveMode, recommended_watcher};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, channel},
    thread,
    time::Duration,
};

use crate::{
    Error,
    config::{ComponentConfig, ComponentType},
};

/// Per notify own documentation, it's advised to have delay of more than 30 sec,
/// so to avoid receiving repetitions of previous events on macOS.
///
/// But, config and topology reload logic can handle:
///  - Invalid config, caused either by user or by data race.
///  - Frequent changes, caused by user/editor modifying/saving file in small chunks.
///    so we can use smaller, more responsive delay.
const CONFIG_WATCH_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Refer to [`crate::cli::WatchConfigMethod`] for details.
pub enum WatcherConfig {
    /// Recommended watcher for the current OS.
    RecommendedWatcher,
    /// A poll-based watcher that checks for file changes at regular intervals.
    PollWatcher(u64),
}

enum Watcher {
    /// recommended watcher for os, usually inotify for linux based systems
    RecommendedWatcher(notify::RecommendedWatcher),
    /// poll based watcher. for watching files from NFS.
    PollWatcher(notify::PollWatcher),
}

impl Watcher {
    fn add_config_paths(&mut self, config_paths: &[PathBuf]) -> Result<(), Error> {
        for path in config_paths {
            if path.exists() {
                self.watch(path, RecursiveMode::Recursive)?;
            } else {
                debug!(message = "Skipping non-existent path.", path = ?path);
            }
        }
        Ok(())
    }

    fn add_component_paths(&mut self, component_configs: &[ComponentConfig]) -> Result<(), Error> {
        for path in component_configs
            .iter()
            .flat_map(|component_config| &component_config.config_paths)
        {
            if let Some(parent) = path.parent().filter(|parent| parent.exists()) {
                self.watch(parent, RecursiveMode::NonRecursive)?;
            }

            if path.exists() {
                self.watch(path, RecursiveMode::Recursive)?;
            } else {
                debug!(message = "Skipping non-existent path.", path = ?path);
            }
        }
        Ok(())
    }

    fn add_paths(
        &mut self,
        config_paths: &[PathBuf],
        component_configs: &[ComponentConfig],
    ) -> Result<(), Error> {
        self.add_config_paths(config_paths)?;
        self.add_component_paths(component_configs)?;
        Ok(())
    }

    fn watch(&mut self, path: &Path, recursive_mode: RecursiveMode) -> Result<(), Error> {
        use notify::Watcher as NotifyWatcher;
        match self {
            Watcher::RecommendedWatcher(watcher) => {
                watcher.watch(path, recursive_mode)?;
            }
            Watcher::PollWatcher(watcher) => {
                watcher.watch(path, recursive_mode)?;
            }
        }
        Ok(())
    }
}

/// Sends a ReloadFromDisk or ReloadEnrichmentTables on config_path changes.
/// Accumulates file changes until no change for given duration has occurred.
/// Has best effort guarantee of detecting all file changes from the end of
/// this function until the main thread stops.
pub fn spawn_thread<'a>(
    watcher_conf: WatcherConfig,
    signal_tx: crate::signal::SignalTx,
    config_paths: impl IntoIterator<Item = &'a PathBuf> + 'a,
    component_configs: Vec<ComponentConfig>,
    delay: impl Into<Option<Duration>>,
) -> Result<(), Error> {
    let config_paths: Vec<_> = config_paths.into_iter().cloned().collect();

    let delay = delay.into().unwrap_or(CONFIG_WATCH_DELAY);

    // Create watcher now so not to miss any changes happening between
    // returning from this function and the thread starting.
    let mut watcher = Some(create_watcher(
        &watcher_conf,
        &config_paths,
        &component_configs,
    )?);

    info!("Watching configuration files.");

    thread::spawn(move || {
        // Send an initial ReloadFromDisk signal before entering the watch loop.
        // This handles the race condition where config files (e.g. vector.json
        // written by log-operator) were already present before the watcher started,
        // so no filesystem event would be generated for them.
        info!("Triggering initial config reload to pick up any pre-existing config changes.");
        _ = signal_tx
            .send(crate::signal::SignalTo::ReloadFromDisk)
            .map_err(|error| {
                error!(
                    message = "Unable to perform initial configuration reload.",
                    cause = %error,
                    internal_log_rate_limit = false,
                )
            });

        loop {
            if let Some((mut watcher, receiver)) = watcher.take() {
                while let Ok(Ok(event)) = receiver.recv() {
                    if matches!(
                        event.kind,
                        EventKind::Create(_) | EventKind::Remove(_) | EventKind::Modify(_)
                    ) {
                        debug!(message = "Configuration file change detected.", event = ?event);

                        // Collect paths from initial event
                        let mut changed_paths: HashSet<PathBuf> = event.paths.into_iter().collect();

                        // Collect paths from subsequent events until delay amount of time has passed
                        while let Ok(Ok(subseq_event)) = receiver.recv_timeout(delay) {
                            if matches!(
                                subseq_event.kind,
                                EventKind::Create(_) | EventKind::Remove(_) | EventKind::Modify(_)
                            ) {
                                changed_paths.extend(subseq_event.paths);
                            }
                        }

                        debug!(
                            message = "Collected file change events during delay period.",
                            paths = changed_paths.len(),
                            delay = ?delay
                        );

                        let changed_components: HashMap<_, _> = component_configs
                            .clone()
                            .into_iter()
                            .flat_map(|p| p.contains(&changed_paths))
                            .collect();
                        let unmatched_config_changed = changed_paths.iter().any(|path| {
                            is_config_path_change(path, &config_paths)
                                && !is_component_path_change(path, &component_configs)
                        });

                        // We need to read paths to resolve any inode changes that may have happened.
                        // And we need to do it before raising sighup to avoid missing any change.
                        if let Err(error) = watcher.add_paths(&config_paths, &component_configs) {
                            error!(message = "Failed to read files to watch.", %error);
                            break;
                        }

                        debug!(message = "Reloaded paths.");

                        info!("Configuration file changed.");
                        if unmatched_config_changed {
                            _ = signal_tx
                                .send(crate::signal::SignalTo::ReloadFromDisk)
                                .map_err(|error| {
                                    error!(
                                        message = "Unable to reload configuration file. Restart Vector to reload it.",
                                        cause = %error,
                                        internal_log_rate_limit = false,
                                    )
                                });
                        } else if !changed_components.is_empty() {
                            info!(
                                "Component {:?} configuration changed.",
                                changed_components.keys()
                            );
                            if changed_components
                                .iter()
                                .all(|(_, t)| *t == ComponentType::EnrichmentTable)
                            {
                                info!("Only enrichment tables have changed.");
                                _ = signal_tx
                                    .send(crate::signal::SignalTo::ReloadEnrichmentTables)
                                    .map_err(|error| {
                                        error!(
                                            message = "Unable to reload enrichment tables.",
                                            cause = %error,
                                            internal_log_rate_limit = false,
                                        )
                                    });
                            } else {
                                _ = signal_tx
                                    .send(crate::signal::SignalTo::ReloadComponents(
                                        changed_components.into_keys().collect(),
                                    ))
                                    .map_err(|error| {
                                        error!(
                                            message = "Unable to reload component configuration. Restart Vector to reload it.",
                                            cause = %error,
                                            internal_log_rate_limit = false,
                                        )
                                    });
                            }
                        } else {
                            debug!(message = "Ignoring unmatched component watch event.", paths = ?changed_paths);
                        }
                    } else {
                        debug!(message = "Ignoring event.", event = ?event)
                    }
                }
            }

            thread::sleep(RETRY_TIMEOUT);

            watcher = create_watcher(&watcher_conf, &config_paths, &component_configs)
                .map_err(|error| error!(message = "Failed to create file watcher.", %error))
                .ok();

            if watcher.is_some() {
                // Config files could have changed while we weren't watching,
                // so for a good measure raise SIGHUP and let reload logic
                // determine if anything changed.
                info!("Speculating that configuration files have changed.");
                _ = signal_tx.send(crate::signal::SignalTo::ReloadFromDisk).map_err(|error| {
                error!(message = "Unable to reload configuration file. Restart Vector to reload it.", cause = %error)
            });
            }
        }
    });

    Ok(())
}

fn create_watcher(
    watcher_conf: &WatcherConfig,
    config_paths: &[PathBuf],
    component_configs: &[ComponentConfig],
) -> Result<(Watcher, Receiver<Result<notify::Event, notify::Error>>), Error> {
    info!("Creating configuration file watcher.");

    let (sender, receiver) = channel();
    let mut watcher = match watcher_conf {
        WatcherConfig::RecommendedWatcher => {
            let recommended_watcher = recommended_watcher(sender)?;
            Watcher::RecommendedWatcher(recommended_watcher)
        }
        WatcherConfig::PollWatcher(interval) => {
            let config =
                notify::Config::default().with_poll_interval(Duration::from_secs(*interval));
            let poll_watcher = notify::PollWatcher::new(sender, config)?;
            Watcher::PollWatcher(poll_watcher)
        }
    };
    watcher.add_paths(config_paths, component_configs)?;
    Ok((watcher, receiver))
}

fn is_config_path_change(changed_path: &Path, config_paths: &[PathBuf]) -> bool {
    config_paths
        .iter()
        .any(|config_path| path_matches_config_path(changed_path, config_path))
}

fn is_component_path_change(changed_path: &Path, component_configs: &[ComponentConfig]) -> bool {
    let changed_paths = HashSet::from_iter([changed_path.to_path_buf()]);
    component_configs
        .iter()
        .any(|component_config| component_config.contains(&changed_paths).is_some())
}

fn path_matches_config_path(changed_path: &Path, config_path: &Path) -> bool {
    if changed_path == config_path {
        return true;
    }

    if config_path.is_dir() && changed_path.starts_with(config_path) {
        return true;
    }

    match (
        fs::canonicalize(changed_path),
        fs::canonicalize(config_path),
    ) {
        (Ok(changed_path), Ok(config_path)) => {
            changed_path == config_path
                || (config_path.is_dir() && changed_path.starts_with(config_path))
        }
        _ => false,
    }
}

#[cfg(all(test, unix, not(target_os = "macos")))] // https://github.com/vectordotdev/vector/issues/5000
mod tests {
    use std::{collections::HashSet, fs::File, io::Write, time::Duration};

    use tokio::sync::broadcast;

    use super::*;
    use crate::{
        config::ComponentKey,
        signal::SignalRx,
        test_util::{temp_dir, temp_file, trace_init},
    };

    /// Drain the initial ReloadFromDisk signal that the watcher sends on startup.
    /// This is expected to be a no-op since no config files have changed since
    /// vector started — the topology reload logic (outside the watcher) reads
    /// configs from disk and diffs against the running config, so an unchanged
    /// config results in no action.
    async fn drain_initial_reload(receiver: &mut SignalRx, timeout: Duration) {
        match tokio::time::timeout(timeout, receiver.recv()).await {
            Ok(Ok(signal)) => {
                assert_eq!(
                    signal,
                    crate::signal::SignalTo::ReloadFromDisk,
                    "Expected initial ReloadFromDisk signal from watcher startup, got {:?}",
                    signal
                );
            }
            Ok(Err(e)) => panic!("Failed to receive initial reload signal: {}", e),
            Err(_) => panic!("Timed out waiting for initial reload signal"),
        }
    }

    async fn test_signal(
        files: &mut [std::fs::File],
        expected_signal: crate::signal::SignalTo,
        timeout: Duration,
        mut receiver: SignalRx,
    ) -> bool {
        // Write and sync each file
        for file in files.iter_mut() {
            if let Err(e) = file.write_all(&[0]) {
                error!("Failed to write to file: {}", e);
                return false;
            }
            if let Err(e) = file.sync_all() {
                error!("Failed to sync file: {}", e);
                return false;
            }
        }

        match tokio::time::timeout(timeout, receiver.recv()).await {
            Ok(Ok(signal)) => signal == expected_signal,
            _ => false,
        }
    }

    async fn recv_signal(
        expected_signal: crate::signal::SignalTo,
        timeout: Duration,
        mut receiver: SignalRx,
    ) -> bool {
        recv_signal_ref(expected_signal, timeout, &mut receiver).await
    }

    async fn recv_signal_ref(
        expected_signal: crate::signal::SignalTo,
        timeout: Duration,
        receiver: &mut SignalRx,
    ) -> bool {
        match tokio::time::timeout(timeout, receiver.recv()).await {
            Ok(Ok(signal)) => signal == expected_signal,
            _ => false,
        }
    }

    async fn no_signal(timeout: Duration, mut receiver: SignalRx) -> bool {
        tokio::time::timeout(timeout, receiver.recv())
            .await
            .is_err()
    }

    fn atomic_replace(path: &Path, contents: &[u8]) {
        let tmp_path = path.with_extension("tmp");
        let mut file = File::create(&tmp_path).unwrap();
        file.write_all(contents).unwrap();
        file.sync_all().unwrap();
        std::fs::rename(&tmp_path, path).unwrap();
    }

    #[tokio::test]
    async fn component_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = vec![dir.join("tls.cert"), dir.join("tls.key")];
        let http_component = ComponentKey::from("http");

        std::fs::create_dir(&dir).unwrap();

        let mut component_files: Vec<std::fs::File> = component_file_path
            .iter()
            .map(|file| File::create(file).unwrap())
            .collect();
        let component_config = ComponentConfig::new(
            component_file_path.clone(),
            http_component.clone(),
            ComponentType::Sink,
        );

        let (signal_tx, signal_rx) = broadcast::channel(128);
        let mut signal_rx = signal_rx.resubscribe();
        let mut signal_rx2 = signal_rx.resubscribe();

        spawn_thread(
            watcher_conf,
            signal_tx,
            &[dir],
            vec![component_config],
            delay,
        )
        .unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;
        drain_initial_reload(&mut signal_rx2, delay * 5).await;

        if !test_signal(
            &mut component_files[0..1],
            crate::signal::SignalTo::ReloadComponents(HashSet::from_iter(vec![
                http_component.clone(),
            ])),
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }

        if !test_signal(
            &mut component_files[1..2],
            crate::signal::SignalTo::ReloadComponents(HashSet::from_iter(vec![
                http_component.clone(),
            ])),
            delay * 5,
            signal_rx2,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn multi_component_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = vec![dir.join("tls.cert"), dir.join("tls.key")];
        let http_component = ComponentKey::from("http");
        let http_component_2 = ComponentKey::from("http2");

        std::fs::create_dir(&dir).unwrap();

        let mut component_files: Vec<std::fs::File> = component_file_path
            .iter()
            .map(|file| File::create(file).unwrap())
            .collect();
        let component_config = ComponentConfig::new(
            component_file_path[0..1].to_vec(),
            http_component.clone(),
            ComponentType::Sink,
        );
        let component_config_2 = ComponentConfig::new(
            component_file_path[1..2].to_vec(),
            http_component_2.clone(),
            ComponentType::Sink,
        );

        let (signal_tx, signal_rx) = broadcast::channel(128);
        let mut signal_rx = signal_rx.resubscribe();

        spawn_thread(
            watcher_conf,
            signal_tx,
            &[dir],
            vec![component_config, component_config_2],
            delay,
        )
        .unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        if !test_signal(
            &mut component_files,
            crate::signal::SignalTo::ReloadComponents(HashSet::from_iter(vec![
                http_component.clone(),
                http_component_2.clone(),
            ])),
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn component_and_config_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = vec![dir.join("tls.cert"), dir.join("vector.toml")];
        let http_component = ComponentKey::from("http");

        std::fs::create_dir(&dir).unwrap();

        let mut component_files: Vec<std::fs::File> = component_file_path
            .iter()
            .map(|file| File::create(file).unwrap())
            .collect();
        let component_config = ComponentConfig::new(
            component_file_path[0..1].to_vec(),
            http_component.clone(),
            ComponentType::Sink,
        );

        let (signal_tx, signal_rx) = broadcast::channel(128);
        let mut signal_rx = signal_rx.resubscribe();

        spawn_thread(
            watcher_conf,
            signal_tx,
            &[dir],
            vec![component_config],
            delay,
        )
        .unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        if !test_signal(
            &mut component_files,
            crate::signal::SignalTo::ReloadFromDisk,
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn missing_component_file_creation_triggers_component_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = dir.join("credential.json");
        let http_component = ComponentKey::from("http");

        std::fs::create_dir(&dir).unwrap();

        let component_config = ComponentConfig::new(
            vec![component_file_path.clone()],
            http_component.clone(),
            ComponentType::Sink,
        );

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[], vec![component_config], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        let mut file = File::create(&component_file_path).unwrap();
        file.write_all(b"{}").unwrap();
        file.sync_all().unwrap();

        if !recv_signal(
            crate::signal::SignalTo::ReloadComponents(HashSet::from_iter(vec![
                http_component.clone(),
            ])),
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn atomic_component_file_creation_triggers_component_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = dir.join("credential.json");
        let http_component = ComponentKey::from("http");

        std::fs::create_dir(&dir).unwrap();

        let component_config = ComponentConfig::new(
            vec![component_file_path.clone()],
            http_component.clone(),
            ComponentType::Sink,
        );

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[], vec![component_config], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        let tmp_dir = dir.join("tmp-write");
        std::fs::create_dir(&tmp_dir).unwrap();
        let tmp_file = tmp_dir.join("credential.json");
        let mut file = File::create(&tmp_file).unwrap();
        file.write_all(b"{}").unwrap();
        file.sync_all().unwrap();
        std::fs::rename(&tmp_file, &component_file_path).unwrap();
        std::fs::remove_dir_all(&tmp_dir).unwrap();

        if !recv_signal(
            crate::signal::SignalTo::ReloadComponents(HashSet::from_iter(vec![
                http_component.clone(),
            ])),
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn atomic_component_file_replacement_triggers_component_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = dir.join("credential.json");
        let http_component = ComponentKey::from("http");

        std::fs::create_dir(&dir).unwrap();
        std::fs::write(&component_file_path, b"{\"version\":1}").unwrap();

        let component_config = ComponentConfig::new(
            vec![component_file_path.clone()],
            http_component.clone(),
            ComponentType::Sink,
        );

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[], vec![component_config], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        atomic_replace(&component_file_path, b"{\"version\":2}");

        if !recv_signal_ref(
            crate::signal::SignalTo::ReloadComponents(HashSet::from_iter(vec![
                http_component.clone(),
            ])),
            delay * 5,
            &mut signal_rx,
        )
        .await
        {
            panic!("Test timed out after first atomic replacement");
        }

        atomic_replace(&component_file_path, b"{\"version\":3}");

        if !recv_signal_ref(
            crate::signal::SignalTo::ReloadComponents(HashSet::from_iter(vec![
                http_component.clone(),
            ])),
            delay * 5,
            &mut signal_rx,
        )
        .await
        {
            panic!("Test timed out after second atomic replacement");
        }
    }

    #[tokio::test]
    async fn atomic_enrichment_table_file_replacement_triggers_enrichment_reload() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = dir.join("workspace.csv");
        let table_component = ComponentKey::from("workspace");

        std::fs::create_dir(&dir).unwrap();
        std::fs::write(&component_file_path, b"key,value\n").unwrap();

        let component_config = ComponentConfig::new(
            vec![component_file_path.clone()],
            table_component,
            ComponentType::EnrichmentTable,
        );

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[], vec![component_config], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        atomic_replace(&component_file_path, b"key,value\nworkspace_id,123\n");

        if !recv_signal_ref(
            crate::signal::SignalTo::ReloadEnrichmentTables,
            delay * 5,
            &mut signal_rx,
        )
        .await
        {
            panic!("Test timed out after first atomic replacement");
        }

        atomic_replace(&component_file_path, b"key,value\nworkspace_id,456\n");

        if !recv_signal_ref(
            crate::signal::SignalTo::ReloadEnrichmentTables,
            delay * 5,
            &mut signal_rx,
        )
        .await
        {
            panic!("Test timed out after second atomic replacement");
        }
    }

    #[tokio::test]
    async fn missing_component_file_creation_and_config_update_triggers_reload_from_disk() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = dir.join("credential.json");
        let config_file_path = dir.join("vector.toml");
        let http_component = ComponentKey::from("http");

        std::fs::create_dir(&dir).unwrap();
        let mut config_file = File::create(&config_file_path).unwrap();

        let component_config = ComponentConfig::new(
            vec![component_file_path.clone()],
            http_component,
            ComponentType::Sink,
        );

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(
            watcher_conf,
            signal_tx,
            &[config_file_path],
            vec![component_config],
            delay,
        )
        .unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        let mut component_file = File::create(&component_file_path).unwrap();
        component_file.write_all(b"{}").unwrap();
        component_file.sync_all().unwrap();
        config_file.write_all(b"[sources.in]\n").unwrap();
        config_file.sync_all().unwrap();

        if !recv_signal(
            crate::signal::SignalTo::ReloadFromDisk,
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn unrelated_parent_directory_event_does_not_reload_from_disk() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let watcher_conf = WatcherConfig::RecommendedWatcher;
        let component_file_path = dir.join("credential.json");
        let http_component = ComponentKey::from("http");

        std::fs::create_dir(&dir).unwrap();

        let component_config = ComponentConfig::new(
            vec![component_file_path],
            http_component,
            ComponentType::Sink,
        );

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[], vec![component_config], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        let tmp_dir = dir.join("tmp-write");
        std::fs::create_dir(&tmp_dir).unwrap();
        std::fs::remove_dir_all(&tmp_dir).unwrap();

        if !no_signal(delay * 2, signal_rx).await {
            panic!("Unexpected reload signal");
        }
    }

    #[tokio::test]
    async fn file_directory_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let file_path = dir.join("vector.toml");
        let watcher_conf = WatcherConfig::RecommendedWatcher;

        std::fs::create_dir(&dir).unwrap();
        let file = File::create(&file_path).unwrap();

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[dir], vec![], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        if !test_signal(
            &mut vec![file],
            crate::signal::SignalTo::ReloadFromDisk,
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn file_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let file_path = temp_file();
        let file = File::create(&file_path).unwrap();
        let watcher_conf = WatcherConfig::RecommendedWatcher;

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[file_path], vec![], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        if !test_signal(
            &mut vec![file],
            crate::signal::SignalTo::ReloadFromDisk,
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn sym_file_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let file_path = temp_file();
        let sym_file = temp_file();
        let file = File::create(&file_path).unwrap();
        std::os::unix::fs::symlink(&file_path, &sym_file).unwrap();

        let watcher_conf = WatcherConfig::RecommendedWatcher;

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[sym_file], vec![], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        if !test_signal(
            &mut vec![file],
            crate::signal::SignalTo::ReloadFromDisk,
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }

    #[tokio::test]
    async fn recursive_directory_file_update() {
        trace_init();

        let delay = Duration::from_secs(3);
        let dir = temp_dir().to_path_buf();
        let sub_dir = dir.join("sources");
        let file_path = sub_dir.join("input.toml");
        let watcher_conf = WatcherConfig::RecommendedWatcher;

        std::fs::create_dir_all(&sub_dir).unwrap();
        let file = File::create(&file_path).unwrap();

        let (signal_tx, mut signal_rx) = broadcast::channel(128);
        spawn_thread(watcher_conf, signal_tx, &[sub_dir], vec![], delay).unwrap();

        drain_initial_reload(&mut signal_rx, delay * 5).await;

        if !test_signal(
            &mut vec![file],
            crate::signal::SignalTo::ReloadFromDisk,
            delay * 5,
            signal_rx,
        )
        .await
        {
            panic!("Test timed out");
        }
    }
}
