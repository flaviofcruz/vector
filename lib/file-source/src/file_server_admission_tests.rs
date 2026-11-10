use super::*;

struct ControlledAdmission {
    observed_metadata: Mutex<Vec<AdmissionMetadata>>,
    reserved_bytes: AtomicU64,
    reconciled_bytes: AtomicU64,
    reject_next_reconciliation: AtomicBool,
    wait_error: Mutex<Option<AdmissionError>>,
    wait_started: Semaphore,
    wait_grant: Semaphore,
    sent_started: Semaphore,
    sent_ack: Semaphore,
    issued_permits: AtomicU64,
    cancelled_permits: AtomicU64,
    tranche_size: AtomicU64,
    tranche_transactions: AtomicU64,
}

struct PollCountingAdmission {
    reservations: AtomicU64,
    polls: AtomicU64,
}

struct BoundedOpportunityAdmission {
    slots: Arc<Semaphore>,
    capacity: usize,
    reservation_limit: u64,
    reservations: AtomicU64,
    max_active: AtomicU64,
    keys: Mutex<HashSet<AdmissionKey>>,
}

impl BoundedOpportunityAdmission {
    fn new(capacity: usize, reservation_limit: u64) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
            reservation_limit,
            reservations: AtomicU64::new(0),
            max_active: AtomicU64::new(0),
            keys: Mutex::new(HashSet::new()),
        })
    }
}

impl FileSourceAdmission for BoundedOpportunityAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        SelectiveDrainAdmission.unattributed_metadata()
    }

    fn reserve(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        async move {
            let slot = Arc::clone(&self.slots)
                .try_acquire_owned()
                .map_err(|_| AdmissionError::BoundsExceeded)?;
            let active = self.capacity - self.slots.available_permits();
            self.max_active.fetch_max(active as u64, Ordering::SeqCst);
            self.reservations
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |reservations| {
                    (reservations < self.reservation_limit).then_some(reservations + 1)
                })
                .map_err(|_| AdmissionError::BoundsExceeded)?;
            self.keys.lock().unwrap().insert(metadata.key);
            Ok(Box::new(BoundedOpportunityPending { slot }) as Box<dyn PendingAdmission>)
        }
        .boxed()
    }
}

struct BoundedOpportunityPending {
    slot: OwnedSemaphorePermit,
}

impl PendingAdmission for BoundedOpportunityPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async move {
            Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                BoundedOpportunityPermit { _slot: self.slot },
            )))
        }
        .boxed()
    }
}

struct BoundedOpportunityPermit {
    _slot: OwnedSemaphorePermit,
}

impl FileSourceAdmissionPermit for BoundedOpportunityPermit {
    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {}

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

impl PollCountingAdmission {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            reservations: AtomicU64::new(0),
            polls: AtomicU64::new(0),
        })
    }
}

impl FileSourceAdmission for PollCountingAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        AdmissionMetadata {
            key: AdmissionKey::Unattributed,
            watch_lease_id: 0,
            observation_sequence: 0,
        }
    }

    fn reserve(
        self: Arc<Self>,
        _metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        async move { Ok(Box::new(PollCountingPending(self)) as Box<dyn PendingAdmission>) }.boxed()
    }
}

struct PollCountingPending(Arc<PollCountingAdmission>);

impl PendingAdmission for PollCountingPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        futures::future::poll_fn(move |_| {
            self.0.polls.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Pending
        })
        .boxed()
    }
}

impl ControlledAdmission {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            observed_metadata: Mutex::new(Vec::new()),
            reserved_bytes: AtomicU64::new(0),
            reconciled_bytes: AtomicU64::new(0),
            reject_next_reconciliation: AtomicBool::new(false),
            wait_error: Mutex::new(None),
            wait_started: Semaphore::new(0),
            wait_grant: Semaphore::new(0),
            sent_started: Semaphore::new(0),
            sent_ack: Semaphore::new(0),
            issued_permits: AtomicU64::new(0),
            cancelled_permits: AtomicU64::new(0),
            tranche_size: AtomicU64::new(ADMISSION_READ_AHEAD_PER_FILE as u64),
            tranche_transactions: AtomicU64::new(0),
        })
    }
}

impl FileSourceAdmission for ControlledAdmission {
    fn reservation_tranche_size(&self) -> usize {
        self.tranche_size.load(Ordering::SeqCst) as usize
    }

    fn unattributed_metadata(&self) -> AdmissionMetadata {
        AdmissionMetadata {
            key: AdmissionKey::Unattributed,
            watch_lease_id: 0,
            observation_sequence: 0,
        }
    }

    fn reserve(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        _request_id: u64,
        max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        self.observed_metadata.lock().unwrap().push(metadata);
        self.reserved_bytes.store(max_line_bytes, Ordering::SeqCst);
        async move { Ok(Box::new(ControlledPending(self)) as Box<dyn PendingAdmission>) }.boxed()
    }

    fn reserve_tranche(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        request_ids: Vec<u64>,
        max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Vec<Box<dyn PendingAdmission>>, AdmissionError>> {
        self.tranche_transactions.fetch_add(1, Ordering::SeqCst);
        async move {
            let mut reservations = Vec::with_capacity(request_ids.len());
            for request_id in request_ids {
                reservations.push(
                    Arc::clone(&self)
                        .reserve(metadata.clone(), request_id, max_line_bytes)
                        .await?,
                );
            }
            Ok(reservations)
        }
        .boxed()
    }
}

struct ControlledPending(Arc<ControlledAdmission>);

impl PendingAdmission for ControlledPending {
    fn reconcile_payload_bytes(
        &mut self,
        actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        self.0
            .reconciled_bytes
            .store(actual_payload_bytes, Ordering::SeqCst);
        let reject = self
            .0
            .reject_next_reconciliation
            .swap(false, Ordering::SeqCst);
        async move {
            if reject {
                Err(AdmissionError::Internal)
            } else {
                Ok(())
            }
        }
        .boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async move {
            self.0.wait_started.add_permits(1);
            if let Some(error) = self.0.wait_error.lock().unwrap().take() {
                return Err(error);
            }
            let grant = self.0.wait_grant.acquire().await.unwrap();
            grant.forget();
            self.0.issued_permits.fetch_add(1, Ordering::SeqCst);
            Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                ControlledPermit(self.0),
            )))
        }
        .boxed()
    }
}

struct ControlledPermit(Arc<ControlledAdmission>);

impl FileSourceAdmissionPermit for ControlledPermit {
    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async move {
            self.0.sent_started.add_permits(1);
            let permit = self.0.sent_ack.acquire().await.unwrap();
            permit.forget();
            Ok(())
        }
        .boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        self.0.cancelled_permits.fetch_add(1, Ordering::SeqCst);
        self.0.wait_grant.add_permits(1);
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {
        self.0.cancelled_permits.fetch_add(1, Ordering::SeqCst);
        self.0.wait_grant.add_permits(1);
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

struct DeliveredPermitPending {
    receive: tokio::sync::oneshot::Receiver<OwnedFileSourceAdmissionPermit>,
}

impl PendingAdmission for DeliveredPermitPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async move { self.receive.await.map_err(|_| AdmissionError::Closed) }.boxed()
    }
}

struct CancelCountingPermit(Arc<AtomicU64>);

impl FileSourceAdmissionPermit for CancelCountingPermit {
    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

struct SelectiveDrainAdmission;

struct SelectiveAcknowledgementAdmission;

struct BoundsRejectingAdmission;

impl FileSourceAdmission for BoundsRejectingAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        SelectiveDrainAdmission.unattributed_metadata()
    }

    fn reserve(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        let rejected =
            matches!(metadata.key, AdmissionKey::PodUid(ref uid) if uid.as_ref() == "blocked");
        async move {
            if rejected {
                Err(AdmissionError::BoundsExceeded)
            } else {
                Ok(Box::new(SelectiveDrainPending { unavailable: false })
                    as Box<dyn PendingAdmission>)
            }
        }
        .boxed()
    }
}

impl FileSourceAdmission for SelectiveDrainAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        AdmissionMetadata {
            key: AdmissionKey::Unattributed,
            watch_lease_id: 0,
            observation_sequence: 0,
        }
    }

    fn reserve(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        let unavailable =
            matches!(metadata.key, AdmissionKey::PodUid(ref uid) if uid.as_ref() == "blocked");
        async move {
            Ok(Box::new(SelectiveDrainPending { unavailable }) as Box<dyn PendingAdmission>)
        }
        .boxed()
    }
}

impl FileSourceAdmission for SelectiveAcknowledgementAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        SelectiveDrainAdmission.unattributed_metadata()
    }

    fn reserve(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        let stalled =
            matches!(metadata.key, AdmissionKey::PodUid(ref uid) if uid.as_ref() == "blocked");
        async move {
            Ok(Box::new(SelectiveAcknowledgementPending { stalled }) as Box<dyn PendingAdmission>)
        }
        .boxed()
    }
}

struct SelectiveAcknowledgementPending {
    stalled: bool,
}

impl PendingAdmission for SelectiveAcknowledgementPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async move {
            Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                SelectiveAcknowledgementPermit {
                    stalled: self.stalled,
                },
            )))
        }
        .boxed()
    }
}

struct SelectiveAcknowledgementPermit {
    stalled: bool,
}

impl FileSourceAdmissionPermit for SelectiveAcknowledgementPermit {
    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async move {
            if self.stalled {
                futures::future::pending().await
            } else {
                Ok(())
            }
        }
        .boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {}

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

struct SelectiveDrainPending {
    unavailable: bool,
}

impl PendingAdmission for SelectiveDrainPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async move {
            if self.unavailable {
                futures::future::pending().await
            } else {
                Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                    SelectiveDrainPermit,
                )))
            }
        }
        .boxed()
    }
}

struct SelectiveDrainPermit;

impl FileSourceAdmissionPermit for SelectiveDrainPermit {
    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {}

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

struct BatchOutcomePermit {
    transactions: Arc<AtomicUsize>,
    result: Result<(), AdmissionError>,
}

impl FileSourceAdmissionPermit for BatchOutcomePermit {
    fn supports_batch(&self) -> bool {
        true
    }

    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        self.transactions.fetch_add(1, Ordering::SeqCst);
        async move { self.result }.boxed()
    }

    fn sent_many(
        self: Box<Self>,
        additional: Vec<Box<dyn FileSourceAdmissionPermit>>,
    ) -> BoxFuture<'static, Vec<Result<(), AdmissionError>>> {
        self.transactions.fetch_add(1, Ordering::SeqCst);
        let mut results = vec![self.result];
        for permit in additional {
            let permit = permit.into_any().downcast::<Self>().unwrap();
            results.push(permit.result);
        }
        async move { results }.boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {}

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

struct AdmissionPathsProvider {
    path: PathBuf,
    info: Arc<Mutex<LogFileInfo>>,
}

struct MultiAdmissionPathsProvider {
    paths: Vec<(LogFileInfo, PathBuf)>,
}

impl PathsProvider for MultiAdmissionPathsProvider {
    type IntoIter = Vec<(Option<LogFileInfo>, PathBuf)>;

    fn paths(&self) -> Self::IntoIter {
        self.paths
            .iter()
            .cloned()
            .map(|(info, path)| (Some(info), path))
            .collect()
    }
}

impl PathsProvider for AdmissionPathsProvider {
    type IntoIter = Vec<(Option<LogFileInfo>, PathBuf)>;

    fn paths(&self) -> Self::IntoIter {
        vec![(Some(self.info.lock().unwrap().clone()), self.path.clone())]
    }
}

fn make_admission_file_server(
    path: PathBuf,
    data_dir: PathBuf,
    info: LogFileInfo,
    file_to_pod_map: Arc<Mutex<HashMap<PathBuf, LogFileInfo>>>,
    admission: Option<Arc<dyn FileSourceAdmission>>,
) -> FileServer<AdmissionPathsProvider, NoErrors> {
    make_admission_file_server_with_info(
        path,
        data_dir,
        Arc::new(Mutex::new(info)),
        file_to_pod_map,
        admission,
    )
}

fn make_admission_file_server_with_info(
    path: PathBuf,
    data_dir: PathBuf,
    info: Arc<Mutex<LogFileInfo>>,
    file_to_pod_map: Arc<Mutex<HashMap<PathBuf, LogFileInfo>>>,
    admission: Option<Arc<dyn FileSourceAdmission>>,
) -> FileServer<AdmissionPathsProvider, NoErrors> {
    FileServer {
        paths_provider: AdmissionPathsProvider { path, info },
        max_read_bytes: 0,
        ignore_checkpoints: false,
        read_from: ReadFrom::Beginning,
        ignore_before: None,
        start_reading_at: None,
        max_line_bytes: 1_024,
        line_delimiter: Bytes::from("\n"),
        data_dir,
        glob_minimum_cooldown: Duration::from_millis(1),
        idle_read_interval: Duration::from_secs(10),
        fingerprinter: Fingerprinter::new(
            FingerprintStrategy::FirstLinesChecksum {
                ignored_header_bytes: 0,
                lines: 1,
            },
            1_024,
            true,
        ),
        oldest_first: false,
        remove_after: None,
        emitter: NoErrors,
        rotate_wait: Duration::from_secs(60),
        ttl_removal_config: None,
        source_context: None,
        file_to_pod_map: Some(file_to_pod_map),
        drain_on_shutdown: false,
        archive_extensions: Vec::new(),
        source_type: "kubernetes_logs",
        read_loop_min_backoff: 1,
        read_loop_max_backoff: 2_048,
        admission,
        test_readiness_hook: None,
        test_line_admission_hook: None,
        test_before_send_hook: None,
        test_checkpoint_flush_hook: None,
    }
}

fn make_multi_admission_file_server(
    paths: Vec<(LogFileInfo, PathBuf)>,
    data_dir: PathBuf,
    file_to_pod_map: Arc<Mutex<HashMap<PathBuf, LogFileInfo>>>,
    admission: Arc<dyn FileSourceAdmission>,
) -> FileServer<MultiAdmissionPathsProvider, NoErrors> {
    FileServer {
        paths_provider: MultiAdmissionPathsProvider { paths },
        max_read_bytes: 0,
        ignore_checkpoints: false,
        read_from: ReadFrom::Beginning,
        ignore_before: None,
        start_reading_at: None,
        max_line_bytes: 1_024,
        line_delimiter: Bytes::from("\n"),
        data_dir,
        glob_minimum_cooldown: Duration::from_millis(1),
        idle_read_interval: Duration::from_secs(10),
        fingerprinter: Fingerprinter::new(
            FingerprintStrategy::FirstLinesChecksum {
                ignored_header_bytes: 0,
                lines: 1,
            },
            1_024,
            true,
        ),
        oldest_first: false,
        remove_after: None,
        emitter: NoErrors,
        rotate_wait: Duration::from_secs(60),
        ttl_removal_config: None,
        source_context: None,
        file_to_pod_map: Some(file_to_pod_map),
        drain_on_shutdown: true,
        archive_extensions: Vec::new(),
        source_type: "kubernetes_logs",
        read_loop_min_backoff: 1,
        read_loop_max_backoff: 2_048,
        admission: Some(admission),
        test_readiness_hook: Some(Arc::new(|_| false)),
        test_line_admission_hook: None,
        test_before_send_hook: None,
        test_checkpoint_flush_hook: None,
    }
}

fn admission_log_info() -> LogFileInfo {
    LogFileInfo {
        pod_namespace: "ns".to_owned(),
        pod_name: "pod-a".to_owned(),
        pod_uid: "uid-a".to_owned(),
        container_name: "container".to_owned(),
        service_system: None,
        source_pod_id: None,
        admission_metadata: Some(AdmissionMetadata {
            key: AdmissionKey::PodUid(Arc::from("uid-a")),
            watch_lease_id: 7,
            observation_sequence: 1,
        }),
    }
}

async fn restart_first_record(
    path: PathBuf,
    data_dir: PathBuf,
    info: LogFileInfo,
    file_to_pod_map: Arc<Mutex<HashMap<PathBuf, LogFileInfo>>>,
) -> Line {
    let server = make_admission_file_server(path, data_dir.clone(), info, file_to_pod_map, None);
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));
    let replay = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("restart should replay the unread stable record")
        .unwrap()
        .into_iter()
        .next()
        .expect("restart batch should contain one record");
    let _ = shutdown_tx.send(());
    running.await.unwrap().unwrap();
    replay
}

async fn restart_records(
    path: PathBuf,
    data_dir: PathBuf,
    info: LogFileInfo,
    file_to_pod_map: Arc<Mutex<HashMap<PathBuf, LogFileInfo>>>,
    expected: usize,
) -> Vec<Line> {
    let server = make_admission_file_server(path, data_dir.clone(), info, file_to_pod_map, None);
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(4);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));
    let mut records = Vec::new();
    while records.len() < expected {
        let mut batch = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("restart should replay every unread stable record")
            .expect("file server closed before replay completed");
        records.append(&mut batch);
    }
    let _ = shutdown_tx.send(());
    running.await.unwrap().unwrap();
    records
}

#[tokio::test]
async fn pending_file_reclassifies_when_same_path_metadata_changes() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();

    let old_info = admission_log_info();
    let old_metadata = old_info.admission_metadata.clone().unwrap();
    let mut new_info = old_info.clone();
    let new_metadata = new_info.admission_metadata.as_mut().unwrap();
    new_metadata.observation_sequence += 1;
    let new_metadata = new_metadata.clone();

    let paths_info = Arc::new(Mutex::new(old_info));
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let admission = ControlledAdmission::new();
    admission.tranche_size.store(1, Ordering::SeqCst);
    let server = make_admission_file_server_with_info(
        path,
        data_dir.clone(),
        Arc::clone(&paths_info),
        file_to_pod_map,
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let checkpointer = Checkpointer::new(&data_dir);
    let checkpoints = checkpointer.view();
    let running = tokio::spawn(server.run(tx, shutdown_rx, checkpoint_shutdown_rx, checkpointer));

    admission.wait_started.acquire().await.unwrap().forget();
    assert!(
        admission
            .observed_metadata
            .lock()
            .unwrap()
            .iter()
            .all(|metadata| metadata == &old_metadata)
    );
    *paths_info.lock().unwrap() = new_info;

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if admission
                .observed_metadata
                .lock()
                .unwrap()
                .contains(&new_metadata)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending work must promptly reserve against the replacement metadata");

    admission.wait_grant.add_permits(1);
    admission.sent_ack.add_permits(1);
    let replayed = next_nonempty_line(&mut rx).await;
    assert_eq!(replayed.text, Bytes::from_static(b"id-1"));
    assert_eq!(checkpoints.get(replayed.file_id), None);
    assert_eq!(
        replayed.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );

    shutdown_tx.send(()).unwrap();
    checkpoint_shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn metadata_refresh_keeps_sent_record_and_reclassifies_buffered_tail() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();

    let old_info = admission_log_info();
    let mut new_info = old_info.clone();
    let new_metadata = new_info.admission_metadata.as_mut().unwrap();
    new_metadata.observation_sequence += 1;
    let new_metadata = new_metadata.clone();

    let paths_info = Arc::new(Mutex::new(old_info));
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let admission = ControlledAdmission::new();
    admission.tranche_size.store(1, Ordering::SeqCst);
    let server = make_admission_file_server_with_info(
        path.clone(),
        data_dir.clone(),
        Arc::clone(&paths_info),
        Arc::clone(&file_to_pod_map),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    admission.wait_started.acquire().await.unwrap().forget();
    admission.wait_grant.add_permits(1);
    let first = next_nonempty_line(&mut rx).await;
    assert_eq!(first.text, Bytes::from_static(b"id-1"));
    admission.wait_started.acquire().await.unwrap().forget();

    *paths_info.lock().unwrap() = new_info;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if file_to_pod_map
                .lock()
                .unwrap()
                .get(&path)
                .and_then(|info| info.admission_metadata.as_ref())
                == Some(&new_metadata)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("discovery must publish replacement metadata while the sent record acknowledges");

    admission.sent_ack.add_permits(1);
    assert_eq!(
        first.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if admission
                .observed_metadata
                .lock()
                .unwrap()
                .contains(&new_metadata)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the unsent tail must reserve against the replacement metadata");

    admission.wait_grant.add_permits(1);
    admission.sent_ack.add_permits(1);
    let second = next_nonempty_line(&mut rx).await;
    assert_eq!(second.text, Bytes::from_static(b"id-2"));
    assert_eq!(
        second.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );

    shutdown_tx.send(()).unwrap();
    checkpoint_shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn rejected_partial_append_replays_the_complete_stable_record() {
    use tokio::io::AsyncWriteExt;

    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"seed\nid-").await.unwrap();
    let admission = ControlledAdmission::new();
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_admission_file_server(
        path.clone(),
        data_dir.clone(),
        admission_log_info(),
        file_to_pod_map,
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    server.glob_minimum_cooldown = Duration::from_secs(60);
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    admission.wait_started.acquire().await.unwrap().forget();
    admission.wait_grant.add_permits(1);
    admission.sent_ack.add_permits(1);
    let seed = next_nonempty_line(&mut rx).await;
    assert_eq!(seed.text, Bytes::from_static(b"seed"));
    assert_eq!(
        seed.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    admission
        .reject_next_reconciliation
        .store(true, Ordering::SeqCst);
    let mut append = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .unwrap();
    append.write_all(b"1\n").await.unwrap();
    append.flush().await.unwrap();
    admission.wait_started.acquire().await.unwrap().forget();
    admission.wait_grant.add_permits(1);
    admission.sent_ack.add_permits(1);
    let line = next_nonempty_line(&mut rx).await;
    assert_eq!(line.text, Bytes::from_static(b"id-1"));
    assert_eq!(line.start_offset, 5);
    assert_eq!(
        line.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());
}

#[tokio::test]
async fn crash_after_gated_checkpoint_flush_restarts_at_the_next_record() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();
    let info = admission_log_info();
    let admission = ControlledAdmission::new();
    admission.wait_grant.add_permits(1);
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_admission_file_server(
        path.clone(),
        data_dir.clone(),
        info.clone(),
        Arc::clone(&file_to_pod_map),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let flush_hook = TestCheckpointFlushHook::new();
    server.test_checkpoint_flush_hook = Some(Arc::clone(&flush_hook));
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let checkpointer = Checkpointer::new(&data_dir);
    let checkpoints = checkpointer.view();
    let running = tokio::spawn(server.run(tx, shutdown_rx, checkpoint_shutdown_rx, checkpointer));

    let mut batch = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("admitted line should cross the source channel")
        .unwrap();
    let line = batch.pop().unwrap();
    admission.sent_started.acquire().await.unwrap().forget();
    admission.sent_ack.add_permits(1);
    assert_eq!(
        line.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    tokio::time::timeout(
        Duration::from_secs(1),
        flush_hook.unarmed_temporary_file_synced.acquire(),
    )
    .await
    .expect("the initial empty checkpoint temporary file must sync before arming")
    .unwrap()
    .forget();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !fs::try_exists(data_dir.join("checkpoints.json"))
            .await
            .unwrap()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the initial empty checkpoint must be stable before arming");
    flush_hook.arm();
    checkpoints.update(line.file_id, line.end_offset);
    tokio::time::timeout(Duration::from_secs(1), flush_hook.started.acquire())
        .await
        .expect("the updated checkpoint must reach the durable temporary file")
        .unwrap()
        .forget();
    flush_hook.resume.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while fs::try_exists(data_dir.join("checkpoints.new.json"))
            .await
            .unwrap()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the file server's checkpoint writer must atomically install the gated offset");

    // Simulate the process stopping immediately after the real periodic checkpoint flush.
    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());
    let mut persisted = Checkpointer::new(&data_dir);
    persisted.read_checkpoints(None).await;
    assert_eq!(persisted.view().get(line.file_id), Some(line.end_offset));
    let replay = restart_first_record(path, data_dir, info, file_to_pod_map).await;
    assert_eq!(replay.text, Bytes::from_static(b"id-2"));
}

// Catches any protocol phase advancing the durable byte offset past an unacknowledged ID,
// failing to refund an issued permit after a channel cancellation, or letting blocked key A
// prevent independently admitted key B from crossing the real source channel.
#[tokio::test]
async fn admission_protocol_all_boundaries_preserve_ids_offsets_and_selective_progress() {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CrashPhase {
        BeforeGrant,
        AfterPermitBeforeSend,
        AfterChannelSendBeforeActorAck,
        AfterActorAckBeforeCheckpointUpdate,
        DuringCheckpointFlush,
    }

    async fn run_crash_phase(phase: CrashPhase) {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("records.log");
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        fs::write(&path, b"id-1\nid-2\nid-3\n").await.unwrap();
        let info = admission_log_info();
        let admission = ControlledAdmission::new();
        let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
        let mut server = make_admission_file_server(
            path.clone(),
            data_dir.clone(),
            info.clone(),
            Arc::clone(&file_to_pod_map),
            Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
        );
        let before_send = Arc::new(Semaphore::new(0));
        let allow_send = Arc::new(Semaphore::new(0));
        if phase == CrashPhase::AfterPermitBeforeSend {
            server.test_before_send_hook = Some(Arc::new({
                let before_send = Arc::clone(&before_send);
                let allow_send = Arc::clone(&allow_send);
                move || {
                    let before_send = Arc::clone(&before_send);
                    let allow_send = Arc::clone(&allow_send);
                    async move {
                        before_send.add_permits(1);
                        allow_send.acquire().await.unwrap().forget();
                    }
                    .boxed()
                }
            }));
        }
        let flush_hook = TestCheckpointFlushHook::new();
        if phase == CrashPhase::DuringCheckpointFlush {
            server.test_checkpoint_flush_hook = Some(Arc::clone(&flush_hook));
        }
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) =
            tokio::sync::oneshot::channel::<()>();
        let checkpointer = Checkpointer::new(&data_dir);
        let checkpoints = checkpointer.view();
        let running =
            tokio::spawn(server.run(tx, shutdown_rx, checkpoint_shutdown_rx, checkpointer));
        let mut delivered = Vec::new();
        let acknowledged_end = match phase {
            CrashPhase::BeforeGrant => {
                admission.wait_started.acquire().await.unwrap().forget();
                0
            }
            CrashPhase::AfterPermitBeforeSend => {
                admission.wait_started.acquire().await.unwrap().forget();
                admission.wait_grant.add_permits(1);
                tokio::time::timeout(Duration::from_secs(1), before_send.acquire())
                    .await
                    .expect("issued permit must reach the pre-send boundary")
                    .unwrap()
                    .forget();
                0
            }
            CrashPhase::AfterChannelSendBeforeActorAck => {
                admission.wait_grant.add_permits(1);
                let line = next_nonempty_line(&mut rx).await;
                delivered.push(line.text.clone());
                admission.sent_started.acquire().await.unwrap().forget();
                0
            }
            CrashPhase::AfterActorAckBeforeCheckpointUpdate => {
                admission.wait_grant.add_permits(1);
                let line = next_nonempty_line(&mut rx).await;
                delivered.push(line.text.clone());
                admission.sent_started.acquire().await.unwrap().forget();
                admission.sent_ack.add_permits(1);
                assert_eq!(
                    line.checkpoint_gate.wait().await,
                    CheckpointGateResult::ActorAcknowledged
                );
                line.end_offset
            }
            CrashPhase::DuringCheckpointFlush => {
                admission.wait_grant.add_permits(1);
                let line = next_nonempty_line(&mut rx).await;
                delivered.push(line.text.clone());
                admission.sent_started.acquire().await.unwrap().forget();
                admission.sent_ack.add_permits(1);
                assert_eq!(
                    line.checkpoint_gate.wait().await,
                    CheckpointGateResult::ActorAcknowledged
                );
                tokio::time::timeout(
                    Duration::from_secs(1),
                    flush_hook.unarmed_temporary_file_synced.acquire(),
                )
                .await
                .expect("the initial empty checkpoint temporary file must sync before arming")
                .unwrap()
                .forget();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !fs::try_exists(data_dir.join("checkpoints.json"))
                        .await
                        .unwrap()
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the initial empty checkpoint must be stable before arming");
                flush_hook.arm();
                assert_eq!(line.end_offset, 5);
                checkpoints.update(line.file_id, line.end_offset);
                let flush_started =
                    tokio::time::timeout(Duration::from_secs(1), flush_hook.started.acquire())
                        .await;
                running.abort();
                let _ = running.await;
                let _ = checkpoint_shutdown_tx.send(());
                flush_started
                    .expect("fault hook must stop an in-progress checkpoint flush")
                    .unwrap()
                    .forget();
                assert_eq!(flush_hook.resume.available_permits(), 0);
                let mut persisted = Checkpointer::new(&data_dir);
                persisted.read_checkpoints(None).await;
                let offset = persisted.view().get(line.file_id).unwrap_or(0);
                assert_eq!(offset, 5);
                let replay = restart_records(path, data_dir, info, file_to_pod_map, 2).await;
                let replay_ids: Vec<_> = replay.iter().map(|line| line.text.as_ref()).collect();
                let expected: Vec<&[u8]> = vec![b"id-2", b"id-3"];
                assert_eq!(replay_ids, expected);
                assert_eq!(delivered, vec![Bytes::from_static(b"id-1")]);
                return;
            }
        };

        running.abort();
        let _ = running.await;
        let _ = checkpoint_shutdown_tx.send(());
        let replay = restart_records(path, data_dir.clone(), info, file_to_pod_map, 3).await;
        let mut persisted = Checkpointer::new(&data_dir);
        persisted.read_checkpoints(None).await;
        let persisted_offset = persisted.view().get(replay[0].file_id).unwrap_or(0);
        assert_eq!(persisted_offset, 0, "phase {phase:?}");
        assert!(persisted_offset <= acknowledged_end, "phase {phase:?}");
        assert_eq!(
            replay
                .iter()
                .map(|line| line.text.as_ref())
                .collect::<Vec<_>>(),
            vec![b"id-1".as_slice(), b"id-2".as_slice(), b"id-3".as_slice()],
            "phase {phase:?}"
        );
        if delivered.is_empty() {
            assert!(matches!(
                phase,
                CrashPhase::BeforeGrant | CrashPhase::AfterPermitBeforeSend
            ));
        } else {
            assert_eq!(delivered, vec![Bytes::from_static(b"id-1")]);
        }
    }

    for phase in [
        CrashPhase::BeforeGrant,
        CrashPhase::AfterPermitBeforeSend,
        CrashPhase::AfterChannelSendBeforeActorAck,
        CrashPhase::AfterActorAckBeforeCheckpointUpdate,
        CrashPhase::DuringCheckpointFlush,
    ] {
        run_crash_phase(phase).await;
    }

    let tmp = tempdir().unwrap();
    let path = tmp.path().join("cancelled.log");
    let data_dir = tmp.path().join("cancelled-data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();
    let admission = ControlledAdmission::new();
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_admission_file_server(
        path.clone(),
        data_dir.clone(),
        admission_log_info(),
        Arc::clone(&file_to_pod_map),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let before_send = Arc::new(Semaphore::new(0));
    let allow_send = Arc::new(Semaphore::new(0));
    server.test_before_send_hook = Some(Arc::new({
        let before_send = Arc::clone(&before_send);
        let allow_send = Arc::clone(&allow_send);
        move || {
            let before_send = Arc::clone(&before_send);
            let allow_send = Arc::clone(&allow_send);
            async move {
                before_send.add_permits(1);
                allow_send.acquire().await.unwrap().forget();
            }
            .boxed()
        }
    }));
    let (tx, rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));
    admission.wait_started.acquire().await.unwrap().forget();
    admission.wait_grant.add_permits(1);
    before_send.acquire().await.unwrap().forget();
    drop(rx);
    allow_send.add_permits(1);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), running)
            .await
            .expect("closed channel must cancel the issued permit")
            .unwrap()
            .is_err()
    );
    let _ = checkpoint_shutdown_tx.send(());
    let issued_permits = admission.issued_permits.load(Ordering::SeqCst);
    assert!((1..=2).contains(&issued_permits));
    assert_eq!(
        admission.cancelled_permits.load(Ordering::SeqCst),
        issued_permits,
        "every permit issued before the file abort must cancel"
    );
    let replay = restart_first_record(path, data_dir, admission_log_info(), file_to_pod_map).await;
    assert_eq!(replay.text, Bytes::from_static(b"id-1"));

    let tmp = tempdir().unwrap();
    let blocked_path = tmp.path().join("a-blocked.log");
    let ready_path = tmp.path().join("b-ready.log");
    let data_dir = tmp.path().join("selective-data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&blocked_path, b"blocked-a\n").await.unwrap();
    fs::write(&ready_path, b"ready-b\n").await.unwrap();
    let mut blocked_info = admission_log_info();
    blocked_info.admission_metadata.as_mut().unwrap().key =
        AdmissionKey::PodUid(Arc::from("blocked"));
    let mut ready_info = admission_log_info();
    ready_info.admission_metadata.as_mut().unwrap().key = AdmissionKey::PodUid(Arc::from("ready"));
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_multi_admission_file_server(
        vec![(blocked_info, blocked_path), (ready_info, ready_path)],
        data_dir.clone(),
        file_to_pod_map,
        Arc::new(SelectiveDrainAdmission),
    );
    server.oldest_first = true;
    server.test_readiness_hook = None;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));
    let ready = next_nonempty_line(&mut rx).await;
    assert_eq!(ready.text, Bytes::from_static(b"ready-b"));
    assert_eq!(
        ready.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    shutdown_tx.send(()).unwrap();
    checkpoint_shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), running)
        .await
        .expect("blocked A must not prevent ready B or finite drain shutdown")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stalled_actor_acknowledgement_does_not_block_another_ready_key() {
    let tmp = tempdir().unwrap();
    let blocked_path = tmp.path().join("a-blocked.log");
    let ready_path = tmp.path().join("b-ready.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&blocked_path, b"blocked-a\n").await.unwrap();
    fs::write(&ready_path, b"ready-b\n").await.unwrap();
    let mut blocked_info = admission_log_info();
    blocked_info.admission_metadata.as_mut().unwrap().key =
        AdmissionKey::PodUid(Arc::from("blocked"));
    let mut ready_info = admission_log_info();
    ready_info.admission_metadata.as_mut().unwrap().key = AdmissionKey::PodUid(Arc::from("ready"));
    let mut server = make_multi_admission_file_server(
        vec![(blocked_info, blocked_path), (ready_info, ready_path)],
        data_dir.clone(),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(SelectiveAcknowledgementAdmission),
    );
    server.oldest_first = true;
    server.test_readiness_hook = None;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(4);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    let first = next_nonempty_line(&mut rx).await;
    let second = next_nonempty_line(&mut rx).await;
    assert_eq!(first.text, Bytes::from_static(b"blocked-a"));
    assert_eq!(second.text, Bytes::from_static(b"ready-b"));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), second.checkpoint_gate.wait())
            .await
            .expect("ready key acknowledgement must bypass the stalled key"),
        CheckpointGateResult::ActorAcknowledged
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(10), first.checkpoint_gate.wait())
            .await
            .is_err()
    );

    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());
}

#[tokio::test]
async fn finished_file_churn_does_not_retain_ready_generations() {
    let mut dispatcher = AdmissionDispatcher::new(4);
    for inode in 0..2_000 {
        let file_id = file_source_common::FileFingerprint::DevInode(1, inode);
        let (checkpoint_gate, gate) = CheckpointGate::closed();
        dispatcher
            .pending_offsets
            .entry(file_id)
            .or_default()
            .push_back(0);
        dispatcher.insert_ready(ReadyAdmission {
            line: Line {
                text: Bytes::from_static(b"record"),
                filename: "file".into(),
                file_id,
                start_offset: 0,
                end_offset: 7,
                admission_key: None,
                checkpoint_gate,
            },
            permit: Err(AdmissionError::Closed),
            gate,
        });
        dispatcher.finish_file(file_id);
    }
    assert!(
        dispatcher.ready_file_generations.is_empty(),
        "completed fingerprints must not accumulate"
    );
    assert!(
        dispatcher.ready_files.is_empty(),
        "no queued reference remains after file completion"
    );
    assert!(dispatcher.next_ready().is_none());
}

/// Reserves a permit that needs no waiting, so `enqueue` skips the wait machinery.
struct ImmediatePending(Arc<AtomicUsize>);

impl PendingAdmission for ImmediatePending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn try_wait(&mut self) -> Option<Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        Some(Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
            BatchOutcomePermit {
                transactions: Arc::clone(&self.0),
                result: Ok(()),
            },
        ))))
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        unreachable!("an immediately ready reservation never waits")
    }
}

/// An immediately issued permit must not overtake an earlier record that is still waiting, and
/// must not register wait state (no future, no abort handle).
#[tokio::test]
async fn immediately_issued_permits_keep_file_order_and_skip_the_wait_machinery() {
    let admission = ControlledAdmission::new();
    let transactions = Arc::new(AtomicUsize::new(0));
    let file_id = file_source_common::FileFingerprint::DevInode(1, 1);
    let mut dispatcher = AdmissionDispatcher::new(ADMISSION_READ_AHEAD_PER_FILE);
    for start_offset in 0..3_u64 {
        let (checkpoint_gate, gate) = CheckpointGate::closed();
        let line = Line {
            text: Bytes::from_static(b"record"),
            filename: "ordered.log".into(),
            file_id,
            start_offset,
            end_offset: start_offset + 1,
            admission_key: None,
            checkpoint_gate,
        };
        let reservation: Box<dyn PendingAdmission> = if start_offset == 1 {
            Box::new(ControlledPending(Arc::clone(&admission)))
        } else {
            Box::new(ImmediatePending(Arc::clone(&transactions)))
        };
        dispatcher.enqueue(line, reservation, gate);
    }
    assert_eq!(dispatcher.pending.len(), 1, "only the waiting record owns a wait");
    assert_eq!(
        dispatcher.pending_abort_handles.get(&file_id).map_or(0, Vec::len),
        1
    );

    let first = dispatcher.next_ready_run(usize::MAX);
    assert_eq!(
        first.iter().map(|ready| ready.line.start_offset).collect::<Vec<_>>(),
        vec![0],
        "record 2 is ready but must stay behind the waiting record 1"
    );
    dispatcher.pending_offsets.get_mut(&file_id).unwrap().pop_front();

    admission.wait_grant.add_permits(1);
    let second = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let run = dispatcher.next_ready_run(usize::MAX);
            if !run.is_empty() {
                return run;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the granted wait must complete");
    // The waited permit does not support batching, so it goes alone and record 2 follows it.
    assert_eq!(
        second.iter().map(|ready| ready.line.start_offset).collect::<Vec<_>>(),
        vec![1]
    );
    dispatcher.pending_offsets.get_mut(&file_id).unwrap().pop_front();
    dispatcher.queue_ready_file(file_id);
    let third = dispatcher.next_ready_run(usize::MAX);
    assert_eq!(
        third.iter().map(|ready| ready.line.start_offset).collect::<Vec<_>>(),
        vec![2]
    );
}

#[tokio::test]
async fn completed_admission_waits_do_not_accumulate_abort_handles_for_a_busy_file() {
    const COMPLETIONS: u64 = 512;

    let admission = ControlledAdmission::new();
    let file_id = file_source_common::FileFingerprint::DevInode(1, 1);
    let mut dispatcher = AdmissionDispatcher::new(ADMISSION_READ_AHEAD_PER_FILE);
    for start_offset in 0..COMPLETIONS {
        let (checkpoint_gate, gate) = CheckpointGate::closed();
        dispatcher.enqueue(
            Line {
                text: Bytes::from_static(b"record"),
                filename: "busy.log".into(),
                file_id,
                start_offset,
                end_offset: start_offset + 1,
                admission_key: None,
                checkpoint_gate,
            },
            Box::new(ControlledPending(Arc::clone(&admission))),
            gate,
        );
        admission.wait_grant.add_permits(1);
        let ready = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(ready) = dispatcher.next_ready() {
                    return ready;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the granted admission wait must complete");
        assert_eq!(ready.line.start_offset, start_offset);
        assert_eq!(
            dispatcher
                .pending_abort_handles
                .get(&file_id)
                .map_or(0, Vec::len),
            0,
            "completed waits must release cancellation state before the file drains"
        );
        assert_eq!(
            dispatcher
                .pending_offsets
                .get_mut(&file_id)
                .unwrap()
                .pop_front(),
            Some(start_offset)
        );
    }
}

#[tokio::test]
async fn cancelling_wait_with_delivered_permit_refunds_capacity() {
    let file_id = file_source_common::FileFingerprint::DevInode(1, 1);
    let cancelled = Arc::new(AtomicU64::new(0));
    let (send, receive) = tokio::sync::oneshot::channel();
    let (checkpoint_gate, gate) = CheckpointGate::closed();
    let mut dispatcher = AdmissionDispatcher::new(ADMISSION_READ_AHEAD_PER_FILE);
    dispatcher.enqueue(
        Line {
            text: Bytes::from_static(b"record"),
            filename: "cancelled.log".into(),
            file_id,
            start_offset: 0,
            end_offset: 6,
            admission_key: None,
            checkpoint_gate,
        },
        Box::new(DeliveredPermitPending { receive }),
        gate,
    );
    assert!(
        send.send(OwnedFileSourceAdmissionPermit::new(Box::new(
            CancelCountingPermit(Arc::clone(&cancelled)),
        )))
        .is_ok()
    );

    dispatcher.cancel_file(file_id).await;

    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ready_cross_file_batch_uses_one_transaction_and_resolves_each_gate() {
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let server = make_admission_file_server(
        tmp.path().join("unused.log"),
        data_dir,
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        None,
    );
    let transactions = Arc::new(AtomicUsize::new(0));
    let outcomes = [Ok(()), Err(AdmissionError::Stale), Ok(())];
    let mut ready = Vec::new();
    for (index, result) in outcomes.into_iter().enumerate() {
        let (checkpoint_gate, gate) = CheckpointGate::closed();
        ready.push(ReadyAdmission {
            line: Line {
                text: Bytes::from(format!("record-{index}")),
                filename: format!("file-{index}"),
                file_id: file_source_common::FileFingerprint::DevInode(1, index as u64),
                start_offset: 0,
                end_offset: 1,
                admission_key: Some(AdmissionKey::PodUid(Arc::from(format!("uid-{index}")))),
                checkpoint_gate,
            },
            permit: Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                BatchOutcomePermit {
                    transactions: Arc::clone(&transactions),
                    result,
                },
            ))),
            gate,
        });
    }
    let (mut tx, mut rx) = mpsc::channel::<Vec<Line>>(1);
    let mut dispatcher = AdmissionDispatcher::new(ADMISSION_READ_AHEAD_PER_FILE);
    let mut watchers = indexmap::IndexMap::new();

    server
        .dispatch_ready_admission_batch(&mut tx, &mut dispatcher, &mut watchers, ready)
        .await
        .unwrap();
    let mut delivered = rx.next().await.unwrap();
    for _ in 0..delivered.len() {
        let acknowledgement = dispatcher.acknowledgements.next().await.unwrap();
        server
            .finish_admission_acknowledgement(&mut dispatcher, None, acknowledgement)
            .await;
    }
    delivered.sort_by(|left, right| left.text.cmp(&right.text));
    let mut gate_results = Vec::new();
    for line in delivered {
        gate_results.push(line.checkpoint_gate.wait().await);
    }

    assert_eq!(transactions.load(Ordering::SeqCst), 1);
    assert_eq!(
        gate_results,
        vec![
            CheckpointGateResult::ActorAcknowledged,
            CheckpointGateResult::Rejected,
            CheckpointGateResult::ActorAcknowledged,
        ]
    );
}

#[tokio::test]
async fn checkpoint_gate_consumer_does_not_deadlock_above_channel_capacity() {
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let mut paths = Vec::new();
    for index in 0..4 {
        let path = tmp.path().join(format!("pod-{index}.log"));
        fs::write(&path, format!("record-{index}\n")).await.unwrap();
        let mut info = admission_log_info();
        let uid = format!("uid-{index}");
        info.pod_uid.clone_from(&uid);
        info.admission_metadata.as_mut().unwrap().key = AdmissionKey::PodUid(Arc::from(uid));
        paths.push((info, path));
    }
    let admission = ControlledAdmission::new();
    admission.wait_grant.add_permits(paths.len());
    admission.sent_ack.add_permits(paths.len());
    let mut server = make_multi_admission_file_server(
        paths,
        data_dir.clone(),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::clone(&admission) as Arc<dyn FileSourceAdmission>,
    );
    server.test_readiness_hook = None;
    server.drain_on_shutdown = false;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    let records = tokio::time::timeout(Duration::from_secs(1), async {
        let mut records = HashSet::new();
        for _ in 0..4 {
            let line = next_nonempty_line(&mut rx).await;
            assert_eq!(
                line.checkpoint_gate.wait().await,
                CheckpointGateResult::ActorAcknowledged
            );
            records.insert(line.text);
        }
        records
    })
    .await
    .expect("actor acknowledgements must unblock a bounded sequential gate consumer");
    assert_eq!(records.len(), 4);

    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());
}

#[tokio::test]
async fn bounded_reservation_scan_reaches_every_continuously_backlogged_key() {
    const KEYS: usize = 256;
    const CAPACITY: usize = 8;

    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let mut paths = Vec::with_capacity(KEYS);
    for index in 0..KEYS {
        let path = tmp.path().join(format!("pod-{index:03}.log"));
        let contents = (0..512)
            .map(|sequence| format!("{index:03}-{sequence:03}\n"))
            .collect::<String>();
        fs::write(&path, contents).await.unwrap();
        let mut info = admission_log_info();
        let uid = format!("uid-{index:03}");
        info.pod_uid.clone_from(&uid);
        info.admission_metadata.as_mut().unwrap().key = AdmissionKey::PodUid(Arc::from(uid));
        paths.push((info, path));
    }
    let reservation_limit = (KEYS * ADMISSION_READ_AHEAD_PER_FILE) as u64;
    let admission = BoundedOpportunityAdmission::new(CAPACITY, reservation_limit);
    let mut server = make_multi_admission_file_server(
        paths,
        data_dir.clone(),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::clone(&admission) as Arc<dyn FileSourceAdmission>,
    );
    server.drain_on_shutdown = false;
    server.glob_minimum_cooldown = Duration::from_secs(60);
    server.test_readiness_hook = None;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(CAPACITY);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));
    let consumer = tokio::spawn(async move {
        while let Some(batch) = rx.next().await {
            for line in batch {
                assert_eq!(
                    line.checkpoint_gate.wait().await,
                    CheckpointGateResult::ActorAcknowledged
                );
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        while admission.reservations.load(Ordering::SeqCst) < reservation_limit {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the bounded process budget must keep turning over");
    assert_eq!(
        admission.keys.lock().unwrap().len(),
        KEYS,
        "every continuously backlogged key needs one four-record reservation tranche before the bounded process budget is exhausted"
    );
    assert!(admission.max_active.load(Ordering::SeqCst) <= CAPACITY as u64);

    shutdown_tx.send(()).unwrap();
    checkpoint_shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
    consumer.await.unwrap();
}

#[tokio::test]
async fn no_admission_path_keeps_batch_reading_behavior() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let records = (0..12)
        .map(|index| format!("id-{index}\n"))
        .collect::<String>();
    fs::write(&path, records).await.unwrap();
    let server = make_file_server(vec![path], data_dir.clone(), false);
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));

    let batch = tokio::time::timeout(Duration::from_secs(1), rx.next())
        .await
        .expect("the no-admission source must read immediately")
        .unwrap();
    assert_eq!(batch.len(), 12);
    assert!(
        batch
            .iter()
            .all(|line| matches!(&line.checkpoint_gate, CheckpointGate::Open))
    );
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn single_credit_transactions_preserve_four_record_read_ahead() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\nid-3\nid-4\nid-5\n")
        .await
        .unwrap();
    let admission = ControlledAdmission::new();
    admission.tranche_size.store(1, Ordering::SeqCst);
    let server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, _rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (_checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    tokio::time::timeout(Duration::from_secs(1), async {
        while admission.wait_started.available_permits() < ADMISSION_READ_AHEAD_PER_FILE {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("single-credit transactions must preserve four-record read-ahead");
    assert_eq!(
        admission.tranche_transactions.load(Ordering::SeqCst),
        ADMISSION_READ_AHEAD_PER_FILE as u64
    );

    running.abort();
    let _ = running.await;
}

#[tokio::test]
async fn cached_large_tranche_preserves_four_record_read_ahead() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let records = (0..64)
        .map(|index| format!("id-{index}\n"))
        .collect::<String>();
    fs::write(&path, records).await.unwrap();
    let admission = ControlledAdmission::new();
    admission.tranche_size.store(16, Ordering::SeqCst);
    let server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, _rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (_checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    tokio::time::timeout(Duration::from_secs(1), async {
        while admission.wait_started.available_permits() < ADMISSION_READ_AHEAD_PER_FILE {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a large cached tranche must fill the four-record read-ahead window");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        admission.wait_started.available_permits(),
        ADMISSION_READ_AHEAD_PER_FILE
    );
    assert_eq!(admission.tranche_transactions.load(Ordering::SeqCst), 1);

    running.abort();
    let _ = running.await;
}

#[tokio::test]
async fn one_file_exposes_bounded_demand_and_sends_in_offset_order() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let records = (1..=12)
        .map(|index| format!("id-{index}\n"))
        .collect::<String>();
    fs::write(&path, records).await.unwrap();
    let admission = ControlledAdmission::new();
    let mut server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    server.max_read_bytes = 1_024;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let checkpointer = Checkpointer::new(&data_dir);
    let checkpoints = checkpointer.view();
    let running = tokio::spawn(server.run(tx, shutdown_rx, checkpoint_shutdown_rx, checkpointer));

    tokio::time::timeout(Duration::from_secs(1), async {
        while admission.wait_started.available_permits() < ADMISSION_READ_AHEAD_PER_FILE {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one file must expose one bounded scheduling tranche");
    assert_eq!(
        admission.wait_started.available_permits(),
        ADMISSION_READ_AHEAD_PER_FILE
    );
    assert_eq!(
        admission.tranche_transactions.load(Ordering::SeqCst),
        1,
        "one bounded file tranche must use one admission transaction"
    );
    let mut checkpoint_offset = 0;
    for expected in (1..=12).map(|index| format!("id-{index}")) {
        admission.wait_grant.add_permits(1);
        let line = next_nonempty_line(&mut rx).await;
        assert_eq!(line.text, Bytes::copy_from_slice(expected.as_bytes()));
        admission.sent_started.acquire().await.unwrap().forget();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), rx.next())
                .await
                .is_err(),
            "the next offset must remain blocked until the prior actor acknowledgement"
        );
        admission.sent_ack.add_permits(1);
        assert_eq!(
            line.checkpoint_gate.wait().await,
            CheckpointGateResult::ActorAcknowledged
        );
        assert!(line.end_offset > checkpoint_offset);
        checkpoint_offset = line.end_offset;
        checkpoints.update(line.file_id, checkpoint_offset);
        assert_eq!(checkpoints.get(line.file_id), Some(checkpoint_offset));
    }

    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());
}

#[tokio::test]
async fn pending_waits_stop_at_the_per_file_read_ahead_bound_without_repolling() {
    const RECORDS: u64 = 512;

    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let contents = (0..RECORDS)
        .map(|index| format!("id-{index}\n"))
        .collect::<String>();
    fs::write(&path, contents).await.unwrap();
    let admission = PollCountingAdmission::new();
    let mut server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    server.max_read_bytes = 8 * 1_024;
    let (tx, _rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    tokio::time::timeout(Duration::from_secs(1), async {
        while admission.polls.load(Ordering::SeqCst) < ADMISSION_READ_AHEAD_PER_FILE as u64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one bounded tranche must be registered with admission");
    assert_eq!(
        admission.reservations.load(Ordering::SeqCst),
        ADMISSION_READ_AHEAD_PER_FILE as u64
    );
    assert_eq!(
        admission.polls.load(Ordering::SeqCst),
        ADMISSION_READ_AHEAD_PER_FILE as u64
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        admission.polls.load(Ordering::SeqCst),
        ADMISSION_READ_AHEAD_PER_FILE as u64,
        "dormant waits must not be rescanned on each file-server loop"
    );
    assert_eq!(
        admission.reservations.load(Ordering::SeqCst),
        ADMISSION_READ_AHEAD_PER_FILE as u64,
        "read-ahead must stop before probing more records or EOF"
    );

    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());
}

#[tokio::test]
async fn missing_path_metadata_uses_unattributed_admission() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\n").await.unwrap();
    let mut info = admission_log_info();
    info.admission_metadata = None;
    let admission = ControlledAdmission::new();
    admission.wait_grant.add_permits(1);
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let server = make_admission_file_server(
        path,
        data_dir.clone(),
        info,
        file_to_pod_map,
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    let batch = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch[0].text, Bytes::from_static(b"id-1"));
    assert_eq!(
        admission.observed_metadata.lock().unwrap()[0].key,
        AdmissionKey::Unattributed
    );
    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());
}

#[tokio::test]
async fn shutdown_drain_admits_before_send_and_downstream_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();
    let admission = ControlledAdmission::new();
    admission.wait_grant.add_permits(2);
    admission.sent_ack.add_permits(2);
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        file_to_pod_map,
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    server.drain_on_shutdown = true;
    server.test_readiness_hook = Some(Arc::new(|_| false));
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let checkpointer = Checkpointer::new(&data_dir);
    let checkpoints = checkpointer.view();
    let running = tokio::spawn(server.run(tx, shutdown_rx, checkpoint_shutdown_rx, checkpointer));
    let _ = shutdown_tx.send(());

    let mut ids = Vec::new();
    let mut drained_file_id = None;
    while let Some(batch) = rx.next().await {
        for line in batch {
            assert_eq!(
                line.checkpoint_gate.wait().await,
                CheckpointGateResult::ActorAcknowledged
            );
            ids.push(line.text.clone());
            drained_file_id = Some(line.file_id);
            checkpoints.update(line.file_id, line.end_offset);
        }
    }
    let _ = checkpoint_shutdown_tx.send(());
    running.await.unwrap().unwrap();
    assert_eq!(
        ids,
        vec![Bytes::from_static(b"id-1"), Bytes::from_static(b"id-2")]
    );
    assert_eq!(
        admission.observed_metadata.lock().unwrap().len(),
        ADMISSION_READ_AHEAD_PER_FILE,
        "shutdown preserves the full per-file reservation tranche even when EOF follows two records"
    );

    let mut persisted = Checkpointer::new(&data_dir);
    persisted.read_checkpoints(None).await;
    assert_eq!(persisted.view().get(drained_file_id.unwrap()), Some(10));
}

#[tokio::test]
async fn shutdown_rewinds_pending_admission_before_draining_later_records() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();
    let admission = ControlledAdmission::new();
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        file_to_pod_map,
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    server.drain_on_shutdown = true;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    admission.wait_started.acquire().await.unwrap().forget();
    shutdown_tx.send(()).unwrap();
    admission.wait_grant.add_permits(2);
    admission.sent_ack.add_permits(2);

    let received = tokio::time::timeout(Duration::from_secs(1), async {
        let mut received = Vec::new();
        while let Some(batch) = rx.next().await {
            for line in batch {
                assert_eq!(
                    line.checkpoint_gate.wait().await,
                    CheckpointGateResult::ActorAcknowledged
                );
                received.push(line.text);
            }
        }
        received
    })
    .await
    .expect("shutdown drain must finish after replaying the pending file");
    let _ = checkpoint_shutdown_tx.send(());
    running.await.unwrap().unwrap();
    assert_eq!(
        received,
        vec![Bytes::from_static(b"id-1"), Bytes::from_static(b"id-2")],
        "observed reservations: {}",
        admission.observed_metadata.lock().unwrap().len()
    );
}

#[tokio::test]
async fn shutdown_drain_terminates_after_admission_actor_closes() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\n").await.unwrap();
    let admission = ControlledAdmission::new();
    *admission.wait_error.lock().unwrap() = Some(AdmissionError::Closed);
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        file_to_pod_map,
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    server.drain_on_shutdown = true;
    server.test_readiness_hook = Some(Arc::new(|_| false));
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    shutdown_tx.send(()).unwrap();
    checkpoint_shutdown_tx.send(()).unwrap();
    let drained = tokio::time::timeout(Duration::from_secs(1), async {
        let mut lines = Vec::new();
        while let Some(batch) = rx.next().await {
            lines.extend(batch);
        }
        lines
    })
    .await
    .expect("a closed actor must not leave drain waiting forever");
    assert!(drained.is_empty());
    tokio::time::timeout(Duration::from_secs(1), running)
        .await
        .expect("closed admission must let shutdown drain finish")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_drain_allows_a_ready_key_past_an_unavailable_key() {
    let tmp = tempdir().unwrap();
    let blocked_path = tmp.path().join("blocked.log");
    let ready_path = tmp.path().join("ready.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&blocked_path, b"blocked\n").await.unwrap();
    fs::write(&ready_path, b"ready\n").await.unwrap();
    let mut blocked_info = admission_log_info();
    blocked_info.pod_uid = "blocked".to_owned();
    blocked_info.admission_metadata.as_mut().unwrap().key =
        AdmissionKey::PodUid(Arc::from("blocked"));
    let mut ready_info = admission_log_info();
    ready_info.pod_uid = "ready".to_owned();
    ready_info.admission_metadata.as_mut().unwrap().key = AdmissionKey::PodUid(Arc::from("ready"));
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let server = make_multi_admission_file_server(
        vec![(blocked_info, blocked_path), (ready_info, ready_path)],
        data_dir.clone(),
        file_to_pod_map,
        Arc::new(SelectiveDrainAdmission),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    shutdown_tx.send(()).unwrap();
    checkpoint_shutdown_tx.send(()).unwrap();
    let mut lines = tokio::time::timeout(Duration::from_secs(1), async {
        let mut lines = Vec::new();
        while let Some(batch) = rx.next().await {
            lines.extend(batch);
        }
        lines
    })
    .await
    .expect("an unavailable key must not block a ready drain key");
    assert_eq!(lines.len(), 1);
    let line = lines.pop().unwrap();
    assert_eq!(line.text, Bytes::from_static(b"ready"));
    assert_eq!(
        line.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    tokio::time::timeout(Duration::from_secs(1), running)
        .await
        .expect("drain must defer unavailable keys and finish")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn oldest_first_admission_bound_failure_does_not_block_another_key() {
    let tmp = tempdir().unwrap();
    let blocked_path = tmp.path().join("blocked.log");
    let ready_path = tmp.path().join("ready.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&blocked_path, b"blocked\n").await.unwrap();
    fs::write(&ready_path, b"ready\n").await.unwrap();
    let mut blocked_info = admission_log_info();
    blocked_info.pod_uid = "blocked".to_owned();
    blocked_info.admission_metadata.as_mut().unwrap().key =
        AdmissionKey::PodUid(Arc::from("blocked"));
    let mut ready_info = admission_log_info();
    ready_info.pod_uid = "ready".to_owned();
    ready_info.admission_metadata.as_mut().unwrap().key = AdmissionKey::PodUid(Arc::from("ready"));
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_multi_admission_file_server(
        vec![(blocked_info, blocked_path), (ready_info, ready_path)],
        data_dir.clone(),
        file_to_pod_map,
        Arc::new(BoundsRejectingAdmission),
    );
    server.drain_on_shutdown = false;
    server.oldest_first = true;
    server.test_readiness_hook = None;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    let line = next_nonempty_line(&mut rx).await;
    assert_eq!(line.text, Bytes::from_static(b"ready"));
    assert_eq!(
        line.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    shutdown_tx.send(()).unwrap();
    checkpoint_shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn admitted_archive_is_not_done_before_downstream_checkpoint() {
    use async_compression::tokio::bufread::GzipEncoder;
    use tokio::io::AsyncReadExt;

    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log.gz");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let mut encoded = Vec::new();
    GzipEncoder::new(&b"id-1\n"[..])
        .read_to_end(&mut encoded)
        .await
        .unwrap();
    fs::write(&path, encoded).await.unwrap();
    let admission = ControlledAdmission::new();
    admission.wait_grant.add_permits(1);
    admission.sent_ack.add_permits(1);
    let file_to_pod_map = Arc::new(Mutex::new(HashMap::new()));
    let mut server = make_admission_file_server(
        path.clone(),
        data_dir.clone(),
        admission_log_info(),
        file_to_pod_map,
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    server.archive_extensions = vec!["gz".to_owned()];
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    let line = next_nonempty_line(&mut rx).await;
    assert_eq!(
        line.checkpoint_gate.wait().await,
        CheckpointGateResult::ActorAcknowledged
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    running.abort();
    let _ = running.await;
    let _ = checkpoint_shutdown_tx.send(());

    let mut known = HashMap::new();
    let mut fingerprinter = Fingerprinter::new(
        FingerprintStrategy::FirstLinesChecksum {
            ignored_header_bytes: 0,
            lines: 1,
        },
        1_024,
        true,
    );
    let fingerprint = fingerprinter
        .fingerprint_or_emit(&path, &mut known, &NoErrors)
        .await
        .unwrap();
    let mut checkpointer = Checkpointer::new(&data_dir);
    checkpointer.read_checkpoints(None).await;
    assert!(!checkpointer.view().get_done(fingerprint));
}

async fn next_nonempty_line(rx: &mut mpsc::Receiver<Vec<Line>>) -> Line {
    loop {
        let mut batch = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("file server should send a batch")
            .expect("file server closed before the expected record");
        if let Some(line) = batch.pop() {
            return line;
        }
    }
}

const TEST_MAX_LINE_BYTES: usize = 1024;

#[derive(Default)]
struct TestProcessPendingBudget {
    by_key: HashMap<String, (usize, usize)>,
    total_events: usize,
    total_bytes: usize,
    max_pre_read_reservation_bytes: usize,
    max_pre_read_key_bytes: usize,
    max_pre_read_total_bytes: usize,
}

impl TestProcessPendingBudget {
    fn try_reserve(&mut self, key: &str, bytes: usize) -> bool {
        const PER_KEY_EVENTS: usize = 1;
        const PER_KEY_BYTES: usize = TEST_MAX_LINE_BYTES;
        const TOTAL_EVENTS: usize = 3;
        const TOTAL_BYTES: usize = 3 * TEST_MAX_LINE_BYTES;

        let (key_events, key_bytes) = self.by_key.get(key).copied().unwrap_or_default();
        if key_events == PER_KEY_EVENTS
            || key_bytes + bytes > PER_KEY_BYTES
            || self.total_events == TOTAL_EVENTS
            || self.total_bytes + bytes > TOTAL_BYTES
        {
            return false;
        }
        self.by_key
            .insert(key.to_owned(), (key_events + 1, key_bytes + bytes));
        self.total_events += 1;
        self.total_bytes += bytes;
        self.max_pre_read_reservation_bytes = self.max_pre_read_reservation_bytes.max(bytes);
        self.max_pre_read_key_bytes = self.max_pre_read_key_bytes.max(key_bytes + bytes);
        self.max_pre_read_total_bytes = self.max_pre_read_total_bytes.max(self.total_bytes);
        true
    }

    fn reconcile(&mut self, key: &str, reserved_bytes: usize, actual_bytes: usize) {
        let released_bytes = reserved_bytes.checked_sub(actual_bytes).unwrap();
        let (_, key_bytes) = self.by_key.get_mut(key).unwrap();
        *key_bytes -= released_bytes;
        self.total_bytes -= released_bytes;
    }

    fn release(&mut self, key: &str, bytes: usize) {
        let (events, key_bytes) = self.by_key.get_mut(key).unwrap();
        *events -= 1;
        *key_bytes -= bytes;
        self.total_events -= 1;
        self.total_bytes -= bytes;
    }
}

// A broken boundary that forwards the reserved A record fails because the downstream consumer
// checkpoints every received line, exactly like the Kubernetes source map.
#[tokio::test]
async fn selective_backpressure_holds_one_key_before_two_source_channels() {
    let tmp = tempdir().unwrap();
    let data_dirs = [tmp.path().join("data-1"), tmp.path().join("data-2")];
    for data_dir in &data_dirs {
        fs::create_dir_all(data_dir).await.unwrap();
    }

    let a_paths = [tmp.path().join("a-1.log"), tmp.path().join("a-2.log")];
    let flowing_paths = [tmp.path().join("b.log"), tmp.path().join("c.log")];
    for path in &a_paths {
        fs::write(path, b"a00\n").await.unwrap();
    }
    fs::write(&flowing_paths[0], b"b00\n").await.unwrap();
    fs::write(&flowing_paths[1], b"c00\n").await.unwrap();

    let blocked_paths: Arc<HashSet<PathBuf>> = Arc::new(a_paths.iter().cloned().collect());
    let budget = Arc::new(Mutex::new(TestProcessPendingBudget::default()));
    let budget_for_readiness = Arc::clone(&budget);
    let blocked_for_readiness = Arc::clone(&blocked_paths);
    let readiness_hook: crate::file_server::TestReadinessHook = Arc::new(move |path: &Path| {
        let key = if blocked_for_readiness.contains(path) {
            "A".to_owned()
        } else {
            path.file_name().unwrap().to_string_lossy().into_owned()
        };
        budget_for_readiness
            .lock()
            .unwrap()
            .try_reserve(&key, TEST_MAX_LINE_BYTES)
    });

    let held_lines = Arc::new(Mutex::new(Vec::<Line>::new()));
    let held_for_admission = Arc::clone(&held_lines);
    let blocked_for_admission = Arc::clone(&blocked_paths);
    let budget_for_admission = Arc::clone(&budget);
    let admission_hook: crate::file_server::TestLineAdmissionHook = Arc::new(move |line: Line| {
        let path = Path::new(&line.filename);
        let key = if blocked_for_admission.contains(path) {
            "A".to_owned()
        } else {
            path.file_name().unwrap().to_string_lossy().into_owned()
        };
        budget_for_admission
            .lock()
            .unwrap()
            .reconcile(&key, TEST_MAX_LINE_BYTES, line.text.len());
        if blocked_for_admission.contains(Path::new(&line.filename)) {
            held_for_admission.lock().unwrap().push(line);
            None
        } else {
            Some(line)
        }
    });
    let mut servers = Vec::new();
    let mut consumers = Vec::new();
    for index in 0..2 {
        let mut file_server = make_file_server(
            vec![a_paths[index].clone(), flowing_paths[index].clone()],
            data_dirs[index].clone(),
            false,
        );
        file_server.max_read_bytes = 0;
        file_server.max_line_bytes = TEST_MAX_LINE_BYTES;
        file_server.test_readiness_hook = Some(Arc::clone(&readiness_hook));
        file_server.test_line_admission_hook = Some(Arc::clone(&admission_hook));

        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let checkpointer = Checkpointer::new(data_dirs[index].as_path());
        let checkpoints = checkpointer.view();
        servers.push(tokio::spawn(file_server.run(
            tx,
            shutdown_rx,
            futures::future::ready(()),
            checkpointer,
        )));

        let flowing_path = flowing_paths[index].clone();
        let blocked_paths = Arc::clone(&blocked_paths);
        let budget = Arc::clone(&budget);
        consumers.push(tokio::spawn(async move {
            let mut shutdown_tx = Some(shutdown_tx);
            while let Some(batch) = rx.next().await {
                let mut saw_flowing = false;
                for line in batch {
                    // This is deliberately unconditional: any A crossing the source channel
                    // becomes checkpoint-eligible, matching the real Kubernetes map.
                    checkpoints.update(line.file_id, line.end_offset);
                    let path = Path::new(&line.filename);
                    if !blocked_paths.contains(path) {
                        let key = path.file_name().unwrap().to_string_lossy();
                        budget.lock().unwrap().release(&key, line.text.len());
                    }
                    saw_flowing |= path == flowing_path;
                }
                if saw_flowing && let Some(shutdown_tx) = shutdown_tx.take() {
                    let _ = shutdown_tx.send(());
                }
            }
        }));
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        for server in servers {
            server.await.unwrap().unwrap();
        }
        for consumer in consumers {
            consumer.await.unwrap();
        }
    })
    .await
    .expect("both unrelated keys should traverse their source channels");

    let mut fingerprinter = Fingerprinter::new(
        FingerprintStrategy::FirstLinesChecksum {
            ignored_header_bytes: 0,
            lines: 1,
        },
        1024,
        true,
    );
    for index in 0..2 {
        let mut persisted = Checkpointer::new(data_dirs[index].as_path());
        persisted.read_checkpoints(None).await;
        let persisted = persisted.view();
        let blocked_fingerprint = fingerprinter
            .fingerprint_or_emit(&a_paths[index], &mut HashMap::new(), &NoErrors)
            .await
            .unwrap();
        assert_eq!(
            persisted.get(blocked_fingerprint),
            None,
            "blocked A crossed source channel {index} and became checkpoint-eligible"
        );
        let flowing_fingerprint = fingerprinter
            .fingerprint_or_emit(&flowing_paths[index], &mut HashMap::new(), &NoErrors)
            .await
            .unwrap();
        assert_eq!(
            persisted.get(flowing_fingerprint),
            Some(fs::metadata(&flowing_paths[index]).await.unwrap().len())
        );
    }

    let budget = budget.lock().unwrap();
    assert_eq!(
        budget.max_pre_read_reservation_bytes, TEST_MAX_LINE_BYTES,
        "pre-read reservation did not use max_line_bytes"
    );
    assert_eq!(
        budget.max_pre_read_key_bytes, TEST_MAX_LINE_BYTES,
        "one key exceeded its conservative pre-read byte cap"
    );
    assert!(
        (TEST_MAX_LINE_BYTES..=3 * TEST_MAX_LINE_BYTES).contains(&budget.max_pre_read_total_bytes),
        "process pre-read byte high-water mark violated its frozen bound"
    );
    assert_eq!(
        budget.total_events, 1,
        "process event cap was claimed twice"
    );
    assert_eq!(budget.total_bytes, 3, "process byte cap was claimed twice");
    assert_eq!(held_lines.lock().unwrap().len(), 1);
}

/// Grants immediately but acknowledges from another task, like the coordinator actor.
struct ActorAcknowledgedAdmission;

impl FileSourceAdmission for ActorAcknowledgedAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        SelectiveDrainAdmission.unattributed_metadata()
    }

    fn reserve(
        self: Arc<Self>,
        _metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        async { Ok(Box::new(ActorAcknowledgedPending) as Box<dyn PendingAdmission>) }.boxed()
    }
}

struct ActorAcknowledgedPending;

impl PendingAdmission for ActorAcknowledgedPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async {
            Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                ActorAcknowledgedPermit,
            )))
        }
        .boxed()
    }
}

struct ActorAcknowledgedPermit;

impl FileSourceAdmissionPermit for ActorAcknowledgedPermit {
    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        let (acknowledge, acknowledged) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = acknowledge.send(());
        });
        async move { acknowledged.await.map_err(|_| AdmissionError::Closed) }.boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {}

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

/// Catches a read loop that only polls acknowledgements between backoff sleeps: each record of an
/// admitted file then costs at least one empty pass plus a timer sleep, capping one file near
/// 400 events/s on a single-worker runtime even when admission never throttles.
#[tokio::test(start_paused = true)]
async fn actor_acknowledgements_do_not_cost_a_backoff_sleep_per_record() {
    const RECORDS: usize = 200;
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let contents: String = (0..RECORDS).map(|i| format!("id-{i}\n")).collect();
    fs::write(&path, contents).await.unwrap();

    let server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::new(ActorAcknowledgedAdmission) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(RECORDS);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let started = tokio::time::Instant::now();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));
    let mut received = 0;
    while received < RECORDS {
        received += next_nonempty_batch(&mut rx).await;
    }
    let elapsed = started.elapsed();
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
    // Virtual time advances only through timers, so per-record sleeps would total >= 400ms.
    assert!(
        elapsed < Duration::from_millis(RECORDS as u64),
        "{RECORDS} immediately admitted records took {elapsed:?} of backoff"
    );
}

async fn next_nonempty_batch(rx: &mut mpsc::Receiver<Vec<Line>>) -> usize {
    loop {
        let batch = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("file server should send a batch")
            .expect("file server closed before the expected records");
        if !batch.is_empty() {
            return batch.len();
        }
    }
}

/// Grants immediately and acknowledges whole batches from another task, like the coordinator.
struct BatchActorAdmission {
    transactions: Arc<AtomicUsize>,
}

impl FileSourceAdmission for BatchActorAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        SelectiveDrainAdmission.unattributed_metadata()
    }

    fn reserve(
        self: Arc<Self>,
        _metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        let transactions = Arc::clone(&self.transactions);
        async move { Ok(Box::new(BatchActorPending(transactions)) as Box<dyn PendingAdmission>) }
            .boxed()
    }
}

struct BatchActorPending(Arc<AtomicUsize>);

impl PendingAdmission for BatchActorPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async move {
            Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                BatchActorPermit(self.0),
            )))
        }
        .boxed()
    }
}

struct BatchActorPermit(Arc<AtomicUsize>);

impl FileSourceAdmissionPermit for BatchActorPermit {
    fn supports_batch(&self) -> bool {
        true
    }

    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        self.sent_many(Vec::new())
            .map(|mut results| results.pop().expect("one result"))
            .boxed()
    }

    fn sent_many(
        self: Box<Self>,
        additional: Vec<Box<dyn FileSourceAdmissionPermit>>,
    ) -> BoxFuture<'static, Vec<Result<(), AdmissionError>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let count = additional.len() + 1;
        let (acknowledge, acknowledged) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = acknowledge.send(());
        });
        async move {
            let result = acknowledged.await.map_err(|_| AdmissionError::Closed);
            vec![result; count]
        }
        .boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {}

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

/// Grants immediately but holds every acknowledgement until the test releases it, recording how
/// many waits were started and how many records each acknowledgement round trip covered.
struct GatedBatchAdmission {
    waits_started: Arc<AtomicUsize>,
    run_sizes: Arc<Mutex<Vec<usize>>>,
    release: Arc<tokio::sync::Semaphore>,
    /// `(run, index)` of the one record the actor rejects, if any.
    reject: Option<(usize, usize)>,
}

impl FileSourceAdmission for GatedBatchAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        SelectiveDrainAdmission.unattributed_metadata()
    }

    fn reserve(
        self: Arc<Self>,
        _metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        async move { Ok(Box::new(GatedBatchPending(self)) as Box<dyn PendingAdmission>) }.boxed()
    }
}

struct GatedBatchPending(Arc<GatedBatchAdmission>);

impl PendingAdmission for GatedBatchPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        self.0.waits_started.fetch_add(1, Ordering::SeqCst);
        async move {
            Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                GatedBatchPermit(self.0),
            )))
        }
        .boxed()
    }
}

struct GatedBatchPermit(Arc<GatedBatchAdmission>);

impl FileSourceAdmissionPermit for GatedBatchPermit {
    fn supports_batch(&self) -> bool {
        true
    }

    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        self.sent_many(Vec::new())
            .map(|mut results| results.pop().expect("one result"))
            .boxed()
    }

    fn sent_many(
        self: Box<Self>,
        additional: Vec<Box<dyn FileSourceAdmissionPermit>>,
    ) -> BoxFuture<'static, Vec<Result<(), AdmissionError>>> {
        let count = additional.len() + 1;
        let run = {
            let mut run_sizes = self.0.run_sizes.lock().unwrap();
            run_sizes.push(count);
            run_sizes.len() - 1
        };
        let release = Arc::clone(&self.0.release);
        let reject = self.0.reject;
        async move {
            release.acquire().await.expect("semaphore stays open").forget();
            let mut results = vec![Ok(()); count];
            if let Some((reject_run, index)) = reject
                && reject_run == run
            {
                results[index] = Err(AdmissionError::Stale);
            }
            results
        }
        .boxed()
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn refund_on_drop(self: Box<Self>) {}

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

/// While one acknowledgement is outstanding the file keeps reading ahead up to its in-flight
/// bound, and those records must then go out as ONE run (one round trip), not one per read pass.
/// The actor holds the first acknowledgement, so the run sizes do not depend on scheduling speed.
#[tokio::test]
async fn records_read_ahead_behind_an_outstanding_acknowledgement_go_out_as_one_run() {
    const RECORDS: usize = 100;
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let contents: String = (0..RECORDS).map(|i| format!("record-{i:04}\n")).collect();
    fs::write(&path, contents).await.unwrap();
    let waits_started = Arc::new(AtomicUsize::new(0));
    let run_sizes = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::new(GatedBatchAdmission {
            waits_started: Arc::clone(&waits_started),
            run_sizes: Arc::clone(&run_sizes),
            release: Arc::clone(&release),
            reject: None,
        }) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(RECORDS);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));

    tokio::time::timeout(Duration::from_secs(10), async {
        while waits_started.load(Ordering::SeqCst) < ADMISSION_IN_FLIGHT_PER_FILE {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("file should read ahead to its in-flight bound while the acknowledgement is held");
    // Reading must stop at the in-flight bound rather than run on to the end of the file.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        waits_started.load(Ordering::SeqCst),
        ADMISSION_IN_FLIGHT_PER_FILE
    );
    let first_run = run_sizes.lock().unwrap().clone();
    assert_eq!(first_run.len(), 1, "only the first run is sent before its acknowledgement");

    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), async {
        while run_sizes.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("records read ahead behind the first run should be sent after its acknowledgement");
    let second_run = run_sizes.lock().unwrap()[1];
    assert!(
        first_run[0] + second_run >= ADMISSION_IN_FLIGHT_PER_FILE
            && second_run >= ADMISSION_IN_FLIGHT_PER_FILE - first_run[0],
        "read-ahead records were split across round trips: first run {}, second run {second_run}",
        first_run[0]
    );

    release.add_permits(RECORDS);
    let mut received = 0;
    while received < RECORDS {
        received += next_nonempty_batch(&mut rx).await;
    }
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
}

/// Checks that a file's records arrive in file order through the batched path and that batching
/// is not defeated outright (one record per round trip would take RECORDS). The count is
/// scheduling dependent (about 8 records per trip here), so the bound only leaves margin for
/// that; the exact batching contract is pinned by the gated-acknowledgement test above.
#[tokio::test]
async fn one_acknowledgement_round_trip_covers_a_run_of_one_files_records() {
    const RECORDS: usize = 512;
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let contents: String = (0..RECORDS).map(|i| format!("record-{i:04}\n")).collect();
    fs::write(&path, contents).await.unwrap();
    let transactions = Arc::new(AtomicUsize::new(0));
    let mut server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::new(BatchActorAdmission {
            transactions: Arc::clone(&transactions),
        }) as Arc<dyn FileSourceAdmission>),
    );
    server.max_read_bytes = 2_048;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(RECORDS);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));
    let mut texts = Vec::new();
    while texts.len() < RECORDS {
        let batch = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("file server should send a batch")
            .expect("file server closed before the expected records");
        texts.extend(batch.into_iter().map(|line| line.text));
    }
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();
    let expected = (0..RECORDS)
        .map(|i| Bytes::from(format!("record-{i:04}")))
        .collect::<Vec<_>>();
    assert_eq!(texts, expected, "records must stay in file order");
    let transactions = transactions.load(Ordering::SeqCst);
    assert!(
        transactions * 4 <= RECORDS,
        "{RECORDS} records took {transactions} acknowledgement round trips"
    );
}

#[tokio::test]
async fn rejection_inside_a_run_rejects_the_rest_of_that_files_run() {
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let server = make_admission_file_server(
        tmp.path().join("unused.log"),
        data_dir,
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        None,
    );
    let file_id = file_source_common::FileFingerprint::DevInode(1, 1);
    let transactions = Arc::new(AtomicUsize::new(0));
    let outcomes = [Ok(()), Err(AdmissionError::Stale), Ok(())];
    let mut dispatcher = AdmissionDispatcher::new(ADMISSION_READ_AHEAD_PER_FILE);
    let mut ready = Vec::new();
    for (index, result) in outcomes.into_iter().enumerate() {
        let (checkpoint_gate, gate) = CheckpointGate::closed();
        dispatcher
            .pending_offsets
            .entry(file_id)
            .or_default()
            .push_back(index as u64);
        ready.push(ReadyAdmission {
            line: Line {
                text: Bytes::from(format!("record-{index}")),
                filename: "busy.log".into(),
                file_id,
                start_offset: index as u64,
                end_offset: index as u64 + 1,
                admission_key: Some(AdmissionKey::PodUid(Arc::from("uid"))),
                checkpoint_gate,
            },
            permit: Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                BatchOutcomePermit {
                    transactions: Arc::clone(&transactions),
                    result,
                },
            ))),
            gate,
        });
    }
    let (mut tx, mut rx) = mpsc::channel::<Vec<Line>>(1);
    let mut watchers = indexmap::IndexMap::new();

    server
        .dispatch_ready_admission_batch(&mut tx, &mut dispatcher, &mut watchers, ready)
        .await
        .unwrap();
    let delivered = rx.next().await.unwrap();
    assert_eq!(delivered.len(), 3);
    let acknowledgement = dispatcher.acknowledgements.next().await.unwrap();
    assert_eq!(acknowledgement.records.len(), 3);
    server
        .finish_admission_acknowledgement(&mut dispatcher, None, acknowledgement)
        .await;
    assert!(dispatcher.acknowledgements.is_empty());
    let mut gate_results = Vec::new();
    for line in delivered {
        gate_results.push(line.checkpoint_gate.wait().await);
    }

    assert_eq!(transactions.load(Ordering::SeqCst), 1);
    assert_eq!(
        gate_results,
        vec![
            CheckpointGateResult::ActorAcknowledged,
            CheckpointGateResult::Rejected,
            CheckpointGateResult::Rejected,
        ]
    );
    assert!(!dispatcher.pending_offsets.contains_key(&file_id));
}

/// The admitted drain must flush an unterminated tail like the unadmitted drain does (see
/// `drain_on_shutdown_emits_live_file_partial_line_exactly_once`), and its checkpoint, set only
/// after the downstream gate, must cover exactly what was acknowledged.
#[tokio::test]
async fn admitted_shutdown_drain_emits_live_file_partial_line_exactly_once() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let contents = b"id-1\nid-2\nid-3\npartial";
    fs::write(&path, contents).await.unwrap();

    async fn drain_once(path: &Path, data_dir: &Path) -> (Vec<Bytes>, Option<file_source_common::FileFingerprint>) {
        let mut server = make_admission_file_server(
            path.to_path_buf(),
            data_dir.to_path_buf(),
            admission_log_info(),
            Arc::new(Mutex::new(HashMap::new())),
            Some(Arc::new(ActorAcknowledgedAdmission) as Arc<dyn FileSourceAdmission>),
        );
        server.drain_on_shutdown = true;
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
        let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) =
            tokio::sync::oneshot::channel::<()>();
        let checkpointer = Checkpointer::new(data_dir);
        let checkpoints = checkpointer.view();
        let running = tokio::spawn(server.run(
            tx,
            futures::future::ready(()),
            checkpoint_shutdown_rx,
            checkpointer,
        ));
        let mut texts = Vec::new();
        let mut file_id = None;
        while let Some(batch) = rx.next().await {
            for line in batch {
                assert_eq!(
                    line.checkpoint_gate.wait().await,
                    CheckpointGateResult::ActorAcknowledged
                );
                texts.push(line.text.clone());
                file_id = Some(line.file_id);
                checkpoints.update(line.file_id, line.end_offset);
            }
        }
        let _ = checkpoint_shutdown_tx.send(());
        running.await.unwrap().expect("FileServer::run failed");
        (texts, file_id)
    }

    let (texts, file_id) = drain_once(&path, &data_dir).await;
    assert_eq!(
        texts,
        vec![
            Bytes::from_static(b"id-1"),
            Bytes::from_static(b"id-2"),
            Bytes::from_static(b"id-3"),
            Bytes::from_static(b"partial"),
        ]
    );
    let mut persisted = Checkpointer::new(&data_dir);
    persisted.read_checkpoints(None).await;
    assert_eq!(
        persisted.view().get(file_id.unwrap()),
        Some(contents.len() as u64),
        "checkpoint covers the acknowledged partial tail"
    );
    // A restart re-delivers nothing.
    assert!(drain_once(&path, &data_dir).await.0.is_empty());
}

/// Topology treats dropping the shutdown output as "this source has finished". It must outlive
/// the final checkpoint write, or a restart that follows immediately re-reads acknowledged lines.
#[tokio::test]
async fn shutdown_output_is_released_only_after_the_final_checkpoint_write() {
    struct ObserveCheckpointsOnDrop {
        checkpoint_file: PathBuf,
        observed: Arc<Mutex<Option<String>>>,
    }
    impl Drop for ObserveCheckpointsOnDrop {
        fn drop(&mut self) {
            *self.observed.lock().unwrap() = std::fs::read_to_string(&self.checkpoint_file).ok();
        }
    }

    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();

    let server = make_admission_file_server(
        path.clone(),
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        None,
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let checkpointer = Checkpointer::new(&data_dir);
    let checkpoints = checkpointer.view();
    let observed = Arc::new(Mutex::new(None));
    let shutdown = futures::future::ready(ObserveCheckpointsOnDrop {
        checkpoint_file: data_dir.join(file_source_common::checkpointer::CHECKPOINT_FILE_NAME),
        observed: Arc::clone(&observed),
    });
    let running = tokio::spawn(server.run(tx, shutdown, checkpoint_shutdown_rx, checkpointer));
    while let Some(batch) = rx.next().await {
        for line in batch {
            checkpoints.update(line.file_id, line.end_offset);
        }
    }
    let _ = checkpoint_shutdown_tx.send(());
    running.await.unwrap().expect("FileServer::run failed");

    let observed = observed.lock().unwrap().take();
    assert!(
        observed.is_some_and(|contents| !contents.contains(r#""checkpoints":[]"#)),
        "shutdown output was released before the final checkpoint write"
    );
}

/// The actor rejects one record in the middle of a batched run. The later records of that run
/// already crossed the source channel, so they are resent after the rewind (at-least-once); the
/// checkpoint may only ever advance contiguously over acknowledged records, and every record is
/// acknowledged exactly once, in file order.
#[tokio::test]
async fn rejection_mid_run_replays_from_the_rejected_record_without_skipping_the_checkpoint() {
    const RECORDS: usize = 100;
    const RECORD_BYTES: u64 = 12;
    const REJECTED_INDEX: usize = 4;
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let contents: String = (0..RECORDS).map(|i| format!("record-{i:04}\n")).collect();
    fs::write(&path, contents).await.unwrap();
    let waits_started = Arc::new(AtomicUsize::new(0));
    let run_sizes = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::new(GatedBatchAdmission {
            waits_started: Arc::clone(&waits_started),
            run_sizes: Arc::clone(&run_sizes),
            release: Arc::clone(&release),
            reject: Some((1, REJECTED_INDEX)),
        }) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(4 * RECORDS);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));

    // Let the second run (the read-ahead behind the held first acknowledgement) form, then let
    // every acknowledgement through.
    tokio::time::timeout(Duration::from_secs(10), async {
        while waits_started.load(Ordering::SeqCst) < ADMISSION_IN_FLIGHT_PER_FILE {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("file should read ahead while the first acknowledgement is held");
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), async {
        while run_sizes.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("second run should be sent");
    assert!(
        run_sizes.lock().unwrap()[1] > REJECTED_INDEX + 1,
        "the rejection must fall inside a run with records behind it"
    );
    release.add_permits(10 * RECORDS);

    let mut checkpoint = 0;
    let mut acknowledged = Vec::new();
    let mut rejected = 0;
    let mut delivered = 0;
    tokio::time::timeout(Duration::from_secs(10), async {
        while acknowledged.len() < RECORDS {
            let Some(batch) = rx.next().await else { break };
            for line in batch {
                delivered += 1;
                match line.checkpoint_gate.wait().await {
                    CheckpointGateResult::ActorAcknowledged => {
                        assert_eq!(
                            line.start_offset, checkpoint,
                            "an acknowledged record must continue the checkpoint exactly"
                        );
                        checkpoint = line.end_offset;
                        acknowledged.push(line.text);
                    }
                    CheckpointGateResult::Rejected => rejected += 1,
                }
            }
        }
    })
    .await
    .expect("every record should be acknowledged after the replay");
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();

    let expected = (0..RECORDS)
        .map(|i| Bytes::from(format!("record-{i:04}")))
        .collect::<Vec<_>>();
    assert_eq!(acknowledged, expected, "each record is acknowledged once, in order");
    assert_eq!(checkpoint, RECORDS as u64 * RECORD_BYTES);
    assert!(rejected > 0, "the rejected record and the rest of its run are rejected");
    assert_eq!(delivered, RECORDS + rejected, "only rejected records are delivered twice");
}

/// Mid-run rejection on a file that was rotated away and replaced: the rewind is refused and the
/// watcher is marked dead after that pass already reaped. The dead watcher still holds the old
/// file open at an un-rewound position; reading it again would send and acknowledge later
/// records and carry the checkpoint over the rejected one. `drain_on_shutdown` selects whether
/// the rejection is handled by the main loop or by the shutdown drain.
async fn dead_watcher_after_refused_rewind_is_never_read_again(drain_on_shutdown: bool) {
    const RECORDS: usize = 100;
    const RECORD_BYTES: u64 = 12;
    const REJECTED_INDEX: usize = 4;
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let contents: String = (0..RECORDS).map(|i| format!("record-{i:04}\n")).collect();
    fs::write(&path, contents).await.unwrap();
    let waits_started = Arc::new(AtomicUsize::new(0));
    let run_sizes = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut server = make_admission_file_server(
        path.clone(),
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::new(GatedBatchAdmission {
            waits_started: Arc::clone(&waits_started),
            run_sizes: Arc::clone(&run_sizes),
            release: Arc::clone(&release),
            reject: Some((1, REJECTED_INDEX)),
        }) as Arc<dyn FileSourceAdmission>),
    );
    server.drain_on_shutdown = drain_on_shutdown;
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(4 * RECORDS);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (checkpoint_shutdown_tx, checkpoint_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        checkpoint_shutdown_rx,
        Checkpointer::new(&data_dir),
    ));

    // Read ahead behind the held first acknowledgement, then replace the path with a different
    // file so the rejected record's rewind finds a different inode.
    tokio::time::timeout(Duration::from_secs(10), async {
        while waits_started.load(Ordering::SeqCst) < ADMISSION_IN_FLIGHT_PER_FILE {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("file should read ahead while the first acknowledgement is held");
    fs::rename(&path, tmp.path().join("records.log.1")).await.unwrap();
    let replacement: String = (0..RECORDS).map(|i| format!("other-{i:05}\n")).collect();
    fs::write(&path, replacement).await.unwrap();
    let mut shutdown_tx = Some(shutdown_tx);
    if drain_on_shutdown {
        shutdown_tx.take().unwrap().send(()).unwrap();
    }
    release.add_permits(10 * RECORDS);

    let mut checkpoint = 0;
    let mut acknowledged = 0;
    let mut rejected = 0;
    // The rejection settles within milliseconds; keep collecting well past it so a read of the
    // dead watcher would show up.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(batch) = rx.next().await {
            for line in batch {
                if !line.text.starts_with(b"record-") {
                    continue;
                }
                match line.checkpoint_gate.wait().await {
                    CheckpointGateResult::ActorAcknowledged => {
                        assert_eq!(
                            line.start_offset, checkpoint,
                            "an acknowledged record must continue the checkpoint exactly"
                        );
                        checkpoint = line.end_offset;
                        acknowledged += 1;
                    }
                    CheckpointGateResult::Rejected => rejected += 1,
                }
            }
        }
    })
    .await;
    if let Some(shutdown_tx) = shutdown_tx.take() {
        shutdown_tx.send(()).unwrap();
    }
    let _ = checkpoint_shutdown_tx.send(());
    running.await.unwrap().unwrap();

    if drain_on_shutdown {
        // Shutdown cancels the read-ahead and its rewind is the refused one, so the drain must
        // not read the dead watcher; only contiguous acknowledged records may exist.
        assert!((1..RECORDS).contains(&acknowledged));
        assert_eq!(checkpoint, acknowledged as u64 * RECORD_BYTES);
    } else {
        assert!(rejected > 0, "the rejected record and the rest of its run are rejected");
        assert_eq!(
            checkpoint,
            (1 + REJECTED_INDEX) as u64 * RECORD_BYTES,
            "the checkpoint stops at the rejected record"
        );
        assert_eq!(acknowledged, 1 + REJECTED_INDEX, "nothing is acknowledged past the rejection");
    }
}

#[tokio::test]
async fn dead_watcher_after_refused_rewind_is_not_read_again_by_the_main_loop() {
    dead_watcher_after_refused_rewind_is_never_read_again(false).await;
}

#[tokio::test]
async fn dead_watcher_after_refused_rewind_is_not_read_again_by_the_shutdown_drain() {
    dead_watcher_after_refused_rewind_is_never_read_again(true).await;
}

/// A failed send of a multi-record run must leave the reader at the run's FIRST record, not the
/// last: none of the run crossed the channel, so a rewind to a later record would skip the rest.
#[tokio::test]
async fn failed_run_send_rewinds_to_the_runs_first_record() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-0\nid-1\nid-2\n").await.unwrap();
    let server = make_admission_file_server(
        path.clone(),
        data_dir,
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        None,
    );
    let file_id = file_source_common::FileFingerprint::DevInode(1, 1);
    let mut watcher = crate::file_watcher::FileWatcher::new(
        path,
        file_source_common::ReadFrom::Beginning,
        None,
        None,
        1_024,
        Bytes::from_static(b"\n"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let transactions = Arc::new(AtomicUsize::new(0));
    let mut dispatcher = AdmissionDispatcher::new(ADMISSION_READ_AHEAD_PER_FILE);
    let mut ready = Vec::new();
    let mut gates = Vec::new();
    for _ in 0..3 {
        let raw = watcher.read_line().await.unwrap().raw_line.unwrap();
        let (checkpoint_gate, gate) = CheckpointGate::closed();
        gates.push(checkpoint_gate);
        dispatcher
            .pending_offsets
            .entry(file_id)
            .or_default()
            .push_back(raw.offset);
        ready.push(ReadyAdmission {
            line: Line {
                text: raw.bytes,
                filename: "records.log".into(),
                file_id,
                start_offset: raw.offset,
                end_offset: watcher.get_file_position(),
                admission_key: Some(AdmissionKey::PodUid(Arc::from("uid"))),
                checkpoint_gate: CheckpointGate::Open,
            },
            permit: Ok(OwnedFileSourceAdmissionPermit::new(Box::new(
                BatchOutcomePermit {
                    transactions: Arc::clone(&transactions),
                    result: Ok(()),
                },
            ))),
            gate,
        });
    }
    assert_eq!(watcher.get_file_position(), 15);
    let (mut tx, rx) = mpsc::channel::<Vec<Line>>(1);
    drop(rx);
    let mut watchers = indexmap::IndexMap::new();
    watchers.insert(file_id, watcher);

    server
        .dispatch_ready_admission_batch(&mut tx, &mut dispatcher, &mut watchers, ready)
        .await
        .unwrap_err();

    assert_eq!(watchers[&file_id].get_file_position(), 0);
    for gate in gates {
        assert_eq!(gate.wait().await, CheckpointGateResult::Rejected);
    }
}

/// Issues permits without waiting for the first `immediate_limit` records and through the wait
/// path afterwards, and rejects the record reserved at `reject_nth` once, so a test can drive one
/// file visit through the fast path, the first waiting record, and a mid-run rejection.
struct VisitAdmission {
    reserved: AtomicUsize,
    immediate_issued: AtomicUsize,
    immediate_limit: usize,
    reject_nth: Option<usize>,
    transactions: Arc<AtomicUsize>,
}

impl VisitAdmission {
    fn new(immediate_limit: usize, reject_nth: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            reserved: AtomicUsize::new(0),
            immediate_issued: AtomicUsize::new(0),
            immediate_limit,
            reject_nth,
            transactions: Arc::new(AtomicUsize::new(0)),
        })
    }
}

impl FileSourceAdmission for VisitAdmission {
    fn unattributed_metadata(&self) -> AdmissionMetadata {
        SelectiveDrainAdmission.unattributed_metadata()
    }

    fn reserve(
        self: Arc<Self>,
        _metadata: AdmissionMetadata,
        _request_id: u64,
        _max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>> {
        let index = self.reserved.fetch_add(1, Ordering::SeqCst);
        let result = if self.reject_nth == Some(index) {
            Err(AdmissionError::Stale)
        } else {
            Ok(())
        };
        async move { Ok(Box::new(VisitPending { admission: self, result }) as Box<dyn PendingAdmission>) }
            .boxed()
    }
}

struct VisitPending {
    admission: Arc<VisitAdmission>,
    result: Result<(), AdmissionError>,
}

impl VisitPending {
    fn permit(&self) -> OwnedFileSourceAdmissionPermit {
        OwnedFileSourceAdmissionPermit::new(Box::new(BatchOutcomePermit {
            transactions: Arc::clone(&self.admission.transactions),
            result: self.result,
        }))
    }
}

impl PendingAdmission for VisitPending {
    fn reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>> {
        async { Ok(()) }.boxed()
    }

    fn try_wait(&mut self) -> Option<Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        let issued = self.admission.immediate_issued.fetch_add(1, Ordering::SeqCst);
        (issued < self.admission.immediate_limit).then(|| Ok(self.permit()))
    }

    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        async move { Ok(self.permit()) }.boxed()
    }
}

/// Per-visit read counts recovered from the interleaved readiness (one per visit) and line hooks.
#[derive(Default)]
struct VisitLog {
    events: Mutex<Vec<(bool, String)>>,
}

impl VisitLog {
    fn install<P: PathsProvider, E: FileSourceInternalEvents>(
        self: &Arc<Self>,
        server: &mut FileServer<P, E>,
    ) {
        let visits = Arc::clone(self);
        server.test_readiness_hook = Some(Arc::new(move |path| {
            visits
                .events
                .lock()
                .unwrap()
                .push((true, path.to_string_lossy().into_owned()));
            true
        }));
        let lines = Arc::clone(self);
        server.test_line_admission_hook = Some(Arc::new(move |line| {
            lines
                .events
                .lock()
                .unwrap()
                .push((false, line.filename.clone()));
            Some(line)
        }));
    }

    /// `(file, lines read)` for every visit that read at least one line, in visit order.
    fn reading_visits(&self) -> Vec<(String, usize)> {
        let mut visits: Vec<(String, usize)> = Vec::new();
        let mut open = false;
        for (is_visit, file) in self.events.lock().unwrap().iter() {
            if *is_visit {
                visits.push((file.clone(), 0));
                open = true;
            } else if open {
                visits.last_mut().unwrap().1 += 1;
            }
        }
        visits.retain(|(_, lines)| *lines > 0);
        visits
    }
}

fn two_record_files(
    tmp: &Path,
    records: usize,
) -> Vec<(LogFileInfo, PathBuf)> {
    ["a", "b"]
        .into_iter()
        .map(|name| {
            let path = tmp.join(format!("{name}.log"));
            let contents: String = (0..records).map(|i| format!("{name}-{i:04}\n")).collect();
            std::fs::write(&path, contents).unwrap();
            let mut info = admission_log_info();
            let uid = format!("uid-{name}");
            info.pod_uid.clone_from(&uid);
            info.admission_metadata.as_mut().unwrap().key = AdmissionKey::PodUid(Arc::from(uid));
            (info, path)
        })
        .collect()
}

/// Drains the server's output, returning each acknowledged record per file in arrival order and
/// the number of rejected deliveries.
async fn collect_acknowledged(
    rx: &mut mpsc::Receiver<Vec<Line>>,
    expected: usize,
) -> (Vec<(String, u64, Bytes)>, usize) {
    let mut acknowledged = Vec::new();
    let mut rejected = 0;
    tokio::time::timeout(Duration::from_secs(10), async {
        while acknowledged.len() < expected {
            let Some(batch) = rx.next().await else { break };
            for line in batch {
                match line.checkpoint_gate.wait().await {
                    CheckpointGateResult::ActorAcknowledged => {
                        acknowledged.push((line.filename, line.start_offset, line.text));
                    }
                    CheckpointGateResult::Rejected => rejected += 1,
                }
            }
        }
    })
    .await
    .expect("every record should be acknowledged");
    (acknowledged, rejected)
}

/// With permits that cost no waiting, one visit reads a run of records, but never more than the
/// visit budget, so each file takes a turn per pass and offsets stay in file order.
#[tokio::test]
async fn immediate_permits_let_a_visit_read_a_bounded_run_and_keep_files_fair() {
    const RECORDS: usize = 4 * ADMISSION_VISIT_LINE_BUDGET;
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let paths = two_record_files(tmp.path(), RECORDS);
    let admission = VisitAdmission::new(usize::MAX, None);
    let mut server = make_multi_admission_file_server(
        paths,
        data_dir.clone(),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::clone(&admission) as Arc<dyn FileSourceAdmission>,
    );
    server.drain_on_shutdown = false;
    let log = Arc::new(VisitLog::default());
    log.install(&mut server);
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(4 * RECORDS);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));
    let (acknowledged, rejected) = collect_acknowledged(&mut rx, 2 * RECORDS).await;
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();

    assert_eq!(rejected, 0);
    for name in ["a", "b"] {
        let texts = acknowledged
            .iter()
            .filter(|(file, _, _)| file.ends_with(&format!("{name}.log")))
            .map(|(_, _, text)| text.clone())
            .collect::<Vec<_>>();
        let expected = (0..RECORDS)
            .map(|i| Bytes::from(format!("{name}-{i:04}")))
            .collect::<Vec<_>>();
        assert_eq!(texts, expected, "file {name} must stay in offset order");
    }
    let visits = log.reading_visits();
    assert!(
        visits.iter().all(|(_, lines)| *lines <= ADMISSION_VISIT_LINE_BUDGET),
        "a visit exceeded its budget: {visits:?}"
    );
    assert!(
        visits[..2].iter().all(|(_, lines)| *lines == ADMISSION_VISIT_LINE_BUDGET),
        "the first visit to each file should use its whole budget: {visits:?}"
    );
    assert_ne!(
        visits[0].0, visits[1].0,
        "the second file is visited before the first file reads again: {visits:?}"
    );
}

/// A record whose permit must wait ends the visit after that record: the run covers the immediate
/// permits plus the first waiting one, so a throttled file does not hold the scan loop.
#[tokio::test]
async fn a_visit_ends_at_the_first_record_that_has_to_wait() {
    const IMMEDIATE: usize = 3;
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let paths = two_record_files(tmp.path(), 2 * ADMISSION_VISIT_LINE_BUDGET);
    let admission = VisitAdmission::new(IMMEDIATE, None);
    let mut server = make_multi_admission_file_server(
        paths[..1].to_vec(),
        data_dir.clone(),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::clone(&admission) as Arc<dyn FileSourceAdmission>,
    );
    server.drain_on_shutdown = false;
    let log = Arc::new(VisitLog::default());
    log.install(&mut server);
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(256);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));
    let (acknowledged, _) = collect_acknowledged(&mut rx, 2 * ADMISSION_VISIT_LINE_BUDGET).await;
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();

    assert_eq!(acknowledged.len(), 2 * ADMISSION_VISIT_LINE_BUDGET);
    let visits = log.reading_visits();
    assert_eq!(
        visits[0].1,
        IMMEDIATE + 1,
        "the first visit reads the immediate records and the first waiting one: {visits:?}"
    );
    assert!(
        visits[1..].iter().all(|(_, lines)| *lines == 1),
        "once permits wait, every visit admits one record: {visits:?}"
    );
}

/// A rejection that lands inside one visit's run replays from the rejected record: every record
/// is acknowledged once and in order, and the checkpoint never skips the rejected one.
#[tokio::test]
async fn rejection_inside_one_visits_run_replays_from_the_rejected_record() {
    const REJECTED: usize = 5;
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    let paths = two_record_files(tmp.path(), 2 * ADMISSION_VISIT_LINE_BUDGET);
    let admission = VisitAdmission::new(usize::MAX, Some(REJECTED));
    let mut server = make_multi_admission_file_server(
        paths[..1].to_vec(),
        data_dir.clone(),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::clone(&admission) as Arc<dyn FileSourceAdmission>,
    );
    server.drain_on_shutdown = false;
    let log = Arc::new(VisitLog::default());
    log.install(&mut server);
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(256);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run(
        tx,
        shutdown_rx,
        futures::future::ready(()),
        Checkpointer::new(&data_dir),
    ));
    let (acknowledged, rejected) =
        collect_acknowledged(&mut rx, 2 * ADMISSION_VISIT_LINE_BUDGET).await;
    shutdown_tx.send(()).unwrap();
    running.await.unwrap().unwrap();

    let expected = (0..2 * ADMISSION_VISIT_LINE_BUDGET)
        .map(|i| Bytes::from(format!("a-{i:04}")))
        .collect::<Vec<_>>();
    assert_eq!(
        acknowledged.iter().map(|(_, _, text)| text.clone()).collect::<Vec<_>>(),
        expected,
        "each record is acknowledged once, in order"
    );
    let mut offset = 0;
    for (_, start, text) in &acknowledged {
        assert_eq!(*start, offset, "acknowledged offsets must be contiguous");
        offset += text.len() as u64 + 1;
    }
    assert!(rejected > 0, "the rejection must fall inside a run");
    assert!(
        log.reading_visits()[0].1 > REJECTED,
        "the rejected record must be inside the first visit's run"
    );
}

/// The event loop holds each sent line behind its closed gate and records the checkpoint only
/// after the gate opens, and the file server's final checkpoint write waits for that loop. A
/// shutdown with an unacknowledged record outstanding must therefore reject the gate instead of
/// leaving it closed until `run` returns, or the two wait on each other.
#[tokio::test]
async fn shutdown_rejects_outstanding_gates_so_the_event_loop_can_finish_first() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("records.log");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).await.unwrap();
    fs::write(&path, b"id-1\nid-2\n").await.unwrap();
    let admission = ControlledAdmission::new();
    admission.wait_grant.add_permits(2);
    let server = make_admission_file_server(
        path,
        data_dir.clone(),
        admission_log_info(),
        Arc::new(Mutex::new(HashMap::new())),
        Some(Arc::clone(&admission) as Arc<dyn FileSourceAdmission>),
    );
    let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (events_done_tx, events_done_rx) = tokio::sync::oneshot::channel::<()>();
    let checkpointer = Checkpointer::new(&data_dir);
    let checkpoints = checkpointer.view();
    let running = tokio::spawn(server.run(tx, shutdown_rx, events_done_rx, checkpointer));
    let event_loop = tokio::spawn(async move {
        let mut results = Vec::new();
        let mut file_id = None;
        while let Some(batch) = rx.next().await {
            for line in batch {
                file_id = Some(line.file_id);
                let result = line.checkpoint_gate.wait().await;
                if result == CheckpointGateResult::ActorAcknowledged {
                    checkpoints.update(line.file_id, line.end_offset);
                }
                results.push(result);
            }
        }
        let _ = events_done_tx.send(());
        (results, file_id)
    });

    // The actor never acknowledges, so the first record's gate stays closed.
    admission.sent_started.acquire().await.unwrap().forget();
    shutdown_tx.send(()).unwrap();

    let (results, file_id) = tokio::time::timeout(Duration::from_secs(10), event_loop)
        .await
        .expect("event loop must not wait on a gate that only the stopped file server can open")
        .unwrap();
    assert!(!results.is_empty());
    assert!(
        results
            .iter()
            .all(|result| *result == CheckpointGateResult::Rejected)
    );
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("file server must finish after the event loop")
        .unwrap()
        .unwrap();

    let mut persisted = Checkpointer::new(&data_dir);
    persisted.read_checkpoints(None).await;
    assert_eq!(
        persisted.view().get(file_id.unwrap()),
        None,
        "a rejected record must not be checkpointed"
    );
}
