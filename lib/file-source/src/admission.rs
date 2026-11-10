//! Dependency-neutral admission interfaces used by file sources.

use std::{any::Any, fmt, sync::Arc};

use futures::future::{BoxFuture, join_all};
use tokio::sync::oneshot;

/// Immutable key shared by every file belonging to one admission identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum AdmissionKey {
    /// Kubernetes pod UID.
    PodUid(Arc<str>),
    /// Identity that cannot be safely attributed.
    Unattributed,
}

/// Immutable runtime registration attached when a path is discovered.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AdmissionMetadata {
    /// Process-wide accounting key.
    pub key: AdmissionKey,
    /// Process-local watch identifier.
    pub watch_lease_id: u64,
    /// Monotonic sequence within the watch lease.
    pub observation_sequence: u64,
}

/// Admission failure before a record becomes checkpoint eligible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    /// A process or per-key pending bound is full.
    BoundsExceeded,
    /// Metadata or topology generation became stale.
    Stale,
    /// The admission actor closed before commitment.
    Closed,
    /// The adapter observed an internal invariant failure.
    Internal,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "file admission error: {self:?}")
    }
}

impl std::error::Error for AdmissionError {}

/// Process-scoped capacity reservation used before reading a line.
pub trait FileSourceAdmission: Send + Sync + 'static {
    /// Returns how many one-event reservations may share one admission transaction.
    fn reservation_tranche_size(&self) -> usize {
        4
    }

    /// Returns the immutable fallback identity for a path without source metadata.
    fn unattributed_metadata(&self) -> AdmissionMetadata;

    /// Reserves one event and `max_line_bytes` against global and per-key bounds.
    fn reserve(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        request_id: u64,
        max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PendingAdmission>, AdmissionError>>;

    /// Reserves a bounded tranche while preserving one affine reservation per event.
    fn reserve_tranche(
        self: Arc<Self>,
        metadata: AdmissionMetadata,
        request_ids: Vec<u64>,
        max_line_bytes: u64,
    ) -> BoxFuture<'static, Result<Vec<Box<dyn PendingAdmission>>, AdmissionError>> {
        Box::pin(async move {
            let mut reservations = Vec::with_capacity(request_ids.len());
            for request_id in request_ids {
                reservations.push(
                    Arc::clone(&self)
                        .reserve(metadata.clone(), request_id, max_line_bytes)
                        .await?,
                );
            }
            Ok(reservations)
        })
    }

    /// Retains known unread work as schedulable demand between ordered file requests.
    fn retain_backlog(
        self: Arc<Self>,
        _metadata: AdmissionMetadata,
    ) -> BoxFuture<'static, Result<Option<Box<dyn FileSourceBacklogLease>>, AdmissionError>> {
        Box::pin(async { Ok(None) })
    }
}

/// Affine per-file ownership of a schedulable backlog hint.
pub trait FileSourceBacklogLease: Send {}

/// Two-phase reservation that exists before the unknown-size file read.
pub trait PendingAdmission: Send {
    /// Releases the conservative byte difference immediately after the read.
    fn reconcile_payload_bytes(
        &mut self,
        actual_payload_bytes: u64,
    ) -> BoxFuture<'_, Result<(), AdmissionError>>;

    /// Reconciles without suspending when the implementation can, so the common fast path costs no
    /// boxed future. `None` means the caller must use [`Self::reconcile_payload_bytes`].
    fn try_reconcile_payload_bytes(
        &mut self,
        _actual_payload_bytes: u64,
    ) -> Option<Result<(), AdmissionError>> {
        None
    }

    /// Waits for entitlement supply while other keys remain independently polled.
    fn wait(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<OwnedFileSourceAdmissionPermit, AdmissionError>>;

    /// Issues the permit immediately when no waiting is needed, consuming the reservation's
    /// entitlement. `None` leaves the reservation untouched and the caller uses [`Self::wait`].
    fn try_wait(&mut self) -> Option<Result<OwnedFileSourceAdmissionPermit, AdmissionError>> {
        None
    }
}

/// Affine issued permit held refundable until the source-channel send succeeds.
pub trait FileSourceAdmissionPermit: Send {
    /// Reports whether this permit can share one implementation transaction with ready peers.
    fn supports_batch(&self) -> bool {
        false
    }

    /// Commits only after the actor acknowledges the successful send.
    fn sent(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>>;

    /// Offers this permit and the additional ready permits in one implementation transaction.
    /// Each member still linearizes independently, so the returned results may be mixed.
    fn sent_many(
        self: Box<Self>,
        additional: Vec<Box<dyn FileSourceAdmissionPermit>>,
    ) -> BoxFuture<'static, Vec<Result<(), AdmissionError>>> {
        let first = self.sent();
        Box::pin(async move {
            let mut results = Vec::with_capacity(additional.len() + 1);
            results.push(first.await);
            results.extend(join_all(additional.into_iter().map(|permit| permit.sent())).await);
            results
        })
    }

    /// Refunds a record that never crossed the source channel.
    fn cancel(self: Box<Self>) -> BoxFuture<'static, Result<(), AdmissionError>>;

    /// Offers this permit and the additional unsent permits in one implementation transaction.
    /// Each member still linearizes independently, so the returned results may be mixed.
    fn cancel_many(
        self: Box<Self>,
        additional: Vec<Box<dyn FileSourceAdmissionPermit>>,
    ) -> BoxFuture<'static, Vec<Result<(), AdmissionError>>> {
        let first = self.cancel();
        Box::pin(async move {
            let mut results = Vec::with_capacity(additional.len() + 1);
            results.push(first.await);
            results.extend(join_all(additional.into_iter().map(|permit| permit.cancel())).await);
            results
        })
    }

    /// Synchronously transfers an unconsumed permit to its refund path.
    ///
    /// This must not depend on polling an async future: admission wait transports can own a permit
    /// when their enclosing future is cancelled. Implementations may enqueue asynchronous cleanup,
    /// but ownership of the permit and its capacity must be transferred before this method returns.
    fn refund_on_drop(self: Box<Self>);

    /// Converts an owned permit for an implementation-specific batch override.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// Cancellation-safe owner for an issued file-source admission permit.
pub struct OwnedFileSourceAdmissionPermit {
    permit: Option<Box<dyn FileSourceAdmissionPermit>>,
}

impl OwnedFileSourceAdmissionPermit {
    /// Wraps a newly issued permit before it crosses an asynchronous wait boundary.
    pub fn new(permit: Box<dyn FileSourceAdmissionPermit>) -> Self {
        Self {
            permit: Some(permit),
        }
    }

    pub(crate) fn supports_batch(&self) -> bool {
        self.permit
            .as_ref()
            .expect("owned admission permit")
            .supports_batch()
    }

    pub(crate) fn sent(mut self) -> BoxFuture<'static, Result<(), AdmissionError>> {
        self.permit.take().expect("owned admission permit").sent()
    }

    pub(crate) fn sent_many(
        mut self,
        additional: Vec<Self>,
    ) -> BoxFuture<'static, Vec<Result<(), AdmissionError>>> {
        let first = self.permit.take().expect("owned admission permit");
        let additional = additional
            .into_iter()
            .map(|mut permit| permit.permit.take().expect("owned admission permit"))
            .collect();
        first.sent_many(additional)
    }

    pub(crate) fn cancel(mut self) -> BoxFuture<'static, Result<(), AdmissionError>> {
        self.permit.take().expect("owned admission permit").cancel()
    }

    pub(crate) fn cancel_many(
        mut self,
        additional: Vec<Self>,
    ) -> BoxFuture<'static, Vec<Result<(), AdmissionError>>> {
        let first = self.permit.take().expect("owned admission permit");
        let additional = additional
            .into_iter()
            .map(|mut permit| permit.permit.take().expect("owned admission permit"))
            .collect();
        first.cancel_many(additional)
    }
}

impl Drop for OwnedFileSourceAdmissionPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.refund_on_drop();
        }
    }
}

/// Result consumed by the Kubernetes checkpoint map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointGateResult {
    /// The actor committed the permit after source-channel send.
    ActorAcknowledged,
    /// The send or actor acknowledgement failed; replay from the prior checkpoint.
    Rejected,
}

/// One-shot checkpoint eligibility gate carried with a source line.
#[derive(Debug)]
pub enum CheckpointGate {
    /// Admission is disabled and current checkpoint behavior is unchanged.
    Open,
    /// Eligibility awaits the post-send actor acknowledgement.
    Pending(oneshot::Receiver<CheckpointGateResult>),
}

impl CheckpointGate {
    /// Creates a closed gate and its single-use resolver.
    pub fn closed() -> (Self, CheckpointGateResolver) {
        let (send, receive) = oneshot::channel();
        (Self::Pending(receive), CheckpointGateResolver(Some(send)))
    }

    /// Resolves checkpoint eligibility. A dropped resolver is rejection.
    pub async fn wait(self) -> CheckpointGateResult {
        match self {
            Self::Open => CheckpointGateResult::ActorAcknowledged,
            Self::Pending(receive) => receive.await.unwrap_or(CheckpointGateResult::Rejected),
        }
    }
}

/// Single-use resolver retained by the file-server admission dispatcher.
pub struct CheckpointGateResolver(Option<oneshot::Sender<CheckpointGateResult>>);

impl CheckpointGateResolver {
    /// Resolves the gate exactly once.
    pub fn resolve(mut self, result: CheckpointGateResult) {
        if let Some(send) = self.0.take() {
            let _ = send.send(result);
        }
    }
}
