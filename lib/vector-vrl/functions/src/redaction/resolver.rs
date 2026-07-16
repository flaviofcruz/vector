//! Resolves a redaction plan to a registered executor handle.
//!
//! Sits between the `apply_redaction` VRL function and the [`vector_redaction_executor`] engine.
//! [`PlanResolver::from_plan_file`] reads a plan and registers it once, up front;
//! [`PlanResolver::handle_for`] returns the registered handle for a [`PlanKey`], and is the only
//! method called per record.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::Arc;

use vector_redaction_executor as executor;

/// Identifies which plan to use for a record. With a single plan the key is a default; the fields
/// support selecting among multiple plans by log topic and policy group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct PlanKey {
    /// Log topic, e.g. `ai-products-event-log`.
    pub log_topic: String,
    /// Policy-group id the plan was built for, e.g. `0`.
    pub policy_group_id: String,
}

/// Why a plan could not be loaded or registered. Surfaced as a VRL compile-time diagnostic.
#[derive(Debug)]
pub enum LoadError {
    /// The plan file could not be read from disk.
    Read(String, io::Error),
    /// The plan bytes were rejected by the executor (bad/empty `RedactionPlanSet`).
    Register(executor::RedactError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Read(path, e) => write!(f, "could not read redaction plan file {path:?}: {e}"),
            LoadError::Register(e) => write!(f, "could not register redaction plan: {e}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// An executor handle that releases its plan from the global registry on drop.
///
/// `register_plan` inserts a plan the executor never frees on its own; wrapping the handle here and
/// releasing it in `Drop` ties the plan's lifetime to the resolver. Held in an `Arc` (see
/// [`PlanResolver`]) so shared clones release the plan exactly once, when the last is dropped —
/// otherwise every recompile would leak a plan.
#[derive(Debug)]
struct PlanHandle(u64);

impl Drop for PlanHandle {
    fn drop(&mut self) {
        executor::release_plan(self.0);
    }
}

/// Owns the executor handles for registered plans. Cheap to clone: the map holds reference-counted
/// handles and the decoded plans live in the executor's registry, keyed by handle.
#[derive(Clone, Debug)]
pub struct PlanResolver {
    handles: HashMap<PlanKey, Arc<PlanHandle>>,
}

impl PlanResolver {
    /// Reads a plan from `path` and registers it once, keyed by the default [`PlanKey`].
    ///
    /// The default key is used when a single plan covers all records (no per-record selection
    /// needed). To support multiple plans — selected at runtime by log type, policy group, or
    /// other record attributes — populate the map with non-default keys instead. The
    /// [`handle_for`](Self::handle_for) lookup is the same either way; the caller supplies the key.
    pub fn from_plan_file(path: &str) -> Result<Self, LoadError> {
        let bytes = std::fs::read(path).map_err(|e| LoadError::Read(path.to_owned(), e))?;
        let handle = executor::register_plan(&bytes).map_err(LoadError::Register)?;
        let mut handles = HashMap::new();
        handles.insert(PlanKey::default(), Arc::new(PlanHandle(handle)));
        Ok(Self { handles })
    }

    /// Returns the executor handle registered for `key`, if any.
    pub fn handle_for(&self, key: &PlanKey) -> Option<u64> {
        self.handles.get(key).map(|h| h.0)
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use vector_redaction_executor::plan_proto;

    use super::*;

    /// Writes a minimal-but-valid single-plan `RedactionPlanSet` to a temp `.pb`, returning its path.
    fn write_minimal_plan() -> String {
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan::default()],
        };
        let path = std::env::temp_dir().join("resolver_minimal_plan.pb");
        std::fs::write(&path, plan.encode_to_vec()).expect("write temp plan");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    /// Dropping the last `PlanResolver` clone releases the plan: the handle is registered while any
    /// clone lives and rejected by the executor once every clone is dropped.
    #[test]
    fn dropping_last_clone_releases_plan() {
        let resolver = PlanResolver::from_plan_file(&write_minimal_plan()).expect("register plan");
        let handle = resolver.handle_for(&PlanKey::default()).expect("handle registered");

        // A clone shares the handle; the plan must stay registered while either owner lives.
        let clone = resolver.clone();
        drop(resolver);
        assert!(
            executor::redact(handle, &[]).is_ok(),
            "plan should stay registered while a clone still holds the handle"
        );

        // Dropping the final owner releases the plan; the handle is now unknown to the executor.
        drop(clone);
        assert!(
            matches!(
                executor::redact(handle, &[]),
                Err(executor::RedactError::UnknownHandle(_))
            ),
            "plan should be released once the last clone is dropped"
        );
    }
}
