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

/// Environment variable holding the fleet HMAC key for `RedactFrom` (HMAC_HASH) derivation. Set by
/// the VA/VAP deployment once the key is provisioned and security-reviewed; until then it is unset
/// and the executor fails closed on any plan carrying a `RedactFrom`.
const HMAC_KEY_ENV_VAR: &str = "LUMBERJACK_PRIME_HMAC_KEY";

/// Identifies which plan to use for a record. With a single plan the key is a default; the fields
/// support selecting among multiple plans by log topic and policy.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct PlanKey {
    /// Log topic, e.g. `ai-products-event-log`.
    pub log_topic: String,
    /// Policy the plan was built for, e.g. `GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO`.
    pub policy: String,
}

/// Why a plan could not be loaded or registered. Surfaced as a VRL compile-time diagnostic.
#[derive(Debug)]
pub enum LoadError {
    /// The plan file could not be read from disk.
    Read(String, io::Error),
    ReadDir(String, io::Error),
    PlanFileName(String),
    PuidLookup(String),
    /// The plan bytes were rejected by the executor (bad/empty `RedactionPlanSet`).
    Register(executor::RedactError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Read(path, e) => {
                write!(f, "could not read redaction plan file {path:?}: {e}")
            }
            LoadError::ReadDir(path, e) => {
                write!(f, "could not list redaction plan directory {path:?}: {e}")
            }
            LoadError::PlanFileName(name) => write!(
                f,
                "redaction plan file {name:?} is not named <POLICY>__<log-topic>.pb"
            ),
            LoadError::PuidLookup(path) => write!(
                f,
                "redaction plan file {path:?} needs a PUID lookup, which is not supported here"
            ),
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
    requires_puid_lookup: bool,
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
        let requires_puid_lookup =
            executor::plan_requires_puid_lookup(&bytes).map_err(LoadError::Register)?;
        // The fleet HMAC key for `RedactFrom` (HMAC_HASH) derivation is read from the environment,
        // where the VA/VAP deployment mounts it (see the LP HMAC-hash design doc — key provisioning
        // + security review are the gating step). Absent/empty ⇒ `None`, and the executor then
        // rejects any plan carrying an HMAC_HASH `RedactFrom` (fail-closed): that transform stays
        // dormant until the key is provisioned. Other transforms, including GENERATE_PUID, do not
        // require this key.
        let hmac_key = std::env::var(HMAC_KEY_ENV_VAR)
            .ok()
            .filter(|k| !k.is_empty());
        let handle =
            executor::register_plan_with_key(&bytes, hmac_key.as_deref().map(str::as_bytes))
                .map_err(LoadError::Register)?;
        let mut handles = HashMap::new();
        handles.insert(PlanKey::default(), Arc::new(PlanHandle(handle)));
        Ok(Self {
            handles,
            requires_puid_lookup,
        })
    }

    pub fn from_plan_dir(dir: &str) -> Result<Self, LoadError> {
        let entries = std::fs::read_dir(dir).map_err(|e| LoadError::ReadDir(dir.to_owned(), e))?;
        let mut handles = HashMap::new();
        for entry in entries {
            let entry = entry.map_err(|e| LoadError::ReadDir(dir.to_owned(), e))?;
            let path = entry.path();
            let path_text = path.to_string_lossy().into_owned();
            let is_file = std::fs::metadata(&path)
                .map_err(|e| LoadError::Read(path_text.clone(), e))?
                .is_file();
            let name = entry.file_name().to_string_lossy().into_owned();
            if !is_file || !name.ends_with(".pb") {
                continue;
            }
            let key = plan_key_from_file_name(&name)
                .ok_or_else(|| LoadError::PlanFileName(name.clone()))?;
            let bytes = std::fs::read(&path).map_err(|e| LoadError::Read(path_text.clone(), e))?;
            if executor::plan_requires_puid_lookup(&bytes).map_err(LoadError::Register)? {
                return Err(LoadError::PuidLookup(path_text));
            }
            let handle = executor::register_plan(&bytes).map_err(LoadError::Register)?;
            handles.insert(key, Arc::new(PlanHandle(handle)));
        }
        Ok(Self {
            handles,
            requires_puid_lookup: false,
        })
    }

    /// Returns the executor handle registered for `key`, if any.
    pub fn handle_for(&self, key: &PlanKey) -> Option<u64> {
        self.handles.get(key).map(|h| h.0)
    }

    /// Returns whether this resolver's plan contains a `GENERATE_PUID` action.
    pub fn requires_puid_lookup(&self) -> bool {
        self.requires_puid_lookup
    }
}

fn plan_key_from_file_name(name: &str) -> Option<PlanKey> {
    let (policy, log_topic) = name.strip_suffix(".pb")?.rsplit_once("__")?;
    let policy_ok = policy.starts_with(|c: char| c.is_ascii_uppercase())
        && policy
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    let topic_ok = log_topic.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && log_topic
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    (policy_ok && topic_ok).then(|| PlanKey {
        log_topic: log_topic.to_owned(),
        policy: policy.to_owned(),
    })
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
        let handle = resolver
            .handle_for(&PlanKey::default())
            .expect("handle registered");

        // A clone shares the handle; the plan must stay registered while either owner lives.
        let clone = resolver.clone();
        drop(resolver);
        assert!(
            executor::redact(handle, &[], executor::ShapeEnforcement::Off).is_ok(),
            "plan should stay registered while a clone still holds the handle"
        );

        // Dropping the final owner releases the plan; the handle is now unknown to the executor.
        drop(clone);
        assert!(
            matches!(
                executor::redact(handle, &[], executor::ShapeEnforcement::Off),
                Err(executor::RedactError::UnknownHandle(_))
            ),
            "plan should be released once the last clone is dropped"
        );
    }

    #[test]
    fn plan_dir_keys_plans_by_file_name_and_releases_them_on_drop() {
        let dir = std::env::temp_dir().join(format!("resolver_plan_dir_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create plan dir");
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan::default()],
        };
        std::fs::write(
            dir.join("GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO__ai-products-event-log.pb"),
            plan.encode_to_vec(),
        )
        .expect("write plan");

        let resolver = PlanResolver::from_plan_dir(dir.to_str().expect("utf-8 plan dir"))
            .expect("register plan dir");
        let key = PlanKey {
            log_topic: "ai-products-event-log".to_owned(),
            policy: "GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO".to_owned(),
        };
        let handle = resolver.handle_for(&key).expect("plan keyed by file name");
        assert_eq!(resolver.handle_for(&PlanKey::default()), None);
        assert!(!resolver.requires_puid_lookup());

        drop(resolver);
        assert!(matches!(
            executor::redact(handle, &[], executor::ShapeEnforcement::Off),
            Err(executor::RedactError::UnknownHandle(_))
        ));
    }

    #[test]
    fn plan_file_names_are_policy_then_log_topic() {
        for (name, expected) in [
            (
                "GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO__ai-products-event-log.pb",
                Some((
                    "ai-products-event-log",
                    "GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO",
                )),
            ),
            ("POLICY_2__span.pb", Some(("span", "POLICY_2"))),
            ("redaction_plan__span__pipeline.pb", None),
            ("POLICY__Span.pb", None),
            ("POLICY__ai_products.pb", None),
            ("policy__span.pb", None),
            ("__span.pb", None),
            ("POLICY__.pb", None),
            ("POLICY.pb", None),
            ("POLICY__span.pb.bak", None),
        ] {
            let expected = expected.map(|(log_topic, policy)| PlanKey {
                log_topic: log_topic.to_owned(),
                policy: policy.to_owned(),
            });
            assert_eq!(plan_key_from_file_name(name), expected, "{name}");
        }
    }
}
