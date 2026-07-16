//! Native (Rust) executor for the streaming proto redactor.
//!
//! Consumes the language-neutral [`RedactionPlanSet`] contract
//! (`.../redaction/proto/redaction_plan.proto`), redacting a serialized log record per the
//! serialized plan.
//!
//! Plans are registered once via [`register_plan`] (which does the decode + validation) and
//! referenced by a `u64` handle from then on, so the per-record [`redact`] path skips re-parsing
//! the plan on every call. Callers must release the handle via [`release_plan`] when done with it.
//!
//! The engine walks the record's proto wire bytes tag-by-tag, looks up each field's [`FieldAction`]
//! in the plan, and copies, drops, recurses into, or rewrites the field accordingly, emitting
//! redacted wire bytes. It handles the structural opcodes only (PassThrough / PassThroughString /
//! Remove / Recurse / MapEntry / MapStripValue / RedactTag); the deferred opcodes (RedactFrom /
//! GenericAny / EmptyString / TodoRemove) never appear in a serialized plan. The executor fails
//! closed on any `FieldAction` whose `oneof` is unset — never emitting the input unredacted.
//!
//! Vendored copy; kept in sync with its upstream source. The only difference is the plan-proto
//! binding below: `build.rs` (prost-build) emits it into `OUT_DIR` and it is `include!`d as the
//! `plan_proto` module.
//!
//! Note: `handle_recurse` recurses on the native call stack via `process_message`, and a `Recurse`
//! action may reference an ancestor plan, so a deeply-nested record could otherwise recurse without
//! bound. [`MAX_RECURSION_DEPTH`] caps this: past the limit `redact` returns
//! [`RedactError::RecursionLimitExceeded`] (fail-closed, per-record) instead of overflowing the
//! native stack (which would abort the whole process).
//!
//! DATABRICKS DIVERGENCE (see `README.databricks.md`): the depth cap is a vector-fork-only change;
//! universe's canonical executor does NOT yet have it. See the `TODO(vendored)` on
//! [`MAX_RECURSION_DEPTH`] — the fix must be ported upstream and re-vendored to converge.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use plan_proto::field_action::Action;
use prost::Message;

/// prost-generated bindings for `proto/redaction_plan.proto`. `build.rs` emits `_bindings.rs`, which
/// wires each proto package into a nested module tree; the plan proto references `compliance.DataLabel`
/// across packages, so the nested layout is required.
///
/// TODO(vendored): this prost-build binding (`OUT_DIR` -> `include!`) is the one intended divergence
/// from universe's executor, which resolves the same proto through a Bazel crate name. Everything
/// else in this file is meant to stay byte-identical to
/// log-sync/structured-logs/common/src/redaction/streaming/executor/src/lib.rs; converge at vendor time.
mod proto_bindings {
    include!(concat!(env!("OUT_DIR"), "/_bindings.rs"));
}

/// The generated `RedactionPlanSet` contract types. Public so callers can construct or inspect plans.
pub use proto_bindings::databricks::logstructuredredactionplan as plan_proto;

/// Protobuf wire types (the low 3 bits of a field tag). Groups (start/end, wire types 3 and 4) are
/// unsupported — the redactor rejects them as malformed.
mod wire_type {
    pub const VARINT: u32 = 0;
    pub const FIXED64: u32 = 1;
    pub const LENGTH_DELIMITED: u32 = 2;
    pub const FIXED32: u32 = 5;
}

/// `logging.Field` field numbers read during REDACT_TAG. `key` (1) and `label` (4) are the two
/// fields kept when a tag's value oneof is stripped; `label` is the varint enum the keep decision
/// gates on.
mod tag_field {
    pub const KEY: u32 = 1;
    pub const LABEL: u32 = 4;
}

/// Maximum submessage nesting the redactor will recurse through before failing closed.
///
/// The engine recurses on the native call stack (`process_message` -> `handle_recurse` ->
/// `process_message`), and a `Recurse` action may point back at an ancestor plan, so without a cap a
/// crafted deeply-nested record could exhaust the stack and abort the process. 100 is chosen to
/// match the protobuf ecosystem's universal default recursion limit (`CodedInputStream` /
/// `google::protobuf` default), while sitting far above any legitimate log record: the deepest
/// `ai-products-event-log` message chain nests ~8 levels, structured-log protos are CI-enforced
/// acyclic ("All log protos are finite"), and the framework's own guidance is that protos "typically
/// nest 3-5 levels". So the cap never rejects a valid record, but bounds the one unbounded-in-
/// principle path (`google.protobuf.Any` payloads, whose nesting follows attacker-influenced input).
///
/// TODO(vendored): DATABRICKS-ONLY divergence from universe's canonical executor
/// (log-sync/structured-logs/common/src/redaction/streaming/executor/src/lib.rs), which has NO depth
/// cap. Port this cap upstream (both the Java `StreamingProtoRedactor` and the Rust executor
/// hand-recurse and are equally exposed), then re-vendor so the copies converge. Until then this file
/// is intentionally NOT byte-identical to universe on this point.
const MAX_RECURSION_DEPTH: usize = 100;

/// Reasons the executor can refuse to redact. Every variant fails closed: the caller must drop the
/// record rather than emit un-redacted bytes.
#[derive(Debug)]
pub enum RedactError {
    /// The plan bytes did not decode as a `RedactionPlanSet`.
    BadPlan(prost::DecodeError),
    /// A well-formed plan that is structurally invalid (e.g. empty plan table).
    InvalidPlan(String),
    /// `redact` was called with a handle that `register_plan` never returned, or that has already
    /// been released.
    UnknownHandle(u64),
    /// The record bytes were not valid protobuf wire format (truncated, bad varint, or an
    /// unsupported group wire type). We fail closed so the caller drops the record rather than
    /// emitting it unredacted.
    MalformedRecord(String),
    /// The record nested deeper than [`MAX_RECURSION_DEPTH`] while recursing into submessages. We
    /// fail closed (drop the record) rather than recurse further, since unbounded native-stack
    /// recursion on a crafted record would abort the whole process instead of erroring per-record.
    RecursionLimitExceeded(usize),
}

impl std::fmt::Display for RedactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedactError::BadPlan(e) => write!(f, "failed to decode RedactionPlanSet: {e}"),
            RedactError::InvalidPlan(msg) => write!(f, "invalid RedactionPlanSet: {msg}"),
            RedactError::UnknownHandle(handle) => write!(f, "unknown plan handle: {handle}"),
            RedactError::MalformedRecord(msg) => write!(f, "malformed record: {msg}"),
            RedactError::RecursionLimitExceeded(limit) => {
                write!(f, "record nesting exceeded recursion limit of {limit}")
            }
        }
    }
}

impl std::error::Error for RedactError {}

// ===== The plan model (recap) =====
//
// A *planset* ([`plan_proto::RedactionPlanSet`]) is a flat table of *plans*. `plans[0]` is the root
// plan for the log-type proto being redacted; nested message types are reached from it via a
// `Recurse` action carrying a `child_plan_index` into the same table (which may point back at an
// ancestor, so cyclic message types are expressed without inlining).
//
// A *plan* ([`plan_proto::MessageRedactionPlan`]) redacts one message type. It is a list of
// *action-entries*, one per proto field: each maps a proto *field number* (the field's tag on the
// wire, i.e. its "column") to a `FieldAction` opcode — PassThrough (copy verbatim), Remove (drop),
// Recurse (redact a nested message with another plan), MapEntry / MapStripValue (json_map handling),
// or RedactTag (keep a tagged value only if its runtime label is centralizable). A field number
// absent from the plan is dropped (allowlist default).
//
// `action_index` below is the prebuilt `field_number -> action-entry index` map (one per plan) so
// the per-record walk is an O(1) lookup instead of a scan. The proto contract in
// `proto/redaction_plan.proto` is the exhaustive definition; this is the reader's-digest at the
// point the runtime model is first used.
type PlanRegistry = HashMap<u64, Arc<RegisteredPlan>>;

/// A decoded plan set plus its prebuilt `field_number -> action-entry index` map (one map per
/// plan). Because the map depends only on the plan — which is fixed for a handle's lifetime — it is
/// built once at [`register_plan`] time and reused by every [`redact`] call on that handle, rather
/// than rebuilt per record.
struct RegisteredPlan {
    plan_set: plan_proto::RedactionPlanSet,
    action_index: Vec<HashMap<u32, usize>>,
}

impl RegisteredPlan {
    /// Decodes the per-plan action index from `plan_set`. Proto field numbers are >= 1; an
    /// absent/invalid field number is ignored.
    fn new(plan_set: plan_proto::RedactionPlanSet) -> Self {
        let action_index = plan_set
            .plans
            .iter()
            .map(|plan| {
                let mut map = HashMap::with_capacity(plan.actions.len());
                for (i, entry) in plan.actions.iter().enumerate() {
                    if let Some(field_number) = entry.field_number {
                        if field_number > 0 {
                            map.insert(field_number as u32, i);
                        }
                    }
                }
                map
            })
            .collect();
        RegisteredPlan {
            plan_set,
            action_index,
        }
    }
}

/// Registered plans, keyed by the handle returned from [`register_plan`]. Values are `Arc`-wrapped
/// so [`redact`] can clone a reference out under a short lock and keep using it even if
/// [`release_plan`] races it from another thread.
///
/// The registry is multi-tenant: it holds one entry per [`register_plan`] call, so many plansets
/// can be registered concurrently (each under its own handle). The M2 `apply_redaction` function
/// happens to register a single planset today, but that is a caller-side choice — the registry
/// itself is not limited to one, which is what lets a caller register per-`(log_type, group)` plans
/// later.
static REGISTRY: LazyLock<Mutex<PlanRegistry>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Source of fresh handles for [`register_plan`]. Starts at 1 so 0 is never a valid handle.
///
/// A handle is an opaque, monotonically increasing `u64` — the key under which one decoded planset
/// lives in [`REGISTRY`], nothing more. It is NOT an index into `plans`: `handle 1` means "the first
/// planset registered in this process," not "`plans[0]`". The `plans[0] == root log-type plan`
/// convention (checked in [`register_plan`]) is a separate, within-planset contract unrelated to the
/// handle value.
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

/// Decodes and validates `plan_bytes` once, caching it under a fresh handle so repeated [`redact`]
/// calls for the same plan skip re-parsing it. Callers must eventually pass the handle to
/// [`release_plan`].
pub fn register_plan(plan_bytes: &[u8]) -> Result<u64, RedactError> {
    // Decode here so a malformed contract fails at the boundary rather than silently mis-redacting
    // (also exercises the prost binding).
    let plan_set =
        plan_proto::RedactionPlanSet::decode(plan_bytes).map_err(RedactError::BadPlan)?;

    // plans[0] is the root log-type plan by contract; a plan table without it cannot be executed.
    if plan_set.plans.is_empty() {
        return Err(RedactError::InvalidPlan("plans table is empty".to_string()));
    }

    // Build the per-plan action index once here so repeated `redact` calls on this handle reuse it
    // rather than rebuilding it per record.
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    registry_guard().insert(handle, Arc::new(RegisteredPlan::new(plan_set)));
    Ok(handle)
}

/// Redact `record_bytes` per the plan registered under `handle` ([`register_plan`]). On error
/// returns [`RedactError`], which the caller must treat as "drop the record", never "emit the
/// input".
pub fn redact(handle: u64, record_bytes: &[u8]) -> Result<Vec<u8>, RedactError> {
    // Clone the Arc out under a short lock so a concurrent `release_plan` can't drop the plan
    // mid-walk: the plan outlives the registry entry.
    let registered = registry_guard()
        .get(&handle)
        .map(Arc::clone)
        .ok_or(RedactError::UnknownHandle(handle))?;

    // plans[0] is the root log-type plan by contract (checked at register time). The action index
    // was prebuilt at register time; `PlanTable` just borrows it.
    let table = PlanTable::new(&registered);
    let mut reader = WireReader::new(record_bytes);
    let mut out = Vec::with_capacity(record_bytes.len());
    process_message(&mut reader, &mut out, &table, 0, 0)?;
    Ok(out)
}

/// Drops the plan registered under `handle`. Idempotent: releasing an already-released or unknown
/// handle is a no-op, since the caller side (`NativeRustProtoTransformer.close()`) has no way to
/// recover from a double-release.
pub fn release_plan(handle: u64) {
    registry_guard().remove(&handle);
}

/// Locks [`REGISTRY`], recovering the guard on poison rather than panicking — none of the
/// operations performed while holding it can panic, so poisoning can't reflect real corruption.
fn registry_guard() -> MutexGuard<'static, PlanRegistry> {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner())
}

// ===== Wire reader =====

/// A cursor over serialized proto bytes. Tracks an absolute offset so kept fields can be sliced
/// verbatim from the input rather than decoded and re-encoded.
struct WireReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        WireReader { buf, pos: 0 }
    }

    fn position(&self) -> usize {
        self.pos
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Reads the next field key. Returns `Ok(None)` only at end of buffer (message terminates);
    /// otherwise decodes a tag and rejects a field number of 0 as malformed.
    ///
    /// Rejecting field 0 is stricter than a bare `tag == 0` break — e.g. wire bytes `0x03` decode to
    /// field 0 / wire type 3, which is invalid protobuf rather than a clean end-of-message. Failing
    /// closed here keeps a malformed record from emitting as a partial one.
    fn read_tag(&mut self) -> Result<Option<(u32, u32)>, RedactError> {
        if self.remaining() == 0 {
            return Ok(None);
        }
        let tag = self.read_varint32()?;
        let field_number = tag >> 3;
        if field_number == 0 {
            return Err(RedactError::MalformedRecord(format!(
                "invalid tag with field number 0 (raw tag {tag})"
            )));
        }
        Ok(Some((field_number, tag & 0x07)))
    }

    /// Reads a base-128 varint (up to 10 bytes).
    fn read_varint64(&mut self) -> Result<u64, RedactError> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            if self.remaining() == 0 {
                return Err(RedactError::MalformedRecord("truncated varint".to_string()));
            }
            let byte = self.buf[self.pos];
            self.pos += 1;
            result |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err(RedactError::MalformedRecord("varint exceeds 64 bits".to_string()));
            }
        }
    }

    /// Reads a varint and truncates to 32 bits. Used for tags and length prefixes, which the proto
    /// wire format limits to at most 29-bit field numbers and 32-bit lengths — values exceeding
    /// `u32::MAX` would indicate a malformed record.
    fn read_varint32(&mut self) -> Result<u32, RedactError> {
        let v = self.read_varint64()?;
        debug_assert!(
            v <= u32::MAX as u64,
            "varint exceeded u32 range — malformed tag or length prefix"
        );
        Ok(v as u32)
    }

    /// Advances past `n` bytes, bounds-checked. `remaining()` avoids overflow (pos <= len always).
    fn skip_bytes(&mut self, n: usize) -> Result<(), RedactError> {
        if n > self.remaining() {
            return Err(RedactError::MalformedRecord("truncated field".to_string()));
        }
        self.pos += n;
        Ok(())
    }

    /// Appends the next `n` bytes to `out` and advances, bounds-checked.
    fn copy_bytes(&mut self, out: &mut Vec<u8>, n: usize) -> Result<(), RedactError> {
        if n > self.remaining() {
            return Err(RedactError::MalformedRecord("truncated field".to_string()));
        }
        out.extend_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(())
    }

    /// Borrows the next `n` bytes and advances, bounds-checked. Used to window a length-delimited
    /// submessage for recursion / sub-field scanning.
    fn take_slice(&mut self, n: usize) -> Result<&'a [u8], RedactError> {
        if n > self.remaining() {
            return Err(RedactError::MalformedRecord(
                "truncated length-delimited field".to_string(),
            ));
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
}

// ===== Wire writer helpers =====
//
// Free functions over `Vec<u8>`. A fresh `Vec` is allocated per nested message.

/// Writes a base-128 varint. A `u32` value zero-extends to `u64`, so the same function encodes both
/// unsigned widths identically.
fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value & !0x7F != 0 {
        out.push(((value & 0x7F) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Writes a field key (`(field_number << 3) | wire_type`).
fn write_tag(out: &mut Vec<u8>, field_number: u32, wire_type: u32) {
    write_varint(out, u64::from((field_number << 3) | wire_type));
}

/// Byte length of the varint encoding of `value`.
fn varint_size(value: u32) -> usize {
    if value & (!0u32 << 7) == 0 {
        1
    } else if value & (!0u32 << 14) == 0 {
        2
    } else if value & (!0u32 << 21) == 0 {
        3
    } else if value & (!0u32 << 28) == 0 {
        4
    } else {
        5
    }
}

/// Byte length of the tag for `field_number`. The wire-type nibble never changes the varint length
/// (it occupies the low 3 bits that `field_number << 3` leaves zero), so this matches the emitted
/// tag size regardless of wire type.
fn tag_size(field_number: u32) -> usize {
    varint_size(field_number << 3)
}

// ===== Plan table =====

/// A borrowed view over a [`RegisteredPlan`], giving the walk an O(1) `field_number -> action`
/// lookup. Cheap to construct (holds two references), so building it per [`redact`] call is fine;
/// the underlying action index is built once at register time. Threaded (with a plan index) through
/// recursion.
struct PlanTable<'a> {
    plans: &'a [plan_proto::MessageRedactionPlan],
    action_index: &'a [HashMap<u32, usize>],
}

impl<'a> PlanTable<'a> {
    fn new(registered: &'a RegisteredPlan) -> Self {
        PlanTable {
            plans: &registered.plan_set.plans,
            action_index: &registered.action_index,
        }
    }

    /// Looks up the action for `field_number` in the plan at `plan_index`.
    ///
    /// - `Ok(None)`: no entry — an unknown field, dropped by the allowlist default.
    /// - `Ok(Some(action))`: the field's structural opcode.
    /// - `Err(InvalidPlan)`: an entry exists but its action `oneof` is unset — a deferred/unknown
    ///   opcode. Fail closed rather than emit the field unredacted.
    fn action(&self, plan_index: usize, field_number: u32) -> Result<Option<&Action>, RedactError> {
        let Some(entry_index) = self
            .action_index
            .get(plan_index)
            .and_then(|map| map.get(&field_number))
        else {
            return Ok(None);
        };
        match self.plans[plan_index].actions[*entry_index]
            .action
            .as_ref()
            .and_then(|field_action| field_action.action.as_ref())
        {
            Some(action) => Ok(Some(action)),
            None => Err(RedactError::InvalidPlan(format!(
                "field {field_number} in plan {plan_index} has no action set \
                 (deferred or unknown opcode)"
            ))),
        }
    }
}

// ===== Redaction engine =====

/// Walks one message from `reader` into `out` per the plan at `plan_index`. Reads each field's tag,
/// looks up its action, and copies / drops / recurses / rewrites accordingly. Unknown fields are
/// dropped (allowlist default). Does not read or write an outer length prefix — the caller owns
/// that (see [`handle_recurse`]).
///
/// `depth` is the current submessage-nesting level (0 at the top-level record); it increments on each
/// recursion into a nested message and is capped at [`MAX_RECURSION_DEPTH`] to fail closed rather
/// than overflow the native stack (see [`handle_recurse`]).
fn process_message(
    reader: &mut WireReader,
    out: &mut Vec<u8>,
    table: &PlanTable,
    plan_index: usize,
    depth: usize,
) -> Result<(), RedactError> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(RedactError::RecursionLimitExceeded(MAX_RECURSION_DEPTH));
    }
    while let Some((field_number, wire_type)) = reader.read_tag()? {
        match table.action(plan_index, field_number)? {
            None => drop_field(reader, wire_type)?,
            Some(action) => dispatch(reader, out, table, field_number, wire_type, action, depth)?,
        }
    }
    Ok(())
}

/// Applies one field's `action`. The `oneof` is exhaustive over the structural opcodes; there is no
/// wildcard, so adding a deferred opcode to the proto forces a decision here rather than silently
/// passing through.
fn dispatch(
    reader: &mut WireReader,
    out: &mut Vec<u8>,
    table: &PlanTable,
    field_number: u32,
    wire_type: u32,
    action: &Action,
    depth: usize,
) -> Result<(), RedactError> {
    match action {
        // PASS_THROUGH_STRING behaves exactly like PASS_THROUGH while shape enforcement is deferred
        // (the serializer emits it with no shapes).
        Action::PassThrough(_) | Action::PassThroughString(_) => {
            write_tag(out, field_number, wire_type);
            copy_field_value(reader, out, wire_type)
        }
        Action::Remove(_) => drop_field(reader, wire_type),
        Action::Recurse(recurse) => {
            handle_recurse(reader, out, table, field_number, wire_type, recurse, depth)
        }
        Action::MapEntry(map_entry) => {
            handle_map_entry(reader, out, field_number, wire_type, map_entry)
        }
        Action::MapStripValue(_) => handle_map_strip_value(reader, out, field_number, wire_type),
        Action::RedactTag(redact_tag) => {
            handle_redact_tag(reader, out, field_number, wire_type, redact_tag)
        }
    }
}

/// Copies a field value verbatim (tag already written by the caller). Varints are decoded and
/// re-encoded in canonical (minimal) form — so a non-canonically-encoded input varint is
/// normalized, NOT byte-copied; the executor is not a pure passthrough for pathologically-encoded
/// varints. Fixed-width and length-delimited payloads are copied byte-for-byte.
fn copy_field_value(
    reader: &mut WireReader,
    out: &mut Vec<u8>,
    wire_type: u32,
) -> Result<(), RedactError> {
    match wire_type {
        wire_type::VARINT => {
            let value = reader.read_varint64()?;
            write_varint(out, value);
            Ok(())
        }
        wire_type::FIXED64 => reader.copy_bytes(out, 8),
        wire_type::LENGTH_DELIMITED => {
            let length = reader.read_varint32()?;
            write_varint(out, u64::from(length));
            reader.copy_bytes(out, length as usize)
        }
        wire_type::FIXED32 => reader.copy_bytes(out, 4),
        other => Err(RedactError::MalformedRecord(format!("unsupported wire type {other}"))),
    }
}

/// Reads and discards a field value, emitting nothing to the output. Used to drop fields that
/// should be redacted. Groups (wire types 3/4) and unknown types are rejected as malformed.
fn drop_field(reader: &mut WireReader, wire_type: u32) -> Result<(), RedactError> {
    match wire_type {
        wire_type::VARINT => {
            reader.read_varint64()?;
            Ok(())
        }
        wire_type::FIXED64 => reader.skip_bytes(8),
        wire_type::LENGTH_DELIMITED => {
            let length = reader.read_varint32()? as usize;
            reader.skip_bytes(length)
        }
        wire_type::FIXED32 => reader.skip_bytes(4),
        other => Err(RedactError::MalformedRecord(format!("cannot drop wire type {other}"))),
    }
}

/// RECURSE: redact a nested length-delimited message with the referenced child plan, then emit
/// tag + new length + redacted bytes.
fn handle_recurse(
    reader: &mut WireReader,
    out: &mut Vec<u8>,
    table: &PlanTable,
    field_number: u32,
    wire_type: u32,
    recurse: &plan_proto::Recurse,
    depth: usize,
) -> Result<(), RedactError> {
    if wire_type != wire_type::LENGTH_DELIMITED {
        // TODO(vendored): DATABRICKS-ONLY divergence — universe passes through verbatim, but
        // RECURSE fields contain content that must be redacted; if we can't recurse, drop instead.
        // Port upstream and re-vendor.
        return drop_field(reader, wire_type);
    }

    let child_index = recurse
        .child_plan_index
        .ok_or_else(|| RedactError::InvalidPlan("Recurse missing child_plan_index".to_string()))?;
    if child_index < 0 || child_index as usize >= table.plans.len() {
        return Err(RedactError::InvalidPlan(format!(
            "Recurse child_plan_index {child_index} out of range"
        )));
    }

    let length = reader.read_varint32()? as usize;
    let content = reader.take_slice(length)?;

    // Recurse into a fresh child buffer since the redacted length isn't known up front. `depth + 1`
    // is checked at the top of `process_message`, bounding native-stack recursion.
    let mut child_reader = WireReader::new(content);
    let mut child_out = Vec::new();
    process_message(&mut child_reader, &mut child_out, table, child_index as usize, depth + 1)?;

    // Emit even when the child redacts to empty (tag + 0).
    write_tag(out, field_number, wire_type::LENGTH_DELIMITED);
    write_varint(out, child_out.len() as u64);
    out.extend_from_slice(&child_out);
    Ok(())
}

/// MAP_ENTRY: keep or drop a whole `json_map` entry by its key's centralizability.
fn handle_map_entry(
    reader: &mut WireReader,
    out: &mut Vec<u8>,
    field_number: u32,
    wire_type: u32,
    map_entry: &plan_proto::MapEntry,
) -> Result<(), RedactError> {
    if wire_type != wire_type::LENGTH_DELIMITED {
        // Map entries are always length-delimited messages; anything else is malformed → drop.
        return drop_field(reader, wire_type);
    }

    let length = reader.read_varint32()? as usize;
    let entry = reader.take_slice(length)?;

    let key_field = map_entry.key_field_number.unwrap_or(0);
    let key = read_string_field(entry, key_field as u32)?;
    if should_keep_key(map_entry, key.as_deref()) {
        write_tag(out, field_number, wire_type::LENGTH_DELIMITED);
        write_varint(out, length as u64);
        out.extend_from_slice(entry);
    }
    Ok(())
}

/// Whether a MAP_ENTRY with the given (raw, not-yet-lowercased) key is kept: a per-key override
/// wins, else the default; an absent key takes the default.
///
/// Operates on proto3 map fields, where each entry is a `{key: string, value: ...}` submessage.
/// The keep decision is per string key. Contrast with [`should_keep_label`], which operates on
/// structured tag fields and gates on a numeric enum label rather than a string key.
fn should_keep_key(map_entry: &plan_proto::MapEntry, key: Option<&str>) -> bool {
    let default = map_entry.default_is_centralizable.unwrap_or(false);
    let Some(key) = key else {
        return default;
    };
    // Decision keys are pre-lowercased by the serializer; lower-case the runtime key to match.
    let lowered = key.to_lowercase();
    map_entry
        .key_decisions
        .iter()
        .find(|decision| decision.key.as_deref() == Some(lowered.as_str()))
        .and_then(|decision| decision.is_centralizable)
        .unwrap_or(default)
}

/// MAP_STRIP_VALUE: keep a proto3 map entry's key (field 1), drop its value (field 2).
fn handle_map_strip_value(
    reader: &mut WireReader,
    out: &mut Vec<u8>,
    field_number: u32,
    wire_type: u32,
) -> Result<(), RedactError> {
    if wire_type != wire_type::LENGTH_DELIMITED {
        return drop_field(reader, wire_type);
    }
    let length = reader.read_varint32()? as usize;
    let entry = reader.take_slice(length)?;
    reemit_selected_fields(out, field_number, entry, |field| field == 1)
}

/// REDACT_TAG: for a `logging.Field` tag, keep the whole value if its label (field 4) is
/// centralizable, else emit only key (1) and label (4).
fn handle_redact_tag(
    reader: &mut WireReader,
    out: &mut Vec<u8>,
    field_number: u32,
    wire_type: u32,
    redact_tag: &plan_proto::RedactTag,
) -> Result<(), RedactError> {
    if wire_type != wire_type::LENGTH_DELIMITED {
        return drop_field(reader, wire_type);
    }
    let length = reader.read_varint32()? as usize;
    let entry = reader.take_slice(length)?;

    let label = read_tag_label(entry)?;
    if should_keep_label(redact_tag, label) {
        write_tag(out, field_number, wire_type::LENGTH_DELIMITED);
        write_varint(out, length as u64);
        out.extend_from_slice(entry);
        Ok(())
    } else {
        reemit_selected_fields(out, field_number, entry, |field| {
            field == tag_field::KEY || field == tag_field::LABEL
        })
    }
}

/// Whether a tag's label is kept: non-negative and in the keepable set. `keepable_labels` is the
/// enum numbers (prost lowers `repeated DataLabel` to `Vec<i32>`), so the comparison is numeric.
///
/// Operates on structured tag fields (e.g. `logging.Field`), where the keep decision is based on
/// a numeric `DataLabel` enum value embedded in the tag. Contrast with [`should_keep_key`], which
/// operates on proto3 map fields and gates on a string key rather than a numeric label.
fn should_keep_label(redact_tag: &plan_proto::RedactTag, label: i32) -> bool {
    label >= 0 && redact_tag.keepable_labels.contains(&label)
}

/// Re-emits a length-delimited submessage keeping only fields matching `keep`. Two passes over the
/// same bytes — measure the kept length for the prefix, then emit. Drops the entry (emits nothing)
/// when no field is kept. `bytes` is the already-windowed submessage, so field spans index it
/// directly (no window offset to add).
fn reemit_selected_fields(
    out: &mut Vec<u8>,
    field_number: u32,
    bytes: &[u8],
    keep: impl Fn(u32) -> bool,
) -> Result<(), RedactError> {
    let mut kept_len = 0usize;
    let mut scan = WireReader::new(bytes);
    while let Some((field, wire_type)) = scan.read_tag()? {
        let value_start = scan.position();
        drop_field(&mut scan, wire_type)?;
        let value_end = scan.position();
        if keep(field) {
            kept_len += tag_size(field) + (value_end - value_start);
        }
    }

    if kept_len == 0 {
        return Ok(());
    }

    write_tag(out, field_number, wire_type::LENGTH_DELIMITED);
    write_varint(out, kept_len as u64);

    let mut emit = WireReader::new(bytes);
    while let Some((field, wire_type)) = emit.read_tag()? {
        let value_start = emit.position();
        drop_field(&mut emit, wire_type)?;
        let value_end = emit.position();
        if keep(field) {
            write_tag(out, field, wire_type);
            out.extend_from_slice(&bytes[value_start..value_end]);
        }
    }
    Ok(())
}

/// Scans a submessage for the first length-delimited field matching `target_field` and returns its
/// UTF-8 string value (lossy decode).
fn read_string_field(bytes: &[u8], target_field: u32) -> Result<Option<String>, RedactError> {
    let mut reader = WireReader::new(bytes);
    while let Some((field, wire_type)) = reader.read_tag()? {
        if field == target_field && wire_type == wire_type::LENGTH_DELIMITED {
            let length = reader.read_varint32()? as usize;
            let value = reader.take_slice(length)?;
            return Ok(Some(String::from_utf8_lossy(value).into_owned()));
        }
        drop_field(&mut reader, wire_type)?;
    }
    Ok(None)
}

/// Scans a `logging.Field` tag for its `label` varint (field 4) and returns the enum number, or 0
/// (the enum default) if absent.
fn read_tag_label(bytes: &[u8]) -> Result<i32, RedactError> {
    let mut reader = WireReader::new(bytes);
    while let Some((field, wire_type)) = reader.read_tag()? {
        if field == tag_field::LABEL && wire_type == wire_type::VARINT {
            return Ok(reader.read_varint64()? as i32);
        }
        drop_field(&mut reader, wire_type)?;
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== Wire-byte fixture helpers =====

    /// Appends a `field_number << 3 | wire_type` tag then a varint value.
    fn push_varint_field(out: &mut Vec<u8>, field_number: u32, value: u64) {
        write_tag(out, field_number, wire_type::VARINT);
        write_varint(out, value);
    }

    /// Appends a length-delimited field (tag + length + bytes).
    fn push_len_field(out: &mut Vec<u8>, field_number: u32, value: &[u8]) {
        write_tag(out, field_number, wire_type::LENGTH_DELIMITED);
        write_varint(out, value.len() as u64);
        out.extend_from_slice(value);
    }

    fn field_entry(field_number: i32, action: Action) -> plan_proto::FieldActionEntry {
        plan_proto::FieldActionEntry {
            field_number: Some(field_number),
            action: Some(plan_proto::FieldAction {
                action: Some(action),
            }),
        }
    }

    /// Runs the engine directly against a plan set (bypassing the handle registry), returning the
    /// raw result so tests can assert either success bytes or a fail-closed error.
    fn try_run(
        plan_set: &plan_proto::RedactionPlanSet,
        record: &[u8],
    ) -> Result<Vec<u8>, RedactError> {
        let registered = RegisteredPlan::new(plan_set.clone());
        let table = PlanTable::new(&registered);
        let mut reader = WireReader::new(record);
        let mut out = Vec::new();
        process_message(&mut reader, &mut out, &table, 0, 0)?;
        Ok(out)
    }

    /// Runs the engine and unwraps, for the common success-path assertions.
    fn run(plan_set: &plan_proto::RedactionPlanSet, record: &[u8]) -> Vec<u8> {
        try_run(plan_set, record).expect("redaction failed")
    }

    /// A single-plan set keeping `field_number` as PASS_THROUGH — enough to reach the wire walk so
    /// the malformed-input tests exercise the reader, not plan lookup.
    fn pass_through_plan(field_number: i32) -> plan_proto::RedactionPlanSet {
        plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(
                    field_number,
                    Action::PassThrough(plan_proto::PassThrough {}),
                )],
                ..Default::default()
            }],
        }
    }

    // ===== Registry / handle lifecycle =====

    #[test]
    fn register_plan_rejects_garbage_bytes() {
        // 0xFF is not a valid protobuf tag/length prefix, so decode must fail closed.
        let err = register_plan(&[0xFF, 0xFF, 0xFF]).unwrap_err();
        assert!(matches!(err, RedactError::BadPlan(_)));
    }

    #[test]
    fn register_plan_rejects_empty_plan_table() {
        let empty = plan_proto::RedactionPlanSet::default().encode_to_vec();
        let err = register_plan(&empty).unwrap_err();
        assert!(matches!(err, RedactError::InvalidPlan(_)));
    }

    #[test]
    fn redact_rejects_unknown_handle() {
        // 0 is never returned by register_plan, so it is unknown regardless of test execution order.
        let err = redact(0, b"record").unwrap_err();
        assert!(matches!(err, RedactError::UnknownHandle(0)));
    }

    #[test]
    fn redact_fails_after_release() {
        // The handle lookup fails before the record is ever parsed, so any bytes are fine here.
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan::default()],
        };
        let handle = register_plan(&plan.encode_to_vec()).unwrap();
        release_plan(handle);
        let err = redact(handle, b"anything").unwrap_err();
        assert!(matches!(err, RedactError::UnknownHandle(_)));
    }

    #[test]
    fn release_plan_is_idempotent() {
        release_plan(u64::MAX); // never registered — must not panic
    }

    // ===== Per-opcode engine tests (hand-built plans + wire bytes) =====

    #[test]
    fn pass_through_keeps_field_verbatim() {
        // Plan: field 1 PASS_THROUGH (varint), field 2 PASS_THROUGH (length-delimited).
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![
                    field_entry(1, Action::PassThrough(plan_proto::PassThrough {})),
                    field_entry(2, Action::PassThroughString(plan_proto::PassThroughString {})),
                ],
                ..Default::default()
            }],
        };
        let mut record = Vec::new();
        push_varint_field(&mut record, 1, 7);
        push_len_field(&mut record, 2, b"keep me");
        // Both fields kept → output is byte-identical to the (canonical) input.
        assert_eq!(run(&plan_set, &record), record);
    }

    #[test]
    fn remove_drops_field_but_keeps_others() {
        // Plan: field 1 PASS_THROUGH, field 2 REMOVE.
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![
                    field_entry(1, Action::PassThrough(plan_proto::PassThrough {})),
                    field_entry(2, Action::Remove(plan_proto::Remove {})),
                ],
                ..Default::default()
            }],
        };
        let mut record = Vec::new();
        push_varint_field(&mut record, 1, 7);
        push_len_field(&mut record, 2, b"secret");
        let mut expected = Vec::new();
        push_varint_field(&mut expected, 1, 7);
        assert_eq!(run(&plan_set, &record), expected);
    }

    #[test]
    fn unknown_field_is_dropped() {
        // Plan only mentions field 1; field 2 has no entry → allowlist default drops it.
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(1, Action::PassThrough(plan_proto::PassThrough {}))],
                ..Default::default()
            }],
        };
        let mut record = Vec::new();
        push_varint_field(&mut record, 1, 7);
        push_len_field(&mut record, 2, b"unmapped");
        let mut expected = Vec::new();
        push_varint_field(&mut expected, 1, 7);
        assert_eq!(run(&plan_set, &record), expected);
    }

    #[test]
    fn recurse_redacts_nested_message_by_child_plan() {
        // plans[0]: field 1 RECURSE -> plans[1]. plans[1]: field 1 PASS_THROUGH, field 2 REMOVE.
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![
                plan_proto::MessageRedactionPlan {
                    actions: vec![field_entry(
                        1,
                        Action::Recurse(plan_proto::Recurse {
                            child_plan_index: Some(1),
                        }),
                    )],
                    ..Default::default()
                },
                plan_proto::MessageRedactionPlan {
                    actions: vec![
                        field_entry(1, Action::PassThrough(plan_proto::PassThrough {})),
                        field_entry(2, Action::Remove(plan_proto::Remove {})),
                    ],
                    ..Default::default()
                },
            ],
        };
        let mut nested = Vec::new();
        push_varint_field(&mut nested, 1, 42);
        push_len_field(&mut nested, 2, b"drop");
        let mut record = Vec::new();
        push_len_field(&mut record, 1, &nested);

        let mut expected_nested = Vec::new();
        push_varint_field(&mut expected_nested, 1, 42);
        let mut expected = Vec::new();
        push_len_field(&mut expected, 1, &expected_nested);
        assert_eq!(run(&plan_set, &record), expected);
    }

    #[test]
    fn recurse_out_of_range_child_fails_closed() {
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(
                    1,
                    Action::Recurse(plan_proto::Recurse {
                        child_plan_index: Some(9),
                    }),
                )],
                ..Default::default()
            }],
        };
        let mut nested = Vec::new();
        push_varint_field(&mut nested, 1, 1);
        let mut record = Vec::new();
        push_len_field(&mut record, 1, &nested);

        let registered = RegisteredPlan::new(plan_set);
        let table = PlanTable::new(&registered);
        let mut reader = WireReader::new(&record);
        let mut out = Vec::new();
        let err = process_message(&mut reader, &mut out, &table, 0, 0).unwrap_err();
        assert!(matches!(err, RedactError::InvalidPlan(_)));
    }

    /// Builds `levels` nested length-delimited messages, each wrapping the next in field 1, with a
    /// terminal varint at the innermost level. Depth `levels` = `levels` recursions of field 1.
    fn nested_record(levels: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        push_varint_field(&mut buf, 1, 1);
        for _ in 0..levels {
            let mut outer = Vec::new();
            push_len_field(&mut outer, 1, &buf);
            buf = outer;
        }
        buf
    }

    // A self-referential plan (field 1 recurses back into plan 0) applied to a record nested past
    // MAX_RECURSION_DEPTH fails closed with RecursionLimitExceeded, rather than overflowing the
    // native stack (which would abort the process instead of dropping the one record).
    #[test]
    fn recursion_deeper_than_limit_fails_closed() {
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(
                    1,
                    Action::Recurse(plan_proto::Recurse {
                        child_plan_index: Some(0),
                    }),
                )],
                ..Default::default()
            }],
        };

        let err = try_run(&plan_set, &nested_record(MAX_RECURSION_DEPTH + 5))
            .expect_err("record nested past the limit should fail closed");
        assert!(
            matches!(err, RedactError::RecursionLimitExceeded(limit) if limit == MAX_RECURSION_DEPTH),
            "unexpected error: {err:?}"
        );

        // A record within the limit still redacts successfully through the same cyclic plan.
        try_run(&plan_set, &nested_record(3)).expect("shallow record should redact fine");
    }

    #[test]
    fn map_entry_keeps_centralizable_key_drops_others() {
        // json_map field 5; entry key field 1. Default drop; "keep_this" kept (case-insensitively).
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(
                    5,
                    Action::MapEntry(plan_proto::MapEntry {
                        key_field_number: Some(1),
                        default_is_centralizable: Some(false),
                        key_decisions: vec![plan_proto::MapKeyDecision {
                            key: Some("keep_this".to_string()),
                            is_centralizable: Some(true),
                        }],
                    }),
                )],
                ..Default::default()
            }],
        };
        // Build two entries: {key:"KEEP_THIS", value:"v1"} and {key:"other", value:"v2"}.
        let mut keep_entry = Vec::new();
        push_len_field(&mut keep_entry, 1, b"KEEP_THIS");
        push_len_field(&mut keep_entry, 2, b"v1");
        let mut drop_entry = Vec::new();
        push_len_field(&mut drop_entry, 1, b"other");
        push_len_field(&mut drop_entry, 2, b"v2");
        let mut record = Vec::new();
        push_len_field(&mut record, 5, &keep_entry);
        push_len_field(&mut record, 5, &drop_entry);

        // Only the (case-insensitively matched) keep entry survives, verbatim.
        let mut expected = Vec::new();
        push_len_field(&mut expected, 5, &keep_entry);
        assert_eq!(run(&plan_set, &record), expected);
    }

    #[test]
    fn map_strip_value_keeps_key_drops_value() {
        // proto3 map<string,string> entry: field 1 = key, field 2 = value. Strip the value.
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(3, Action::MapStripValue(plan_proto::MapStripValue {}))],
                ..Default::default()
            }],
        };
        let mut entry = Vec::new();
        push_len_field(&mut entry, 1, b"k");
        push_len_field(&mut entry, 2, b"secret");
        let mut record = Vec::new();
        push_len_field(&mut record, 3, &entry);

        let mut expected_entry = Vec::new();
        push_len_field(&mut expected_entry, 1, b"k");
        let mut expected = Vec::new();
        push_len_field(&mut expected, 3, &expected_entry);
        assert_eq!(run(&plan_set, &record), expected);
    }

    #[test]
    fn redact_tag_keeps_value_for_centralizable_label() {
        // logging.Field tag: key(1), value(2), label(4). Keepable label 3 → whole tag kept.
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(
                    7,
                    Action::RedactTag(plan_proto::RedactTag {
                        keepable_labels: vec![3],
                    }),
                )],
                ..Default::default()
            }],
        };
        let mut tag = Vec::new();
        push_len_field(&mut tag, 1, b"key");
        push_len_field(&mut tag, 2, b"val");
        push_varint_field(&mut tag, 4, 3); // centralizable label
        let mut record = Vec::new();
        push_len_field(&mut record, 7, &tag);
        // Label kept → tag re-emitted verbatim.
        assert_eq!(run(&plan_set, &record), record);
    }

    #[test]
    fn redact_tag_strips_value_for_noncentralizable_label() {
        // Label 9 not in keepable set → keep only key(1) and label(4), drop value(2).
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![field_entry(
                    7,
                    Action::RedactTag(plan_proto::RedactTag {
                        keepable_labels: vec![3],
                    }),
                )],
                ..Default::default()
            }],
        };
        let mut tag = Vec::new();
        push_len_field(&mut tag, 1, b"key");
        push_len_field(&mut tag, 2, b"val");
        push_varint_field(&mut tag, 4, 9); // non-centralizable label
        let mut record = Vec::new();
        push_len_field(&mut record, 7, &tag);

        let mut expected_tag = Vec::new();
        push_len_field(&mut expected_tag, 1, b"key");
        push_varint_field(&mut expected_tag, 4, 9);
        let mut expected = Vec::new();
        push_len_field(&mut expected, 7, &expected_tag);
        assert_eq!(run(&plan_set, &record), expected);
    }

    #[test]
    fn field_with_unset_action_fails_closed() {
        // An entry present but with no action oneof set → deferred/unknown opcode → fail closed.
        let plan_set = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![plan_proto::FieldActionEntry {
                    field_number: Some(1),
                    action: Some(plan_proto::FieldAction { action: None }),
                }],
                ..Default::default()
            }],
        };
        let mut record = Vec::new();
        push_varint_field(&mut record, 1, 7);
        let registered = RegisteredPlan::new(plan_set);
        let table = PlanTable::new(&registered);
        let mut reader = WireReader::new(&record);
        let mut out = Vec::new();
        let err = process_message(&mut reader, &mut out, &table, 0, 0).unwrap_err();
        assert!(matches!(err, RedactError::InvalidPlan(_)));
    }

    // ===== Malformed wire input (fails closed) =====

    #[test]
    fn field_number_zero_tag_fails_closed() {
        // 0x03 = field number 0, wire type 3. Field number 0 is invalid protobuf, not an
        // end-of-message marker, so we must fail closed rather than silently skip it and emit a
        // partial record.
        let err = try_run(&pass_through_plan(1), &[0x03]).unwrap_err();
        assert!(matches!(err, RedactError::MalformedRecord(_)));
    }

    #[test]
    fn literal_zero_byte_tag_fails_closed() {
        // A literal 0 byte is field number 0 too (the whole tag varint is 0) — invalid, not a clean
        // terminator. Only running out of bytes ends a message.
        let err = try_run(&pass_through_plan(1), &[0x00]).unwrap_err();
        assert!(matches!(err, RedactError::MalformedRecord(_)));
    }

    #[test]
    fn truncated_varint_value_fails_closed() {
        // Field 1 varint whose value bytes all set the continuation bit and then the buffer ends:
        // the varint never terminates → truncated, must fail closed.
        let mut record = Vec::new();
        write_tag(&mut record, 1, wire_type::VARINT);
        record.extend_from_slice(&[0x80, 0x80, 0x80]); // continuation set, no final byte
        let err = try_run(&pass_through_plan(1), &record).unwrap_err();
        assert!(matches!(err, RedactError::MalformedRecord(_)));
    }

    #[test]
    fn overlong_varint_exceeding_64_bits_fails_closed() {
        // 11 continuation bytes: exceeds the 64-bit varint width, must fail closed rather than wrap.
        let mut record = Vec::new();
        write_tag(&mut record, 1, wire_type::VARINT);
        record.extend_from_slice(&[0x80; 10]);
        record.push(0x01);
        let err = try_run(&pass_through_plan(1), &record).unwrap_err();
        assert!(matches!(err, RedactError::MalformedRecord(_)));
    }

    #[test]
    fn length_prefix_past_end_of_buffer_fails_closed() {
        // Field 1 length-delimited claiming 50 bytes but only 3 follow → the length overruns the
        // buffer, must fail closed (the take_slice / skip bounds check).
        let mut record = Vec::new();
        write_tag(&mut record, 1, wire_type::LENGTH_DELIMITED);
        write_varint(&mut record, 50);
        record.extend_from_slice(b"abc");
        let err = try_run(&pass_through_plan(1), &record).unwrap_err();
        assert!(matches!(err, RedactError::MalformedRecord(_)));
    }

    #[test]
    fn group_wire_type_fails_closed() {
        // Wire type 3 (start-group) on a mapped field is unsupported (deprecated group encoding);
        // copy_field_value rejects it as malformed.
        let mut record = Vec::new();
        write_tag(&mut record, 1, 3); // field 1, wire type 3 = start group
        let err = try_run(&pass_through_plan(1), &record).unwrap_err();
        assert!(matches!(err, RedactError::MalformedRecord(_)));
    }

    #[test]
    fn group_wire_type_on_dropped_field_fails_closed() {
        // Same group wire type but on an unmapped field, so drop_field (not copy_field_value) sees
        // it — still unsupported, still fails closed.
        let mut record = Vec::new();
        write_tag(&mut record, 2, 3); // field 2 (no plan entry), wire type 3
        let err = try_run(&pass_through_plan(1), &record).unwrap_err();
        assert!(matches!(err, RedactError::MalformedRecord(_)));
    }
}
