//! Streaming wire-format to Arrow encoder.
//!
//! Parses proto wire bytes in a single pass and appends values directly into
//! Arrow `RecordBatch` column builders, skipping the `DynamicMessage` /
//! `LogEvent` intermediate representations used by the generic
//! `ProtobufDeserializer` + `ArrowStreamSerializer` path.
//!
//! Used as a [`BatchSerializerConfig`] variant — upstream is expected to
//! stash original proto wire bytes in the event's `message` field (Vector
//! convention).
//!
//! Failure semantics are split:
//!   * Event-shape problems (missing `message` field, non-`Bytes` value) fail
//!     the batch — the pipeline is misconfigured if any event reaches here in
//!     the wrong shape.
//!   * Wire-format decode errors are isolated to the offending row: the row
//!     is dropped from the output `RecordBatch`, counted via the
//!     `wire_to_arrow_rows_dropped` metric, and a sample error is logged.
//!     One poison message can't poison the whole batch.
//!
//! ## Scope
//!
//! The encoder takes one `MessageDescriptor` and decodes the bytes in
//! `event.message` against it, emitting one `RecordBatch` row per event. It
//! is agnostic to how the caller produced those bytes and to what any
//! particular schema represents. If the incoming payload requires any
//! pre-processing — multi-frame unwrapping, decompression, merging bytes from
//! multiple sources, sink-time / build-time stamps — perform it upstream (in
//! VRL or a custom transform) so that `event.message` holds a single
//! self-contained byte stream that matches the configured descriptor.
//!
//! ## Supported today
//!
//! - Scalar proto fields (int32/int64/uint32/uint64/sint32/sint64/fixed*/float/double/bool/string/bytes/enum)
//! - Singular nested messages -> Arrow `Struct`
//! - Repeated nested messages -> Arrow `List<Struct>`
//! - Repeated scalars (packed and unpacked) -> Arrow `List<primitive>`
//! - Proto maps (`map<K, V>`) -> Arrow `Map<Struct(key, value)>`
//! - Oneof variants
//! - `int64 -> Timestamp(Microsecond, tz)` coercion
//!
//! Benchmarks live at `benches/codecs/wire_to_arrow_bench.rs`.
//!
//! [`BatchSerializerConfig`]: crate::encoding::BatchSerializerConfig

mod append;
mod builders;
mod errors;
mod plan;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::{Fields, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use prost_reflect::MessageDescriptor;
use vector_config::configurable_component;
use vector_core::{
    config::DataType,
    event::{Event, Value},
    schema,
};
use vrl::protobuf::descriptor::get_message_descriptor;

pub use errors::WireToArrowError;

use zeroparser::wire::{WireValue, decode_zigzag32, decode_zigzag64, try_parse_field};

use append::{
    append_repeated_scalar, append_scalar_from_wire, expect_len, validate_repeated_scalar,
    validate_scalar_from_wire,
};
use builders::BuilderNodeList;
use errors::Result;
use plan::{MessagePlan, PlanSlot};

/// Configuration for the wire-to-Arrow batch serializer.
///
/// `desc_file` + `message_type` identify the proto descriptor for the
/// *incoming* wire bytes — the user must supply them directly, mirroring
/// [`ProtobufSerializerOptions`]. The sink injects the output Arrow `schema`
/// at build time (typically derived from its own schema source).
///
/// The descriptor must describe the exact bytes present in `event.message`;
/// decoding uses the descriptor's field numbers as-is. If the payload needs
/// any pre-processing before it matches the descriptor, do it upstream.
///
/// [`ProtobufSerializerOptions`]: crate::encoding::format::ProtobufSerializerOptions
#[configurable_component]
#[derive(Clone, Default)]
pub struct WireToArrowSerializerConfig {
    /// Path to the protobuf descriptor set file describing the incoming wire bytes.
    ///
    /// Must correspond to the exact proto type serialized in `event.message`.
    /// Typically the output of `protoc -I <include path> -o <desc output path> <proto>`.
    #[configurable(metadata(docs::examples = "/etc/vector/protobuf_descriptor_set.desc"))]
    pub desc_file: PathBuf,

    /// The fully-qualified message type within the descriptor file. Must name
    /// the type of the bytes in `event.message`.
    #[configurable(metadata(docs::examples = "package.Message"))]
    pub message_type: String,

    /// The Arrow schema of the output `RecordBatch`. Injected by the sink.
    #[serde(skip)]
    #[configurable(derived)]
    pub schema: Option<arrow::datatypes::Schema>,
}

impl std::fmt::Debug for WireToArrowSerializerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireToArrowSerializerConfig")
            .field("desc_file", &self.desc_file)
            .field("message_type", &self.message_type)
            .field(
                "schema",
                &self
                    .schema
                    .as_ref()
                    .map(|s| format!("{} fields", s.fields().len())),
            )
            .finish()
    }
}

impl WireToArrowSerializerConfig {
    /// The data type of events accepted by this serializer.
    pub fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

/// Batch serializer that decodes proto wire bytes directly into an Arrow
/// `RecordBatch`, bypassing the generic `ProtobufDeserializer` chain.
#[derive(Clone, Debug)]
pub struct WireToArrowSerializer {
    encoder: Arc<WireToArrowEncoder>,
}

impl WireToArrowSerializer {
    /// Build a serializer from the given configuration. Loads the proto
    /// descriptor from `desc_file` + `message_type`; the output Arrow schema
    /// must have been injected (via `config.schema`) by the sink.
    pub fn new(config: WireToArrowSerializerConfig) -> Result<Self> {
        let descriptor = get_message_descriptor(&config.desc_file, &config.message_type)
            .map_err(|message| WireToArrowError::DescriptorLoad { message })?;
        let schema = config
            .schema
            .ok_or_else(|| WireToArrowError::ConfigurationMissing { field: "schema" })?;
        Self::from_descriptor(descriptor, schema)
    }

    /// Build a serializer from an already-resolved descriptor and schema.
    /// Mostly useful for tests and for callers that have the descriptor in
    /// memory already.
    pub fn from_descriptor(descriptor: MessageDescriptor, schema: Schema) -> Result<Self> {
        let encoder = WireToArrowEncoder::new(&descriptor, schema)?;
        Ok(Self {
            encoder: Arc::new(encoder),
        })
    }

    /// Encode a batch of events into a single Arrow `RecordBatch`.
    ///
    /// Every event must carry a `Value::Bytes`-typed `message` field holding
    /// the original proto wire bytes; any miss rejects the batch.
    pub fn encode_to_record_batch(&self, events: &[Event]) -> Result<RecordBatch> {
        if events.is_empty() {
            return Err(WireToArrowError::NoEvents);
        }
        let mut wire_bytes = Vec::with_capacity(events.len());
        for event in events {
            match event.as_log().get_message() {
                Some(Value::Bytes(b)) => wire_bytes.push(b.clone()),
                Some(_) => return Err(WireToArrowError::MessageBytesWrongType),
                None => return Err(WireToArrowError::MessageBytesMissing),
            }
        }
        self.encoder.encode_batch(&wire_bytes)
    }
}

/// Streaming wire-format encoder. Build once per (proto message type,
/// Arrow schema) pair, then call [`WireToArrowEncoder::encode_batch`]
/// repeatedly.
pub struct WireToArrowEncoder {
    plan: Arc<MessagePlan>,
    schema: Arc<Schema>,
}

impl std::fmt::Debug for WireToArrowEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireToArrowEncoder")
            .field("schema_fields", &self.schema.fields().len())
            .finish()
    }
}

impl WireToArrowEncoder {
    /// Compile a plan for the given proto descriptor + Arrow schema.
    ///
    /// Every field in `schema` must exist (by name) in `descriptor`. Proto
    /// fields absent from `schema` are silently skipped at scan time.
    pub fn new(descriptor: &MessageDescriptor, schema: Schema) -> Result<Self> {
        let plan = MessagePlan::build(descriptor, &Fields::from(schema.fields().clone()))?;
        Ok(Self {
            plan: Arc::new(plan),
            schema: Arc::new(schema),
        })
    }

    /// Encode a batch of serialized proto messages into a single `RecordBatch`.
    ///
    /// Per-row isolation: each message is pre-validated via
    /// [`validate_message`] before any builder is touched. Rows that fail
    /// validation are dropped from the output batch and counted via the
    /// `wire_to_arrow_rows_dropped` metric (plus a rate-limit-friendly
    /// warn log carrying a sample error). Returning an empty `RecordBatch`
    /// is acceptable when every row was malformed.
    ///
    /// Errors out of this method are reserved for batch-level failures
    /// that aren't attributable to a single row: a code-bug surface
    /// (`PlanBuilderMismatch`, scan-vs-validate divergence) or a
    /// `RecordBatchAssembly` rejection from Arrow.
    pub fn encode_batch(&self, messages: &[Bytes]) -> Result<RecordBatch> {
        let capacity = messages.len();
        let mut builders = BuilderNodeList::with_capacity(&self.plan, capacity);
        let mut dropped = 0u64;
        let mut sample_err: Option<WireToArrowError> = None;

        for msg_bytes in messages {
            // Pre-validate so a malformed row drops without poisoning any
            // builder. Arrow `*Builder` has no public rollback API, and
            // nested-struct `finalize_row` calls inside `scan_message` are
            // not reversible, so an upfront validation pass is how we
            // isolate per-row decode failures.
            if let Err(err) = validate_message(&self.plan, msg_bytes) {
                dropped += 1;
                if sample_err.is_none() {
                    sample_err = Some(err);
                }
                continue;
            }
            builders.reset_present();
            scan_message(&self.plan, msg_bytes, &mut builders)?;
            builders.finalize_row(&self.plan);
        }

        if dropped > 0 {
            metrics::counter!("wire_to_arrow_rows_dropped").increment(dropped);
            tracing::warn!(
                message = "wire-to-Arrow dropped malformed rows from batch",
                dropped,
                batch_size = messages.len(),
                sample_error = ?sample_err,
            );
        }

        let arrays = builders.finish(&self.plan)?;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|source| WireToArrowError::RecordBatchAssembly { source })
    }
}

/// Scan one proto message's wire bytes, appending values into `builders`.
///
/// Sets `builders.present[i] = true` for each slot `i` touched by any tag in
/// this message. The caller is responsible for resetting `present` (via
/// [`BuilderNodeList::reset_present`]) before invoking, and for calling
/// [`BuilderNodeList::finalize_row`] afterwards. Sub-messages reuse their own
/// level's `present` buffer, so no per-occurrence allocation happens on the
/// hot path.
fn scan_message(
    plan: &MessagePlan,
    mut bytes: &[u8],
    builders: &mut BuilderNodeList,
) -> Result<()> {
    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes)?;
        bytes = rest;
        let field_number = field.field_num as usize;

        let Some(Some(slot_idx)) = plan.slot_by_proto_field.get(field_number).copied() else {
            // Unknown field — `try_parse_field` already consumed it.
            continue;
        };
        let slot_idx = slot_idx as usize;

        let slot = &plan.slots[slot_idx];
        // Split-borrow `nodes` and `present` so we can mutate the dispatched
        // node and flag `present[slot_idx]` in the same iteration.
        let BuilderNodeList { nodes, present } = &mut *builders;
        let node = &mut nodes[slot_idx];

        match (slot, node) {
            (PlanSlot::Scalar(sk), builders::BuilderNode::Scalar(tb)) => {
                append_scalar_from_wire(*sk, &field.value, tb)?;
                present[slot_idx] = true;
            }
            (PlanSlot::Struct(sub_plan), builders::BuilderNode::Struct { children, .. }) => {
                let sub_bytes = expect_len(&field.value)?;
                children.reset_present();
                scan_message(sub_plan, sub_bytes, children)?;
                children.finalize_row(sub_plan);
                present[slot_idx] = true;
            }
            (
                PlanSlot::RepeatedMessage(sub_plan),
                builders::BuilderNode::RepeatedMessage {
                    children,
                    current_offset,
                    ..
                },
            )
            | (
                PlanSlot::Map(sub_plan),
                builders::BuilderNode::Map {
                    children,
                    current_offset,
                    ..
                },
            ) => {
                let sub_bytes = expect_len(&field.value)?;
                children.reset_present();
                scan_message(sub_plan, sub_bytes, children)?;
                children.finalize_row(sub_plan);
                *current_offset += 1;
                present[slot_idx] = true;
            }
            (
                PlanSlot::RepeatedScalar(sk),
                builders::BuilderNode::RepeatedScalar {
                    values,
                    current_offset,
                    ..
                },
            ) => {
                append_repeated_scalar(*sk, &field.value, values, current_offset)?;
                present[slot_idx] = true;
            }
            _ => return Err(WireToArrowError::PlanBuilderMismatch),
        }
    }
    Ok(())
}

/// Walk one proto message's wire bytes without touching any builders, surfacing
/// every decode error that [`scan_message`] would produce for the same input.
/// [`WireToArrowEncoder::encode_batch`] runs this as a pre-pass per message so
/// rows that fail can be dropped from the batch cleanly — no half-appended
/// leaves, no finalized nested sub-rows — and replaced with a `dropped`
/// counter instead of failing the entire batch.
///
/// The two-pass cost is acceptable because (a) the parse walk is small
/// relative to value appends + buffer growth on the real scan, and (b) Arrow
/// `*Builder` types expose no public rollback API, so an in-place
/// "snapshot + truncate on error" alternative isn't viable.
///
/// Must stay in lock-step with [`scan_message`]: any wire byte sequence that
/// is accepted here must also be accepted there, and vice versa. If the two
/// diverge (validate accepts but scan errors), the real scan's `?` in
/// `encode_batch` will bubble it out as a batch-level failure — that's a
/// clear signal of a code bug rather than user input.
fn validate_message(plan: &MessagePlan, mut bytes: &[u8]) -> Result<()> {
    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes)?;
        bytes = rest;
        let field_number = field.field_num as usize;

        let Some(Some(slot_idx)) = plan.slot_by_proto_field.get(field_number).copied() else {
            continue;
        };
        let slot_idx = slot_idx as usize;
        let slot = &plan.slots[slot_idx];

        match slot {
            PlanSlot::Scalar(sk) => validate_scalar_from_wire(*sk, &field.value)?,
            PlanSlot::Struct(sub_plan)
            | PlanSlot::RepeatedMessage(sub_plan)
            | PlanSlot::Map(sub_plan) => {
                let sub_bytes = expect_len(&field.value)?;
                validate_message(sub_plan, sub_bytes)?;
            }
            PlanSlot::RepeatedScalar(sk) => validate_repeated_scalar(*sk, &field.value)?,
            // No proto field number ever points at an Absent slot (Absent
            // slots are Arrow columns the proto descriptor lacks), so this
            // arm is unreachable in practice. Mirror `scan_message`'s
            // fall-through and surface it as a code-bug signal.
            PlanSlot::Absent => return Err(WireToArrowError::PlanBuilderMismatch),
        }
    }
    Ok(())
}
