//! Streaming wire-format to Arrow encoder.
//!
//! Parses proto wire bytes in a single pass and appends values directly into
//! Arrow `RecordBatch` column builders, skipping the `DynamicMessage` /
//! `LogEvent` intermediate representations used by the generic
//! `ProtobufDeserializer` + `ArrowStreamSerializer` path.
//!
//! Used as a [`BatchSerializerConfig`] variant — upstream is expected to
//! stash original proto wire bytes in the event's `message` field (Vector
//! convention). The serializer is all-or-nothing: a batch fails if any event
//! lacks a `Bytes`-typed message or the wire decode errors.
//!
//! ## Scope: single-descriptor, no wrappers
//!
//! The encoder takes one proto descriptor and assumes the bytes in
//! `event.message` match it directly. That fits direct-ingest pipelines where
//! the producer emits target-table proto bytes. It does **not** cover the
//! Lumberjack Prime path where wire bytes are actually `LogDaemonWrapper`
//! (optionally zstd-compressed, optionally wrapping a `logging.LogEntry`
//! which in turn wraps the target proto). Configuring
//! [`WireToArrowSerializerConfig`] with `LogDaemonWrapper` would produce Arrow
//! columns for the wrapper's fields, not the target table's columns;
//! configuring it with the target type would fail to decode because the
//! incoming tags are LogDaemonWrapper's. Multi-frame unwrap (plus optional
//! zstd, plus column projection from multiple proto frames, plus sink-time
//! metadata stamps) is a phase-2 scope expansion — track it against the
//! existing `TransformLumberjackPrime` VRL if that's your migration target.
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
//! ## Not supported (out of scope for phase 1)
//!
//! - Multi-level proto wrappers (`LogDaemonWrapper` → `LogEntry` → target)
//! - zstd or other inner-byte decompression
//! - Columns sourced from multiple proto frames
//! - Sink-time stamps (`now()`, `get_hostname()`) or build-time env-var
//!   injections — anything currently produced by VRL before the sink
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

use append::{append_repeated_scalar, append_scalar_from_wire, expect_len};
use builders::BuilderNodeList;
use errors::Result;
use plan::{MessagePlan, PlanSlot};

/// Configuration for the wire-to-Arrow batch serializer.
///
/// `desc_file` + `message_type` identify the proto descriptor for the *incoming*
/// wire bytes — the user must supply them directly, mirroring
/// [`ProtobufSerializerOptions`]. The sink injects the output Arrow `schema`
/// at build time (typically derived from its own schema source).
///
/// **The descriptor must describe the bytes actually present in
/// `event.message`, not a wrapper around them.** The encoder decodes the
/// wire bytes directly against this descriptor; if the bytes are a different
/// proto type (e.g. a `LogDaemonWrapper` wrapping the target payload),
/// decoding will mis-align tags or produce Arrow columns for the wrapper's
/// fields instead of the target's. See the module-level doc for the full
/// scope limitation.
///
/// [`ProtobufSerializerOptions`]: crate::encoding::format::ProtobufSerializerOptions
#[configurable_component]
#[derive(Clone, Default)]
pub struct WireToArrowSerializerConfig {
    /// Path to the protobuf descriptor set file describing the incoming wire bytes.
    ///
    /// Must correspond to the exact proto type serialized in `event.message`
    /// — not an outer wrapper. Typically the output of
    /// `protoc -I <include path> -o <desc output path> <proto>`.
    #[configurable(metadata(docs::examples = "/etc/vector/protobuf_descriptor_set.desc"))]
    pub desc_file: PathBuf,

    /// The fully-qualified message type within the descriptor file. Must name
    /// the type of the bytes in `event.message` (not a wrapper type).
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
    pub fn encode_batch(&self, messages: &[Bytes]) -> Result<RecordBatch> {
        let capacity = messages.len();
        let mut builders = BuilderNodeList::with_capacity(&self.plan, capacity);
        let mut present = vec![false; self.plan.slots.len()];

        for msg_bytes in messages {
            present.iter_mut().for_each(|p| *p = false);
            scan_message(&self.plan, msg_bytes, &mut builders, &mut present)?;
            builders.finalize_row(&self.plan, &present);
        }

        let arrays = builders.finish(&self.plan)?;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|source| WireToArrowError::RecordBatchAssembly { source })
    }
}

/// Scan one proto message's wire bytes, appending values into `builders`.
/// `present[i]` is set to `true` if slot `i` was touched by any tag in this
/// message.
fn scan_message(
    plan: &MessagePlan,
    mut bytes: &[u8],
    builders: &mut BuilderNodeList,
    present: &mut [bool],
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
        let node = &mut builders.nodes[slot_idx];

        match (slot, node) {
            (PlanSlot::Scalar(sk), builders::BuilderNode::Scalar(tb)) => {
                append_scalar_from_wire(*sk, &field.value, tb)?;
                present[slot_idx] = true;
            }
            (PlanSlot::Struct(sub_plan), builders::BuilderNode::Struct { children, .. }) => {
                let sub_bytes = expect_len(&field.value)?;
                let mut sub_present = vec![false; sub_plan.slots.len()];
                scan_message(sub_plan, sub_bytes, children, &mut sub_present)?;
                children.finalize_row(sub_plan, &sub_present);
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
                let mut sub_present = vec![false; sub_plan.slots.len()];
                scan_message(sub_plan, sub_bytes, children, &mut sub_present)?;
                children.finalize_row(sub_plan, &sub_present);
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
