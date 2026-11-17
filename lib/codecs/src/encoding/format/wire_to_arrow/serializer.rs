//! Public-facing config + serializer types.
//!
//! [`WireToArrowSerializerConfig`] is the `BatchSerializerConfig` variant
//! callers wire into a sink; [`WireToArrowSerializer`] is the runtime
//! object built from a config that turns a batch of events into a
//! `RecordBatch` via an underlying [`WireToArrowEncoder`].

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use prost_reflect::MessageDescriptor;
use vector_config::configurable_component;
use vector_core::{
    config::DataType,
    event::{Event, Value},
    schema,
};
use vrl::protobuf::descriptor::get_message_descriptor;

use super::encoder::WireToArrowEncoder;
use super::errors::{Result, WireToArrowError};

/// What the encoder does when a known field's wire type disagrees with the
/// proto descriptor (protobuf-java parses such occurrences as unknown fields
/// and keeps the message; the value is self-delimiting by wire type, so it
/// can always be skipped).
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OnWireTypeMismatch {
    /// Skip the mismatched occurrence: its column is left null for that
    /// occurrence, sibling fields and other repeated elements are unaffected,
    /// and the row is kept. Matches protobuf-java parsing. Each skip
    /// increments the `wire_to_arrow_fields_skipped_total` metric (label
    /// `reason="wire_type_mismatch"`) and feeds a rate-limited warning.
    #[default]
    SkipField,
    /// Drop the entire row from the output batch — the encoder's behavior
    /// before this option existed. Still counted by
    /// `wire_to_arrow_rows_dropped`.
    DropRow,
}

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

    /// How to handle a known field whose wire type disagrees with the proto
    /// descriptor (for example a producer built against a diverged proto
    /// sending `LEN` where the descriptor says `varint`).
    #[serde(default)]
    #[configurable(derived)]
    pub on_wire_type_mismatch: OnWireTypeMismatch,

    /// Top-level proto field names sampled into skip/drop warnings for
    /// producer attribution — fields that decode independently of the
    /// mismatched one, such as workspace / cluster / application identifiers.
    /// Unknown names are ignored with a warning at serializer build. Values
    /// are truncated (64 chars per field, 8 fields max) and message payloads
    /// are never logged; only the sampled row's configured fields are.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "app_id"))]
    pub attribution_fields: Vec<String>,

    /// How a VARIANT column backed by a nested proto message encodes its value.
    #[serde(default, skip_serializing_if = "vector_core::serde::is_default")]
    #[configurable(derived)]
    pub variant_message_encoding: VariantMessageEncoding,
}

/// How a VARIANT column backed by a nested proto message encodes its value. Columns backed by a
/// JSON text field are unaffected.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VariantMessageEncoding {
    /// The message's proto JSON mapping: proto field names, numeric 64-bit integers, enum names,
    /// present fields only, bytes as base64, well-known types in their JSON forms.
    #[default]
    ProtoJson,

    /// The value Spark's `to_variant_object(from_protobuf(...))` gives with the classic variant
    /// union's decode options (`emit.default.values`, Timestamp and Duration as
    /// `{seconds, nanos}`): native binary, float and integer types, unset fields as null keys,
    /// proto3 defaults written out, protobuf-java's closed-enum handling. A row whose message
    /// lacks a `required` field is dropped, as Spark fails its decode.
    Spark,

    /// `spark`, with null keys left out at every depth, as `variant_strip_nulls` stores it.
    SparkStripNulls,
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
            .field("on_wire_type_mismatch", &self.on_wire_type_mismatch)
            .field("attribution_fields", &self.attribution_fields)
            .field("variant_message_encoding", &self.variant_message_encoding)
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
        let encoder = WireToArrowEncoder::from_parts(
            &descriptor,
            schema,
            config.on_wire_type_mismatch,
            &config.attribution_fields,
            config.variant_message_encoding,
        )?;
        Ok(Self {
            encoder: Arc::new(encoder),
        })
    }

    /// Build a serializer from an already-resolved descriptor and schema.
    /// Mostly useful for tests and for callers that have the descriptor in
    /// memory already. Uses the default mismatch policy (`SkipField`) and no
    /// attribution fields.
    pub fn from_descriptor(descriptor: MessageDescriptor, schema: Schema) -> Result<Self> {
        Self::from_descriptor_with_variant_message_encoding(
            descriptor,
            schema,
            VariantMessageEncoding::default(),
        )
    }

    /// [`WireToArrowSerializer::from_descriptor`], with `encoding` for every VARIANT column backed
    /// by a nested proto message. Uses the default mismatch policy (`SkipField`) and no
    /// attribution fields.
    pub fn from_descriptor_with_variant_message_encoding(
        descriptor: MessageDescriptor,
        schema: Schema,
        encoding: VariantMessageEncoding,
    ) -> Result<Self> {
        let encoder =
            WireToArrowEncoder::with_variant_message_encoding(&descriptor, schema, encoding)?;
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
