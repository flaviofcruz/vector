//! Direct proto-wire → Parquet Variant encoding for message-backed VARIANT
//! columns.
//!
//! The reference path ([`encode_variant_from_message`]) decodes into a
//! `DynamicMessage`, serializes that to a `serde_json::Value`, then walks the
//! JSON into a `VariantBuilder`. [`VariantMessagePlan`] produces the same
//! Variant straight from the wire bytes using a descriptor-compiled plan,
//! skipping both intermediate trees.
//!
//! Parity contract: for every input the result equals the reference's, value
//! for value and Variant type for type (integers take the narrowest width, as
//! `append_json` picks them). Inputs whose reference semantics are costly to
//! mirror — maps, well-known types, extensions, conflicting oneof members, a
//! repeated singular sub-message, and any wire the walker rejects — make the
//! direct walk bail out and the whole value is re-encoded by the reference
//! path, which also produces the reference's error for malformed input.
//!
//! Under the `skip_field` mismatch policy a known field whose wire type
//! disagrees with the descriptor is instead omitted from the Variant
//! (protobuf-java treats it as an unknown field) and reported to the caller.
//! If the walk still bails for a reference-only shape elsewhere in the same
//! message, the caller first sanitizes the bytes with
//! [`VariantMessagePlan::sanitize_for_reference`] — stripping every
//! wire-type-mismatched occurrence the reference decoder would reject — and
//! hands the sanitized bytes to the reference decoder, so the row survives
//! with the mismatched fields omitted. Malformed input survives sanitization
//! untouched and still drops exactly as before.
//!
//! [`encode_variant_from_message`]: super::append::encode_variant_from_message

use std::collections::HashMap;

use base64::Engine as _;
use parquet_variant::{BuilderSpecificState, ListBuilder, ObjectBuilder, Variant, VariantBuilder};
use prost_reflect::prost::encoding::{WireType, encode_key, encode_varint};
use prost_reflect::{Cardinality, FieldDescriptor, Kind, MessageDescriptor};
use zeroparser::wire::{
    WireValue, decode_zigzag32, decode_zigzag64, try_parse_field, try_read_varint,
};

use super::append::{encode_variant_from_message, expect_len, wire_type_byte};
use super::errors::Result;
use super::plan::{WT_I32, WT_I64, WT_LEN, WT_VARINT};
use super::serializer::VariantMessageEncoding;
use super::variant_spark::SparkVariantEncoder;

/// Recursion limit of the reference `DynamicMessage` decode (prost's default);
/// deeper input falls back so the reference decides.
pub(super) const MAX_DEPTH: usize = 100;

/// Field-number range served by a dense lookup table; wider messages use a map.
const DENSE_FIELD_NUMBER_LIMIT: u32 = 4096;

/// Marker for "let the reference path encode this value".
struct Fallback;

type Walk<T = ()> = std::result::Result<T, Fallback>;

#[derive(Clone, Copy)]
enum ScalarType {
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Float,
    Double,
    Bool,
    String,
    Bytes,
}

impl ScalarType {
    /// Wire type of a single (unpacked) value of this kind.
    fn wire_type(self) -> u8 {
        match self {
            Self::Fixed32 | Self::Sfixed32 | Self::Float => WT_I32,
            Self::Fixed64 | Self::Sfixed64 | Self::Double => WT_I64,
            Self::String | Self::Bytes => WT_LEN,
            _ => WT_VARINT,
        }
    }

    fn from_kind(kind: &Kind) -> Option<Self> {
        Some(match kind {
            Kind::Int32 => Self::Int32,
            Kind::Int64 => Self::Int64,
            Kind::Uint32 => Self::Uint32,
            Kind::Uint64 => Self::Uint64,
            Kind::Sint32 => Self::Sint32,
            Kind::Sint64 => Self::Sint64,
            Kind::Fixed32 => Self::Fixed32,
            Kind::Fixed64 => Self::Fixed64,
            Kind::Sfixed32 => Self::Sfixed32,
            Kind::Sfixed64 => Self::Sfixed64,
            Kind::Float => Self::Float,
            Kind::Double => Self::Double,
            Kind::Bool => Self::Bool,
            Kind::String => Self::String,
            Kind::Bytes => Self::Bytes,
            Kind::Message(_) | Kind::Enum(_) => return None,
        })
    }

    /// Wire layout of one packed element; `None` for length-delimited kinds,
    /// which cannot be packed.
    fn packed_width(self) -> Option<PackedWidth> {
        match self {
            Self::Fixed32 | Self::Sfixed32 | Self::Float => Some(PackedWidth::Fixed4),
            Self::Fixed64 | Self::Sfixed64 | Self::Double => Some(PackedWidth::Fixed8),
            Self::String | Self::Bytes => None,
            _ => Some(PackedWidth::Varint),
        }
    }
}

#[derive(Clone, Copy)]
enum PackedWidth {
    Varint,
    Fixed4,
    Fixed8,
}

enum FieldType {
    Scalar(ScalarType),
    /// Enum value number → name, resolved once with the reference's own
    /// `EnumDescriptor::get_value`; numbers without a name serialize as ints.
    Enum {
        names: HashMap<i32, Box<str>>,
        default_number: i32,
    },
    Message(usize),
    /// Maps, well-known types, `google.protobuf.NullValue`: reference only.
    Fallback,
}

struct FieldPlan {
    /// Proto field number, for skip attribution.
    number: u32,
    name: Box<str>,
    field_type: FieldType,
    repeated: bool,
    /// Explicit presence: a present default value is still serialized.
    has_presence: bool,
    /// Index of the containing (real or synthetic) oneof, if any.
    oneof: Option<u8>,
}

impl FieldPlan {
    /// Wire type a single unpacked value of this field carries; reported as
    /// `expected` in skip records (mirroring the frame-level validators,
    /// which name the native type even for fields that also accept packed).
    fn singular_wire_type(&self) -> u8 {
        match &self.field_type {
            FieldType::Scalar(scalar) => scalar.wire_type(),
            FieldType::Enum { .. } => WT_VARINT,
            FieldType::Message(_) | FieldType::Fallback => WT_LEN,
        }
    }
}

enum FieldLookup {
    Field(u16),
    /// An extension of this message: the reference serializes it, so fall back.
    Extension,
    Unknown,
}

const DENSE_UNKNOWN: u16 = u16::MAX;
const DENSE_EXTENSION: u16 = u16::MAX - 1;

enum FieldIndex {
    Dense(Vec<u16>),
    Sparse(HashMap<u32, FieldLookup>),
}

struct MessageVariantPlan {
    fields: Vec<FieldPlan>,
    /// One wire-type accept mask per field (bit n = wire type n), parallel to
    /// `fields` — the skip-mode walk filters mismatched occurrences with it
    /// at gather time, mirroring the frame-level `MessagePlan::wt_accept_masks`.
    accept_masks: Vec<u8>,
    index: FieldIndex,
}

/// Wire types a field occurrence may carry, as a 1-bit-per-wire-type mask,
/// mirroring `PlanSlot::accept_wire_type_mask`: singular values use the
/// native type, repeated numerics/enums also accept a packed `LEN` blob.
/// `Fallback` fields are the reference path's job and never mismatch here.
fn field_accept_mask(field_type: &FieldType, repeated: bool) -> u8 {
    match field_type {
        FieldType::Fallback => 0xff,
        FieldType::Message(_) => 1 << WT_LEN,
        FieldType::Enum { .. } if repeated => (1 << WT_VARINT) | (1 << WT_LEN),
        FieldType::Enum { .. } => 1 << WT_VARINT,
        FieldType::Scalar(scalar) => match (repeated, scalar.packed_width()) {
            (true, Some(_)) => (1 << scalar.wire_type()) | (1 << WT_LEN),
            _ => 1 << scalar.wire_type(),
        },
    }
}

/// Skip reporting for one VARIANT walk under the `skip_field` policy.
/// `None` keeps the reference-parity behavior: a mismatched occurrence
/// aborts the direct walk and the reference path re-encodes the value.
pub(super) struct SkipReport<'a> {
    /// Root-to-leaf proto field path up to the value being walked, extended
    /// with each nested message field as the walk descends. Caller-owned and
    /// reused across rows.
    pub(super) path: &'a mut Vec<u32>,
    /// Called once per skipped occurrence with the enclosing path, the proto
    /// field number, the wire type the field expects, and the wire type found.
    pub(super) sink: SkipSinkFn<'a>,
}

type SkipSinkFn<'a> = &'a mut dyn FnMut(&[u32], u32, u8, u8);

/// Wire-type accept mask and `expected` value for one descriptor field,
/// derived straight from the descriptor with the same rules as
/// [`field_accept_mask`]. Sanitization must strip exactly the occurrences the
/// reference prost decoder rejects as wire-type mismatches — prost is the
/// decoder that runs on the sanitized bytes, so the descriptor (not the
/// compiled walker plan) is the source of truth here, and it covers shapes
/// the walker plan never compiles (map entries, well-known types).
fn descriptor_accept_mask(field: &FieldDescriptor) -> (u8, u8) {
    let repeated = field.cardinality() == Cardinality::Repeated;
    match field.kind() {
        Kind::Message(_) => (1 << WT_LEN, WT_LEN),
        Kind::Enum(_) if repeated => ((1 << WT_VARINT) | (1 << WT_LEN), WT_VARINT),
        Kind::Enum(_) => (1 << WT_VARINT, WT_VARINT),
        kind => {
            let scalar =
                ScalarType::from_kind(&kind).expect("descriptor kind is scalar after Message/Enum");
            let expected = scalar.wire_type();
            let mask = match (repeated, scalar.packed_width()) {
                (true, Some(_)) => (1 << expected) | (1 << WT_LEN),
                _ => 1 << expected,
            };
            (mask, expected)
        }
    }
}

/// The walker's view of the optional [`SkipReport`], so the emit functions
/// never match on the option themselves.
struct WalkReport<'a, 'r>(&'r mut Option<SkipReport<'a>>);

impl WalkReport<'_, '_> {
    fn active(&self) -> bool {
        self.0.is_some()
    }

    /// Descend into a nested message field for skip attribution.
    fn enter(&mut self, field_number: u32) {
        if let Some(report) = self.0.as_mut() {
            report.path.push(field_number);
        }
    }

    /// Leave a nested message field; paired with [`WalkReport::enter`] before
    /// errors propagate so a failed walk leaves the path balanced.
    fn leave(&mut self) {
        if let Some(report) = self.0.as_mut() {
            report.path.pop();
        }
    }

    fn skip(&mut self, field_number: u32, expected: u8, actual: u8) {
        if let Some(report) = self.0.as_mut() {
            let SkipReport { path, sink } = report;
            (sink)(path, field_number, expected, actual);
        }
    }
}

impl MessageVariantPlan {
    #[inline]
    fn lookup(&self, number: u32) -> FieldLookup {
        match &self.index {
            FieldIndex::Dense(table) => match table.get(number as usize).copied() {
                None | Some(DENSE_UNKNOWN) => FieldLookup::Unknown,
                Some(DENSE_EXTENSION) => FieldLookup::Extension,
                Some(idx) => FieldLookup::Field(idx),
            },
            FieldIndex::Sparse(map) => match map.get(&number) {
                Some(FieldLookup::Field(idx)) => FieldLookup::Field(*idx),
                Some(FieldLookup::Extension) => FieldLookup::Extension,
                _ => FieldLookup::Unknown,
            },
        }
    }
}

/// One tag of the message being walked, gathered before emission so that
/// repeated occurrences, last-wins scalars and oneof conflicts are resolved
/// the way a `DynamicMessage` decode resolves them.
#[derive(Clone, Copy)]
struct Entry<'a> {
    field: u16,
    value: WireValue<'a>,
}

/// Compiled plan for one VARIANT column backed by a nested proto message.
pub(crate) struct VariantMessagePlan {
    descriptor: MessageDescriptor,
    messages: Vec<MessageVariantPlan>,
    /// The column's own message is a well-known type: always use the reference.
    root_fallback: bool,
    /// Set for [`VariantMessageEncoding::Spark`] and
    /// [`VariantMessageEncoding::SparkStripNulls`]: replaces both the direct walk and the reference.
    spark: Option<SparkVariantEncoder>,
}

impl std::fmt::Debug for VariantMessagePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VariantMessagePlan")
            .field("message", &self.descriptor.full_name())
            .field("compiled_messages", &self.messages.len())
            .field("spark", &self.spark.is_some())
            .finish()
    }
}

fn is_well_known(desc: &MessageDescriptor) -> bool {
    desc.full_name().starts_with("google.protobuf.")
}

impl VariantMessagePlan {
    pub(crate) fn new(descriptor: &MessageDescriptor) -> Self {
        let mut plan = Self {
            descriptor: descriptor.clone(),
            messages: Vec::new(),
            root_fallback: is_well_known(descriptor),
            spark: None,
        };
        if !plan.root_fallback {
            plan.compile(descriptor, &mut HashMap::new());
        }
        plan
    }

    /// A plan for `descriptor` that encodes with `encoding`. The proto JSON encoding is
    /// [`VariantMessagePlan::new`]; the Spark encodings skip the direct-walk compile.
    pub(crate) fn with_encoding(
        descriptor: &MessageDescriptor,
        encoding: VariantMessageEncoding,
    ) -> Self {
        let strip_nulls = match encoding {
            VariantMessageEncoding::ProtoJson => return Self::new(descriptor),
            VariantMessageEncoding::Spark => false,
            VariantMessageEncoding::SparkStripNulls => true,
        };
        Self {
            descriptor: descriptor.clone(),
            messages: Vec::new(),
            root_fallback: true,
            spark: Some(SparkVariantEncoder::new(descriptor, strip_nulls)),
        }
    }

    /// Compile `desc` (and, recursively, its message-typed fields) and return
    /// its index in `self.messages`. Self-referential types reuse the index.
    fn compile(&mut self, desc: &MessageDescriptor, seen: &mut HashMap<String, usize>) -> usize {
        if let Some(&idx) = seen.get(desc.full_name()) {
            return idx;
        }
        let idx = self.messages.len();
        seen.insert(desc.full_name().to_string(), idx);
        self.messages.push(MessageVariantPlan {
            fields: Vec::new(),
            accept_masks: Vec::new(),
            index: FieldIndex::Sparse(HashMap::new()),
        });

        let oneofs: Vec<String> = desc.oneofs().map(|o| o.full_name().to_string()).collect();
        let mut fields = Vec::new();
        let mut numbers = Vec::new();
        for field in desc.fields() {
            let field_type = match field.kind() {
                Kind::Message(_) if field.is_map() => FieldType::Fallback,
                Kind::Message(m) if is_well_known(&m) => FieldType::Fallback,
                Kind::Message(m) => FieldType::Message(self.compile(&m, seen)),
                Kind::Enum(e) if e.full_name() == "google.protobuf.NullValue" => {
                    FieldType::Fallback
                }
                Kind::Enum(e) => {
                    let names = e
                        .values()
                        .filter_map(|v| {
                            let number = v.number();
                            e.get_value(number)
                                .map(|canonical| (number, canonical.name().into()))
                        })
                        .collect();
                    // Only consulted for implicit presence (proto3), where a
                    // field default is always the enum's first value.
                    let default_number = e.default_value().number();
                    FieldType::Enum {
                        names,
                        default_number,
                    }
                }
                kind => FieldType::Scalar(
                    ScalarType::from_kind(&kind).expect("non-message, non-enum kind is scalar"),
                ),
            };
            let oneof = field.containing_oneof().and_then(|o| {
                oneofs
                    .iter()
                    .position(|name| name == o.full_name())
                    .and_then(|p| u8::try_from(p).ok())
            });
            // More oneofs than the 64-bit conflict mask tracks: treat the
            // field as reference-only rather than mis-resolve a conflict.
            let field_type = match oneof {
                Some(o) if o >= 64 => FieldType::Fallback,
                _ if field.containing_oneof().is_some() && oneof.is_none() => FieldType::Fallback,
                _ => field_type,
            };
            numbers.push((field.number(), fields.len() as u16));
            fields.push(FieldPlan {
                number: field.number(),
                name: field.name().into(),
                field_type,
                repeated: field.cardinality() == Cardinality::Repeated,
                has_presence: field.supports_presence(),
                oneof,
            });
        }
        let extensions: Vec<u32> = desc.extensions().map(|e| e.number()).collect();

        let max_number = numbers
            .iter()
            .map(|(n, _)| *n)
            .chain(extensions.iter().copied())
            .max()
            .unwrap_or(0);
        let index =
            if max_number < DENSE_FIELD_NUMBER_LIMIT && fields.len() < DENSE_EXTENSION as usize {
                let mut table = vec![DENSE_UNKNOWN; max_number as usize + 1];
                for (number, idx) in &numbers {
                    table[*number as usize] = *idx;
                }
                for number in &extensions {
                    table[*number as usize] = DENSE_EXTENSION;
                }
                FieldIndex::Dense(table)
            } else {
                let mut map: HashMap<u32, FieldLookup> = numbers
                    .iter()
                    .map(|(number, idx)| (*number, FieldLookup::Field(*idx)))
                    .collect();
                for number in extensions {
                    map.insert(number, FieldLookup::Extension);
                }
                FieldIndex::Sparse(map)
            };
        let accept_masks = fields
            .iter()
            .map(|f| field_accept_mask(&f.field_type, f.repeated))
            .collect();
        self.messages[idx] = MessageVariantPlan {
            fields,
            accept_masks,
            index,
        };
        idx
    }

    /// Encode one occurrence of the column's message (its wire bytes) into the
    /// Parquet Variant `(metadata, value)` pair. A wire-type-mismatched
    /// occurrence aborts the direct walk, deferring to the reference decoder —
    /// which rejects it — so the caller's policy decides what a mismatch costs.
    pub(crate) fn encode(&self, wire: &WireValue) -> Result<(Vec<u8>, Vec<u8>)> {
        if let Some(spark) = &self.spark {
            return spark.encode(expect_len(wire)?);
        }
        match wire {
            WireValue::Len(bytes) => match self.encode_direct(bytes, &mut None) {
                Some(built) => Ok(built),
                None => encode_variant_from_message(&self.descriptor, wire),
            },
            _ => encode_variant_from_message(&self.descriptor, wire),
        }
    }

    /// `skip_field` counterpart of [`encode`]: the caller (in `scan.rs`) runs
    /// [`Self::encode_direct`] with an active [`SkipReport`] so mismatched
    /// occurrences are omitted from the Variant, and on fallback sanitizes the
    /// bytes ([`Self::sanitize_for_reference`]) before re-encoding via the
    /// reference decoder ([`Self::encode_reference`]).
    ///
    /// [`encode`]: Self::encode
    pub(super) fn encode_direct(
        &self,
        bytes: &[u8],
        report: &mut Option<SkipReport<'_>>,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.root_fallback {
            return None;
        }
        let mut builder = VariantBuilder::new();
        let mut scratch = Vec::new();
        let mut object = builder.new_object();
        let mut report = WalkReport(report);
        self.emit_message(0, bytes, 0, &mut scratch, &mut object, &mut report)
            .ok()?;
        object.finish();
        Some(builder.finish())
    }

    /// Reference-path encode of one occurrence — the shapes the direct walk
    /// rejects (and the sanitized replay of those shapes) go through here.
    /// A Spark-encoded plan never walks directly, so every occurrence lands
    /// here and is encoded with Spark's semantics.
    pub(super) fn encode_reference(&self, wire: &WireValue<'_>) -> Result<(Vec<u8>, Vec<u8>)> {
        if let Some(spark) = &self.spark {
            return spark.encode(expect_len(wire)?);
        }
        encode_variant_from_message(&self.descriptor, wire)
    }

    /// Strip every wire-type-mismatched occurrence of a known field from one
    /// occurrence of the column's message, recursively through nested
    /// messages, map entries and well-known types, so the reference prost
    /// decoder accepts the remainder instead of rejecting the whole row.
    /// Skips are reported through the [`SkipReport`] sink.
    ///
    /// Returns `None` when the bytes are malformed (a parse error, a group
    /// wire type, or past prost's recursion limit) — the caller then re-encodes
    /// the *original* bytes so the reference decoder produces its own error
    /// and the row drops exactly as it would without sanitization. Anything
    /// else malformed-but-wire-type-compatible (a truncated packed blob,
    /// invalid UTF-8) is copied verbatim and still fails the reference decode,
    /// unchanged.
    pub(super) fn sanitize_for_reference(
        &self,
        wire: &WireValue<'_>,
        report: &mut Option<SkipReport<'_>>,
    ) -> Option<Vec<u8>> {
        let WireValue::Len(bytes) = wire else {
            return None;
        };
        let mut report = WalkReport(report);
        sanitize_message(&self.descriptor, bytes, &mut report, 0)
    }

    fn emit_message<'a, S: BuilderSpecificState>(
        &self,
        message: usize,
        mut bytes: &'a [u8],
        depth: usize,
        scratch: &mut Vec<Entry<'a>>,
        object: &mut ObjectBuilder<'_, S>,
        report: &mut WalkReport<'_, '_>,
    ) -> Walk {
        if depth >= MAX_DEPTH {
            return Err(Fallback);
        }
        let plan = &self.messages[message];
        let start = scratch.len();
        while !bytes.is_empty() {
            let (field, rest) = try_parse_field(bytes).map_err(|_| Fallback)?;
            bytes = rest;
            match plan.lookup(field.field_num as u32) {
                FieldLookup::Field(idx) => {
                    // Under the skip policy a mismatched occurrence is dropped
                    // before grouping, so it can neither win a singular merge
                    // nor trip the merge-deferred fallbacks.
                    if report.active() {
                        let actual = wire_type_byte(&field.value);
                        if plan.accept_masks[idx as usize] & (1 << actual) == 0 {
                            let field_plan = &plan.fields[idx as usize];
                            report.skip(field_plan.number, field_plan.singular_wire_type(), actual);
                            continue;
                        }
                    }
                    scratch.push(Entry {
                        field: idx,
                        value: field.value,
                    })
                }
                FieldLookup::Extension => return Err(Fallback),
                FieldLookup::Unknown => {}
            }
        }
        // Stable: repeated elements keep wire order, the last scalar wins.
        scratch[start..].sort_by_key(|entry| entry.field);

        let mut oneofs_seen = 0u64;
        let mut i = start;
        while i < scratch.len() {
            let field = scratch[i].field;
            let mut end = i + 1;
            while end < scratch.len() && scratch[end].field == field {
                end += 1;
            }
            let field_plan = &plan.fields[field as usize];
            if let Some(oneof) = field_plan.oneof {
                // A second member of the same oneof: the decode keeps only the
                // last one set, which depends on wire order. Rare; defer.
                let bit = 1u64 << oneof;
                if oneofs_seen & bit != 0 {
                    return Err(Fallback);
                }
                oneofs_seen |= bit;
            }
            let run = i..end;
            if field_plan.repeated {
                self.emit_repeated(field_plan, run, depth, scratch, object, report)?;
            } else {
                self.emit_singular(field_plan, run, depth, scratch, object, report)?;
            }
            i = end;
        }
        scratch.truncate(start);
        Ok(())
    }

    fn emit_singular<'a, S: BuilderSpecificState>(
        &self,
        field_plan: &FieldPlan,
        run: std::ops::Range<usize>,
        depth: usize,
        scratch: &mut Vec<Entry<'a>>,
        object: &mut ObjectBuilder<'_, S>,
        report: &mut WalkReport<'_, '_>,
    ) -> Walk {
        let (first, end) = (run.start, run.end);
        match &field_plan.field_type {
            FieldType::Message(sub) => {
                // Repeated occurrences of a singular message merge; defer.
                if end - first != 1 {
                    return Err(Fallback);
                }
                let WireValue::Len(bytes) = scratch[first].value else {
                    return Err(Fallback);
                };
                // Extend the skip-attribution path for the nested walk; popped
                // before the result propagates so a failed walk leaves the
                // path balanced for the caller.
                report.enter(field_plan.number);
                let mut child = object.new_object(&field_plan.name);
                let walked = self.emit_message(*sub, bytes, depth + 1, scratch, &mut child, report);
                report.leave();
                walked?;
                child.finish();
            }
            FieldType::Scalar(scalar) => {
                let mut last = None;
                for entry in &scratch[first..end] {
                    last = Some(scalar_value(*scalar, &entry.value)?);
                }
                let value = last.expect("non-empty field run");
                if field_plan.has_presence || !value.is_default() {
                    value.insert_into(object, &field_plan.name);
                }
            }
            FieldType::Enum {
                names,
                default_number,
            } => {
                let WireValue::Varint(raw) = scratch[end - 1].value else {
                    return Err(Fallback);
                };
                for entry in &scratch[first..end] {
                    if !matches!(entry.value, WireValue::Varint(_)) {
                        return Err(Fallback);
                    }
                }
                let number = raw as i32;
                if field_plan.has_presence || number != *default_number {
                    insert_enum(object, &field_plan.name, names, number);
                }
            }
            FieldType::Fallback => return Err(Fallback),
        }
        Ok(())
    }

    fn emit_repeated<'a, S: BuilderSpecificState>(
        &self,
        field_plan: &FieldPlan,
        run: std::ops::Range<usize>,
        depth: usize,
        scratch: &mut Vec<Entry<'a>>,
        object: &mut ObjectBuilder<'_, S>,
        report: &mut WalkReport<'_, '_>,
    ) -> Walk {
        let (first, end) = (run.start, run.end);
        match &field_plan.field_type {
            FieldType::Message(sub) => {
                let mut list = object.new_list(&field_plan.name);
                for k in first..end {
                    let WireValue::Len(bytes) = scratch[k].value else {
                        return Err(Fallback);
                    };
                    report.enter(field_plan.number);
                    let mut child = list.new_object();
                    let walked =
                        self.emit_message(*sub, bytes, depth + 1, scratch, &mut child, report);
                    report.leave();
                    walked?;
                    child.finish();
                }
                list.finish();
            }
            FieldType::Scalar(scalar) => {
                let packable = scalar.packed_width();
                // An empty packed run decodes to an empty list, which the
                // reference omits like any default-valued field.
                let empty = packable.is_some()
                    && scratch[first..end]
                        .iter()
                        .all(|e| matches!(e.value, WireValue::Len(b) if b.is_empty()));
                if empty {
                    return Ok(());
                }
                let mut list = object.new_list(&field_plan.name);
                for entry in &scratch[first..end] {
                    match (packable, entry.value) {
                        (Some(width), WireValue::Len(packed)) => {
                            for_each_packed(width, packed, |wire| {
                                scalar_value(*scalar, &wire)?.append_to(&mut list);
                                Ok(())
                            })?;
                        }
                        (_, wire) => scalar_value(*scalar, &wire)?.append_to(&mut list),
                    }
                }
                list.finish();
            }
            FieldType::Enum { names, .. } => {
                let empty = scratch[first..end]
                    .iter()
                    .all(|e| matches!(e.value, WireValue::Len(b) if b.is_empty()));
                if empty {
                    return Ok(());
                }
                let mut list = object.new_list(&field_plan.name);
                for entry in &scratch[first..end] {
                    match entry.value {
                        WireValue::Len(packed) => {
                            for_each_packed(PackedWidth::Varint, packed, |wire| {
                                let WireValue::Varint(raw) = wire else {
                                    return Err(Fallback);
                                };
                                append_enum(&mut list, names, raw as i32);
                                Ok(())
                            })?;
                        }
                        WireValue::Varint(raw) => append_enum(&mut list, names, raw as i32),
                        _ => return Err(Fallback),
                    }
                }
                list.finish();
            }
            FieldType::Fallback => return Err(Fallback),
        }
        Ok(())
    }
}

fn sanitize_message(
    desc: &MessageDescriptor,
    mut bytes: &[u8],
    report: &mut WalkReport<'_, '_>,
    depth: usize,
) -> Option<Vec<u8>> {
    if depth >= MAX_DEPTH {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len());
    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes).ok()?;
        let raw = &bytes[..bytes.len() - rest.len()];
        bytes = rest;
        let number = field.field_num as u32;
        let Some(field_desc) = desc.get_field(number) else {
            // Unknown fields are kept verbatim; the reference decoder
            // reaches the same verdict on them it would have reached
            // without sanitization.
            out.extend_from_slice(raw);
            continue;
        };
        let (accept_mask, expected) = descriptor_accept_mask(&field_desc);
        let actual = wire_type_byte(&field.value);
        if accept_mask & (1 << actual) == 0 {
            report.skip(number, expected, actual);
            continue;
        }
        if let (Some(sub_desc), WireValue::Len(body)) =
            (field_desc.kind().as_message(), &field.value)
        {
            report.enter(number);
            let sanitized = sanitize_message(&sub_desc.clone(), body, report, depth + 1);
            report.leave();
            let sanitized = sanitized?;
            // A strip always shortens the payload, and a no-strip pass
            // copies bytes verbatim, so equal length means unmodified.
            if sanitized.len() == body.len() {
                out.extend_from_slice(raw);
            } else {
                encode_key(number, WireType::LengthDelimited, &mut out);
                encode_varint(sanitized.len() as u64, &mut out);
                out.extend_from_slice(&sanitized);
            }
        } else {
            out.extend_from_slice(raw);
        }
    }
    Some(out)
}

/// Strips every wire-type-mismatched occurrence of a known field from a whole
/// row, recursively and without reporting. `None` means the bytes are malformed
/// (parse error, group, or past the recursion limit); callers then keep the
/// original bytes so the reference decoder reports the malformation.
pub(super) fn strip_wire_type_mismatches(
    desc: &MessageDescriptor,
    bytes: &[u8],
) -> Option<Vec<u8>> {
    sanitize_message(desc, bytes, &mut WalkReport(&mut None), 0)
}

fn for_each_packed<'a>(
    width: PackedWidth,
    mut packed: &'a [u8],
    mut f: impl FnMut(WireValue<'a>) -> Walk,
) -> Walk {
    while !packed.is_empty() {
        let wire = match width {
            PackedWidth::Varint => {
                let (v, rest) = try_read_varint(packed).map_err(|_| Fallback)?;
                packed = rest;
                WireValue::Varint(v)
            }
            PackedWidth::Fixed4 => {
                let (chunk, rest) = packed.split_first_chunk::<4>().ok_or(Fallback)?;
                packed = rest;
                WireValue::I32(u32::from_le_bytes(*chunk))
            }
            PackedWidth::Fixed8 => {
                let (chunk, rest) = packed.split_first_chunk::<8>().ok_or(Fallback)?;
                packed = rest;
                WireValue::I64(u64::from_le_bytes(*chunk))
            }
        };
        f(wire)?;
    }
    Ok(())
}

/// One decoded scalar in the shape the reference's JSON step gives it.
enum ScalarValue<'a> {
    Int(i64),
    /// A `uint64`/`fixed64` above `i64::MAX`: JSON keeps it unsigned and
    /// `append_json` then stores it as a double.
    LargeUnsigned(u64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Str(&'a str),
    Bytes(&'a [u8]),
}

fn scalar_value<'a>(scalar: ScalarType, wire: &WireValue<'a>) -> Walk<ScalarValue<'a>> {
    use ScalarType as T;
    Ok(match (scalar, *wire) {
        (T::Int32, WireValue::Varint(v)) => ScalarValue::Int(v as i32 as i64),
        (T::Int64, WireValue::Varint(v)) => ScalarValue::Int(v as i64),
        (T::Uint32, WireValue::Varint(v)) => ScalarValue::Int(v as u32 as i64),
        (T::Uint64, WireValue::Varint(v)) | (T::Fixed64, WireValue::I64(v)) => {
            match i64::try_from(v) {
                Ok(i) => ScalarValue::Int(i),
                Err(_) => ScalarValue::LargeUnsigned(v),
            }
        }
        (T::Sint32, WireValue::Varint(v)) => ScalarValue::Int(decode_zigzag32(v as u32) as i64),
        (T::Sint64, WireValue::Varint(v)) => ScalarValue::Int(decode_zigzag64(v)),
        (T::Fixed32, WireValue::I32(v)) => ScalarValue::Int(v as i64),
        (T::Sfixed32, WireValue::I32(v)) => ScalarValue::Int(v as i32 as i64),
        (T::Sfixed64, WireValue::I64(v)) => ScalarValue::Int(v as i64),
        (T::Float, WireValue::I32(v)) => ScalarValue::Float32(f32::from_bits(v)),
        (T::Double, WireValue::I64(v)) => ScalarValue::Float64(f64::from_bits(v)),
        (T::Bool, WireValue::Varint(v)) => ScalarValue::Bool(v != 0),
        // Invalid UTF-8 fails the reference decode; let it report the error.
        (T::String, WireValue::Len(b)) => {
            ScalarValue::Str(std::str::from_utf8(b).map_err(|_| Fallback)?)
        }
        (T::Bytes, WireValue::Len(b)) => ScalarValue::Bytes(b),
        _ => return Err(Fallback),
    })
}

impl ScalarValue<'_> {
    /// Proto3 implicit-presence default (the reference skips these).
    fn is_default(&self) -> bool {
        match self {
            Self::Int(i) => *i == 0,
            Self::LargeUnsigned(_) => false,
            Self::Float32(f) => *f == 0.0,
            Self::Float64(f) => *f == 0.0,
            Self::Bool(b) => !*b,
            Self::Str(s) => s.is_empty(),
            Self::Bytes(b) => b.is_empty(),
        }
    }

    fn insert_into<S: BuilderSpecificState>(&self, object: &mut ObjectBuilder<'_, S>, key: &str) {
        match self {
            Self::Bytes(b) => object.insert(
                key,
                base64::engine::general_purpose::STANDARD.encode(b).as_str(),
            ),
            other => object.insert(key, other.variant()),
        }
    }

    fn append_to<S: BuilderSpecificState>(&self, list: &mut ListBuilder<'_, S>) {
        match self {
            Self::Bytes(b) => {
                list.append_value(base64::engine::general_purpose::STANDARD.encode(b).as_str())
            }
            other => list.append_value(other.variant()),
        }
    }

    fn variant(&self) -> Variant<'static, '_> {
        match self {
            Self::Int(i) => narrowest_int(*i),
            Self::LargeUnsigned(u) => Variant::Double(*u as f64),
            Self::Float32(f) => float_variant(*f as f64),
            Self::Float64(f) => float_variant(*f),
            Self::Bool(b) => Variant::from(*b),
            Self::Str(s) => Variant::from(*s),
            Self::Bytes(_) => unreachable!("bytes are base64-encoded by the caller"),
        }
    }
}

/// `append_json` stores a JSON integer in the narrowest Variant int that holds it.
fn narrowest_int(i: i64) -> Variant<'static, 'static> {
    if let Ok(v) = i8::try_from(i) {
        Variant::Int8(v)
    } else if let Ok(v) = i16::try_from(i) {
        Variant::Int16(v)
    } else if let Ok(v) = i32::try_from(i) {
        Variant::Int32(v)
    } else {
        Variant::Int64(i)
    }
}

/// The reference serializes non-finite floats as the JSON strings protobuf's
/// JSON mapping defines.
fn float_variant(f: f64) -> Variant<'static, 'static> {
    if f.is_finite() {
        Variant::Double(f)
    } else if f.is_nan() {
        Variant::from("NaN")
    } else if f > 0.0 {
        Variant::from("Infinity")
    } else {
        Variant::from("-Infinity")
    }
}

fn insert_enum<S: BuilderSpecificState>(
    object: &mut ObjectBuilder<'_, S>,
    key: &str,
    names: &HashMap<i32, Box<str>>,
    number: i32,
) {
    match names.get(&number) {
        Some(name) => object.insert(key, &**name),
        None => object.insert(key, narrowest_int(number as i64)),
    }
}

fn append_enum<S: BuilderSpecificState>(
    list: &mut ListBuilder<'_, S>,
    names: &HashMap<i32, Box<str>>,
    number: i32,
) {
    match names.get(&number) {
        Some(name) => list.append_value(&**name),
        None => list.append_value(narrowest_int(number as i64)),
    }
}

#[cfg(test)]
#[path = "variant_message_tests.rs"]
mod tests;
