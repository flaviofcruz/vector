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
//! [`encode_variant_from_message`]: super::append::encode_variant_from_message

use std::collections::HashMap;

use base64::Engine as _;
use parquet_variant::{BuilderSpecificState, ListBuilder, ObjectBuilder, Variant, VariantBuilder};
use prost_reflect::{Cardinality, Kind, MessageDescriptor};
use zeroparser::wire::{
    WireValue, decode_zigzag32, decode_zigzag64, try_parse_field, try_read_varint,
};

use super::append::encode_variant_from_message;
use super::errors::Result;

/// Recursion limit of the reference `DynamicMessage` decode (prost's default);
/// deeper input falls back so the reference decides.
const MAX_DEPTH: usize = 100;

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
    name: Box<str>,
    field_type: FieldType,
    repeated: bool,
    /// Explicit presence: a present default value is still serialized.
    has_presence: bool,
    /// Index of the containing (real or synthetic) oneof, if any.
    oneof: Option<u8>,
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
    index: FieldIndex,
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
}

impl std::fmt::Debug for VariantMessagePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VariantMessagePlan")
            .field("message", &self.descriptor.full_name())
            .field("compiled_messages", &self.messages.len())
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
        };
        if !plan.root_fallback {
            plan.compile(descriptor, &mut HashMap::new());
        }
        plan
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
        self.messages[idx] = MessageVariantPlan { fields, index };
        idx
    }

    /// Encode one occurrence of the column's message (its wire bytes) into the
    /// Parquet Variant `(metadata, value)` pair.
    pub(crate) fn encode(&self, wire: &WireValue) -> Result<(Vec<u8>, Vec<u8>)> {
        match wire {
            WireValue::Len(bytes) => match self.encode_direct(bytes) {
                Some(built) => Ok(built),
                None => encode_variant_from_message(&self.descriptor, wire),
            },
            _ => encode_variant_from_message(&self.descriptor, wire),
        }
    }

    /// The direct walk alone; `None` when the input needs the reference path.
    fn encode_direct(&self, bytes: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.root_fallback {
            return None;
        }
        let mut builder = VariantBuilder::new();
        let mut scratch = Vec::new();
        let mut object = builder.new_object();
        self.emit_message(0, bytes, 0, &mut scratch, &mut object)
            .ok()?;
        object.finish();
        Some(builder.finish())
    }

    fn emit_message<'a, S: BuilderSpecificState>(
        &self,
        message: usize,
        mut bytes: &'a [u8],
        depth: usize,
        scratch: &mut Vec<Entry<'a>>,
        object: &mut ObjectBuilder<'_, S>,
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
                FieldLookup::Field(idx) => scratch.push(Entry {
                    field: idx,
                    value: field.value,
                }),
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
            if field_plan.repeated {
                self.emit_repeated(field_plan, i, end, depth, scratch, object)?;
            } else {
                self.emit_singular(field_plan, i, end, depth, scratch, object)?;
            }
            i = end;
        }
        scratch.truncate(start);
        Ok(())
    }

    fn emit_singular<'a, S: BuilderSpecificState>(
        &self,
        field_plan: &FieldPlan,
        first: usize,
        end: usize,
        depth: usize,
        scratch: &mut Vec<Entry<'a>>,
        object: &mut ObjectBuilder<'_, S>,
    ) -> Walk {
        match &field_plan.field_type {
            FieldType::Message(sub) => {
                // Repeated occurrences of a singular message merge; defer.
                if end - first != 1 {
                    return Err(Fallback);
                }
                let WireValue::Len(bytes) = scratch[first].value else {
                    return Err(Fallback);
                };
                let mut child = object.new_object(&field_plan.name);
                self.emit_message(*sub, bytes, depth + 1, scratch, &mut child)?;
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
        first: usize,
        end: usize,
        depth: usize,
        scratch: &mut Vec<Entry<'a>>,
        object: &mut ObjectBuilder<'_, S>,
    ) -> Walk {
        match &field_plan.field_type {
            FieldType::Message(sub) => {
                let mut list = object.new_list(&field_plan.name);
                for k in first..end {
                    let WireValue::Len(bytes) = scratch[k].value else {
                        return Err(Fallback);
                    };
                    let mut child = list.new_object();
                    self.emit_message(*sub, bytes, depth + 1, scratch, &mut child)?;
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
