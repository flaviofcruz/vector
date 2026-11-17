//! Spark-parity encoding for message-backed VARIANT columns.
//!
//! Classic Lumberjack's variant union stores a payload as
//! `to_variant_object(from_protobuf(bytes, PROTO_DECODE_OPTIONS))`, wrapped in
//! `variant_strip_nulls` when `lumberjack.strip_null_payload_keys` is on (Universe,
//! `log-sync/structured-logs/logfoods-deployments/lumberjack/variant_union_common/notebooks/`
//! `variant_union_pipeline_lib.py`, `decode_to_variant`). `PROTO_DECODE_OPTIONS`
//! (`variant_union_reshape.py`) is `convert.timestamp.duration.to.native=false` plus
//! `emit.default.values=true`. [`SparkVariantEncoder`] builds that value from the wire bytes. Per
//! field, following Spark 4.0's `from_protobuf` (`ProtobufDeserializer`, `SchemaConverters`) and
//! `to_variant_object` (`VariantExpressionEvalUtils.buildVariant`):
//!
//! - Presence: a repeated or map field is always written (`[]` / `{}` when empty). A singular
//!   field is written when present, when it declares an explicit default, or when it has no
//!   presence (proto3 implicit scalars, written as their zero). Any other absent field is a null
//!   key (`ProtobufDeserializer.getFieldValue`).
//! - Integers keep their Java value and take the narrowest Variant int, as
//!   `VariantBuilder.appendLong` does. `uint32`/`fixed32` are Java `int`s and `uint64`/`fixed64`
//!   Java `long`s, so values past the signed range wrap negative.
//! - `float` stays a Variant float and `double` a double, NaN and infinities included.
//! - `bytes` is Variant binary, `string` a string, `bool` a boolean.
//! - Enums are their value names. An unknown number reads as `UNKNOWN_ENUM_VALUE_<Enum>_<n>` in
//!   an open enum field. A closed one (a proto2 field, or a proto2 enum:
//!   `FieldDescriptor.legacyEnumFieldTreatedAsClosed`) is what protobuf-java's
//!   `MessageReflection.mergeFieldFrom` moves an unknown number of into the unknown fields before
//!   merging, so the field keeps its earlier value (or oneof member), a repeated field loses that
//!   element, and a map entry reads its value as unset.
//! - A field whose wire type disagrees with the descriptor is an unknown field to protobuf-java.
//!   Under the `skip_field` mismatch policy the caller strips such occurrences before
//!   [`SparkVariantEncoder::encode`] (`scan.rs`, `encode_variant_slot`); under `drop_row` the
//!   decode rejects them and the row drops.
//! - A missing `required` field anywhere in the message fails the decode, as
//!   `DynamicMessage.parseFrom` (`buildParsed`, `isInitialized`) does, so classic's PERMISSIVE
//!   decode drops the row. Java checks after merging every occurrence of the payload, so
//!   `encode_batch` merges a row's occurrences and encodes again before dropping it.
//! - Messages are objects. Well-known types get no special mapping: `Timestamp` and `Duration`
//!   are `{seconds, nanos}`, `Any` is `{type_url, value}`, wrappers are `{value}`.
//! - Map keys become strings (the union's `variant_safe_type` cast to STRING).
//! - A message type with no fields (directly, or only through fields of such types) is dropped
//!   from the schema (`retain.empty.message.types=false`), so its fields never appear.
//!
//! With `strip_nulls`, null keys are left out at every depth, which is what `variant_strip_nulls`
//! stores.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use parquet_variant::{BuilderSpecificState, ListBuilder, ObjectBuilder, Variant, VariantBuilder};
use prost_reflect::prost::encoding::{WireType, encode_key, encode_varint};
use prost_reflect::{
    Cardinality, DynamicMessage, EnumDescriptor, FieldDescriptor, Kind, MapKey, MessageDescriptor,
    ReflectMessage, Syntax, Value,
};
use zeroparser::wire::try_read_varint;

use super::errors::{Result, WireToArrowError};
use super::variant_message::MAX_DEPTH;

/// Encodes one message-backed VARIANT value with Spark's semantics (see the module docs).
pub(crate) struct SparkVariantEncoder {
    descriptor: MessageDescriptor,
    strip_nulls: bool,
    /// Message types that `from_protobuf` drops because they have no fields left.
    empty_messages: HashSet<String>,
    /// Message types that have a closed-enum field, directly or in a nested message.
    closed_enum_messages: HashSet<String>,
    /// Message types that have a `required` field, directly or in a nested message.
    required_messages: HashSet<String>,
}

impl SparkVariantEncoder {
    pub(crate) fn new(descriptor: &MessageDescriptor, strip_nulls: bool) -> Self {
        let mut empty = HashMap::new();
        collect_empty_messages(descriptor, &mut empty, &mut HashSet::new());
        let reachable = reachable_messages(descriptor);
        Self {
            descriptor: descriptor.clone(),
            strip_nulls,
            empty_messages: empty
                .into_iter()
                .filter_map(|(name, is_empty)| is_empty.then_some(name))
                .collect(),
            closed_enum_messages: messages_reaching(&reachable, |field| {
                closed_enum(field).is_some()
            }),
            required_messages: messages_reaching(&reachable, |field| {
                field.cardinality() == Cardinality::Required
            }),
        }
    }

    /// Encodes one occurrence of the column's message (its wire bytes) into the Parquet Variant
    /// `(metadata, value)` pair.
    pub(crate) fn encode(&self, bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        // Malformed bytes are left as they are for the decode to reject.
        let filtered = self
            .without_unknown_closed_enums(&self.descriptor, bytes, None, 0)
            .ok()
            .and_then(|(filtered, _)| filtered);
        let message = DynamicMessage::decode(
            self.descriptor.clone(),
            filtered.as_deref().unwrap_or(bytes),
        )
        .map_err(|source| WireToArrowError::VariantMessageDecode { source })?;
        if let Some(field) = self.missing_required_field(&message) {
            return Err(WireToArrowError::VariantMessageMissingRequiredField { field });
        }
        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        self.write_message(&message, &mut object);
        object.finish();
        Ok(builder.finish())
    }

    fn write_message<S: BuilderSpecificState>(
        &self,
        message: &DynamicMessage,
        object: &mut ObjectBuilder<'_, S>,
    ) {
        for field in message.descriptor().fields() {
            if self.is_dropped(&field) {
                continue;
            }
            let name = field.name();
            if field.is_map() {
                self.write_map(message, &field, object);
            } else if field.is_list() {
                self.write_list(message, &field, object);
            } else {
                match spark_singular_value(message, &field) {
                    Some(value) => self.insert(object, name, &field.kind(), &value),
                    None if self.strip_nulls => {}
                    None => object.insert(name, Variant::Null),
                }
            }
        }
    }

    fn write_list<S: BuilderSpecificState>(
        &self,
        message: &DynamicMessage,
        field: &FieldDescriptor,
        object: &mut ObjectBuilder<'_, S>,
    ) {
        let kind = field.kind();
        let mut list = object.new_list(field.name());
        if let Value::List(items) = message.get_field(field).as_ref() {
            for item in items {
                self.append(&mut list, &kind, item);
            }
        }
        list.finish();
    }

    fn write_map<S: BuilderSpecificState>(
        &self,
        message: &DynamicMessage,
        field: &FieldDescriptor,
        object: &mut ObjectBuilder<'_, S>,
    ) {
        let value_kind = match field.kind() {
            Kind::Message(entry) => entry.map_entry_value_field().kind(),
            _ => return,
        };
        let mut map_object = object.new_object(field.name());
        if let Value::Map(entries) = message.get_field(field).as_ref() {
            for (key, value) in entries {
                self.insert(&mut map_object, &map_key_string(key), &value_kind, value);
            }
        }
        map_object.finish();
    }

    fn insert<S: BuilderSpecificState>(
        &self,
        object: &mut ObjectBuilder<'_, S>,
        key: &str,
        kind: &Kind,
        value: &Value,
    ) {
        match value {
            Value::Message(child) => {
                let mut child_object = object.new_object(key);
                self.write_message(child, &mut child_object);
                child_object.finish();
            }
            Value::EnumNumber(number) => object.insert(key, enum_name(kind, *number).as_str()),
            scalar => object.insert(key, scalar_variant(scalar)),
        }
    }

    fn append<S: BuilderSpecificState>(
        &self,
        list: &mut ListBuilder<'_, S>,
        kind: &Kind,
        value: &Value,
    ) {
        match value {
            Value::Message(child) => {
                let mut child_object = list.new_object();
                self.write_message(child, &mut child_object);
                child_object.finish();
            }
            Value::EnumNumber(number) => list.append_value(enum_name(kind, *number).as_str()),
            scalar => list.append_value(scalar_variant(scalar)),
        }
    }

    /// Whether `from_protobuf` drops `field` from the schema: a message (or a map whose value is a
    /// message) of a type with no fields left.
    fn is_dropped(&self, field: &FieldDescriptor) -> bool {
        match field.kind() {
            Kind::Message(entry) if field.is_map() => matches!(
                entry.map_entry_value_field().kind(),
                Kind::Message(value) if self.empty_messages.contains(value.full_name())
            ),
            Kind::Message(message) => self.empty_messages.contains(message.full_name()),
            _ => false,
        }
    }

    /// The full name of a `required` field that `message` or a message nested in it lacks, as
    /// `DynamicMessage.isInitialized` finds it.
    fn missing_required_field(&self, message: &DynamicMessage) -> Option<String> {
        let descriptor = message.descriptor();
        if !self.required_messages.contains(descriptor.full_name()) {
            return None;
        }
        if let Some(field) = descriptor
            .fields()
            .find(|field| field.cardinality() == Cardinality::Required && !message.has_field(field))
        {
            return Some(field.full_name().to_owned());
        }
        message.fields().find_map(|(_, value)| match value {
            Value::Message(child) => self.missing_required_field(child),
            Value::List(items) => items.iter().find_map(|item| match item {
                Value::Message(child) => self.missing_required_field(child),
                _ => None,
            }),
            Value::Map(entries) => entries.values().find_map(|value| match value {
                Value::Message(child) => self.missing_required_field(child),
                _ => None,
            }),
            _ => None,
        })
    }

    /// The wire bytes of a `descriptor` message (or, with `group`, of a group body ending at that
    /// field's END_GROUP tag) without the closed-enum values protobuf-java parses into unknown
    /// fields: an occurrence of a closed-enum field, or an element of a packed one, whose number
    /// the enum does not declare. Returns the rewritten bytes (None when nothing is removed) and
    /// the length consumed, a group's END_GROUP tag included.
    fn without_unknown_closed_enums(
        &self,
        descriptor: &MessageDescriptor,
        bytes: &[u8],
        group: Option<u32>,
        depth: usize,
    ) -> std::result::Result<(Option<Vec<u8>>, usize), MalformedWire> {
        if depth >= MAX_DEPTH {
            return Err(MalformedWire);
        }
        let mut output: Option<Vec<u8>> = None;
        let mut position = 0;
        while position < bytes.len() {
            let start = position;
            let (number, wire_type, after_tag) = read_tag(bytes, position)?;
            if wire_type == WireType::EndGroup {
                return match group {
                    Some(group_number) if group_number == number => Ok((output, after_tag)),
                    _ => Err(MalformedWire),
                };
            }
            let field = descriptor.get_field(number);
            let (end, rewrite) = match wire_type {
                WireType::Varint => {
                    let (value, end) = read_varint(bytes, after_tag)?;
                    let drop = field
                        .as_ref()
                        .and_then(closed_enum)
                        .is_some_and(|enum_desc| is_unknown(&enum_desc, value));
                    (end, drop.then(Vec::new))
                }
                WireType::SixtyFourBit => (fixed_end(bytes, after_tag, 8)?, None),
                WireType::ThirtyTwoBit => (fixed_end(bytes, after_tag, 4)?, None),
                WireType::LengthDelimited => {
                    let (body_start, end) = read_len(bytes, after_tag)?;
                    let body = &bytes[body_start..end];
                    let rewrite = match field.as_ref() {
                        Some(field) => match closed_enum(field) {
                            Some(enum_desc) if field.is_list() => {
                                known_packed_enums(&enum_desc, body)?
                            }
                            _ => self.nested_without_unknown_closed_enums(field, body, depth)?,
                        }
                        .map(|body| length_delimited(number, &body)),
                        None => None,
                    };
                    (end, rewrite)
                }
                WireType::StartGroup => {
                    let group_descriptor = field
                        .as_ref()
                        .filter(|field| field.is_group())
                        .and_then(|field| match field.kind() {
                            Kind::Message(message) => Some(message),
                            _ => None,
                        })
                        .filter(|message| self.closed_enum_messages.contains(message.full_name()));
                    let body = &bytes[after_tag..];
                    let (rewritten, consumed) = match &group_descriptor {
                        Some(message) => self.without_unknown_closed_enums(
                            message,
                            body,
                            Some(number),
                            depth + 1,
                        )?,
                        None => (None, skip_group(body, number, depth + 1)?),
                    };
                    let rewrite = rewritten.map(|body| {
                        let mut field_bytes = Vec::with_capacity(body.len() + 8);
                        encode_key(number, WireType::StartGroup, &mut field_bytes);
                        field_bytes.extend_from_slice(&body);
                        encode_key(number, WireType::EndGroup, &mut field_bytes);
                        field_bytes
                    });
                    (after_tag + consumed, rewrite)
                }
                WireType::EndGroup => unreachable!("handled above"),
            };
            match rewrite {
                Some(field_bytes) => output
                    .get_or_insert_with(|| bytes[..start].to_vec())
                    .extend_from_slice(&field_bytes),
                None => {
                    if let Some(output) = &mut output {
                        output.extend_from_slice(&bytes[start..end]);
                    }
                }
            }
            position = end;
        }
        match group {
            // A group body runs out before its END_GROUP tag.
            Some(_) => Err(MalformedWire),
            None => Ok((output, position)),
        }
    }

    /// [`Self::without_unknown_closed_enums`] for the body of a length-delimited message field
    /// (map entries included), when its type has closed-enum fields.
    fn nested_without_unknown_closed_enums(
        &self,
        field: &FieldDescriptor,
        body: &[u8],
        depth: usize,
    ) -> std::result::Result<Option<Vec<u8>>, MalformedWire> {
        match field.kind() {
            Kind::Message(message)
                if !field.is_group() && self.closed_enum_messages.contains(message.full_name()) =>
            {
                Ok(self
                    .without_unknown_closed_enums(&message, body, None, depth + 1)?
                    .0)
            }
            _ => Ok(None),
        }
    }
}

/// Wire bytes the closed-enum filter cannot walk; the decode reports the error.
#[derive(Debug)]
struct MalformedWire;

fn length_delimited(number: u32, body: &[u8]) -> Vec<u8> {
    let mut field_bytes = Vec::with_capacity(body.len() + 8);
    encode_key(number, WireType::LengthDelimited, &mut field_bytes);
    encode_varint(body.len() as u64, &mut field_bytes);
    field_bytes.extend_from_slice(body);
    field_bytes
}

fn read_varint(bytes: &[u8], position: usize) -> std::result::Result<(u64, usize), MalformedWire> {
    let (value, rest) = try_read_varint(&bytes[position..]).map_err(|_| MalformedWire)?;
    Ok((value, bytes.len() - rest.len()))
}

/// The start and end of the body of a length-delimited value whose length is at `position`.
fn read_len(bytes: &[u8], position: usize) -> std::result::Result<(usize, usize), MalformedWire> {
    let (len, body_start) = read_varint(bytes, position)?;
    let end = usize::try_from(len)
        .ok()
        .and_then(|len| body_start.checked_add(len))
        .filter(|&end| end <= bytes.len())
        .ok_or(MalformedWire)?;
    Ok((body_start, end))
}

fn read_tag(
    bytes: &[u8],
    position: usize,
) -> std::result::Result<(u32, WireType, usize), MalformedWire> {
    let (tag, after_tag) = read_varint(bytes, position)?;
    let number = u32::try_from(tag >> 3)
        .ok()
        .filter(|number| (1..=536_870_911).contains(number))
        .ok_or(MalformedWire)?;
    let wire_type = WireType::try_from(tag & 0x7).map_err(|_| MalformedWire)?;
    Ok((number, wire_type, after_tag))
}

fn fixed_end(
    bytes: &[u8],
    position: usize,
    width: usize,
) -> std::result::Result<usize, MalformedWire> {
    Some(position + width)
        .filter(|&end| end <= bytes.len())
        .ok_or(MalformedWire)
}

/// The length of a group body of field `number` up to and including its END_GROUP tag.
fn skip_group(
    bytes: &[u8],
    number: u32,
    depth: usize,
) -> std::result::Result<usize, MalformedWire> {
    if depth >= MAX_DEPTH {
        return Err(MalformedWire);
    }
    let mut position = 0;
    while position < bytes.len() {
        let (field_number, wire_type, after_tag) = read_tag(bytes, position)?;
        position = match wire_type {
            WireType::Varint => read_varint(bytes, after_tag)?.1,
            WireType::SixtyFourBit => fixed_end(bytes, after_tag, 8)?,
            WireType::ThirtyTwoBit => fixed_end(bytes, after_tag, 4)?,
            WireType::LengthDelimited => read_len(bytes, after_tag)?.1,
            WireType::StartGroup => {
                after_tag + skip_group(&bytes[after_tag..], field_number, depth + 1)?
            }
            WireType::EndGroup if field_number == number => return Ok(after_tag),
            WireType::EndGroup => return Err(MalformedWire),
        };
    }
    Err(MalformedWire)
}

/// A packed closed-enum body without its unknown numbers; None when every number is known.
fn known_packed_enums(
    enum_desc: &EnumDescriptor,
    body: &[u8],
) -> std::result::Result<Option<Vec<u8>>, MalformedWire> {
    let mut output: Option<Vec<u8>> = None;
    let mut position = 0;
    while position < body.len() {
        let (value, end) = read_varint(body, position)?;
        if is_unknown(enum_desc, value) {
            output.get_or_insert_with(|| body[..position].to_vec());
        } else if let Some(output) = &mut output {
            output.extend_from_slice(&body[position..end]);
        }
        position = end;
    }
    Ok(output)
}

/// Whether a wire varint, read as protobuf-java's `readEnum` does (its low 32 bits), names no
/// value of `enum_desc`.
fn is_unknown(enum_desc: &EnumDescriptor, value: u64) -> bool {
    enum_desc.get_value(value as i32).is_none()
}

/// Every message type `descriptor` reaches through its fields, itself included.
fn reachable_messages(descriptor: &MessageDescriptor) -> Vec<MessageDescriptor> {
    let mut seen = HashSet::new();
    let mut pending = vec![descriptor.clone()];
    let mut reachable = Vec::new();
    while let Some(message) = pending.pop() {
        if !seen.insert(message.full_name().to_owned()) {
            continue;
        }
        for field in message.fields() {
            if let Kind::Message(child) = field.kind() {
                pending.push(child);
            }
        }
        reachable.push(message);
    }
    reachable
}

/// The names of the `messages` that have a field matching `predicate`, directly or in a nested
/// message.
fn messages_reaching(
    messages: &[MessageDescriptor],
    predicate: impl Fn(&FieldDescriptor) -> bool,
) -> HashSet<String> {
    let mut reaching = HashSet::new();
    loop {
        let before = reaching.len();
        for message in messages {
            if reaching.contains(message.full_name()) {
                continue;
            }
            let reaches = message.fields().any(|field| {
                predicate(&field)
                    || match field.kind() {
                        Kind::Message(child) => reaching.contains(child.full_name()),
                        _ => false,
                    }
            });
            if reaches {
                reaching.insert(message.full_name().to_owned());
            }
        }
        if reaching.len() == before {
            return reaching;
        }
    }
}

/// Records, for `descriptor` and every message type it reaches, whether Spark's schema for it has
/// no fields (so a field of that type is dropped). A type that refers back to itself is kept:
/// `from_protobuf` rejects recursive schemas, so classic never decodes one.
fn collect_empty_messages(
    descriptor: &MessageDescriptor,
    empty: &mut HashMap<String, bool>,
    visiting: &mut HashSet<String>,
) -> bool {
    if let Some(&is_empty) = empty.get(descriptor.full_name()) {
        return is_empty;
    }
    if !visiting.insert(descriptor.full_name().to_owned()) {
        return false;
    }
    let mut is_empty = true;
    for field in descriptor.fields() {
        let dropped = match field.kind() {
            Kind::Message(entry) if field.is_map() => match entry.map_entry_value_field().kind() {
                Kind::Message(value) => collect_empty_messages(&value, empty, visiting),
                _ => false,
            },
            Kind::Message(message) => collect_empty_messages(&message, empty, visiting),
            _ => false,
        };
        is_empty &= dropped;
    }
    visiting.remove(descriptor.full_name());
    empty.insert(descriptor.full_name().to_owned(), is_empty);
    is_empty
}

/// The value `from_protobuf` reads for a singular field, or None for a null
/// (`ProtobufDeserializer.getFieldValue`).
fn spark_singular_value<'a>(
    message: &'a DynamicMessage,
    field: &FieldDescriptor,
) -> Option<Cow<'a, Value>> {
    // Present, or absent with a declared default or no presence (emit.default.values): `get_field`
    // returns the value, the declared default, or the zero.
    (message.has_field(field)
        || field.field_descriptor_proto().default_value.is_some()
        || !field.supports_presence())
    .then(|| message.get_field(field))
}

/// The enum of a field protobuf-java treats as closed (`legacyEnumFieldTreatedAsClosed`): a field
/// declared in a proto2 file (the `legacy_closed_enum` Java feature default there), or of an enum
/// declared in one.
fn closed_enum(field: &FieldDescriptor) -> Option<EnumDescriptor> {
    match field.kind() {
        Kind::Enum(enum_desc)
            if field.parent_file().syntax() == Syntax::Proto2
                || enum_desc.parent_file().syntax() == Syntax::Proto2 =>
        {
            Some(enum_desc)
        }
        _ => None,
    }
}

/// The enum value's name; an unknown number of an open enum reads as protobuf-java's
/// `EnumDescriptor.findValueByNumberCreatingIfUnknown` names it.
fn enum_name(kind: &Kind, number: i32) -> String {
    match kind {
        Kind::Enum(enum_desc) => match enum_desc.get_value(number) {
            Some(value) => value.name().to_owned(),
            None => format!("UNKNOWN_ENUM_VALUE_{}_{number}", enum_desc.name()),
        },
        _ => number.to_string(),
    }
}

/// A scalar as `to_variant_object` writes its Spark value.
fn scalar_variant(value: &Value) -> Variant<'static, '_> {
    match value {
        Value::Bool(b) => Variant::from(*b),
        Value::I32(i) => narrowest_int(i64::from(*i)),
        Value::I64(i) => narrowest_int(*i),
        // Spark reads uint32/fixed32 as a Java int and uint64/fixed64 as a Java long.
        Value::U32(u) => narrowest_int(i64::from(*u as i32)),
        Value::U64(u) => narrowest_int(*u as i64),
        Value::F32(f) => Variant::Float(*f),
        Value::F64(f) => Variant::Double(*f),
        Value::String(s) => Variant::from(s.as_str()),
        Value::Bytes(b) => Variant::Binary(b),
        // Enums, messages, lists and maps are written by the callers.
        Value::EnumNumber(_) | Value::Message(_) | Value::List(_) | Value::Map(_) => Variant::Null,
    }
}

/// `VariantBuilder.appendLong`: the narrowest of int8, int16, int32 and int64 that holds `i`.
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

/// A map key as Spark's `CAST(key AS STRING)` renders it.
fn map_key_string(key: &MapKey) -> String {
    match key {
        MapKey::Bool(b) => b.to_string(),
        MapKey::I32(i) => i.to_string(),
        MapKey::I64(i) => i.to_string(),
        MapKey::U32(u) => (*u as i32).to_string(),
        MapKey::U64(u) => (*u as i64).to_string(),
        MapKey::String(s) => s.clone(),
    }
}

#[cfg(test)]
#[path = "variant_spark_tests.rs"]
mod tests;
