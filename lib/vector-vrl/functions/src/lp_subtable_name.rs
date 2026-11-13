//! The `lp_subtable_name` VRL function: the sub-table route of a Lumberjack Prime union row.
//!
//! The classic pipeline routes each record to a sub-table with `SubTableNameExtractor` (Universe,
//! `log-sync/structured-logs/common/src/regional/etl/SubTableNameExtractor.scala`): the populated
//! member of a message's `oneof` gives the route. A named member routes by its
//! (digit-normalized) field name. A `generic_*` member is a `google.protobuf.Any`, and it routes by
//! the last segment of its type URL, looked up in the catalog's proto-type map. Unset oneofs and
//! unknown Any types route to null, so the row stays in the union but is not routed to any
//! sub-table.
//!
//! The walk reads the record's wire bytes; nothing is decoded. The catalog supplies the maps, keyed
//! by proto field number, so the function holds no catalog knowledge itself:
//!
//! - `container`: the field number of the message that holds the oneof, or 0 when the record is
//!   that message. The walk takes the last such field of the record.
//! - `routes`: each named member's field number (as a string key) to its route name.
//! - `generic_field`: the field number of the Any member, or 0 when there is none.
//! - `generic_routes`: the Any type's short name to its route name.
//!
//! A malformed or truncated record is an error, so the row is dropped (the caller fails closed).
//! Opt-in by construction: the function is only called from a VRL program that names it.

use std::collections::BTreeMap;

use crate::lp_wire::{MAX_FIELD_NUMBER, WireField, fields};
use vrl::prelude::*;

/// The Any message's `type_url` field number.
const ANY_TYPE_URL_FIELD: u64 = 1;

#[derive(Clone, Copy, Debug)]
pub struct LpSubtableName;

impl Function for LpSubtableName {
    fn identifier(&self) -> &'static str {
        "lp_subtable_name"
    }

    fn summary(&self) -> &'static str {
        "the sub-table route of a Lumberjack Prime union row, from the populated oneof member"
    }

    fn usage(&self) -> &'static str {
        indoc! {"
            Returns the route of the populated member of the oneof in `message` (the record's wire
            bytes), or null when no member is set or the member has no route. `container` is the
            field number of the message that holds the oneof (0 when `message` is that message).
            `routes` maps each named member's field number to its route; `generic_field` is the
            number of the Any member (0 when none), and `generic_routes` maps the Any type's short
            name to its route.

            Fallible (call as `lp_subtable_name!(...)`): errors on a malformed or truncated record.
        "}
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "message",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "container",
                kind: kind::INTEGER,
                required: true,
            },
            Parameter {
                keyword: "routes",
                kind: kind::OBJECT,
                required: true,
            },
            Parameter {
                keyword: "generic_field",
                kind: kind::INTEGER,
                required: true,
            },
            Parameter {
                keyword: "generic_routes",
                kind: kind::OBJECT,
                required: true,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }

    fn compile(
        &self,
        state: &state::TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let message = arguments.required("message");
        let container = field_number(
            "container",
            arguments.required_literal("container", state)?,
            true,
        )?;
        let routes = field_routes(arguments.required_literal("routes", state)?)?;
        let generic_field = field_number(
            "generic_field",
            arguments.required_literal("generic_field", state)?,
            true,
        )?;
        let generic_routes = type_routes(arguments.required_literal("generic_routes", state)?)?;
        Ok(LpSubtableNameFn {
            message,
            container,
            routes,
            generic_field,
            generic_routes,
        }
        .as_expr())
    }
}

fn compile_error(message: String) -> Box<dyn DiagnosticMessage> {
    Box::new(ExpressionError::Error {
        message: format!("lp_subtable_name: {message}"),
        labels: vec![],
        notes: vec![],
    })
}

/// A literal field number. Zero is allowed only when `allow_zero` (the "no container" and "no
/// Any member" cases).
fn field_number(
    name: &str,
    value: Value,
    allow_zero: bool,
) -> Result<u64, Box<dyn DiagnosticMessage>> {
    let Value::Integer(number) = value else {
        return Err(compile_error(format!("{name} must be an integer literal")));
    };
    let number = u64::try_from(number)
        .ok()
        .filter(|number| *number <= MAX_FIELD_NUMBER)
        .ok_or_else(|| compile_error(format!("{name} is not a valid field number")))?;
    if number == 0 && !allow_zero {
        return Err(compile_error(format!("{name} must be a field number")));
    }
    Ok(number)
}

/// The `routes` literal: field number (a string key) to route name.
fn field_routes(value: Value) -> Result<BTreeMap<u64, String>, Box<dyn DiagnosticMessage>> {
    let Value::Object(map) = value else {
        return Err(compile_error("routes must be an object literal".into()));
    };
    map.into_iter()
        .map(|(key, route)| {
            let number = key
                .as_str()
                .parse::<u64>()
                .ok()
                .filter(|number| (1..=MAX_FIELD_NUMBER).contains(number))
                .ok_or_else(|| {
                    compile_error(format!("routes key {} is not a field number", key.as_str()))
                })?;
            Ok((number, route_name(route)?))
        })
        .collect()
}

/// The `generic_routes` literal: Any type short name to route name.
fn type_routes(value: Value) -> Result<BTreeMap<String, String>, Box<dyn DiagnosticMessage>> {
    let Value::Object(map) = value else {
        return Err(compile_error(
            "generic_routes must be an object literal".into(),
        ));
    };
    map.into_iter()
        .map(|(name, route)| Ok((name.as_str().to_owned(), route_name(route)?)))
        .collect()
}

fn route_name(value: Value) -> Result<String, Box<dyn DiagnosticMessage>> {
    value
        .try_bytes_utf8_lossy()
        .map(|route| route.into_owned())
        .map_err(|_| compile_error("a route must be a string".into()))
}

#[derive(Clone, Debug)]
struct LpSubtableNameFn {
    message: Box<dyn Expression>,
    container: u64,
    routes: BTreeMap<u64, String>,
    generic_field: u64,
    generic_routes: BTreeMap<String, String>,
}

impl FunctionExpression for LpSubtableNameFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let message = self.message.resolve(ctx)?;
        let bytes = message.try_bytes()?;
        let route = subtable_route(
            &bytes,
            self.container,
            &self.routes,
            self.generic_field,
            &self.generic_routes,
        )
        .map_err(|e| format!("lp_subtable_name: {e}"))?;
        Ok(match route {
            Some(route) => Value::Bytes(route.into()),
            None => Value::Null,
        })
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::bytes().or_null().fallible()
    }
}

/// The route of the populated oneof member of the message in `buf` (or of its `container` field),
/// or None. Mirrors `SubTableNameExtractor.extractOneof`: an unset oneof routes to nothing, a named
/// member routes by its precomputed route, and an Any member routes by its type URL's last segment
/// through `generic_routes`. When several members are on the wire, the last one wins, as in a
/// oneof decode. A member that is not length-delimited is an error.
fn subtable_route(
    buf: &[u8],
    container: u64,
    routes: &BTreeMap<u64, String>,
    generic_field: u64,
    generic_routes: &BTreeMap<String, String>,
) -> Result<Option<String>, String> {
    let scope = if container == 0 {
        buf.to_vec()
    } else {
        match last_field(&fields(buf)?, container) {
            Some(field) => message_value(field)?.to_vec(),
            None => return Ok(None),
        }
    };
    let mut member = None;
    for field in fields(&scope)? {
        let is_member = routes.contains_key(&field.number)
            || (generic_field != 0 && field.number == generic_field);
        if is_member {
            member = Some(field);
        }
    }
    let Some(member) = member else {
        return Ok(None);
    };
    if member.number != generic_field {
        message_value(&member)?;
        return Ok(routes.get(&member.number).cloned());
    }
    let type_url = match last_field(&fields(message_value(&member)?)?, ANY_TYPE_URL_FIELD) {
        Some(field) => {
            std::str::from_utf8(field.value).map_err(|_| "Any type_url is not UTF-8".to_string())?
        }
        None => return Ok(None),
    };
    let short_name = type_url.rsplit('.').next().unwrap_or(type_url);
    Ok(generic_routes.get(short_name).cloned())
}

/// The last field numbered `number` in wire order.
fn last_field<'a, 'b>(found: &'b [WireField<'a>], number: u64) -> Option<&'b WireField<'a>> {
    found.iter().rev().find(|field| field.number == number)
}

/// The payload of a message-typed field, which must be length-delimited.
fn message_value<'a>(field: &WireField<'a>) -> Result<&'a [u8], String> {
    if field.wire_type == 2 {
        Ok(field.value)
    } else {
        Err(format!(
            "field {} is not a message (wire type {})",
            field.number, field.wire_type
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lp_wire::test_fields::{bytes_field, varint_field};

    const ANY: u64 = 3;

    fn routes() -> BTreeMap<u64, String> {
        BTreeMap::from([
            (1, "model_serving_proxy".to_owned()),
            (2, "cloud_2sub".to_owned()),
            (5, "armeria_reference".to_owned()),
        ])
    }

    fn generic_routes() -> BTreeMap<String, String> {
        BTreeMap::from([(
            "OpenSearchDpProxyActivityLog".to_owned(),
            "opensearch_dp_proxy".to_owned(),
        )])
    }

    fn any(type_url: &str) -> Vec<u8> {
        bytes_field(ANY_TYPE_URL_FIELD, type_url.as_bytes())
    }

    fn route(buf: &[u8]) -> Result<Option<String>, String> {
        subtable_route(buf, 0, &routes(), ANY, &generic_routes())
    }

    #[test]
    fn named_member_routes_by_its_field_number() {
        assert_eq!(
            route(&bytes_field(1, b"")).unwrap(),
            Some("model_serving_proxy".to_owned())
        );
    }

    #[test]
    fn set_but_empty_member_still_routes() {
        // The extractor keys on the oneof's presence, not on the sub-message's content.
        assert_eq!(
            route(&bytes_field(2, &[])).unwrap(),
            Some("cloud_2sub".to_owned())
        );
    }

    #[test]
    fn last_member_on_the_wire_wins() {
        let mut buf = bytes_field(1, b"");
        buf.extend(bytes_field(2, b""));
        assert_eq!(route(&buf).unwrap(), Some("cloud_2sub".to_owned()));
    }

    #[test]
    fn no_member_set_routes_to_nothing() {
        assert_eq!(route(&varint_field(9, 1)).unwrap(), None);
    }

    #[test]
    fn empty_record_routes_to_nothing() {
        assert_eq!(route(&[]).unwrap(), None);
    }

    #[test]
    fn unknown_member_routes_to_nothing() {
        // Field 4 is a field of the message but not a routed member.
        assert_eq!(route(&bytes_field(4, b"x")).unwrap(), None);
    }

    #[test]
    fn member_without_a_route_entry_routes_to_nothing() {
        // The member is in the message but not in `routes` (no catalog route for it).
        assert_eq!(route(&bytes_field(7, b"x")).unwrap(), None);
    }

    #[test]
    fn generic_member_routes_by_type_url_short_name() {
        let url = "type.googleapis.com/logging.opensearchdpproxy.OpenSearchDpProxyActivityLog";
        let buf = bytes_field(ANY, &any(url));
        assert_eq!(route(&buf).unwrap(), Some("opensearch_dp_proxy".to_owned()));
    }

    #[test]
    fn generic_member_with_unknown_type_routes_to_nothing() {
        let buf = bytes_field(ANY, &any("type.googleapis.com/logging.Unknown"));
        assert_eq!(route(&buf).unwrap(), None);
    }

    #[test]
    fn generic_member_without_type_url_routes_to_nothing() {
        assert_eq!(route(&bytes_field(ANY, &[])).unwrap(), None);
    }

    #[test]
    fn generic_member_with_non_utf8_type_url_is_an_error() {
        let buf = bytes_field(ANY, &bytes_field(ANY_TYPE_URL_FIELD, &[0xff, 0xfe]));
        assert!(route(&buf).is_err());
    }

    #[test]
    fn container_path_reads_the_member_from_the_nested_message() {
        let inner = bytes_field(1, b"");
        let buf = bytes_field(8, &inner);
        assert_eq!(
            subtable_route(&buf, 8, &routes(), ANY, &generic_routes()).unwrap(),
            Some("model_serving_proxy".to_owned())
        );
    }

    #[test]
    fn container_path_uses_the_last_container_field() {
        let mut buf = bytes_field(8, &bytes_field(1, b""));
        buf.extend(bytes_field(8, &bytes_field(2, b"")));
        assert_eq!(
            subtable_route(&buf, 8, &routes(), ANY, &generic_routes()).unwrap(),
            Some("cloud_2sub".to_owned())
        );
    }

    #[test]
    fn missing_container_routes_to_nothing() {
        let buf = bytes_field(1, b"");
        assert_eq!(
            subtable_route(&buf, 8, &routes(), ANY, &generic_routes()).unwrap(),
            None
        );
    }

    #[test]
    fn container_that_is_not_a_message_is_an_error() {
        // Field 8 carried as a varint, not a message.
        let buf = varint_field(8, 1);
        assert!(subtable_route(&buf, 8, &routes(), ANY, &generic_routes()).is_err());
    }

    #[test]
    fn member_that_is_not_a_message_is_an_error() {
        // Field 1 is a routed member and must be length-delimited.
        assert!(route(&varint_field(1, 7)).is_err());
    }

    #[test]
    fn malformed_varint_is_an_error() {
        assert!(route(&[0xff; 11]).is_err());
        assert!(route(&[0x80]).is_err());
    }

    #[test]
    fn truncated_member_is_an_error() {
        // Member 1 declares 9 bytes and carries 1.
        assert!(route(&[(1 << 3) | 2, 9, 0]).is_err());
    }

    #[test]
    fn truncated_container_is_an_error() {
        // The container declares 20 bytes and carries 2.
        let buf = [(8 << 3) | 2, 20, 1, 2];
        assert!(subtable_route(&buf, 8, &routes(), ANY, &generic_routes()).is_err());
    }

    #[test]
    fn field_number_zero_in_the_record_is_an_error() {
        assert!(route(&[0x00, 0x00]).is_err());
    }

    #[test]
    fn group_wire_type_is_an_error() {
        assert!(route(&[(1 << 3) | 3]).is_err());
    }
}
