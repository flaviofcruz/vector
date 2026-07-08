//! ClickHouse type parsing and conversion to Arrow types.

use arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

const DECIMAL32_PRECISION: u8 = 9;
const DECIMAL64_PRECISION: u8 = 18;
const DECIMAL128_PRECISION: u8 = 38;
const DECIMAL256_PRECISION: u8 = 76;

/// Represents a ClickHouse type with its modifiers and nested structure.
#[derive(Debug, PartialEq, Clone)]
pub enum ClickHouseType<'a> {
    /// A primitive type like String, Int64, DateTime, etc.
    Primitive(&'a str),
    /// Nullable(T)
    Nullable(Box<ClickHouseType<'a>>),
    /// LowCardinality(T)
    LowCardinality(Box<ClickHouseType<'a>>),
}

impl<'a> ClickHouseType<'a> {
    /// Returns true if this type or any of its nested types is Nullable.
    pub fn is_nullable(&self) -> bool {
        match self {
            ClickHouseType::Nullable(_) => true,
            ClickHouseType::LowCardinality(inner) => inner.is_nullable(),
            _ => false,
        }
    }

    /// Returns the innermost base type, unwrapping all modifiers.
    /// For example: LowCardinality(Nullable(String)) -> Primitive("String")
    pub fn base_type(&self) -> &ClickHouseType<'a> {
        match self {
            ClickHouseType::Nullable(inner) | ClickHouseType::LowCardinality(inner) => {
                inner.base_type()
            }
            _ => self,
        }
    }
}

/// Parses a ClickHouse type string into a structured representation.
pub fn parse_ch_type(ty: &str) -> ClickHouseType<'_> {
    let ty = ty.trim();

    // Recursively strip and parse type modifiers
    if let Some(inner) = strip_wrapper(ty, "Nullable") {
        return ClickHouseType::Nullable(Box::new(parse_ch_type(inner)));
    }
    if let Some(inner) = strip_wrapper(ty, "LowCardinality") {
        return ClickHouseType::LowCardinality(Box::new(parse_ch_type(inner)));
    }

    // Base case: return primitive type for anything without modifiers
    ClickHouseType::Primitive(ty)
}

/// Helper function to strip a wrapper from a type string.
/// Returns the inner content if the type matches the wrapper pattern.
fn strip_wrapper<'a>(ty: &'a str, wrapper_name: &str) -> Option<&'a str> {
    ty.strip_prefix(wrapper_name)?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')
}

/// Unwraps ClickHouse type modifiers like Nullable() and LowCardinality().
/// Returns a tuple of (base_type, is_nullable).
/// For example: "LowCardinality(Nullable(String))" -> ("String", true)
pub fn unwrap_type_modifiers(ch_type: &str) -> (&str, bool) {
    let parsed = parse_ch_type(ch_type);
    let is_nullable = parsed.is_nullable();

    match parsed.base_type() {
        ClickHouseType::Primitive(base) => (base, is_nullable),
        _ => (ch_type, is_nullable),
    }
}

/// Converts a ClickHouse type string to an Arrow DataType.
/// Returns a tuple of (DataType, is_nullable).
pub fn clickhouse_type_to_arrow(ch_type: &str) -> Result<(DataType, bool), String> {
    let is_low_cardinality = matches!(parse_ch_type(ch_type), ClickHouseType::LowCardinality(_));
    let (base_type, is_nullable) = unwrap_type_modifiers(ch_type);
    let (type_name, _) = extract_identifier(base_type);

    // LowCardinality(String) -> Arrow dictionary so ClickHouse ingests it directly without
    // rebuilding; other LowCardinality inner types fall through to the inner mapping.
    if is_low_cardinality && matches!(type_name, "String" | "FixedString") {
        return Ok((
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            is_nullable,
        ));
    }

    let data_type = match type_name {
        // Numeric
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Bool" => DataType::Boolean,
        "Decimal" | "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => {
            parse_decimal_type(base_type)?
        }

        // Strings
        "String" | "FixedString" => DataType::Utf8,

        // JSON is sent as a string; ClickHouse parses it into the JSON column on
        // insert (Arrow Utf8 -> JSON is converted server-side).
        "JSON" => DataType::Utf8,

        // Date and time types (timezones not currently handled, defaults to UTC)
        "Date" | "Date32" => DataType::Date32,
        "DateTime" => DataType::Timestamp(TimeUnit::Second, None),
        "DateTime64" => parse_datetime64_precision(base_type)?,

        // Arrow List/Struct/Map; ClickHouse converts these back to Array/Tuple/Map on insert.
        "Array" => parse_array_type(base_type)?,
        "Tuple" => parse_tuple_type(base_type)?,
        "Map" => parse_map_type(base_type)?,

        // Unknown
        _ => {
            return Err(format!(
                "Unknown ClickHouse type '{}'. This type cannot be automatically converted.",
                type_name
            ));
        }
    };

    Ok((data_type, is_nullable))
}

/// Extracts an identifier from the start of a string.
/// Returns (identifier, remaining_string).
fn extract_identifier(input: &str) -> (&str, &str) {
    for (i, c) in input.char_indices() {
        if c.is_alphabetic() || c == '_' || (i > 0 && c.is_numeric()) {
            continue;
        }
        return (&input[..i], &input[i..]);
    }
    (input, "")
}

/// Parses comma-separated arguments from a parenthesized string.
/// Input: "(arg1, arg2, arg3)" -> Output: Ok(vec!["arg1".to_string(), "arg2".to_string(), "arg3".to_string()])
/// Returns an error if parentheses are malformed.
fn parse_args(input: &str) -> Result<Vec<String>, String> {
    let trimmed = input.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(format!(
            "Expected parentheses around arguments in '{}'",
            input
        ));
    }

    let inner = trimmed[1..trimmed.len() - 1].trim();
    if inner.is_empty() {
        return Ok(vec![]);
    }

    // Split by comma, handling nested parentheses and quotes
    let mut args = Vec::new();
    let mut current_arg = String::new();
    let mut depth = 0;
    let mut in_quotes = false;

    for c in inner.chars() {
        match c {
            '\'' if !in_quotes => in_quotes = true,
            '\'' if in_quotes => in_quotes = false,
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes => depth -= 1,
            ',' if depth == 0 && !in_quotes => {
                args.push(current_arg.trim().to_string());
                current_arg = String::new();
                continue;
            }
            _ => {}
        }
        current_arg.push(c);
    }

    if !current_arg.trim().is_empty() {
        args.push(current_arg.trim().to_string());
    }

    Ok(args)
}

/// Parses ClickHouse Decimal types and returns the appropriate Arrow decimal type.
/// ClickHouse formats:
/// - Decimal(P, S) -> generic decimal with precision P and scale S
/// - Decimal32(S) -> precision up to 9, scale S
/// - Decimal64(S) -> precision up to 18, scale S
/// - Decimal128(S) -> precision up to 38, scale S
/// - Decimal256(S) -> precision up to 76, scale S
///
/// Uses metadata from ClickHouse's system.columns when available, otherwise falls back to parsing the type string.
fn parse_decimal_type(ch_type: &str) -> Result<DataType, String> {
    // Parse from type string
    let (type_name, args_str) = extract_identifier(ch_type);

    let result = parse_args(args_str).ok().and_then(|args| match type_name {
        "Decimal" if args.len() == 2 => args[0].parse::<u8>().ok().zip(args[1].parse::<i8>().ok()),
        "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" if args.len() == 1 => {
            args[0].parse::<i8>().ok().map(|scale| {
                let precision = match type_name {
                    "Decimal32" => DECIMAL32_PRECISION,
                    "Decimal64" => DECIMAL64_PRECISION,
                    "Decimal128" => DECIMAL128_PRECISION,
                    "Decimal256" => DECIMAL256_PRECISION,
                    _ => unreachable!(),
                };
                (precision, scale)
            })
        }
        _ => None,
    });

    result
        .map(|(precision, scale)| {
            if precision <= DECIMAL128_PRECISION {
                DataType::Decimal128(precision, scale)
            } else {
                DataType::Decimal256(precision, scale)
            }
        })
        .ok_or_else(|| format!("Could not parse Decimal type '{}'.", ch_type))
}

/// Parses DateTime64 precision and returns the appropriate Arrow timestamp type.
/// DateTime64(0) -> Second
/// DateTime64(3) -> Millisecond
/// DateTime64(6) -> Microsecond
/// DateTime64(9) -> Nanosecond
///
fn parse_datetime64_precision(ch_type: &str) -> Result<DataType, String> {
    // Parse from type string
    let (_type_name, args_str) = extract_identifier(ch_type);

    let args = parse_args(args_str).map_err(|e| {
        format!(
            "Could not parse DateTime64 arguments from '{}': {}. Expected format: DateTime64(0-9) or DateTime64(0-9, 'timezone')",
            ch_type, e
        )
    })?;

    // DateTime64(precision) or DateTime64(precision, 'timezone')
    if args.is_empty() {
        return Err(format!(
            "DateTime64 type '{}' has no precision argument. Expected format: DateTime64(0-9) or DateTime64(0-9, 'timezone')",
            ch_type
        ));
    }

    // Parse the precision (first argument)
    match args[0].parse::<u8>() {
        Ok(0) => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        Ok(1..=3) => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        Ok(4..=6) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        Ok(7..=9) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(format!(
            "Unsupported DateTime64 precision in '{}'. Precision must be 0-9",
            ch_type
        )),
    }
}

/// Builds an Arrow child `Field` for a nested inner ClickHouse type via `clickhouse_type_to_arrow`,
/// rejecting element types the codec cannot build in a collection so the failure surfaces at schema
/// resolution (caught by the sink's JSONEachRow fallback) rather than on every batch at encode time.
fn ch_type_to_arrow_field(name: &str, inner_ch_type: &str) -> Result<Field, String> {
    let (data_type, nullable) = clickhouse_type_to_arrow(inner_ch_type)?;
    reject_unsupported_nested_type(inner_ch_type, &data_type)?;
    Ok(Field::new(name, data_type, nullable))
}

/// The collection builders (`build_map_value_array` / `build_list_item_array`) handle a narrower set
/// than a top-level column. This admits only what they build, rejecting the rest (`Dictionary`/
/// LowCardinality, `Int8/16`, `UInt8/16`, `Decimal`, `Date`/`DateTime`). `Struct`/`List`/`Map` pass
/// through, and their own inner fields are validated recursively as they are parsed.
fn reject_unsupported_nested_type(ch_type: &str, data_type: &DataType) -> Result<(), String> {
    let supported = matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::Map(_, _)
    );
    if supported {
        Ok(())
    } else {
        Err(format!(
            "ClickHouse type '{}' (Arrow {:?}) is not supported as a collection element/key/value; \
             only String, Binary, Bool, Int32/64, UInt32/64, Float32/64, and nested \
             Array/Tuple/Map are supported inside a collection.",
            ch_type, data_type
        ))
    }
}

/// `Array(T)` -> Arrow `List<item: T>`. ClickHouse converts the Arrow List back to `Array` on insert.
fn parse_array_type(ch_type: &str) -> Result<DataType, String> {
    let (_, args_str) = extract_identifier(ch_type);
    let args = parse_args(args_str)?;
    if args.len() != 1 {
        return Err(format!(
            "Array type '{}' must have exactly one element type, found {}",
            ch_type,
            args.len()
        ));
    }
    Ok(DataType::List(Arc::new(ch_type_to_arrow_field(
        "item", &args[0],
    )?)))
}

/// `Map(K, V)` -> Arrow `Map<entries: Struct<key: K, value: V>>`. Keys are non-nullable.
fn parse_map_type(ch_type: &str) -> Result<DataType, String> {
    let (_, args_str) = extract_identifier(ch_type);
    let args = parse_args(args_str)?;
    if args.len() != 2 {
        return Err(format!(
            "Map type '{}' must have exactly a key and a value type, found {}",
            ch_type,
            args.len()
        ));
    }
    // Validate the key type against the collection-element set, then force it non-nullable
    // (ClickHouse map keys cannot be null).
    let (key_type, _) = clickhouse_type_to_arrow(&args[0])?;
    reject_unsupported_nested_type(&args[0], &key_type)?;
    let key_field = Field::new("key", key_type, false);
    let value_field = ch_type_to_arrow_field("value", &args[1])?;
    let entries = Field::new(
        "entries",
        DataType::Struct(vec![key_field, value_field].into()),
        false,
    );
    Ok(DataType::Map(Arc::new(entries), false))
}

/// `Tuple(...)` -> Arrow `Struct`. Named elements become named fields; unnamed elements get
/// positional names ("1", "2", ...). Only named elements populate from event data.
fn parse_tuple_type(ch_type: &str) -> Result<DataType, String> {
    let (_, args_str) = extract_identifier(ch_type);
    let args = parse_args(args_str)?;
    if args.is_empty() {
        return Err(format!(
            "Tuple type '{}' must have at least one element",
            ch_type
        ));
    }
    let mut fields = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let (name, elem_type) = split_tuple_element(arg, i);
        fields.push(ch_type_to_arrow_field(&name, &elem_type)?);
    }
    Ok(DataType::Struct(fields.into()))
}

/// Splits one `Tuple` element into (field_name, element_type). A named element is `<name> <Type>`;
/// an unnamed element is `<Type>`, named by its 1-based position.
fn split_tuple_element(arg: &str, index: usize) -> (String, String) {
    let arg = arg.trim();
    if let Some(ws) = arg.find(char::is_whitespace) {
        let (name, rest) = arg.split_at(ws);
        let rest = rest.trim_start();
        if !rest.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return (name.to_string(), rest.to_string());
        }
    }
    ((index + 1).to_string(), arg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function for tests that don't need metadata
    fn convert_type_no_metadata(ch_type: &str) -> Result<(DataType, bool), String> {
        clickhouse_type_to_arrow(ch_type)
    }

    #[test]
    fn test_clickhouse_type_mapping() {
        assert_eq!(
            convert_type_no_metadata("String").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, false)
        );
        assert_eq!(
            convert_type_no_metadata("Int64").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Int64, false)
        );
        assert_eq!(
            convert_type_no_metadata("Float64")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Float64, false)
        );
        assert_eq!(
            convert_type_no_metadata("Bool").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Boolean, false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Second, None), false)
        );
    }

    #[test]
    fn test_datetime64_precision_mapping() {
        assert_eq!(
            convert_type_no_metadata("DateTime64(0)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Second, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(3)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(6)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(9)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
        // Test with timezones
        assert_eq!(
            convert_type_no_metadata("DateTime64(9, 'UTC')")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(6, 'UTC')")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(9, 'America/New_York')")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
        // Test edge cases for precision ranges
        assert_eq!(
            convert_type_no_metadata("DateTime64(1)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(4)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(7)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
    }

    #[test]
    fn test_nullable_type_mapping() {
        // Non-nullable types
        assert_eq!(
            convert_type_no_metadata("String").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, false)
        );
        assert_eq!(
            convert_type_no_metadata("Int64").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Int64, false)
        );

        // Nullable types
        assert_eq!(
            convert_type_no_metadata("Nullable(String)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, true)
        );
        assert_eq!(
            convert_type_no_metadata("Nullable(Int64)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Int64, true)
        );
        assert_eq!(
            convert_type_no_metadata("Nullable(Float64)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Float64, true)
        );
    }

    #[test]
    fn test_lowcardinality_type_mapping() {
        // LowCardinality(String) maps to an Arrow dictionary, not the plain inner Utf8.
        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(
            convert_type_no_metadata("LowCardinality(String)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (dict.clone(), false)
        );
        assert_eq!(
            convert_type_no_metadata("LowCardinality(FixedString(10))")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (dict.clone(), false)
        );
        // Nullable + LowCardinality
        assert_eq!(
            convert_type_no_metadata("LowCardinality(Nullable(String))")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (dict, true)
        );
    }

    #[test]
    fn test_decimal_type_mapping() {
        // Generic Decimal(P, S)
        assert_eq!(
            convert_type_no_metadata("Decimal(10, 2)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(10, 2), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal(38, 6)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(38, 6), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal(50, 10)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal256(50, 10), false)
        );

        // Generic Decimal without spaces and with spaces
        assert_eq!(
            convert_type_no_metadata("Decimal(10,2)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(10, 2), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal( 18 , 6 )")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 6), false)
        );

        // Decimal32(S) - precision up to 9
        assert_eq!(
            convert_type_no_metadata("Decimal32(2)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(9, 2), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal32(4)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(9, 4), false)
        );

        // Decimal64(S) - precision up to 18
        assert_eq!(
            convert_type_no_metadata("Decimal64(4)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 4), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal64(8)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 8), false)
        );

        // Decimal128(S) - precision up to 38
        assert_eq!(
            convert_type_no_metadata("Decimal128(10)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(38, 10), false)
        );

        // Decimal256(S) - precision up to 76
        assert_eq!(
            convert_type_no_metadata("Decimal256(20)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal256(76, 20), false)
        );

        // With Nullable wrapper
        assert_eq!(
            convert_type_no_metadata("Nullable(Decimal(18, 6))")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 6), true)
        );
    }

    #[test]
    fn test_extract_identifier() {
        assert_eq!(extract_identifier("Decimal(10, 2)"), ("Decimal", "(10, 2)"));
        assert_eq!(extract_identifier("DateTime64(3)"), ("DateTime64", "(3)"));
        assert_eq!(extract_identifier("Int32"), ("Int32", ""));
        assert_eq!(
            extract_identifier("LowCardinality(String)"),
            ("LowCardinality", "(String)")
        );
        assert_eq!(extract_identifier("Decimal128(10)"), ("Decimal128", "(10)"));
    }

    #[test]
    fn test_parse_args() {
        // Simple cases
        assert_eq!(
            parse_args("(10, 2)").unwrap(),
            vec!["10".to_string(), "2".to_string()]
        );
        assert_eq!(parse_args("(3)").unwrap(), vec!["3".to_string()]);
        assert_eq!(parse_args("()").unwrap(), Vec::<String>::new());

        // With spaces
        assert_eq!(
            parse_args("( 10 , 2 )").unwrap(),
            vec!["10".to_string(), "2".to_string()]
        );

        // With nested parentheses
        assert_eq!(
            parse_args("(Nullable(String))").unwrap(),
            vec!["Nullable(String)".to_string()]
        );
        assert_eq!(
            parse_args("(Array(Int32), String)").unwrap(),
            vec!["Array(Int32)".to_string(), "String".to_string()]
        );

        // With quotes
        assert_eq!(
            parse_args("(3, 'UTC')").unwrap(),
            vec!["3".to_string(), "'UTC'".to_string()]
        );
        assert_eq!(
            parse_args("(9, 'America/New_York')").unwrap(),
            vec!["9".to_string(), "'America/New_York'".to_string()]
        );

        // Complex nested case
        assert_eq!(
            parse_args("(Tuple(Int32, String), Array(Float64))").unwrap(),
            vec![
                "Tuple(Int32, String)".to_string(),
                "Array(Float64)".to_string()
            ]
        );

        // Error cases
        assert!(parse_args("10, 2").is_err()); // Missing parentheses
        assert!(parse_args("(10, 2").is_err()); // Missing closing paren
    }

    #[test]
    fn test_array_type_maps_to_list() {
        let (dt, nullable) =
            convert_type_no_metadata("Array(Int32)").expect("Array should map to Arrow List");
        assert_eq!(
            dt,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
        );
        assert!(!nullable);
    }

    #[test]
    fn test_tuple_type_maps_to_struct() {
        // Unnamed elements get positional names "1", "2".
        let (dt, _) = convert_type_no_metadata("Tuple(String, Int64)")
            .expect("Tuple should map to Arrow Struct");
        assert_eq!(
            dt,
            DataType::Struct(
                vec![
                    Field::new("1", DataType::Utf8, false),
                    Field::new("2", DataType::Int64, false),
                ]
                .into()
            )
        );
        // Named elements keep their names.
        let (dt_named, _) = convert_type_no_metadata("Tuple(a String, b Int64)")
            .expect("named Tuple should map to Arrow Struct");
        assert_eq!(
            dt_named,
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Utf8, false),
                    Field::new("b", DataType::Int64, false),
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_map_type_maps_to_map() {
        let (dt, nullable) =
            convert_type_no_metadata("Map(String, String)").expect("Map should map to Arrow Map");
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );
        assert_eq!(dt, DataType::Map(Arc::new(entries), false));
        assert!(!nullable);
    }

    #[test]
    fn test_nested_collection_recursion() {
        // Map(String, Array(Int64)) exercises a string key + list value + recursion, all of which
        // the codec's collection builders support.
        let (dt, _) =
            convert_type_no_metadata("Map(String, Array(Int64))").expect("nested Map should map");
        let DataType::Map(entries, _) = dt else {
            panic!("expected Map, got {:?}", dt);
        };
        let DataType::Struct(kv) = entries.data_type() else {
            panic!("expected Struct entries, got {:?}", entries.data_type());
        };
        assert_eq!(kv[0].data_type(), &DataType::Utf8);
        assert_eq!(
            kv[1].data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int64, false)))
        );
    }

    #[test]
    fn test_unsupported_nested_element_types_rejected_at_parse() {
        // Element/key/value types the codec's collection builders cannot build must be rejected at
        // parse (schema resolution), so the sink's JSONEachRow fallback catches them instead of
        // crashing on every batch at encode time.
        for ty in [
            "Array(Int8)",                         // Int8 not a supported list element
            "Array(Decimal(10, 2))",               // Decimal not supported inside a collection
            "Array(DateTime)",                     // Timestamp not supported inside a collection
            "Map(LowCardinality(String), String)", // Dictionary key not supported
            "Map(String, LowCardinality(String))", // Dictionary value not supported
            "Map(Int64, DateTime64(3))",           // DateTime64 value not supported
            "Tuple(a Int16, b String)",            // Int16 tuple element not supported
        ] {
            let result = convert_type_no_metadata(ty);
            assert!(
                result.is_err(),
                "expected '{}' to be rejected as an unsupported collection element, got {:?}",
                ty,
                result
            );
        }
    }

    #[test]
    fn test_supported_nested_element_types_accepted() {
        // The element/key/value types the builders DO support must still parse.
        for ty in [
            "Array(String)",
            "Array(Int64)",
            "Array(Float64)",
            "Array(Bool)",
            "Map(String, String)",
            "Map(Int64, String)",
            "Map(String, Array(String))",
            "Map(String, Map(String, String))",
            "Tuple(a String, b Int64)",
        ] {
            assert!(
                convert_type_no_metadata(ty).is_ok(),
                "expected '{}' to be a supported collection element",
                ty
            );
        }
    }

    #[test]
    fn test_unknown_type_fails() {
        // Unknown types should return an error
        let result = convert_type_no_metadata("UnknownType");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unknown ClickHouse type"));
    }

    #[test]
    fn test_parse_ch_type_primitives() {
        assert_eq!(parse_ch_type("String"), ClickHouseType::Primitive("String"));
        assert_eq!(parse_ch_type("Int64"), ClickHouseType::Primitive("Int64"));
        assert_eq!(
            parse_ch_type("DateTime64(3)"),
            ClickHouseType::Primitive("DateTime64(3)")
        );
    }

    #[test]
    fn test_parse_ch_type_nullable() {
        assert_eq!(
            parse_ch_type("Nullable(String)"),
            ClickHouseType::Nullable(Box::new(ClickHouseType::Primitive("String")))
        );
        assert_eq!(
            parse_ch_type("Nullable(Int64)"),
            ClickHouseType::Nullable(Box::new(ClickHouseType::Primitive("Int64")))
        );
    }

    #[test]
    fn test_parse_ch_type_lowcardinality() {
        assert_eq!(
            parse_ch_type("LowCardinality(String)"),
            ClickHouseType::LowCardinality(Box::new(ClickHouseType::Primitive("String")))
        );
        assert_eq!(
            parse_ch_type("LowCardinality(Nullable(String))"),
            ClickHouseType::LowCardinality(Box::new(ClickHouseType::Nullable(Box::new(
                ClickHouseType::Primitive("String")
            ))))
        );
    }

    #[test]
    fn test_parse_ch_type_is_nullable() {
        assert!(!parse_ch_type("String").is_nullable());
        assert!(parse_ch_type("Nullable(String)").is_nullable());
        assert!(parse_ch_type("LowCardinality(Nullable(String))").is_nullable());
        assert!(!parse_ch_type("LowCardinality(String)").is_nullable());
    }

    #[test]
    fn test_parse_ch_type_base_type() {
        let parsed = parse_ch_type("LowCardinality(Nullable(String))");
        assert_eq!(parsed.base_type(), &ClickHouseType::Primitive("String"));

        let parsed = parse_ch_type("Nullable(Int64)");
        assert_eq!(parsed.base_type(), &ClickHouseType::Primitive("Int64"));

        let parsed = parse_ch_type("String");
        assert_eq!(parsed.base_type(), &ClickHouseType::Primitive("String"));
    }
}
