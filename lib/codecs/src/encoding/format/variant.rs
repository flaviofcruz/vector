//! Shared helpers for encoding `VARIANT` columns.
//!
//! A UC `VARIANT` column arrives as an Arrow `Struct<metadata: LargeBinary,
//! value: LargeBinary>` carrying the canonical `arrow.parquet.variant`
//! extension marker (the zerobus SDK stamps it when
//! `ArrowSchemaOptions::annotate_variant_extension` is set). Both batch
//! encoders (`arrow` stream and `wire_to_arrow`) detect the marker and encode
//! the source value into the Parquet Variant binary form (a `metadata` buffer
//! and a `value` buffer) rather than treating the field as an ordinary struct.

use arrow::datatypes::Field;
use arrow::error::ArrowError;
use parquet_variant::VariantBuilder;

/// Arrow extension name marking a Parquet Variant field. Matches the SDK's
/// `VARIANT_EXTENSION_NAME` and Arrow's `VariantType::NAME`.
const VARIANT_EXTENSION_NAME: &str = "arrow.parquet.variant";

/// Arrow's `EXTENSION_TYPE_NAME_KEY` (`arrow_schema::extension`). Spelled out
/// here to avoid depending on the exact re-export path across arrow versions.
const EXTENSION_TYPE_NAME_KEY: &str = "ARROW:extension:name";

/// True iff `field` carries the canonical `arrow.parquet.variant` extension
/// marker, i.e. it is a VARIANT column.
pub(super) fn is_variant_field(field: &Field) -> bool {
    field
        .metadata()
        .get(EXTENSION_TYPE_NAME_KEY)
        .map(String::as_str)
        == Some(VARIANT_EXTENSION_NAME)
}

/// Encode a JSON value into the Parquet Variant binary form, returning the
/// `(metadata, value)` buffer pair that fills the two `LargeBinary` children of
/// a VARIANT struct column.
///
/// ponytail: source values reach here as `serde_json::Value`, so temporal and
/// binary values take their JSON forms (RFC3339 strings, base64/array bytes)
/// rather than native Variant temporal/binary types. Upgrade to a direct
/// `vrl::Value` -> Variant walk if that lossiness matters downstream.
pub(super) fn value_to_variant(json: &serde_json::Value) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
    let mut builder = VariantBuilder::new();
    parquet_variant_json::append_json(json, &mut builder)?;
    Ok(builder.finish())
}
