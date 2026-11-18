//! Source-local encoding of string labels as Parquet VARIANT.

use arrow::{
    array::{ArrayRef, MapArray, StructArray},
    compute::cast,
    datatypes::{DataType, Field, Fields},
};
use parquet_variant_compute::cast_to_variant;
use std::sync::Arc;
use vector_lib::codecs::encoding::ArrowEncodingError;

/// The VARIANT struct fields (`metadata`, `value`), as the Zerobus SDK types a UC VARIANT column.
pub(super) fn variant_struct_fields() -> Fields {
    vec![
        Field::new("metadata", DataType::LargeBinary, false),
        Field::new("value", DataType::LargeBinary, false),
    ]
    .into()
}

/// Encode a string-keyed, string-valued map array as a VARIANT struct column with
/// [`variant_struct_fields`]. For upstream components that pre-encode a VARIANT column once for
/// several sinks.
pub(super) fn map_array_to_variant(map: &MapArray) -> Result<ArrayRef, ArrowEncodingError> {
    let to_error = |source| ArrowEncodingError::RecordBatchCreation { source };
    let (_, children, nulls) = cast_to_variant(map)
        .map_err(to_error)?
        .into_inner()
        .into_parts();
    // The library emits BinaryView children; the Zerobus SDK types a VARIANT column as LargeBinary.
    let children = children
        .iter()
        .map(|c| cast(c, &DataType::LargeBinary))
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_error)?;
    let array = StructArray::try_new(variant_struct_fields(), children, nulls).map_err(to_error)?;
    Ok(Arc::new(array))
}
