//! Error types for the streaming wire-to-Arrow encoder.

use snafu::Snafu;

/// Errors that can occur when building an encoding plan or encoding a batch.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum WireToArrowError {
    /// Proto descriptor is missing a field named in the Arrow schema.
    #[snafu(display("proto field '{name}' not found in descriptor"))]
    MissingProtoField { name: String },

    /// The proto field's Kind cannot be represented by any supported scalar.
    #[snafu(display(
        "unsupported proto kind for field '{name}': {kind} (scalar PoC only)"
    ))]
    UnsupportedKind { name: String, kind: String },

    /// The combination of proto kind, Arrow type, and cardinality isn't supported.
    #[snafu(display(
        "field '{name}': unsupported combination \
         proto kind {kind} / arrow type {arrow_type} / repeated {repeated}"
    ))]
    UnsupportedCombination {
        name: String,
        kind: String,
        arrow_type: String,
        repeated: bool,
    },

    /// A repeated message field's Arrow element type isn't Struct.
    #[snafu(display(
        "field '{name}': repeated message requires List<Struct>, got List<{element}>"
    ))]
    RepeatedNonStructList { name: String, element: String },

    /// Ran out of wire bytes before finishing a tag/field.
    #[snafu(display("unexpected end of wire input"))]
    UnexpectedEof,

    /// Varint exceeded the max 10-byte encoding.
    #[snafu(display("varint exceeds 10 bytes"))]
    VarintOverflow,

    /// Unknown proto wire type (should be 0, 1, 2, or 5).
    #[snafu(display("invalid proto wire type {wire_type}"))]
    InvalidWireType { wire_type: u8 },

    /// Wire type for a field doesn't match the plan's expectation.
    #[snafu(display(
        "wire type mismatch: plan expected {expected}, wire bytes had {actual}"
    ))]
    WireTypeMismatch { expected: u8, actual: u8 },

    /// String field contained non-UTF-8 bytes.
    #[snafu(display("invalid UTF-8 in proto string field"))]
    InvalidUtf8,

    /// Plan and builder trees diverged during scan (build bug).
    #[snafu(display("internal: plan/builder tree mismatch"))]
    PlanBuilderMismatch,

    /// `arrow::record_batch::RecordBatch::try_new` rejected the assembled arrays.
    #[snafu(display("failed to assemble RecordBatch: {source}"))]
    RecordBatchAssembly {
        source: arrow::error::ArrowError,
    },

    /// `arrow::array::StructArray::try_new` / `ListArray::try_new` rejected
    /// the assembled arrays.
    #[snafu(display("failed to assemble {kind} array: {source}"))]
    ArrayAssembly {
        kind: &'static str,
        source: arrow::error::ArrowError,
    },
}

pub type Result<T> = std::result::Result<T, WireToArrowError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_is_informative() {
        let e = WireToArrowError::MissingProtoField {
            name: "foo".to_string(),
        };
        let msg = format!("{e}");
        assert!(msg.contains("foo"), "display must contain field name: {msg}");
    }

    #[test]
    fn wire_type_mismatch_displays_both_numbers() {
        let e = WireToArrowError::WireTypeMismatch {
            expected: 2,
            actual: 0,
        };
        let msg = format!("{e}");
        assert!(msg.contains("2") && msg.contains("0"), "got: {msg}");
    }
}
