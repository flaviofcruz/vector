//! [`WireToArrowEncoder`]: builds a [`MessagePlan`] once per
//! (proto descriptor, Arrow schema) pair and encodes batches of proto wire
//! bytes into Arrow `RecordBatch`es.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use zeroparser::wire::{WireValue, try_parse_field};

use super::builders::BuilderNodeList;
use super::errors::{Result, WireToArrowError};
use super::normalize::normalize_message;
use super::plan::MessagePlan;
use super::scan::{BuiltVariants, ValidateCtx, scan_message, validate_message};
use super::serializer::{OnWireTypeMismatch, VariantMessageEncoding};
use super::variant_message::strip_wire_type_mismatches;

/// How the first validation pass over a row's original bytes treats a
/// needs-normalization verdict (see `ValidateCtx::needs_normalization`).
#[derive(Clone, Copy)]
enum FirstPass {
    /// Park the verdict and record the rest of the row's wire-type skips.
    DeferToNormalization,
    /// Return the verdict at once, recording nothing past it: the reference
    /// behavior for [`WireToArrowEncoder::assert_rows_match_first_pass_stopping_at_verdict`].
    #[cfg(test)]
    StopAtVerdict,
}

/// One encoded batch with the per-batch drop and skip tallies reported once
/// the batch is built.
struct EncodedRows {
    batch: RecordBatch,
    kept: Vec<u32>,
    dropped: u64,
    sample_err: Option<WireToArrowError>,
    first_dropped_row: Option<Bytes>,
    skips: super::scan::SkipAccumulator,
}

/// Upper bounds for attribution values in skip/drop warnings: per-field char
/// cap and per-row field cap. Bounded so a hostile or oversized payload can
/// neither bloat logs nor leak unbounded content — attribution fields are
/// identifiers (workspace / cluster / app ids), never payloads.
const ATTRIBUTION_VALUE_MAX_CHARS: usize = 64;
const ATTRIBUTION_FIELDS_MAX: usize = 8;

/// Streaming wire-format encoder. Build once per (proto message type,
/// Arrow schema) pair, then call [`WireToArrowEncoder::encode_batch`]
/// repeatedly.
pub struct WireToArrowEncoder {
    plan: Arc<MessagePlan>,
    schema: Arc<Schema>,
    descriptor: MessageDescriptor,
    on_wire_type_mismatch: OnWireTypeMismatch,
    /// Producer-attribution fields resolved to proto field numbers:
    /// (field number, display name), in config order, capped at
    /// [`ATTRIBUTION_FIELDS_MAX`]. Empty when unconfigured.
    attribution_fields: Vec<(u32, String)>,
}

impl std::fmt::Debug for WireToArrowEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireToArrowEncoder")
            .field("schema_fields", &self.schema.fields().len())
            .field("on_wire_type_mismatch", &self.on_wire_type_mismatch)
            .finish()
    }
}

impl WireToArrowEncoder {
    /// Compile a plan for the given proto descriptor + Arrow schema.
    ///
    /// Every field in `schema` must exist (by name) in `descriptor`. Proto
    /// fields absent from `schema` are silently skipped at scan time.
    ///
    /// Uses the default wire-type-mismatch policy ([`OnWireTypeMismatch::
    /// SkipField`], protobuf-java semantics) and no attribution fields.
    pub fn new(descriptor: &MessageDescriptor, schema: Schema) -> Result<Self> {
        Self::with_options(descriptor, schema, OnWireTypeMismatch::SkipField, &[])
    }

    /// [`WireToArrowEncoder::new`], with `encoding` for every VARIANT column backed by a nested
    /// proto message.
    pub fn with_variant_message_encoding(
        descriptor: &MessageDescriptor,
        schema: Schema,
        encoding: VariantMessageEncoding,
    ) -> Result<Self> {
        Self::from_parts(
            descriptor,
            schema,
            OnWireTypeMismatch::SkipField,
            &[],
            encoding,
        )
    }

    /// Compile a plan with an explicit mismatch policy and producer-attribution
    /// field names for skip/drop warnings. Names are resolved to top-level
    /// field numbers on `descriptor`; unknown names are dropped with a warning
    /// (a typoed attribution field must not fail the pipeline).
    pub fn with_options(
        descriptor: &MessageDescriptor,
        schema: Schema,
        on_wire_type_mismatch: OnWireTypeMismatch,
        attribution_field_names: &[String],
    ) -> Result<Self> {
        Self::from_parts(
            descriptor,
            schema,
            on_wire_type_mismatch,
            attribution_field_names,
            VariantMessageEncoding::default(),
        )
    }

    /// [`WireToArrowEncoder::with_options`], with `encoding` for every VARIANT column backed by
    /// a nested proto message.
    pub(crate) fn from_parts(
        descriptor: &MessageDescriptor,
        schema: Schema,
        on_wire_type_mismatch: OnWireTypeMismatch,
        attribution_field_names: &[String],
        encoding: VariantMessageEncoding,
    ) -> Result<Self> {
        let plan = MessagePlan::build(descriptor, &schema.fields().clone(), encoding)?;
        let mut attribution_fields = Vec::new();
        for name in attribution_field_names.iter().take(ATTRIBUTION_FIELDS_MAX) {
            match descriptor.get_field_by_name(name) {
                Some(field) => attribution_fields.push((field.number(), name.clone())),
                None => tracing::warn!(
                    message = "wire-to-Arrow attribution field not found in the proto descriptor; \
                               it will not appear in skip/drop warnings",
                    field = %name,
                    descriptor = %descriptor.full_name(),
                ),
            }
        }
        Ok(Self {
            plan: Arc::new(plan),
            schema: Arc::new(schema),
            descriptor: descriptor.clone(),
            on_wire_type_mismatch,
            attribution_fields,
        })
    }

    /// Encode a batch of serialized proto messages into a single `RecordBatch`.
    ///
    /// See [`WireToArrowEncoder::encode_batch_with_kept`]; this drops the
    /// survivor indices.
    pub fn encode_batch(&self, messages: &[Bytes]) -> Result<RecordBatch> {
        self.encode_batch_with_kept(messages)
            .map(|(batch, _kept)| batch)
    }

    /// Encode a batch of serialized proto messages into a single `RecordBatch`
    /// plus the indices of the messages that produced an output row, in input
    /// order. Callers carrying per-message metadata use the indices to drop
    /// the metadata of rows that failed validation.
    ///
    /// Per-row isolation: each message is pre-validated via
    /// [`validate_message`] before any builder is touched. Rows that fail
    /// validation are dropped from the output batch and counted via the
    /// `wire_to_arrow_rows_dropped` metric (plus a rate-limit-friendly
    /// warn log carrying a sample error). Returning an empty `RecordBatch`
    /// is acceptable when every row was malformed.
    ///
    /// Known fields whose wire type disagrees with the descriptor are handled
    /// per [`OnWireTypeMismatch`]: by default the occurrence is skipped
    /// (protobuf-java semantics — column null, row kept) and counted via
    /// `wire_to_arrow_fields_skipped_total`. Skips are tallied per row and
    /// committed only for rows that are kept, so the metric and the "field
    /// left null, row kept" warning never describe a row the batch dropped.
    ///
    /// Errors out of this method are reserved for batch-level failures
    /// that aren't attributable to a single row: a code-bug surface
    /// (scan-vs-validate divergence surfaced as `PlanBuilderMismatch`) or
    /// a `RecordBatchAssembly` rejection from Arrow. Row-finalize is
    /// infallible, so it doesn't appear in this list.
    pub fn encode_batch_with_kept(&self, messages: &[Bytes]) -> Result<(RecordBatch, Vec<u32>)> {
        let encoded = self.encode_rows(messages, FirstPass::DeferToNormalization);
        #[cfg(test)]
        self.assert_rows_match_first_pass_stopping_at_verdict(messages, &encoded);
        let encoded = encoded?;
        self.report_dropped_and_skipped(messages.len(), &encoded);
        Ok((encoded.batch, encoded.kept))
    }

    fn encode_rows(&self, messages: &[Bytes], first_pass: FirstPass) -> Result<EncodedRows> {
        let capacity = messages.len();
        let mut builders = BuilderNodeList::with_capacity(&self.plan, capacity)?;
        let mut dropped = 0u64;
        let mut sample_err: Option<WireToArrowError> = None;
        let mut first_dropped_row: Option<Bytes> = None;
        let mut variants = BuiltVariants::default();
        let mut kept = Vec::with_capacity(capacity);
        let mut skips = super::scan::SkipAccumulator::default();
        let mut row_skips = super::scan::RowSkips::default();
        // Sink for the revalidation pass, which must not record: the first
        // pass already counted every mismatch of the original row.
        let mut revalidation_skips = super::scan::RowSkips::default();
        let mut path = Vec::new();

        for (index, msg_bytes) in messages.iter().enumerate() {
            variants.clear();
            // Skips are tallied per row and committed only once the row is
            // kept below. `capture` stops snapshotting paths once the batch
            // has its warning sample, keeping the hot path allocation-free.
            row_skips.reset(skips.sample.is_none());
            // Pre-validate so a malformed row drops without poisoning any
            // builder. Arrow `*Builder` has no public rollback API, and
            // nested-struct `finalize_row` calls inside `scan_message` are
            // not reversible, so an upfront validation pass is how we
            // isolate per-row decode failures.
            //
            // The pass is the single skip recorder for the row: normalization
            // strips mismatched occurrences silently, and its canonical bytes
            // no longer show the row as the producer sent it. Once the row
            // needs normalization the pass stops validating — the verdict on
            // the occurrences after it belongs to the canonical bytes — and
            // only records the rest of the row's wire-type skips.
            let mut needs_normalization = None;
            let mut validation = {
                let mut ctx = ValidateCtx {
                    on_mismatch: self.on_wire_type_mismatch,
                    row: msg_bytes,
                    skips: &mut row_skips,
                    path: &mut path,
                    needs_normalization: match first_pass {
                        FirstPass::DeferToNormalization => Some(&mut needs_normalization),
                        #[cfg(test)]
                        FirstPass::StopAtVerdict => None,
                    },
                };
                validate_message(&self.plan, msg_bytes, &mut variants, &mut ctx)
            }
            .and_then(|()| needs_normalization.map_or(Ok(()), Err));
            // Singular-field merges and oneof switches are legal protobuf. Normalize only
            // these rows before touching Arrow builders, preserving explicit scalar defaults.
            // A VARIANT payload missing a required field may be completed by a later
            // occurrence; protobuf-java merges every occurrence before its initialization
            // check, so that verdict retries here too.
            let canonical = if matches!(
                &validation,
                Err(WireToArrowError::DuplicateSingularField { .. }
                    | WireToArrowError::OneofNeedsNormalization { .. }
                    | WireToArrowError::VariantMessageMissingRequiredField { .. })
            ) {
                // The decode guard keeps rows that prost-reflect itself
                // rejects from being canonicalized — in particular so a
                // malformed occurrence cannot hide behind a later well-typed
                // overwrite. prost-reflect is also strict about *wire types*,
                // which under `SkipField` is stricter than this encoder's
                // policy, so the guard there runs on a copy with every
                // wire-type-mismatched occurrence stripped first: any error
                // left is a real malformation, independent of how many
                // mismatches the row carries or where they sit.
                let decode_guard = |bytes: &Bytes| {
                    DynamicMessage::decode(self.descriptor.clone(), bytes.clone())
                        .map(|_| ())
                        .map_err(|source| WireToArrowError::ProtoMessageDecode { source })
                };
                let normalized = match self.on_wire_type_mismatch {
                    OnWireTypeMismatch::DropRow => decode_guard(msg_bytes)
                        .and_then(|()| normalize_message(&self.plan, msg_bytes, false)),
                    OnWireTypeMismatch::SkipField => {
                        // `None` means the bytes are malformed; guard the
                        // original so prost reports it and the row drops.
                        let stripped = strip_wire_type_mismatches(&self.descriptor, msg_bytes)
                            .map(Bytes::from);
                        decode_guard(stripped.as_ref().unwrap_or(msg_bytes))
                            .and_then(|()| normalize_message(&self.plan, msg_bytes, true))
                    }
                };
                match normalized {
                    Ok(bytes) => Some(Bytes::from(bytes)),
                    Err(err) => {
                        validation = Err(err);
                        None
                    }
                }
            } else {
                None
            };
            if let Some(bytes) = &canonical {
                variants.clear();
                // The canonical bytes still carry every mismatch inside a
                // message-backed VARIANT payload (normalization concatenates
                // those payloads unchanged), all already counted by the first
                // pass, so this pass records into a discarded tally.
                revalidation_skips.reset(false);
                let mut ctx = ValidateCtx {
                    on_mismatch: self.on_wire_type_mismatch,
                    row: msg_bytes,
                    skips: &mut revalidation_skips,
                    path: &mut path,
                    needs_normalization: None,
                };
                validation = validate_message(&self.plan, bytes, &mut variants, &mut ctx);
            }
            if let Err(err) = validation {
                dropped += 1;
                if sample_err.is_none() {
                    sample_err = Some(err);
                    first_dropped_row = Some(msg_bytes.clone());
                }
                continue;
            }
            builders.reset_present();
            scan_message(
                &self.plan,
                canonical.as_ref().unwrap_or(msg_bytes),
                &mut builders,
                &mut variants,
            )?;
            builders.finalize_row(&self.plan);
            kept.push(index as u32);
            row_skips.commit(&mut skips, msg_bytes);
        }

        let arrays = builders.finish(&self.plan)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|source| WireToArrowError::RecordBatchAssembly { source })?;
        Ok(EncodedRows {
            batch,
            kept,
            dropped,
            sample_err,
            first_dropped_row,
            skips,
        })
    }

    /// Emit the batch's drop and skip metrics and their sampled warnings.
    fn report_dropped_and_skipped(&self, batch_size: usize, encoded: &EncodedRows) {
        let EncodedRows {
            dropped,
            sample_err,
            first_dropped_row,
            skips,
            ..
        } = encoded;
        let dropped = *dropped;
        if dropped > 0 {
            metrics::counter!("wire_to_arrow_rows_dropped").increment(dropped);
            let attribution = first_dropped_row
                .as_deref()
                .map(|row| self.attribution_from_row(row))
                .unwrap_or_default();
            tracing::warn!(
                message = "wire-to-Arrow dropped malformed rows from batch",
                dropped,
                batch_size,
                sample_error = ?sample_err,
                attribution = %attribution,
            );
        }

        if skips.skipped > 0 {
            metrics::counter!(
                "wire_to_arrow_fields_skipped_total",
                "reason" => "wire_type_mismatch",
            )
            .increment(skips.skipped);
            // One warning per batch (the batch cadence is the rate limit, as
            // with the drop warning above), sampled from the first skip.
            if let Some(sample) = &skips.sample {
                let attribution = self.attribution_from_row(&sample.row);
                tracing::warn!(
                    message = "wire-to-Arrow skipped wire-type-mismatched fields; \
                               field left null, row kept",
                    skipped = skips.skipped,
                    batch_size,
                    field_numbers = ?sample.field_numbers,
                    expected_wire_type = sample.expected,
                    actual_wire_type = sample.actual,
                    attribution = %attribution,
                );
            }
        }
    }

    /// Test-build differential check run on every `encode_batch_with_kept`
    /// call, so the whole test corpus asserts it: recording skips past a
    /// needs-normalization verdict must never change which rows are kept,
    /// their output, or the drop sample — only skip telemetry. The reference
    /// is the first pass that stops at the verdict, as before skips past it
    /// were recorded.
    #[cfg(test)]
    fn assert_rows_match_first_pass_stopping_at_verdict(
        &self,
        messages: &[Bytes],
        encoded: &Result<EncodedRows>,
    ) {
        let reference = self.encode_rows(messages, FirstPass::StopAtVerdict);
        match (encoded, &reference) {
            (Ok(encoded), Ok(reference)) => {
                assert_eq!(encoded.kept, reference.kept, "kept rows diverged");
                assert_eq!(encoded.batch, reference.batch, "row output diverged");
                assert_eq!(encoded.dropped, reference.dropped);
                assert_eq!(
                    format!("{:?}", encoded.sample_err),
                    format!("{:?}", reference.sample_err),
                    "drop sample diverged"
                );
                assert_eq!(encoded.first_dropped_row, reference.first_dropped_row);
            }
            (Err(encoded), Err(reference)) => {
                assert_eq!(format!("{encoded:?}"), format!("{reference:?}"));
            }
            _ => panic!(
                "batch outcome diverged: {:?} vs {:?}",
                encoded.as_ref().map(|_| ()),
                reference.as_ref().map(|_| ())
            ),
        }
    }

    /// Extract the configured attribution fields from one row's top-level
    /// wire bytes, as a `name=value` list for skip/drop warnings. Walks only
    /// top-level tags (attribution fields are identifiers that decode
    /// independently of the mismatched field), truncates each value, and
    /// never logs message payloads. Unparseable rows yield an empty string.
    pub(crate) fn attribution_from_row(&self, row: &[u8]) -> String {
        if self.attribution_fields.is_empty() {
            return String::new();
        }
        let mut parts: Vec<String> = Vec::with_capacity(self.attribution_fields.len());
        let mut seen_numbers: Vec<u32> = Vec::new();
        let mut bytes = row;
        while !bytes.is_empty() && parts.len() < self.attribution_fields.len() {
            let Ok((field, rest)) = try_parse_field(bytes) else {
                break;
            };
            bytes = rest;
            let field_number = field.field_num as u32;
            let Some((_, name)) = self
                .attribution_fields
                .iter()
                .find(|(number, _)| *number == field_number)
            else {
                continue;
            };
            // First occurrence wins for repeated fields; duplicates ignored.
            if seen_numbers.contains(&field_number) {
                continue;
            }
            seen_numbers.push(field_number);
            let value = match &field.value {
                WireValue::Varint(v) => v.to_string(),
                WireValue::I64(v) => v.to_string(),
                WireValue::I32(v) => v.to_string(),
                WireValue::Len(b) => {
                    String::from_utf8_lossy(&b[..b.len().min(ATTRIBUTION_VALUE_MAX_CHARS)])
                        .into_owned()
                        .chars()
                        .take(ATTRIBUTION_VALUE_MAX_CHARS)
                        .collect()
                }
            };
            parts.push(format!("{name}={value}"));
        }
        parts.join(",")
    }
}
