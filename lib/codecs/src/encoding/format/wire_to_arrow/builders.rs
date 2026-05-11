//! Column builders used by the wire-to-Arrow encoder.
//!
//! Leaves (`TypedBuilder`) wrap `arrow::array::*Builder` without trait-object
//! indirection. Branch nodes (`BuilderNode::Struct`, `BuilderNode::RepeatedMessage`)
//! own their children + per-row bookkeeping (validity, list offsets).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    LargeBinaryBuilder, LargeStringBuilder, ListArray, MapArray, StructArray,
    TimestampMicrosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, TimeUnit};

use super::errors::{Result, WireToArrowError};
use super::plan::{MessagePlan, PlanSlot};

/// Leaf builder: one Arrow primitive column. Type-specific, no dyn dispatch.
pub enum TypedBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    LargeUtf8(LargeStringBuilder),
    LargeBinary(LargeBinaryBuilder),
    /// `TimestampMicrosecondBuilder` for the `_event_time` coercion and any
    /// other proto int64 field whose Arrow column is declared as
    /// `Timestamp(Microsecond, ...)`. The underlying i64 is written as
    /// microseconds since Unix epoch — we don't transform values, only the
    /// Arrow column type.
    TimestampMicros(TimestampMicrosecondBuilder),
}

/// Rough per-value byte-length hint used to pre-size the data buffer for
/// `LargeStringBuilder` / `LargeBinaryBuilder`. The builder grows on overflow,
/// so this only avoids the first few reallocations — picked to be in the right
/// order of magnitude for typical log-field values (short ids, short strings)
/// without over-allocating for columns that turn out to be mostly empty.
const AVG_VARLEN_BYTES_PER_VALUE: usize = 16;

impl TypedBuilder {
    /// True iff `dt` is one of the Arrow leaf data types this encoder can
    /// build. Used at plan-build to reject unsupported types up front
    /// instead of panicking in [`TypedBuilder::new`] on the first batch.
    pub fn supports(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Int32
                | DataType::Int64
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Boolean
                | DataType::LargeUtf8
                | DataType::LargeBinary
                | DataType::Timestamp(TimeUnit::Microsecond, _)
        )
    }

    /// Construct a typed builder matching the given Arrow DataType.
    ///
    /// # Panics
    /// Panics on unsupported primitive types. Plan-build validates the
    /// Arrow leaf types via [`supports`](Self::supports) before any
    /// builder is constructed, so reaching the panic here indicates a
    /// plan/builder mismatch (build bug, not user-controllable input).
    pub fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Int32 => TypedBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => TypedBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt32 => TypedBuilder::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::UInt64 => TypedBuilder::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => TypedBuilder::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => TypedBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => TypedBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::LargeUtf8 => TypedBuilder::LargeUtf8(LargeStringBuilder::with_capacity(
                capacity,
                capacity * AVG_VARLEN_BYTES_PER_VALUE,
            )),
            DataType::LargeBinary => TypedBuilder::LargeBinary(LargeBinaryBuilder::with_capacity(
                capacity,
                capacity * AVG_VARLEN_BYTES_PER_VALUE,
            )),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(capacity);
                if let Some(tz) = tz {
                    builder = builder.with_timezone(tz.clone());
                }
                TypedBuilder::TimestampMicros(builder)
            }
            other => panic!("unsupported leaf DataType {other:?} (PoC scope)"),
        }
    }

    pub fn append_null(&mut self) {
        match self {
            TypedBuilder::Int32(b) => b.append_null(),
            TypedBuilder::Int64(b) => b.append_null(),
            TypedBuilder::UInt32(b) => b.append_null(),
            TypedBuilder::UInt64(b) => b.append_null(),
            TypedBuilder::Float32(b) => b.append_null(),
            TypedBuilder::Float64(b) => b.append_null(),
            TypedBuilder::Boolean(b) => b.append_null(),
            TypedBuilder::LargeUtf8(b) => b.append_null(),
            TypedBuilder::LargeBinary(b) => b.append_null(),
            TypedBuilder::TimestampMicros(b) => b.append_null(),
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            TypedBuilder::Int32(b) => Arc::new(b.finish()),
            TypedBuilder::Int64(b) => Arc::new(b.finish()),
            TypedBuilder::UInt32(b) => Arc::new(b.finish()),
            TypedBuilder::UInt64(b) => Arc::new(b.finish()),
            TypedBuilder::Float32(b) => Arc::new(b.finish()),
            TypedBuilder::Float64(b) => Arc::new(b.finish()),
            TypedBuilder::Boolean(b) => Arc::new(b.finish()),
            TypedBuilder::LargeUtf8(b) => Arc::new(b.finish()),
            TypedBuilder::LargeBinary(b) => Arc::new(b.finish()),
            TypedBuilder::TimestampMicros(b) => Arc::new(b.finish()),
        }
    }
}

/// A tree of builders mirroring a `MessagePlan`.
pub struct BuilderNodeList {
    pub(crate) nodes: Vec<BuilderNode>,
    /// Per-row scratch: `present[i]` is `true` if slot `i` was touched while
    /// scanning the current message at this level. Owned alongside `nodes` so
    /// each sub-plan reuses its own buffer instead of allocating a fresh
    /// `Vec<bool>` per nested-struct / list / map occurrence on the hot path.
    /// Reset between rows / sub-rows via [`BuilderNodeList::reset_present`].
    pub(crate) present: Vec<bool>,
}

pub enum BuilderNode {
    Scalar(TypedBuilder),
    /// Singular nested message. `validity[i]` tells whether row `i` had this
    /// field present (true) or absent (false — child values are null-filled).
    Struct {
        children: BuilderNodeList,
        validity: Vec<bool>,
    },
    /// Repeated nested message -> Arrow `List<Struct>`.
    /// `offsets[i]` = total element count after row `i`. `offsets[0] = 0`.
    /// `current_offset` tracks the running count across scan.
    RepeatedMessage {
        children: BuilderNodeList,
        offsets: Vec<i32>,
        current_offset: i32,
    },
    /// Repeated scalar -> Arrow `List<primitive>`. Same offset+current_offset
    /// bookkeeping as `RepeatedMessage`, but the child is a single typed
    /// primitive builder rather than a tree.
    RepeatedScalar {
        values: TypedBuilder,
        offsets: Vec<i32>,
        current_offset: i32,
    },
    /// Proto map -> Arrow `Map<Struct(key, value)>`. Wire-level handling is
    /// identical to `RepeatedMessage` (proto maps are `repeated MapEntry`),
    /// but the finish step assembles a `MapArray` with `key_value` entry name
    /// matching the zerobus sink's existing convention.
    Map {
        children: BuilderNodeList,
        offsets: Vec<i32>,
        current_offset: i32,
    },
}

impl BuilderNodeList {
    /// Allocate a builder tree matching `plan`, with capacity for `capacity` rows.
    ///
    /// Called **once per batch** by [`WireToArrowEncoder::encode_batch`], not
    /// once per sink — Arrow's `*Builder::finish()` consumes the internal
    /// buffers to produce the output `ArrayRef`, so the tree is single-use.
    /// The shared, immutable state (`Arc<MessagePlan>`, `Arc<Schema>`) lives
    /// on the encoder and is what costs once per sink.
    ///
    /// [`WireToArrowEncoder::encode_batch`]: super::WireToArrowEncoder::encode_batch
    pub fn with_capacity(plan: &MessagePlan, capacity: usize) -> Self {
        let mut nodes = Vec::with_capacity(plan.slots.len());
        for (slot, field) in plan.slots.iter().zip(plan.arrow_fields.iter()) {
            let node = match slot {
                PlanSlot::Scalar(_) => {
                    BuilderNode::Scalar(TypedBuilder::new(field.data_type(), capacity))
                }
                PlanSlot::Struct(sub_plan) => BuilderNode::Struct {
                    children: BuilderNodeList::with_capacity(sub_plan, capacity),
                    validity: Vec::with_capacity(capacity),
                },
                PlanSlot::RepeatedMessage(sub_plan) => {
                    let mut offsets = Vec::with_capacity(capacity + 1);
                    offsets.push(0);
                    BuilderNode::RepeatedMessage {
                        // List lengths tend to be small; 2x rows is a rough guess.
                        children: BuilderNodeList::with_capacity(sub_plan, capacity * 2),
                        offsets,
                        current_offset: 0,
                    }
                }
                PlanSlot::RepeatedScalar(_) => {
                    let element_type = match field.data_type() {
                        DataType::List(element_field) => element_field.data_type(),
                        other => {
                            panic!("RepeatedScalar slot requires List Arrow type, got {other:?}")
                        }
                    };
                    let mut offsets = Vec::with_capacity(capacity + 1);
                    offsets.push(0);
                    BuilderNode::RepeatedScalar {
                        values: TypedBuilder::new(element_type, capacity * 2),
                        offsets,
                        current_offset: 0,
                    }
                }
                PlanSlot::Map(sub_plan) => {
                    let mut offsets = Vec::with_capacity(capacity + 1);
                    offsets.push(0);
                    BuilderNode::Map {
                        children: BuilderNodeList::with_capacity(sub_plan, capacity * 2),
                        offsets,
                        current_offset: 0,
                    }
                }
                PlanSlot::Absent => build_absent_node(field, capacity),
            };
            nodes.push(node);
        }
        let present = vec![false; plan.slots.len()];
        Self { nodes, present }
    }

    /// Zero `present` ahead of scanning a row / sub-row at this level. Cheap
    /// in-place loop — buffer capacity is preserved across calls.
    #[inline]
    pub fn reset_present(&mut self) {
        self.present.iter_mut().for_each(|p| *p = false);
    }

    /// After scanning one message, push per-row bookkeeping (struct validity,
    /// list offsets) and fill nulls for scalars whose tag wasn't seen.
    ///
    /// Reads from `self.present`, which the caller must have populated via
    /// [`reset_present`](Self::reset_present) + per-tag dispatch in
    /// `scan_message`.
    pub fn finalize_row(&mut self, plan: &MessagePlan) {
        // Split-borrow so we can mutate `nodes` while reading `present`.
        let Self { nodes, present } = self;
        for (idx, (slot, node)) in plan.slots.iter().zip(nodes.iter_mut()).enumerate() {
            match (slot, node) {
                // Scalar builders (PlanSlot::Scalar or PlanSlot::Absent paired
                // with a scalar Arrow type). Both produce a primitive column
                // that just needs null-padding when the row didn't touch it.
                (_, BuilderNode::Scalar(tb)) => {
                    if !present[idx] {
                        tb.append_null();
                    }
                }
                (_, BuilderNode::Struct { children, validity }) => {
                    let was_present = present[idx];
                    validity.push(was_present);
                    if !was_present {
                        children.fill_null_row();
                    }
                }
                // All list-flavored slots push an offsets marker per row.
                // For proto repeated fields (including maps), the outer list
                // itself is never null — absent just means empty list.
                (_, BuilderNode::RepeatedMessage {
                    offsets,
                    current_offset,
                    ..
                })
                | (_, BuilderNode::RepeatedScalar {
                    offsets,
                    current_offset,
                    ..
                })
                | (_, BuilderNode::Map {
                    offsets,
                    current_offset,
                    ..
                }) => {
                    offsets.push(*current_offset);
                }
            }
        }
    }

    /// Recursively append nulls / empty lists to the entire subtree so row
    /// counts line up when a parent struct is null.
    pub fn fill_null_row(&mut self) {
        for node in self.nodes.iter_mut() {
            match node {
                BuilderNode::Scalar(tb) => tb.append_null(),
                BuilderNode::Struct { children, validity } => {
                    validity.push(false);
                    children.fill_null_row();
                }
                BuilderNode::RepeatedMessage {
                    offsets,
                    current_offset,
                    ..
                }
                | BuilderNode::RepeatedScalar {
                    offsets,
                    current_offset,
                    ..
                }
                | BuilderNode::Map {
                    offsets,
                    current_offset,
                    ..
                } => {
                    offsets.push(*current_offset);
                }
            }
        }
    }

    /// Finalize this level and return the resulting Arrow arrays in schema order.
    pub fn finish(&mut self, plan: &MessagePlan) -> Result<Vec<ArrayRef>> {
        let mut out: Vec<ArrayRef> = Vec::with_capacity(plan.slots.len());
        for (idx, (slot, node)) in plan
            .slots
            .iter()
            .zip(self.nodes.iter_mut())
            .enumerate()
        {
            let arrow_field = &plan.arrow_fields[idx];
            let arr: ArrayRef = match (slot, node) {
                (PlanSlot::Scalar(_), BuilderNode::Scalar(tb)) => tb.finish(),
                (PlanSlot::Struct(sub_plan), BuilderNode::Struct { children, validity }) => {
                    let child_arrays = children.finish(sub_plan)?;
                    let null_buf = NullBuffer::from(std::mem::take(validity));
                    Arc::new(
                        StructArray::try_new(
                            sub_plan.arrow_fields.clone(),
                            child_arrays,
                            Some(null_buf),
                        )
                        .map_err(|e| WireToArrowError::ArrayAssembly {
                            kind: "struct",
                            source: e,
                        })?,
                    )
                }
                (
                    PlanSlot::RepeatedMessage(sub_plan),
                    BuilderNode::RepeatedMessage {
                        children, offsets, ..
                    },
                ) => {
                    let child_arrays = children.finish(sub_plan)?;
                    let struct_arr = StructArray::try_new(
                        sub_plan.arrow_fields.clone(),
                        child_arrays,
                        None,
                    )
                    .map_err(|e| WireToArrowError::ArrayAssembly {
                        kind: "list element struct",
                        source: e,
                    })?;
                    let offset_buffer =
                        OffsetBuffer::new(ScalarBuffer::from(std::mem::take(offsets)));
                    let element_field = Arc::new(Field::new(
                        "item",
                        DataType::Struct(sub_plan.arrow_fields.clone()),
                        true,
                    ));
                    Arc::new(
                        ListArray::try_new(
                            element_field,
                            offset_buffer,
                            Arc::new(struct_arr),
                            None,
                        )
                        .map_err(|e| WireToArrowError::ArrayAssembly {
                            kind: "list",
                            source: e,
                        })?,
                    )
                }
                (
                    PlanSlot::RepeatedScalar(_),
                    BuilderNode::RepeatedScalar {
                        values, offsets, ..
                    },
                ) => {
                    let values_array = values.finish();
                    let offset_buffer =
                        OffsetBuffer::new(ScalarBuffer::from(std::mem::take(offsets)));
                    // Preserve the element field the schema declared (name
                    // typically "item", but follow the caller's choice).
                    let element_field = match arrow_field.data_type() {
                        DataType::List(f) => Arc::clone(f),
                        other => {
                            return Err(WireToArrowError::UnsupportedCombination {
                                name: arrow_field.name().to_string(),
                                kind: "RepeatedScalar".to_string(),
                                arrow_type: format!("{other:?}"),
                                repeated: true,
                            });
                        }
                    };
                    Arc::new(
                        ListArray::try_new(element_field, offset_buffer, values_array, None)
                            .map_err(|e| WireToArrowError::ArrayAssembly {
                                kind: "list (scalar)",
                                source: e,
                            })?,
                    )
                }
                (
                    PlanSlot::Map(sub_plan),
                    BuilderNode::Map {
                        children, offsets, ..
                    },
                ) => {
                    let child_arrays = children.finish(sub_plan)?;
                    let struct_arr = StructArray::try_new(
                        sub_plan.arrow_fields.clone(),
                        child_arrays,
                        None,
                    )
                    .map_err(|e| WireToArrowError::ArrayAssembly {
                        kind: "map entry struct",
                        source: e,
                    })?;
                    let offset_buffer =
                        OffsetBuffer::new(ScalarBuffer::from(std::mem::take(offsets)));
                    // Match zerobus's `proto_descriptor_to_arrow_schema` convention:
                    // entry field is named "key_value" and carries the sub-plan's
                    // struct fields (key at position 0, value at position 1).
                    let entry_field = Arc::new(Field::new(
                        "key_value",
                        DataType::Struct(sub_plan.arrow_fields.clone()),
                        false,
                    ));
                    Arc::new(
                        MapArray::try_new(
                            entry_field,
                            offset_buffer,
                            struct_arr,
                            None,
                            false,
                        )
                        .map_err(|e| WireToArrowError::ArrayAssembly {
                            kind: "map",
                            source: e,
                        })?,
                    )
                }
                // Absent columns: the builder tree was shaped to match the
                // Arrow type and stays fully null-filled. `finish` assembles
                // the same Arrow type as the regular path but every row is
                // null / empty list.
                (PlanSlot::Absent, BuilderNode::Scalar(tb)) => tb.finish(),
                (PlanSlot::Absent, BuilderNode::Struct { children, validity }) => {
                    let inner_fields = match arrow_field.data_type() {
                        DataType::Struct(fs) => fs.clone(),
                        _ => {
                            return Err(WireToArrowError::PlanBuilderMismatch {
                                site: "finish:absent_struct_non_struct_arrow",
                            });
                        }
                    };
                    let sub_plan = MessagePlan::all_absent(&inner_fields);
                    let child_arrays = children.finish(&sub_plan)?;
                    let null_buf = NullBuffer::from(std::mem::take(validity));
                    Arc::new(
                        StructArray::try_new(inner_fields, child_arrays, Some(null_buf))
                            .map_err(|e| WireToArrowError::ArrayAssembly {
                                kind: "struct",
                                source: e,
                            })?,
                    )
                }
                (
                    PlanSlot::Absent,
                    BuilderNode::RepeatedMessage {
                        children, offsets, ..
                    },
                ) => {
                    let inner_fields = match arrow_field.data_type() {
                        DataType::List(element_field) => match element_field.data_type() {
                            DataType::Struct(fs) => fs.clone(),
                            _ => {
                                return Err(WireToArrowError::PlanBuilderMismatch {
                                    site: "finish:absent_repeated_message_inner_non_struct",
                                });
                            }
                        },
                        _ => {
                            return Err(WireToArrowError::PlanBuilderMismatch {
                                site: "finish:absent_repeated_message_non_list",
                            });
                        }
                    };
                    let sub_plan = MessagePlan::all_absent(&inner_fields);
                    let child_arrays = children.finish(&sub_plan)?;
                    let struct_arr =
                        StructArray::try_new(inner_fields.clone(), child_arrays, None).map_err(
                            |e| WireToArrowError::ArrayAssembly {
                                kind: "list element struct",
                                source: e,
                            },
                        )?;
                    let offset_buffer =
                        OffsetBuffer::new(ScalarBuffer::from(std::mem::take(offsets)));
                    let element_field =
                        Arc::new(Field::new("item", DataType::Struct(inner_fields), true));
                    Arc::new(
                        ListArray::try_new(
                            element_field,
                            offset_buffer,
                            Arc::new(struct_arr),
                            None,
                        )
                        .map_err(|e| WireToArrowError::ArrayAssembly {
                            kind: "list",
                            source: e,
                        })?,
                    )
                }
                (
                    PlanSlot::Absent,
                    BuilderNode::RepeatedScalar {
                        values, offsets, ..
                    },
                ) => {
                    let values_array = values.finish();
                    let offset_buffer =
                        OffsetBuffer::new(ScalarBuffer::from(std::mem::take(offsets)));
                    let element_field = match arrow_field.data_type() {
                        DataType::List(f) => Arc::clone(f),
                        _ => {
                            return Err(WireToArrowError::PlanBuilderMismatch {
                                site: "finish:absent_repeated_scalar_non_list",
                            });
                        }
                    };
                    Arc::new(
                        ListArray::try_new(element_field, offset_buffer, values_array, None)
                            .map_err(|e| WireToArrowError::ArrayAssembly {
                                kind: "list (scalar)",
                                source: e,
                            })?,
                    )
                }
                (
                    PlanSlot::Absent,
                    BuilderNode::Map {
                        children, offsets, ..
                    },
                ) => {
                    let inner_fields = match arrow_field.data_type() {
                        DataType::Map(entry_field, _) => match entry_field.data_type() {
                            DataType::Struct(fs) => fs.clone(),
                            _ => {
                                return Err(WireToArrowError::PlanBuilderMismatch {
                                    site: "finish:absent_map_entry_non_struct",
                                });
                            }
                        },
                        _ => {
                            return Err(WireToArrowError::PlanBuilderMismatch {
                                site: "finish:absent_map_non_map",
                            });
                        }
                    };
                    let sub_plan = MessagePlan::all_absent(&inner_fields);
                    let child_arrays = children.finish(&sub_plan)?;
                    let struct_arr =
                        StructArray::try_new(inner_fields.clone(), child_arrays, None).map_err(
                            |e| WireToArrowError::ArrayAssembly {
                                kind: "map entry struct",
                                source: e,
                            },
                        )?;
                    let offset_buffer =
                        OffsetBuffer::new(ScalarBuffer::from(std::mem::take(offsets)));
                    let entry_field =
                        Arc::new(Field::new("key_value", DataType::Struct(inner_fields), false));
                    Arc::new(
                        MapArray::try_new(entry_field, offset_buffer, struct_arr, None, false)
                            .map_err(|e| WireToArrowError::ArrayAssembly {
                                kind: "map",
                                source: e,
                            })?,
                    )
                }
                _ => {
                    return Err(WireToArrowError::PlanBuilderMismatch {
                        site: "finish:slot_builder_mismatch",
                    });
                }
            };
            out.push(arr);
        }
        Ok(out)
    }
}

/// Build a null-filled builder tree matching the Arrow `field`'s shape, used
/// for [`PlanSlot::Absent`] columns. The builder is the same shape as a normal
/// column of that Arrow type, but no wire tags ever dispatch to it so it stays
/// fully null-padded by `finalize_row` / `fill_null_row`.
fn build_absent_node(field: &Field, capacity: usize) -> BuilderNode {
    match field.data_type() {
        DataType::Struct(inner_fields) => {
            let sub_plan = MessagePlan::all_absent(inner_fields);
            BuilderNode::Struct {
                children: BuilderNodeList::with_capacity(&sub_plan, capacity),
                validity: Vec::with_capacity(capacity),
            }
        }
        DataType::List(element_field) => {
            let mut offsets = Vec::with_capacity(capacity + 1);
            offsets.push(0);
            match element_field.data_type() {
                DataType::Struct(inner_fields) => {
                    let sub_plan = MessagePlan::all_absent(inner_fields);
                    BuilderNode::RepeatedMessage {
                        children: BuilderNodeList::with_capacity(&sub_plan, capacity * 2),
                        offsets,
                        current_offset: 0,
                    }
                }
                _ => BuilderNode::RepeatedScalar {
                    values: TypedBuilder::new(element_field.data_type(), capacity * 2),
                    offsets,
                    current_offset: 0,
                },
            }
        }
        DataType::Map(entry_field, _) => {
            let inner_fields = match entry_field.data_type() {
                DataType::Struct(fs) => fs,
                other => panic!("Map entry must be a Struct, got {other:?}"),
            };
            let sub_plan = MessagePlan::all_absent(inner_fields);
            let mut offsets = Vec::with_capacity(capacity + 1);
            offsets.push(0);
            BuilderNode::Map {
                children: BuilderNodeList::with_capacity(&sub_plan, capacity * 2),
                offsets,
                current_offset: 0,
            }
        }
        // Scalar Arrow types — build a primitive builder; `TypedBuilder::new`
        // already panics on unsupported types.
        _ => BuilderNode::Scalar(TypedBuilder::new(field.data_type(), capacity)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};

    #[test]
    fn int32_builder_roundtrip() {
        let mut tb = TypedBuilder::new(&DataType::Int32, 4);
        if let TypedBuilder::Int32(b) = &mut tb {
            b.append_value(1);
            b.append_value(2);
        }
        tb.append_null();
        let arr = tb.finish();
        let i32arr = arr.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(i32arr.len(), 3);
        assert_eq!(i32arr.value(0), 1);
        assert_eq!(i32arr.value(1), 2);
        assert!(i32arr.is_null(2));
    }

    #[test]
    fn string_builder_roundtrip() {
        let mut tb = TypedBuilder::new(&DataType::LargeUtf8, 4);
        if let TypedBuilder::LargeUtf8(b) = &mut tb {
            b.append_value("hello");
            b.append_value("world");
        }
        tb.append_null();
        let arr = tb.finish();
        assert_eq!(arr.len(), 3);
    }

    #[test]
    #[should_panic(expected = "unsupported leaf DataType")]
    fn unsupported_type_panics() {
        // Date32 is not in our scope.
        let _ = TypedBuilder::new(&DataType::Date32, 1);
    }
}
