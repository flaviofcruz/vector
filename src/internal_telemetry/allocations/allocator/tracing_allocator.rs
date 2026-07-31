use std::{
    alloc::{GlobalAlloc, Layout},
    sync::atomic::Ordering,
};

use super::{
    token::{AllocationGroupId, try_with_suspended_allocation_group},
    tracer::Tracer,
};
use crate::internal_telemetry::allocations::TRACK_ALLOCATIONS;

/// A tracing allocator that groups allocation events by groups.
///
/// This allocator can only be used when specified via `#[global_allocator]`.
pub struct GroupedTraceableAllocator<A, T> {
    allocator: A,
    tracer: T,
}

impl<A, T> GroupedTraceableAllocator<A, T> {
    /// Creates a new `GroupedTraceableAllocator` that wraps the given allocator and tracer.
    #[must_use]
    pub const fn new(allocator: A, tracer: T) -> Self {
        Self { allocator, tracer }
    }
}

unsafe impl<A: GlobalAlloc, T: Tracer> GlobalAlloc for GroupedTraceableAllocator<A, T> {
    #[inline]
    unsafe fn alloc(&self, object_layout: Layout) -> *mut u8 {
        unsafe {
            // Header is always written regardless of TRACK_ALLOCATIONS so the layout is stable
            // across the flag flip; only the bookkeeping is gated. See README.databricks.md #156.
            let (actual_layout, offset_to_group_id) = get_wrapped_layout(object_layout);
            let actual_ptr = self.allocator.alloc(actual_layout);
            if actual_ptr.is_null() {
                return actual_ptr;
            }

            let group_id_ptr = actual_ptr.add(offset_to_group_id).cast::<u16>();
            // Default to ROOT so dealloc always reads a valid group id.
            group_id_ptr.write(AllocationGroupId::ROOT.as_raw());

            if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
                let object_size = object_layout.size();
                try_with_suspended_allocation_group(
                    #[inline(always)]
                    |group_id| {
                        group_id_ptr.write(group_id.as_raw());
                        self.tracer.trace_allocation(object_size, group_id);
                    },
                );
            }
            actual_ptr
        }
    }

    #[inline]
    unsafe fn dealloc(&self, object_ptr: *mut u8, object_layout: Layout) {
        unsafe {
            // Always free with the wrapped layout — every object carries the header (see alloc).
            let (wrapped_layout, offset_to_group_id) = get_wrapped_layout(object_layout);

            let raw_group_id = object_ptr.add(offset_to_group_id).cast::<u16>().read();

            // Deallocate before tracking, just to make sure we're reclaiming memory as soon as possible.
            self.allocator.dealloc(object_ptr, wrapped_layout);

            if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
                let object_size = object_layout.size();
                let source_group_id = AllocationGroupId::from_raw(raw_group_id);
                try_with_suspended_allocation_group(
                    #[inline(always)]
                    |_| {
                        self.tracer.trace_deallocation(object_size, source_group_id);
                    },
                );
            }
        }
    }
}

#[inline(always)]
fn get_wrapped_layout(object_layout: Layout) -> (Layout, usize) {
    static HEADER_LAYOUT: Layout = Layout::new::<u16>();

    // Append the group-id header after the object so dealloc can read it back.
    let (actual_layout, offset_to_group_id) = object_layout
        .extend(HEADER_LAYOUT)
        .expect("wrapping requested layout resulted in overflow");

    (actual_layout.pad_to_align(), offset_to_group_id)
}
