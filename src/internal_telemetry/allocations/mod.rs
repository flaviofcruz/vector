//! Allocation tracking exposed via internal telemetry.

mod allocator;
use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use metrics::{counter, gauge};
use rand_distr::num_traits::ToPrimitive;

use self::allocator::Tracer;
pub(crate) use self::allocator::{
    AllocationGroupId, AllocationLayer, GroupedTraceableAllocator, without_allocation_tracing,
};

// Must exceed the deployment's component count (~346 for logging-agent) with headroom.
const NUM_GROUPS: usize = 2048;

/// Clamp a group index to `[0, NUM_GROUPS)`. A panic inside the allocator hook runs in a
/// destructor and aborts the process, so out-of-bounds values fall back to ROOT instead.
#[inline(always)]
fn clamp_group(group: usize) -> usize {
    if group < NUM_GROUPS {
        group
    } else {
        AllocationGroupId::ROOT.as_raw() as usize
    }
}

// Allocations are not tracked during startup.
// We use the Relaxed ordering for both stores and loads of this atomic as no other threads exist when
// this code is running, and all future threads will have a happens-after relationship with
// this thread -- the main thread -- ensuring that they see the latest value of TRACK_ALLOCATIONS.
pub static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);

pub fn is_allocation_tracing_enabled() -> bool {
    TRACK_ALLOCATIONS.load(Ordering::Acquire)
}

/// Track allocations and deallocations separately.
struct GroupMemStatsStorage {
    allocations: [AtomicU64; NUM_GROUPS],
    deallocations: [AtomicU64; NUM_GROUPS],
}

// Reporting interval in milliseconds.
pub static REPORTING_INTERVAL_MS: AtomicU64 = AtomicU64::new(5000);

/// A registry for tracking each thread's group memory statistics.
static THREAD_LOCAL_REFS: Mutex<Vec<&'static GroupMemStatsStorage>> = Mutex::new(Vec::new());

/// Group memory statistics per thread.
struct GroupMemStats {
    stats: &'static GroupMemStatsStorage,
}

impl GroupMemStats {
    /// Allocates a [`GroupMemStatsStorage`], and updates the global [`THREAD_LOCAL_REFS`] registry
    /// with a reference to this newly allocated memory.
    pub fn new() -> Self {
        let mut mutex = THREAD_LOCAL_REFS.lock().unwrap();
        // Allocate zeroed on the heap directly — this runs in the allocator hook where a ~32 KB
        // stack temporary would overflow the thread stack.
        // SAFETY: all-zero is valid for AtomicU64; the allocation is leaked for `'static`.
        let layout = Layout::new::<GroupMemStatsStorage>();
        let ptr = unsafe { alloc_zeroed(layout) as *mut GroupMemStatsStorage };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        let stats_ref: &'static GroupMemStatsStorage = unsafe { &*ptr };
        let group_mem_stats = GroupMemStats { stats: stats_ref };
        mutex.push(stats_ref);
        group_mem_stats
    }
}

thread_local! {
    static GROUP_MEM_STATS: GroupMemStats = GroupMemStats::new();
}

struct GroupInfo {
    component_kind: String,
    component_type: String,
    component_id: String,
}

impl GroupInfo {
    const fn new() -> Self {
        Self {
            component_id: String::new(),
            component_kind: String::new(),
            component_type: String::new(),
        }
    }
}

static GROUP_INFO: [Mutex<GroupInfo>; NUM_GROUPS] =
    [const { Mutex::new(GroupInfo::new()) }; NUM_GROUPS];

// Maps component_id to its group id so config reloads reuse the same group, avoiding metric collision.
static GROUP_BY_NAME: Mutex<BTreeMap<String, AllocationGroupId>> = Mutex::new(BTreeMap::new());

pub type Allocator<A> = GroupedTraceableAllocator<A, MainTracer>;

pub const fn get_grouped_tracing_allocator<A>(allocator: A) -> Allocator<A> {
    GroupedTraceableAllocator::new(allocator, MainTracer)
}

pub struct MainTracer;

impl Tracer for MainTracer {
    #[inline(always)]
    fn trace_allocation(&self, object_size: usize, group_id: AllocationGroupId) {
        // Defensive clamp — a panic in the allocator hook aborts the process.
        let group = clamp_group(group_id.as_raw() as usize);
        // Handle the case when thread local destructor is ran.
        _ = GROUP_MEM_STATS.try_with(|t| {
            t.stats.allocations[group].fetch_add(object_size as u64, Ordering::Relaxed);
        });
    }

    #[inline(always)]
    fn trace_deallocation(&self, object_size: usize, source_group_id: AllocationGroupId) {
        let group = clamp_group(source_group_id.as_raw() as usize);
        // Handle the case when thread local destructor is ran.
        _ = GROUP_MEM_STATS.try_with(|t| {
            t.stats.deallocations[group].fetch_add(object_size as u64, Ordering::Relaxed);
        });
    }
}

/// Initializes allocation tracing.
pub fn init_allocation_tracing() {
    for group in &GROUP_INFO {
        let mut writer = group.lock().unwrap();
        *writer = GroupInfo {
            component_id: "root".to_string(),
            component_kind: "root".to_string(),
            component_type: "root".to_string(),
        };
    }
    let alloc_processor = thread::Builder::new().name("vector-alloc-processor".to_string());
    alloc_processor
        .spawn(|| {
            without_allocation_tracing(|| loop {
                for group_idx in 0..NUM_GROUPS {
                    let mut allocations_diff = 0u64;
                    let mut deallocations_diff = 0u64;
                    {
                        let mutex = THREAD_LOCAL_REFS.lock().unwrap();
                        for stats in mutex.iter() {
                            allocations_diff +=
                                stats.allocations[group_idx].swap(0, Ordering::Relaxed);
                            deallocations_diff +=
                                stats.deallocations[group_idx].swap(0, Ordering::Relaxed);
                        }
                    }
                    if allocations_diff == 0 && deallocations_diff == 0 {
                        continue;
                    }
                    let mem_used_diff = allocations_diff as i64 - deallocations_diff as i64;
                    let group_info = GROUP_INFO[group_idx].lock().unwrap();
                    if allocations_diff > 0 {
                        counter!(
                            "component_allocated_bytes_total", "component_kind" => group_info.component_kind.clone(),
                            "component_type" => group_info.component_type.clone(),
                            "component_id" => group_info.component_id.clone()).increment(allocations_diff);
                    }
                    if deallocations_diff > 0 {
                        counter!(
                            "component_deallocated_bytes_total", "component_kind" => group_info.component_kind.clone(),
                            "component_type" => group_info.component_type.clone(),
                            "component_id" => group_info.component_id.clone()).increment(deallocations_diff);
                    }
                    if mem_used_diff > 0 {
                        gauge!(
                            "component_allocated_bytes", "component_type" => group_info.component_type.clone(),
                            "component_id" => group_info.component_id.clone(),
                            "component_kind" => group_info.component_kind.clone())
                            .increment(mem_used_diff.to_f64().expect("failed to convert mem_used from int to float"));
                    }
                    if mem_used_diff < 0 {
                        gauge!(
                            "component_allocated_bytes", "component_type" => group_info.component_type.clone(),
                            "component_id" => group_info.component_id.clone(),
                            "component_kind" => group_info.component_kind.clone())
                            .decrement((-mem_used_diff).to_f64().expect("failed to convert mem_used from int to float"));
                    }
                }
                thread::sleep(Duration::from_millis(
                    REPORTING_INTERVAL_MS.load(Ordering::Relaxed),
                ));
            })
        })
        .unwrap();
}

/// Acquires an allocation group ID.
///
/// This creates an allocation group which allows callers to enter/exit the allocation group context, associating all
/// (de)allocations within the context with that group. An allocation group ID must be "attached" to
/// a [`tracing::Span`] to achieve this" we utilize the logical invariants provided by spans --
/// entering, exiting, and how spans exist as a stack -- in order to handle keeping the "current
/// allocation group" accurate across all threads.
pub fn acquire_allocation_group_id(
    component_id: String,
    component_type: String,
    component_kind: String,
) -> AllocationGroupId {
    let mut by_name = GROUP_BY_NAME.lock().unwrap();
    if let Some(&existing) = by_name.get(&component_id) {
        return existing;
    }

    if let Some(group_id) = AllocationGroupId::register()
        && let Some(group_lock) = GROUP_INFO.get(group_id.as_raw() as usize)
    {
        *group_lock.lock().unwrap() = GroupInfo {
            component_id: component_id.clone(),
            component_kind,
            component_type,
        };
        by_name.insert(component_id, group_id);
        return group_id;
    }

    warn!(
        "Maximum number of registrable allocation group IDs reached ({}). Allocations for component '{}' will be attributed to the root allocation group.",
        NUM_GROUPS, component_id
    );
    AllocationGroupId::ROOT
}
