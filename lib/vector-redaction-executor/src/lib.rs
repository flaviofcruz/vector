//! Safe Rust wrapper around the prebuilt canonical redaction executor C ABI.
//!
//! # Safety model
//!
//! Unsafe code is confined to the FFI calls and the zero-copy view of buffers allocated by the
//! canonical library. Rust slices passed to the ABI remain live for each call, output pointers are
//! distinct initialized locals, and every returned allocation has one owner that invokes the
//! library's matching deallocator on drop.

#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(feature = "static-executor")]
use std::{
    ffi::c_void,
    panic::{self, AssertUnwindSafe},
    ptr, slice,
    sync::OnceLock,
};

#[cfg(feature = "static-executor")]
use bytes::Bytes;
#[cfg(feature = "static-executor")]
use prost::Message;

mod proto_bindings {
    include!(concat!(env!("OUT_DIR"), "/_bindings.rs"));
}

pub use proto_bindings::compliance::DataShape;
pub use proto_bindings::databricks::logstructuredredactionplan as plan_proto;

#[cfg(feature = "static-executor")]
const EXPECTED_ABI_VERSION: u32 = 2;

#[cfg(feature = "static-executor")]
const PUID_LOOKUP_FOUND: i32 = 0;
#[cfg(feature = "static-executor")]
const PUID_LOOKUP_MISSING: i32 = 1;
#[cfg(feature = "static-executor")]
const PUID_LOOKUP_ERROR: i32 = 2;

#[cfg(feature = "static-executor")]
type PuidLookupCallback = unsafe extern "C-unwind" fn(*mut c_void, i64, *mut i64) -> i32;

#[cfg(feature = "static-executor")]
#[repr(C)]
struct ForeignBuffer {
    data: *mut u8,
    len: usize,
    _capacity: usize,
}

#[cfg(feature = "static-executor")]
#[derive(Clone, Copy)]
#[repr(C)]
struct ForeignStringView {
    data: *const u8,
    len: usize,
}

#[cfg(feature = "static-executor")]
impl ForeignStringView {
    const EMPTY: Self = Self {
        data: ptr::null(),
        len: 0,
    };
}

#[cfg(feature = "static-executor")]
impl ForeignBuffer {
    const EMPTY: Self = Self {
        data: ptr::null_mut(),
        len: 0,
        _capacity: 0,
    };

    fn into_bytes(self) -> Bytes {
        if self.data.is_null() || self.len == 0 {
            return Bytes::new();
        }
        Bytes::from_owner(self)
    }

    fn diagnostic(&self) -> String {
        String::from_utf8_lossy(self.as_ref()).into_owned()
    }
}

#[cfg(feature = "static-executor")]
impl AsRef<[u8]> for ForeignBuffer {
    fn as_ref(&self) -> &[u8] {
        if self.data.is_null() || self.len == 0 {
            return &[];
        }
        // SAFETY: Successful ABI calls return a non-null, immutable allocation containing at least
        // len initialized bytes. This wrapper owns it exclusively until Drop invokes the matching
        // deallocator, so the slice cannot outlive or alias a mutation of the allocation.
        unsafe { slice::from_raw_parts(self.data, self.len) }
    }
}

// SAFETY: ForeignBuffer uniquely owns an immutable allocation and exposes no mutation API. The
// allocator used by the linked Rust static library supports deallocation from any thread.
#[cfg(feature = "static-executor")]
unsafe impl Send for ForeignBuffer {}

#[cfg(feature = "static-executor")]
impl Drop for ForeignBuffer {
    fn drop(&mut self) {
        // SAFETY: This is empty or the sole owner of an unmodified live buffer returned by this ABI;
        // Drop runs once and the ABI resets the buffer while freeing it.
        unsafe { db_redactor_buffer_free(self) };
    }
}

#[cfg(feature = "static-executor")]
// SAFETY: These declarations exactly mirror vector_redactor.h from the archive that the build script
// links. The safe wrappers below uphold each function's pointer and ownership contract.
unsafe extern "C" {
    fn db_redactor_abi_version() -> u32;
    fn db_redactor_register_plan(
        plan: *const u8,
        plan_len: usize,
        hmac_key: *const u8,
        hmac_key_len: usize,
        out_handle: *mut u64,
        out_error: *mut ForeignBuffer,
    ) -> i32;
    fn db_redactor_redact(
        handle: u64,
        record: *const u8,
        record_len: usize,
        shape_enforcement_mode: u8,
        keep_original: u8,
        shard_name: ForeignStringView,
        workspace_fallback: ForeignStringView,
        account_fallback: ForeignStringView,
        metastore_fallback: ForeignStringView,
        out_record: *mut ForeignBuffer,
        out_error: *mut ForeignBuffer,
    ) -> i32;
    fn db_redactor_redact_with_puid_lookup(
        handle: u64,
        record: *const u8,
        record_len: usize,
        shape_enforcement_mode: u8,
        keep_original: u8,
        shard_name: ForeignStringView,
        workspace_fallback: ForeignStringView,
        account_fallback: ForeignStringView,
        metastore_fallback: ForeignStringView,
        lookup: Option<PuidLookupCallback>,
        lookup_context: *mut c_void,
        out_record: *mut ForeignBuffer,
        out_error: *mut ForeignBuffer,
    ) -> i32;
    fn db_redactor_field_value_conforms(
        shapes: *const i32,
        shapes_len: usize,
        value: *const u8,
        value_len: usize,
        out_conforms: *mut u8,
    ) -> i32;
    fn db_redactor_release_plan(handle: u64);
    fn db_redactor_buffer_free(buffer: *mut ForeignBuffer);
}

/// Reasons the canonical executor or its ABI refused an operation.
#[cfg(feature = "static-executor")]
#[derive(Debug)]
pub enum RedactError {
    AbiVersion { expected: u32, actual: u32 },
    InvalidPlan(String),
    UnknownHandle(String),
    RedactionFailed(String),
    InvalidArgument(String),
    ExecutorPanic(String),
    UnknownStatus(i32, String),
}

#[cfg(feature = "static-executor")]
impl std::fmt::Display for RedactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AbiVersion { expected, actual } => {
                write!(
                    f,
                    "redactor ABI version mismatch: expected {expected}, got {actual}"
                )
            }
            Self::InvalidPlan(message) => write!(f, "invalid redaction plan: {message}"),
            Self::UnknownHandle(message) => write!(f, "unknown redaction plan handle: {message}"),
            Self::RedactionFailed(message) => write!(f, "redaction failed: {message}"),
            Self::InvalidArgument(message) => write!(f, "invalid redactor argument: {message}"),
            Self::ExecutorPanic(message) => write!(f, "redactor panic: {message}"),
            Self::UnknownStatus(status, message) => {
                write!(f, "redactor returned unknown status {status}: {message}")
            }
        }
    }
}

#[cfg(feature = "static-executor")]
impl std::error::Error for RedactError {}

#[cfg(feature = "static-executor")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShapeEnforcement {
    Off,
    On,
}

#[cfg(feature = "static-executor")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepMode {
    Wipe,
    Keep,
}

/// Result of looking up one globally unique positive user ID.
#[cfg(feature = "static-executor")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PuidLookupResult {
    Found(i64),
    Missing,
    Error,
}

/// Validates that every `GENERATE_PUID` action targets a native int64 and returns whether the plan
/// needs the regional lookup. String PUIDs are a Classic-only legacy contract.
#[cfg(feature = "static-executor")]
pub fn plan_requires_puid_lookup(plan_bytes: &[u8]) -> Result<bool, RedactError> {
    use plan_proto::field_action::Action;

    let plan = plan_proto::RedactionPlanSet::decode(plan_bytes)
        .map_err(|error| RedactError::InvalidPlan(error.to_string()))?;
    let mut requires_puid_lookup = false;
    for entry in plan.plans.iter().flat_map(|plan| &plan.actions) {
        let Some(Action::RedactFrom(redact_from)) = entry
            .action
            .as_ref()
            .and_then(|action| action.action.as_ref())
        else {
            continue;
        };
        if redact_from.transform != Some(plan_proto::RedactFromTransform::GeneratePuid as i32) {
            continue;
        }
        if redact_from.puid_target_type != Some(plan_proto::PuidTargetType::Int64 as i32) {
            return Err(RedactError::InvalidPlan(
                "Vector GENERATE_PUID targets must be INT64".to_owned(),
            ));
        }
        requires_puid_lookup = true;
    }
    Ok(requires_puid_lookup)
}

#[cfg(feature = "static-executor")]
pub fn register_plan(plan_bytes: &[u8]) -> Result<u64, RedactError> {
    register_plan_with_key(plan_bytes, None)
}

#[cfg(feature = "static-executor")]
pub fn register_plan_with_key(
    plan_bytes: &[u8],
    hmac_key: Option<&[u8]>,
) -> Result<u64, RedactError> {
    ensure_abi_version()?;
    let (key_ptr, key_len) = hmac_key
        .map(|key| (key.as_ptr(), key.len()))
        .unwrap_or((ptr::null(), 0));
    let mut handle = 0;
    let mut error = ForeignBuffer::EMPTY;
    // SAFETY: Both input slices remain live for the call. The output pointers reference distinct,
    // aligned, initialized locals and do not alias either input.
    let status = unsafe {
        db_redactor_register_plan(
            plan_bytes.as_ptr(),
            plan_bytes.len(),
            key_ptr,
            key_len,
            &mut handle,
            &mut error,
        )
    };
    status_result(status, &error)?;
    if handle == 0 {
        return Err(RedactError::InvalidPlan(
            "executor returned an empty handle".to_owned(),
        ));
    }
    Ok(handle)
}

#[cfg(feature = "static-executor")]
pub fn redact(
    handle: u64,
    record: &[u8],
    enforcement: ShapeEnforcement,
) -> Result<Bytes, RedactError> {
    redact_with_mode(handle, record, enforcement, KeepMode::Wipe)
}

#[cfg(feature = "static-executor")]
pub fn redact_with_mode(
    handle: u64,
    record: &[u8],
    enforcement: ShapeEnforcement,
    keep_mode: KeepMode,
) -> Result<Bytes, RedactError> {
    ensure_abi_version()?;
    let mut output = ForeignBuffer::EMPTY;
    let mut error = ForeignBuffer::EMPTY;
    // SAFETY: The record remains live for the call. The output pointers reference distinct,
    // aligned, initialized locals and do not alias the input.
    let status = unsafe {
        db_redactor_redact(
            handle,
            record.as_ptr(),
            record.len(),
            u8::from(enforcement == ShapeEnforcement::On),
            u8::from(keep_mode == KeepMode::Keep),
            // TODO: Plumb shard_name, workspace_fallback, account_fallback, and
            // metastore_fallback from the Vector event context.
            ForeignStringView::EMPTY,
            ForeignStringView::EMPTY,
            ForeignStringView::EMPTY,
            ForeignStringView::EMPTY,
            &mut output,
            &mut error,
        )
    };
    status_result(status, &error)?;
    Ok(output.into_bytes())
}

/// Redacts a record while resolving `GENERATE_PUID` actions through a synchronous lookup.
#[cfg(feature = "static-executor")]
pub fn redact_with_puid_lookup<F>(
    handle: u64,
    record: &[u8],
    enforcement: ShapeEnforcement,
    keep_mode: KeepMode,
    lookup: &F,
) -> Result<Bytes, RedactError>
where
    F: Fn(i64) -> PuidLookupResult,
{
    ensure_abi_version()?;
    let mut output = ForeignBuffer::EMPTY;
    let mut error = ForeignBuffer::EMPTY;
    let lookup_context = ptr::from_ref(lookup).cast_mut().cast::<c_void>();
    // SAFETY: The record and lookup remain live for this synchronous call. The callback context
    // points to `lookup`, and both output pointers reference distinct initialized locals.
    let status = unsafe {
        db_redactor_redact_with_puid_lookup(
            handle,
            record.as_ptr(),
            record.len(),
            u8::from(enforcement == ShapeEnforcement::On),
            u8::from(keep_mode == KeepMode::Keep),
            ForeignStringView::EMPTY,
            ForeignStringView::EMPTY,
            ForeignStringView::EMPTY,
            ForeignStringView::EMPTY,
            Some(puid_lookup_callback::<F>),
            lookup_context,
            &mut output,
            &mut error,
        )
    };
    status_result(status, &error)?;
    Ok(output.into_bytes())
}

#[cfg(feature = "static-executor")]
unsafe extern "C-unwind" fn puid_lookup_callback<F>(
    context: *mut c_void,
    user_id: i64,
    out_puid: *mut i64,
) -> i32
where
    F: Fn(i64) -> PuidLookupResult,
{
    if context.is_null() || out_puid.is_null() {
        return PUID_LOOKUP_ERROR;
    }
    // SAFETY: `redact_with_puid_lookup` supplies a live `F` for the synchronous ABI call.
    let lookup = unsafe { &*context.cast::<F>() };
    match panic::catch_unwind(AssertUnwindSafe(|| lookup(user_id))) {
        Ok(PuidLookupResult::Found(puid)) => {
            // SAFETY: The canonical executor supplies a writable i64 output slot.
            unsafe { out_puid.write(puid) };
            PUID_LOOKUP_FOUND
        }
        Ok(PuidLookupResult::Missing) => PUID_LOOKUP_MISSING,
        Ok(PuidLookupResult::Error) | Err(_) => PUID_LOOKUP_ERROR,
    }
}

#[cfg(feature = "static-executor")]
pub fn release_plan(handle: u64) {
    // SAFETY: Handles are opaque integers and release accepts unknown handles.
    unsafe { db_redactor_release_plan(handle) };
}

#[cfg(feature = "static-executor")]
pub fn field_value_conforms(shapes: &[i32], value: &str) -> Result<bool, RedactError> {
    ensure_abi_version()?;
    let mut conforms = 0;
    // SAFETY: Both input slices remain live for the call, and out_conforms points to a distinct,
    // aligned, initialized local.
    let status = unsafe {
        db_redactor_field_value_conforms(
            shapes.as_ptr(),
            shapes.len(),
            value.as_ptr(),
            value.len(),
            &mut conforms,
        )
    };
    status_result(status, &ForeignBuffer::EMPTY)?;
    match conforms {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(RedactError::InvalidArgument(format!(
            "executor returned invalid conformance flag {value}"
        ))),
    }
}

#[cfg(feature = "static-executor")]
fn ensure_abi_version() -> Result<(), RedactError> {
    static ACTUAL_ABI_VERSION: OnceLock<u32> = OnceLock::new();
    let actual = *ACTUAL_ABI_VERSION.get_or_init(|| {
        // SAFETY: This function has no preconditions.
        unsafe { db_redactor_abi_version() }
    });
    if actual == EXPECTED_ABI_VERSION {
        Ok(())
    } else {
        Err(RedactError::AbiVersion {
            expected: EXPECTED_ABI_VERSION,
            actual,
        })
    }
}

#[cfg(feature = "static-executor")]
fn status_result(status: i32, error: &ForeignBuffer) -> Result<(), RedactError> {
    let message = error.diagnostic();
    match status {
        0 => Ok(()),
        1 => Err(RedactError::InvalidArgument(message)),
        2 => Err(RedactError::InvalidPlan(message)),
        3 => Err(RedactError::UnknownHandle(message)),
        4 => Err(RedactError::RedactionFailed(message)),
        5 => Err(RedactError::ExecutorPanic(message)),
        status => Err(RedactError::UnknownStatus(status, message)),
    }
}

#[cfg(all(test, feature = "static-executor"))]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn uses_current_abi_version() {
        assert_eq!(
            // SAFETY: This function has no preconditions.
            unsafe { db_redactor_abi_version() },
            EXPECTED_ABI_VERSION
        );
    }

    #[test]
    fn rejects_invalid_plan_through_real_static_library() {
        assert!(matches!(
            register_plan(b"not a plan"),
            Err(RedactError::InvalidPlan(_))
        ));
    }

    #[test]
    fn unknown_handle_fails_closed() {
        assert!(matches!(
            redact(0, b"record", ShapeEnforcement::Off),
            Err(RedactError::UnknownHandle(_))
        ));
    }

    #[test]
    fn registers_redacts_and_releases_through_real_static_library() {
        // RedactionPlanSet { plans: [MessageRedactionPlan {}] }.
        let handle = register_plan_with_key(&[0x0a, 0x00], Some(b"test-key"))
            .expect("minimal plan should register");
        assert_eq!(
            redact(handle, &[], ShapeEnforcement::Off).expect("empty record should redact"),
            Bytes::new()
        );
        release_plan(handle);
        assert!(matches!(
            redact(handle, &[], ShapeEnforcement::Off),
            Err(RedactError::UnknownHandle(_))
        ));
    }

    #[test]
    fn transfers_redacted_output_without_copying() {
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![plan_proto::FieldActionEntry {
                    field_number: Some(1),
                    action: Some(plan_proto::FieldAction {
                        action: Some(plan_proto::field_action::Action::PassThrough(
                            plan_proto::PassThrough {},
                        )),
                    }),
                }],
                ..Default::default()
            }],
        };
        let handle = register_plan(&plan.encode_to_vec()).expect("plan should register");
        let record = [0x08, 0x07];
        let mut output = ForeignBuffer::EMPTY;
        let mut error = ForeignBuffer::EMPTY;

        // SAFETY: The record remains live and outputs point to initialized locals.
        let status = unsafe {
            db_redactor_redact(
                handle,
                record.as_ptr(),
                record.len(),
                0,
                0,
                ForeignStringView::EMPTY,
                ForeignStringView::EMPTY,
                ForeignStringView::EMPTY,
                ForeignStringView::EMPTY,
                &mut output,
                &mut error,
            )
        };
        status_result(status, &error).expect("record should redact");
        let foreign_data = output.data;
        let redacted = output.into_bytes();

        assert_eq!(redacted.as_ref(), record);
        assert_eq!(redacted.as_ptr(), foreign_data);
        release_plan(handle);
    }

    #[test]
    fn validates_shapes_through_real_static_library() {
        assert!(field_value_conforms(
            &[DataShape::Uuid as i32],
            "01ef1742-bfef-1d54-8e8f-0d79b856b0f3"
        )
        .expect("shape validation should succeed"));
        assert!(
            !field_value_conforms(&[DataShape::Uuid as i32], "not-a-uuid")
                .expect("shape validation should succeed")
        );
    }
}
