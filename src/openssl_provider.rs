use openssl::error::ErrorStack;
use openssl_sys::{OSSL_LIB_CTX, OSSL_PROVIDER};
use std::{
    ffi::{CStr, c_char, c_int, c_void},
    ptr,
};

// These OpenSSL 3 functions are not exposed by the pinned openssl-sys version.
unsafe extern "C" {
    fn OSSL_PROVIDER_do_all(
        ctx: *mut OSSL_LIB_CTX,
        callback: unsafe extern "C" fn(*mut OSSL_PROVIDER, *mut c_void) -> c_int,
        callback_data: *mut c_void,
    ) -> c_int;
    fn OSSL_PROVIDER_get0_name(provider: *const OSSL_PROVIDER) -> *const c_char;
}

pub(crate) fn log_loaded_providers() {
    match loaded_providers() {
        Ok(providers) if providers.is_empty() => {
            warn!("No loaded OpenSSL providers were found.");
        }
        Ok(providers) => {
            for provider in providers {
                info!(
                    message = "OpenSSL provider loaded.",
                    provider = %provider,
                    internal_log_rate_limit = false,
                );
            }
        }
        Err(error) => warn!(%error, "Failed to enumerate loaded OpenSSL providers."),
    }
}

fn loaded_providers() -> Result<Vec<String>, ErrorStack> {
    openssl::init();
    let mut providers = Vec::<String>::new();
    // NULL selects the same default library context used by TLS and signing.
    // The callback runs synchronously; providers remains exclusively borrowed.
    let result = unsafe {
        OSSL_PROVIDER_do_all(
            ptr::null_mut(),
            collect_provider_name,
            (&mut providers as *mut Vec<String>).cast(),
        )
    };
    if result != 1 {
        return Err(ErrorStack::get());
    }
    providers.sort_unstable();
    providers.dedup();
    Ok(providers)
}

unsafe extern "C" fn collect_provider_name(
    provider: *mut OSSL_PROVIDER,
    callback_data: *mut c_void,
) -> c_int {
    if provider.is_null() || callback_data.is_null() {
        return 0;
    }
    let name = unsafe { OSSL_PROVIDER_get0_name(provider) };
    if name.is_null() {
        return 0;
    }
    // Copy the borrowed name before OpenSSL releases the callback's provider.
    let name = unsafe { CStr::from_ptr(name) }
        .to_string_lossy()
        .into_owned();
    unsafe { &mut *callback_data.cast::<Vec<String>>() }.push(name);
    1
}

#[cfg(test)]
mod tests {
    use super::loaded_providers;

    #[test]
    fn enumerates_loaded_providers() {
        let providers = loaded_providers().unwrap();
        assert!(!providers.is_empty());
        assert!(providers.iter().all(|name| !name.is_empty()));
        if let Ok(expected) = std::env::var("VECTOR_TEST_EXPECT_PROVIDER") {
            assert!(providers.contains(&expected), "{providers:?}");
        }
    }
}
