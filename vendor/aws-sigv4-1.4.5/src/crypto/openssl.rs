use super::CryptoError;
use foreign_types::ForeignType;
use openssl::{md::Md, md_ctx::MdCtx};
use openssl_sys as ffi;
use std::ffi::{c_char, CStr};
use std::ptr::{self, NonNull};

// These OpenSSL 3 functions are not exposed by the pinned openssl-sys version.
unsafe extern "C" {
    fn EVP_MD_get0_provider(md: *const ffi::EVP_MD) -> *const ffi::OSSL_PROVIDER;
    fn OSSL_PROVIDER_get0_name(provider: *const ffi::OSSL_PROVIDER) -> *const c_char;
    fn OSSL_PARAM_construct_utf8_string(
        key: *const c_char,
        value: *mut c_char,
        size: usize,
    ) -> ffi::OSSL_PARAM;
}

struct FetchedMac(NonNull<ffi::EVP_MAC>);

impl Drop for FetchedMac {
    fn drop(&mut self) {
        // This is the reference returned by EVP_MAC_fetch.
        unsafe { ffi::EVP_MAC_free(self.0.as_ptr()) };
    }
}

struct MacContext(NonNull<ffi::EVP_MAC_CTX>);

impl Drop for MacContext {
    fn drop(&mut self) {
        // The context also owns a reference to its fetched MAC implementation.
        unsafe { ffi::EVP_MAC_CTX_free(self.0.as_ptr()) };
    }
}

pub(crate) fn sha256(bytes: &[u8]) -> Result<[u8; 32], CryptoError> {
    openssl::init();
    // Default context/properties inherit process configuration, including OPENSSL_CONF.
    let digest = Md::fetch(None, "SHA256", None)?;
    let mut context = MdCtx::new()?;
    context.digest_init(&digest)?;
    context.digest_update(bytes)?;
    let mut output = [0; 32];
    let length = context.digest_final(&mut output)?;
    assert_eq!(length, output.len(), "SHA-256 is 32 bytes");
    // The fetched digest used by this context still owns the provider reference.
    unsafe { log_algorithm_provider("SHA256", EVP_MD_get0_provider(digest.as_ptr())) };
    Ok(output)
}

pub(crate) fn hmac_sha256(key: &[u8], bytes: &[u8]) -> Result<[u8; 32], CryptoError> {
    openssl::init();
    // Fetch and use the same object so the log identifies this operation's provider.
    // NULL context/properties leave all provider policy to process configuration.
    let mac = unsafe { ffi::EVP_MAC_fetch(ptr::null_mut(), c"HMAC".as_ptr(), ptr::null()) };
    let mac = FetchedMac(NonNull::new(mac).ok_or_else(CryptoError::get)?);
    let ctx = unsafe { ffi::EVP_MAC_CTX_new(mac.0.as_ptr()) };
    let ctx = MacContext(NonNull::new(ctx).ok_or_else(CryptoError::get)?);
    let mut digest_name = *b"SHA256\0";
    let mut signature = [0; 32];
    let mut length = 0;
    // All buffers and parameters outlive these calls; ctx owns the MAC reference.
    unsafe {
        let params = [
            OSSL_PARAM_construct_utf8_string(
                c"digest".as_ptr(),
                digest_name.as_mut_ptr().cast(),
                0,
            ),
            ffi::OSSL_PARAM_construct_end(),
        ];
        if ffi::EVP_MAC_init(ctx.0.as_ptr(), key.as_ptr(), key.len(), params.as_ptr()) != 1
            || ffi::EVP_MAC_update(ctx.0.as_ptr(), bytes.as_ptr(), bytes.len()) != 1
            || ffi::EVP_MAC_final(
                ctx.0.as_ptr(),
                signature.as_mut_ptr(),
                &mut length,
                signature.len(),
            ) != 1
        {
            return Err(CryptoError::get());
        }
    }
    assert_eq!(length, signature.len(), "HMAC-SHA256 is 32 bytes");
    // mac owns the implementation used to create this operation's context.
    unsafe { log_algorithm_provider("HMAC-SHA256", ffi::EVP_MAC_get0_provider(mac.0.as_ptr())) };
    Ok(signature)
}

// The caller keeps the implementation owning the provider alive for this call.
unsafe fn log_algorithm_provider(algorithm: &'static str, provider: *const ffi::OSSL_PROVIDER) {
    if !tracing::enabled!(tracing::Level::TRACE) {
        return;
    }
    let provider = if provider.is_null() {
        "unknown"
    } else {
        let name = unsafe { OSSL_PROVIDER_get0_name(provider) };
        if name.is_null() {
            "unknown"
        } else {
            unsafe { CStr::from_ptr(name) }
                .to_str()
                .unwrap_or("non-UTF-8")
        }
    };
    // Vector rate-limits console events by default; tracing must retain every operation.
    tracing::trace!(
        algorithm,
        provider,
        internal_log_rate_limit = false,
        "OpenSSL signing operation completed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_answers() {
        for (input, expected) in [
            (
                "",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                "abc",
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
            ),
        ] {
            assert_eq!(hex::encode(sha256(input.as_bytes()).unwrap()), expected);
        }
    }

    #[test]
    fn rfc4231_hmac_known_answers() {
        for (key, input, expected) in [
            (
                vec![0x0b; 20],
                "Hi There",
                "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
            ),
            (
                vec![0xaa; 131],
                "Test Using Larger Than Block-Size Key - Hash Key First",
                "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
            ),
        ] {
            assert_eq!(
                hex::encode(hmac_sha256(&key, input.as_bytes()).unwrap()),
                expected
            );
        }
    }

    // OpenSSL configuration is process-global; isolate each configuration in a
    // subprocess before either wrapper initializes the library.
    fn run_with_config(test: &str, config: &std::path::Path) -> bool {
        const CHILD: &str = "AWS_SIGV4_OPENSSL_CONFIG_TEST";
        if std::env::var_os(CHILD).is_some() {
            return false;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env(CHILD, "1")
            .env("OPENSSL_CONF", config)
            .env("OPENSSL_MODULES", "/nonexistent-sigv4-provider-directory")
            .env("OPENSSL_FORCE_FIPS_MODE", "0")
            .env_remove("AWS_SIGV4_TEST_EXPECT_PROVIDER")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        true
    }

    #[test]
    fn uses_default_provider_without_fips_configuration() {
        if run_with_config(
            "crypto::openssl::tests::uses_default_provider_without_fips_configuration",
            std::path::Path::new("/dev/null"),
        ) {
            return;
        }
        sha256_known_answers();
        rfc4231_hmac_known_answers();
    }

    #[test]
    fn traces_each_operation_without_request_data() {
        if run_with_config(
            "crypto::openssl::tests::traces_each_operation_without_request_data",
            std::path::Path::new("/dev/null"),
        ) {
            return;
        }
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};
        use tracing::{field::Visit, span, Event, Metadata, Subscriber};

        #[derive(Clone)]
        struct Logs(Arc<Mutex<Vec<BTreeMap<String, String>>>>, tracing::Level);

        struct Fields(BTreeMap<String, String>);

        impl Visit for Fields {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().into(), value.into());
            }

            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }
        }

        impl Subscriber for Logs {
            fn enabled(&self, metadata: &Metadata<'_>) -> bool {
                metadata.level() <= &self.1
            }
            fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
                span::Id::from_u64(1)
            }
            fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
            fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
            fn enter(&self, _: &span::Id) {}
            fn exit(&self, _: &span::Id) {}
            fn event(&self, event: &Event<'_>) {
                assert_eq!(event.metadata().level(), &tracing::Level::TRACE);
                let mut fields = Fields(BTreeMap::new());
                event.record(&mut fields);
                self.0.lock().unwrap().push(fields.0);
            }
        }

        let debug_logs = Logs(Arc::default(), tracing::Level::DEBUG);
        tracing::subscriber::with_default(debug_logs.clone(), || {
            sha256(b"request before trace logging is enabled").unwrap();
            hmac_sha256(b"private signing key", b"request before trace logging").unwrap();
        });
        assert!(debug_logs.0.lock().unwrap().is_empty());

        let logs = Logs(Arc::default(), tracing::Level::TRACE);
        tracing::subscriber::with_default(logs.clone(), || {
            sha256(b"private request bytes").unwrap();
        });
        assert_eq!(logs.0.lock().unwrap().len(), 1);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let logs = logs.clone();
                scope.spawn(move || {
                    tracing::subscriber::with_default(logs, || {
                        sha256(b"private request bytes").unwrap();
                        hmac_sha256(b"private signing key", b"private signing input").unwrap();
                    });
                });
            }
        });
        tracing::subscriber::with_default(logs.clone(), || {
            hmac_sha256(b"private signing key", b"private signing input").unwrap();
        });
        let events = logs.0.lock().unwrap();
        assert_eq!(events.len(), 18);
        let expected_provider =
            std::env::var("AWS_SIGV4_TEST_EXPECT_PROVIDER").unwrap_or_else(|_| "default".into());
        for event in events.iter() {
            assert!(matches!(
                event["algorithm"].as_str(),
                "SHA256" | "HMAC-SHA256"
            ));
            assert_eq!(
                event,
                &BTreeMap::from([
                    (
                        "message".into(),
                        "OpenSSL signing operation completed".into()
                    ),
                    ("algorithm".into(), event["algorithm"].clone()),
                    ("provider".into(), expected_provider.clone()),
                    ("internal_log_rate_limit".into(), "false".into()),
                ])
            );
        }
        for algorithm in ["SHA256", "HMAC-SHA256"] {
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event["algorithm"] == algorithm)
                    .count(),
                9
            );
        }
    }

    #[test]
    fn missing_provider_has_no_fallback() {
        let config = std::path::Path::new(file!())
            .with_file_name("fips-required-test.cnf")
            .canonicalize()
            .unwrap();
        if run_with_config(
            "crypto::openssl::tests::missing_provider_has_no_fallback",
            &config,
        ) {
            return;
        }

        // The process configuration must prohibit fallback even when the
        // default provider is loaded and available for an explicit lookup.
        let _default_provider = ::openssl::provider::Provider::load(None, "default").unwrap();
        ::openssl::md::Md::fetch(None, "SHA256", Some("provider=default,fips=no")).unwrap();
        assert!(sha256(b"payload").is_err());
        let key = [0x0b; 32];
        assert!(hmac_sha256(&key, b"message").is_err());
        assert!(crate::sign::v4::try_generate_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            std::time::SystemTime::UNIX_EPOCH,
            "us-east-1",
            "s3",
        )
        .is_err());
        assert!(crate::sign::v4::try_calculate_signature(key, b"message").is_err());
        assert!(
            std::panic::catch_unwind(|| crate::sign::v4::calculate_signature(key, b"message"))
                .is_err()
        );

        #[cfg(feature = "sign-http")]
        request_errors_reach_caller();
        #[cfg(feature = "sign-eventstream")]
        eventstream_errors_reach_caller();
    }

    #[cfg(feature = "sign-http")]
    fn request_errors_reach_caller() {
        use crate::http_request::{
            sign, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
        };
        let identity = aws_credential_types::Credentials::for_tests().into();
        for location in [SignatureLocation::Headers, SignatureLocation::QueryParams] {
            // Bytes fails at payload hashing; UnsignedPayload fails at canonical
            // request hashing. Neither may return partial signing instructions.
            for body in [
                SignableBody::Bytes(b"payload"),
                SignableBody::UnsignedPayload,
            ] {
                let settings = SigningSettings {
                    signature_location: location,
                    expires_in: Some(std::time::Duration::from_secs(60)),
                    ..Default::default()
                };
                let params = crate::sign::v4::SigningParams::builder()
                    .identity(&identity)
                    .region("us-east-1")
                    .name("s3")
                    .time(std::time::SystemTime::UNIX_EPOCH)
                    .settings(settings)
                    .build()
                    .unwrap()
                    .into();
                let request = SignableRequest::new(
                    "PUT",
                    "https://s3.us-east-1.amazonaws.com/bucket/key",
                    std::iter::empty(),
                    body,
                )
                .unwrap();
                let error = sign(request, &params).unwrap_err();
                assert!(std::error::Error::source(&error).is_some());
            }
        }
    }

    #[cfg(feature = "sign-eventstream")]
    fn eventstream_errors_reach_caller() {
        use crate::event_stream::{sign_empty_message, sign_message};
        let identity = aws_credential_types::Credentials::for_tests().into();
        let params = crate::sign::v4::SigningParams::builder()
            .identity(&identity)
            .region("us-east-1")
            .name("kinesis")
            .time(std::time::SystemTime::UNIX_EPOCH)
            .settings(())
            .build()
            .unwrap();
        let message = aws_smithy_types::event_stream::Message::new(&b"payload"[..]);
        assert!(sign_message(&message, "previous-signature", &params).is_err());
        assert!(sign_empty_message("previous-signature", &params).is_err());
    }
}
