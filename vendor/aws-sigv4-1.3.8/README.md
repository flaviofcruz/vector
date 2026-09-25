# aws-sigv4

Low-level SigV4 request signing implementations.

<!-- anchor_start:footer -->
This crate is part of the [AWS SDK for Rust](https://awslabs.github.io/aws-sdk-rust/) and the [smithy-rs](https://github.com/smithy-lang/smithy-rs) code generator. In most cases, it should not be used directly.
<!-- anchor_end:footer -->

## Databricks OpenSSL backend

Vector enables this crate's `openssl` feature whenever it includes the AWS signer.
There is no separate Vector signing-backend switch.
SHA-256 uses the safe `openssl::md::Md` and `openssl::md_ctx::MdCtx` APIs;
HMAC uses OpenSSL 3's `EVP_MAC` API with owned implementation/context wrappers.
Provider identity comes from the fetched implementations used by these operations.
These use the process-wide OpenSSL configuration shared with TLS. The original
public function signatures and AWS signature bytes are preserved. Standalone crate
builds without `openssl` retain the upstream RustCrypto backend; OpenSSL builds
reject SigV4a.

Both vendored/static and system/shared builds are supported;
this crate does not choose the linkage mode. Vector's existing build embeds OpenSSL
and loads the external FIPS provider at runtime. The OpenSSL dependencies reuse
versions already in Vector's lockfile.

For FIPS, configure the Canonical provider and `default_properties = fips=yes`
before OpenSSL initializes, using `OPENSSL_CONF` and `OPENSSL_MODULES`. The feature
selects OpenSSL, not FIPS mode: without that configuration, it can use OpenSSL's
ordinary provider. Vector startup enforcement is a separate rollout prerequisite.
The exact OpenSSL build/provider pairing, configuration and host must be qualified;
selecting this feature alone does not establish CMVP compliance.

Every successful SHA-256 or HMAC operation emits a trace event with `algorithm`
and `provider`, read from the fetched implementation used by that operation.
Provider names are inspected only when trace logging is enabled. Events contain
no keys, inputs, digests or signatures. To inspect signing operations in Vector,
set `VECTOR_LOG=info,aws_sigv4::crypto::openssl=trace`.

Vector independently enumerates its loaded OpenSSL providers at startup and logs
each name at info level, including when no AWS components are enabled. This
inventory describes loaded providers; operation trace events identify the provider
used for a particular signing operation. Neither diagnostic asserts approved-service
or CMVP status. Runtime policy must be configured before OpenSSL initializes.

Small `extern "C"` declarations cover OpenSSL 3 functions missing from the pinned
`openssl-sys` bindings. There is no signer build script or additional C library;
both operations use the same OpenSSL linkage as TLS. The signer does not load
providers, set properties, or reject the default provider.

HTTP and event-stream signing propagate crypto errors through their existing error
returns. Private fallible helpers preserve that behavior without adding public
APIs. The original `generate_signing_key` and `calculate_signature` helpers panic
on crypto failure because their upstream return types have no error case.
Caller-supplied `SignableBody::Precomputed` hashes remain the caller's responsibility.

To test this crate outside Vector's workspace, copy this directory to an isolated
location and run `cargo test --locked --lib --features sign-eventstream` there.
Repeat with `--features sign-eventstream,openssl` using OpenSSL 3 and the FIPS runtime
settings above. Tests cover AWS signing fixtures, SHA-256/HMAC known answers,
ordinary OpenSSL configuration, operation-provider logging, and errors when the test
configuration requires FIPS but its provider is unavailable (`fips-required-test.cnf`).
Run SigV4a tests separately without `openssl`.
