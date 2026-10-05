# Redaction executor test fixture

`linux-x86_64/libvector_redactor.a` is a test-only copy of the canonical Universe redaction
executor. It lets `scripts/test-databricks-redaction.sh` exercise the real static ABI and the
`apply_redaction` and `enforce_metadata_shape` VRL functions without a Universe checkout.

The fixture was built from Universe commit `7d36addfdbb09595b725cf6373e278ff419251f4` using
`//woodchuck/vector-redaction-ffi:vector-redaction-ffi_staticlib` for
`//bazel/platforms:linux-x86_64`, then had every defined global except the `db_redactor_*` ABI
exports namespaced in the same way as the production Vector Docker build. It exposes ABI version 2;
its SHA-256 is `7cb19e9e718c22627bc74d508ac0a3c2c072b6bef79258660e37355c6f72f8a9`.

This archive supports Linux x86_64 tests only and must never be used by production builds. Refresh
it deliberately when the ABI contract or the wrapper's required behavior changes; ordinary
standalone Vector builds remain independent of it.
