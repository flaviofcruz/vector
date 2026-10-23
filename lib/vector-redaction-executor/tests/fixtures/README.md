# Redaction executor test fixture

`linux-x86_64/libvector_redactor.a` is a test-only copy of the canonical Universe redaction
executor. It lets `scripts/test-databricks-redaction.sh` exercise the real static ABI and the
`apply_redaction`, `enforce_metadata_shape` and `lp_splice_metadata` VRL functions without a
Universe checkout.

The fixture was built from Universe commit `0b6b56b53ac1e62c0a7f27566bdb7796a8025556` using
`//woodchuck/vector-redaction-ffi:vector-redaction-ffi_staticlib` for
`//bazel/platforms:linux-x86_64`, then had every defined global except the `db_redactor_*` ABI
exports namespaced in the same way as the production Vector Docker build. It exposes ABI version 2;
its SHA-256 is `6922f738ef8966952214eff8752e78494582748a09f0e7281d65a15c23209824`.

This archive supports Linux x86_64 tests only and must never be used by production builds. Refresh
it deliberately when the ABI contract or the wrapper's required behavior changes; ordinary
standalone Vector builds remain independent of it.
