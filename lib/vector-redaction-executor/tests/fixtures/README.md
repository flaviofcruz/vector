# Redaction executor test fixture

`linux-x86_64/libvector_redactor.a` is a test-only copy of the canonical Universe redaction
executor. It lets `scripts/test-databricks-redaction.sh` exercise the real static ABI and the
`apply_redaction`, `enforce_metadata_shape` and `lp_splice_metadata` VRL functions without a
Universe checkout.

The fixture was built from Universe commit `2f3ec9820708c22e8337926d36e8d287286d7f91` using
`//woodchuck/vector-redaction-ffi:vector-redaction-ffi_staticlib` for Linux x86_64, then had every
defined global except the `db_redactor_*` ABI
exports namespaced in the same way as the production Vector Docker build. It exposes ABI version 2;
its SHA-256 is `2913e3b4e7253b7fd8fc13330927fd33441e15c928d8e99a85b2a6ad8f09219e`.

This revision supports HEX64 STRING output for numeric PUID mappings without changing log-schema
annotations.

This archive supports Linux x86_64 tests only and must never be used by production builds. Refresh
it deliberately when the ABI contract or the wrapper's required behavior changes; ordinary
standalone Vector builds remain independent of it.
