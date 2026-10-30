# Redaction executor test fixture

`linux-x86_64/libvector_redactor.a` is a test-only copy of the canonical Universe redaction
executor. It lets `scripts/test-databricks-redaction.sh` exercise the real static ABI and the
`apply_redaction`, `enforce_metadata_shape` and `lp_splice_metadata` VRL functions without a
Universe checkout.

The fixture was built from Universe commit `2d5f8be731e6f737d0d3c93b11455428f67e1197` using
`//woodchuck/vector-redaction-ffi:vector-redaction-ffi_staticlib` for Linux x86_64, then had every
defined global except the `db_redactor_*` ABI
exports namespaced in the same way as the production Vector Docker build. It exposes ABI version 2;
its SHA-256 is `05ec8a74d9b2b6621323ce294665b3c8b3a461cf612228800ba8bf601b1e6359`.

This revision supports HEX64 STRING output for numeric PUID mappings without changing log-schema
annotations.

This archive supports Linux x86_64 tests only and must never be used by production builds. Refresh
it deliberately when the ABI contract or the wrapper's required behavior changes; ordinary
standalone Vector builds remain independent of it.
