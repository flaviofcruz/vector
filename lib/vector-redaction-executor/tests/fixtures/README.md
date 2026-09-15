# Redaction executor test fixture

`linux-x86_64/libvector_redactor.a` is a test-only copy of the canonical Universe redaction
executor. It lets `scripts/test-databricks-redaction.sh` exercise the real static ABI and the
`apply_redaction` and `enforce_metadata_shape` VRL functions without a Universe checkout.

The fixture was built from Universe commit `27e8f99e9b912742b4abb035faed1c0e34c00f15` using
`//woodchuck/vector-redaction-ffi:vector-redaction-ffi_staticlib_linux_x86_64`, then had every
defined global except the `db_redactor_*` ABI exports namespaced in the same way as the production
Vector Docker build. Its SHA-256 is
`f54e3a6a7928643b80ab2f4fa9812f6fb99aac1b9da5e4fecf05f4ff7f7bb199`.

This archive supports Linux x86_64 tests only and must never be used by production builds. Refresh
it deliberately when the ABI contract or the wrapper's required behavior changes; ordinary
standalone Vector builds remain independent of it.
