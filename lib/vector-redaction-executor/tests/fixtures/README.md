# Redaction executor test fixture

`linux-x86_64/libvector_redactor.a` is a test-only copy of the canonical Universe redaction
executor. It lets `scripts/test-databricks-redaction.sh` exercise the real static ABI and the
`apply_redaction` and `enforce_metadata_shape` VRL functions without a Universe checkout.

The fixture was built from Universe commit `37a77756f79ea5743fc18bac8b25a4a36c42dea7` using
`//woodchuck/vector-redaction-ffi:vector-redaction-ffi_staticlib_linux_x86_64`, then had every
defined global except the `db_redactor_*` ABI exports namespaced in the same way as the production
Vector Docker build. Its SHA-256 is
`8d882d457d501818c314b3f2f93605ed878491c3e5d06a1ee7008216d34853f4`.

This archive supports Linux x86_64 tests only and must never be used by production builds. Refresh
it deliberately when the ABI contract or the wrapper's required behavior changes; ordinary
standalone Vector builds remain independent of it.
