Accept `"gzip"` and `"none"` for the `vector` sink `compression` option alongside the boolean, so one rendered config loads on this build and newer Vector releases. `"zstd"` is rejected with a clear error; boolean behavior is unchanged.
https://github.com/databricks-eng/vector/pull/864
