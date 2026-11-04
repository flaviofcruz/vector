azure_blob source: read every page of a blob (chained ranged GETs pinned by ETag) instead of only the first 16 MiB, and redeliver on a short read rather than acking it; `tolerate_truncated_archives` now applies only to fully read, torn archives. Adds `blob_read_chunk_size_bytes` (default 16 MiB) to set the ranged-GET size.
https://github.com/databricks-eng/vector/pull/876
