The azure_blob source now emits `component_errors_total` for every queue, blob-fetch, blob-read and delivery failure (including permanent ones and delivery failures under `delete_failed_message`), with Vector-standard `error_type`, a bounded per-cause `error_code` (e.g. `get_blob_throttled`, `blob_not_found`, `invalid_json`) and `stage`.
https://github.com/databricks-eng/vector/pull/874
