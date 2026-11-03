Parse and merge the optional global `pod_rate_limit` block: `mode` (`disabled`, `shadow`, or `enforce`), `events_per_minute_per_lu`, `node_capacity_lu`, and `unattributed_reserve_lu`. The reserve accepts an environment-interpolated numeric string. This parser-only change does not enable rate limiting.
https://github.com/databricks-eng/vector/pull/745
