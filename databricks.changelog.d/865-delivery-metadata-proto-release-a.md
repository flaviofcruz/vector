Add fork-only delivery metadata copies at protobuf fields 100-102. Readers prefer them and fall back to 8-10. Writers keep writing 8-10, so this release is safe with plain 0.53 peers. Upstream uses 8 and 9 for trace_layout and datadog_metric_unit.
https://github.com/databricks-eng/vector/pull/865
