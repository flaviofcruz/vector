Repoll the Azure blob queue immediately after a batch is successfully processed and deleted, as documented for poll_secs. Preserve backoff for empty, failed, or undeleted batches and keep byte-rate pacing.
https://github.com/databricks-eng/vector/pull/903
