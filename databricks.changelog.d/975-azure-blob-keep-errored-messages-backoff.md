azure_blob source: a message whose events the sink reports as errored is no longer deleted (delete_failed_message now applies to rejected messages only). It stays on the queue and is retried once its visibility timeout expires.
https://github.com/databricks-eng/vector/pull/975
