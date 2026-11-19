The `aws_s3` `sqs`, `azure_blob` `queue` and `gcp_gcs` `pubsub` sources now report an `object_storage_client_concurrency` gauge with the configured `client_concurrency` ceiling, even when adaptive concurrency is on. It is refreshed while the source runs, including when idle.
https://github.com/databricks-eng/vector/pull/996
