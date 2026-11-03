The gcp_gcs source now emits `queue_notification_creation_delay_seconds` for native GCS Pub/Sub notifications, using the `eventTime` attribute, matching AWS S3 and Azure Blob.
https://github.com/databricks-eng/vector/pull/873
