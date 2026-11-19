Add Azure Blob and GCS fakes to the Databricks bench harness (`fake-azure-blob`, `fake-gcs`) and the `azure_blob` and `gcs` sink kinds, so `check`, `bench` and `verify` run the Hydra pipeline with the `azure_blob` and `gcp_cloud_storage` sinks; their objects read back as S3's do.
https://github.com/databricks-eng/vector/pull/997
