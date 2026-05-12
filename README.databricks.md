This lists custom changes merged in Databricks fork of Vector.
1. Fix premature ack when data file is full. https://github.com/databricks/vector/pull/1
2. Retry s3 request even when observing ConstructionFailure to avoid data loss. https://github.com/databricks/vector/pull/2
3. Updating/adding INFO logs for when vector sends to cloud storage. https://github.com/databricks/vector/pull/5
4. Allow retries on all sink exceptions https://github.com/databricks/vector/pull/7
5. Also allowing retries on AccessDenied exceptions in AWS https://github.com/databricks/vector/pull/12
6. Updating version to also carry a Databricks version https://github.com/databricks/vector/pull/13
7. Add a new event for successful upload to cloud storage (+ rework old send) https://github.com/databricks/vector/pull/14
8. Add new Vector events for topology events (new source/sink creation, vector start/stop) https://github.com/databricks/vector/pull/17
9. Provide an option to override the Content-Encoding header for files uploaded by Google Cloud Storage sink https://github.com/databricks/vector/pull/30
10. Add functionality to derive topic from file upload path https://github.com/databricks/vector/pull/33
11. Update event logs to support emitting granular upload events https://github.com/databricks/vector/pull/35
12. Add event logs for GCP sink https://github.com/databricks-eng/vector/pull/148
13. Provide an option to attach additional headers to the HTTP request made by the prometheus remote write sink https://github.com/databricks-eng/vector/pull/201
14. Add option to limit number of concurrent requests in http and vector https://github.com/databricks-eng/vector/pull/208
15. Changed `generate` so that both JSON and YAML generated configurations are compared on their content, not on the ordering to fix failing tests. https://github.com/databricks-eng/vector/pull/239
16. Added an option to reload any sink by watching a set of files for changes https://github.com/databricks-eng/vector/pull/232
17. Added custom workflows to validate Rust Formatting, pre-commit checks and ensuring all code changes make an addition to this change log to ensure the PR is merged https://github.com/databricks-eng/vector/pull/241
18. reverts a previous change to print info log. https://github.com/databricks-eng/vector/pull/249
19. Add new trace filter to avoid sending event logs to stdout https://github.com/databricks-eng/vector/pull/236
20. Added custom Kafka Producer Proxy (KPP) Sink to enable sending event logs to Databricks' internal Kafka server and adds topic blacklisting https://github.com/databricks-eng/vector/pull/256/. Edit: improved logging in https://github.com/databricks-eng/vector/pull/262
21. Enhance throttle transform to drop with configured status https://github.com/databricks-eng/vector/pull/260
22. Add starting_reading_at to file source to only pick up the file created after a specified timestamp upon scanning. https://github.com/databricks-eng/vector/pull/263
23. Add support for SP based certificate authentication for "azure_blob" source. https://github.com/databricks-eng/vector/pull/266
24. Make vrl repo dependency as path dependency https://github.com/databricks-eng/vector/pull/268
25. Add new option in file source to specify TTL removal behavior. https://github.com/databricks-eng/vector/pull/267
26. Modify the start_reading_at field to take iso 8601 timestamp format. https://github.com/databricks-eng/vector/pull/270
27. Add AI rule files in .gitignore and format the codebase. https://github.com/databricks-eng/vector/pull/275
28. Modify the `aws_s3` and `azure_blob` sources to support metric for ingestion lag. https://github.com/databricks-eng/vector/pull/273
29. Do not register a FileWatcher for inactive files. https://github.com/databricks-eng/vector/pull/279
30. do not shutdown internal sources on shutdown trigger. https://github.com/databricks-eng/vector/pull/280
31. Add validation for iso-8601 timestamp parsing for start_reading_at. https://github.com/databricks-eng/vector/pull/282
32. Add new delivery event INFO logs https://github.com/databricks-eng/vector/pull/281
33. Initialize the `clickhouse_dedupe` helper module for sources. https://github.com/databricks-eng/vector/pull/274
34. Add `clickhouse_dedupe` configuration options for `aws_s3` and `azure_blob` sources. https://github.com/databricks-eng/vector/pull/283
35. Add support for specifying WIF authentication for AWS authentication. https://github.com/databricks-eng/vector/pull/285
36. Disable unhelpful GitHub workflows and add workflows for running build / unit / formatting tests. https://github.com/databricks-eng/vector/pull/276
37. Make enrichment table hot reload on file changes. https://github.com/databricks-eng/vector/pull/287
38. Reapply missed change during version upgrade. https://github.com/databricks-eng/vector/pull/295
39. Handle file change event meant for reload from disk. https://github.com/databricks-eng/vector/pull/296
40. Add `redact` transform based on the logging redactor in `universe` https://github.com/databricks-eng/vector/pull/293
41. Fix Databricks GitHub workflows to point to the correct VRL branch and build correctly. https://github.com/databricks-eng/vector/pull/297
42. Add back missing GCP send events.  https://github.com/databricks-eng/vector/pull/300
43. Add a Databricks ZeroBus sink to vector. https://github.com/databricks-eng/vector/pull/294 https://github.com/databricks-eng/vector/pull/305 https://github.com/databricks-eng/vector/pull/350 https://github.com/databricks-eng/vector/pull/406
44. Add metrics for the `redact` transform https://github.com/databricks-eng/vector/pull/298
45. Fix to handle shutdown signal in case of disk buffer with batched sink https://github.com/databricks-eng/vector/pull/306
46. Turn off internal log rate limits for VEL https://github.com/databricks-eng/vector/pull/308
47. Add back file events w/o rate limiting https://github.com/databricks-eng/vector/pull/309
48. Adding prometheus_k8s_scrape source to identify the pods with named port and scrape them https://github.com/databricks-eng/vector/pull/311
49. Adding a new field `emit_pod_metadata` to `prometheus_k8s_scrape` source https://github.com/databricks-eng/vector/pull/314
50. Emit new file send events for log locator https://github.com/databricks-eng/vector/pull/316
51. Add source context config field for file source https://github.com/databricks-eng/vector/pull/321
52. Instrument vector sink with delivery events + common event log structures https://github.com/databricks-eng/vector/pull/303
53. Add source context to file read events https://github.com/databricks-eng/vector/pull/335
54. Insturment S3/Kinesis sinks with delivery events https://github.com/databricks-eng/vector/pull/334
55. Adds an option to the `kubernetes_logs` to allow it to scrape Databricks container log locations https://github.com/databricks-eng/vector/pull/312
56. Adds a raw log parsing option to `kubernetes_logs` which will be enabled when databricks file extraction is enabled. https://github.com/databricks-eng/vector/pull/318
57. Emit file event from new spot (after multiline) https://github.com/databricks-eng/vector/pull/340
58. Adds a mapping to the kubernetes_logs source which will allow it to annotate logs that don't match the standard kubernetes file pattern. https://github.com/databricks-eng/vector/pull/339
59. Adds an option to the kuberenetes_logs source which allows it to specify a FileTTLRemovalConfig, similar to the file source. https://github.com/databricks-eng/vector/pull/346
60. Adds an option to the kuberenetes_logs source which splits the pod log discovery behavior explicitly by whether or not we expect to rely on the logging hostpath annotation to discovery the log directory. https://github.com/databricks-eng/vector/pull/347
61. Allow for safe fallback on kuberenetes_logs source file-annotation failure. https://github.com/databricks-eng/vector/pull/351
62. Instrument blackhole sink with delivery events https://github.com/databricks-eng/vector/pull/352
63. Parse file paths for container name for deployment logs https://github.com/databricks-eng/vector/pull/360 https://github.com/databricks-eng/vector/pull/362
64. Add $POD_NAME substitution in hostpath logging annotation for kubernetes_logs source https://github.com/databricks-eng/vector/pull/361
65. Add remove_after, source_context, start_reading_at and multiline config in k8s log https://github.com/databricks-eng/vector/pull/363 https://github.com/databricks-eng/vector/pull/366
66. Add trace logging to kubernetes_logs source for path resolution and debugging; use dblet.dev/pod-name annotation for $POD_NAME substitution https://github.com/databricks-eng/vector/pull/365
67. Refactor send events + emit error events https://github.com/databricks-eng/vector/pull/364
68. refactor delivery event log in k8s log source https://github.com/databricks-eng/vector/pull/367
69. temporarily disable some logs for known issue to stop polluting logs https://github.com/databricks-eng/vector/pull/368
70. Instrument file sink with delivery events https://github.com/databricks-eng/vector/pull/371
71. Setting THANOS-TENANT header in prometheus remote write sink - https://github.com/databricks-eng/vector/pull/378
72. Getting pod name and namespace from pod metadata - https://github.com/databricks-eng/vector/pull/379
73. Adding Vector File Event as a Vector Event Type to the Internal Events - https://github.com/databricks-eng/vector/pull/380
74. Reorganize list of directories scanned by the DB logs source https://github.com/databricks-eng/vector/pull/369
75. Add Unity Catalog schema fetching for Databricks ZeroBus sink with OAuth authentication, dynamic protobuf schema generation from Unity Catalog API, full complex type support (structs, arrays, maps), and comprehensive unit tests for schema conversion https://github.com/databricks-eng/vector/pull/382
76. Add overridable message count option for event logs https://github.com/databricks-eng/vector/pull/370
77. Adding annotation name based pod discovery for prometheus k8s scrape source - https://github.com/databricks-eng/vector/pull/389
78. make inter component buffer parameter configurable. https://github.com/databricks-eng/vector/pull/400
79. Add observability metrics for ClickHouse sink batching and insert latency https://github.com/databricks-eng/vector/pull/401
80. Add a bricklens ingest sink - https://github.com/databricks-eng/vector/pull/385
81. Instrument Kafka sink https://github.com/databricks-eng/vector/pull/409
82. Use option to use dynamic allocation in fingerprinter https://github.com/databricks-eng/vector/pull/413
83. Add support for static bearer token authentication in GCP auth config. https://github.com/databricks-eng/vector/pull/415
84. Add support for direct ingest messages in aws_s3 source via SQS https://github.com/databricks-eng/vector/pull/416
85. Add support for direct ingest messages in azure_blob source via Azure Queue https://github.com/databricks-eng/vector/pull/423
86. Delete the unused clickhouse deduplication client from the aws_s3 and azure_blob sources https://github.com/databricks-eng/vector/pull/424
87. Fix MAP fields nested inside structs to use label=Repeated so they are correctly encoded instead of being silently dropped https://github.com/databricks-eng/vector/pull/428
88. Add `last_config_reload_success` gauge metric to reflect the current state of the most recent config reload attempt https://github.com/databricks-eng/vector/pull/426
89. Add JSON encoding support for file enrichment tables, enabling hot-reload of JSON files as enrichment sources without requiring CSV conversion. https://github.com/databricks-eng/vector/pull/434
90. Implement two-wave shutdown for vector-daemon, gated by a flag. https://github.com/databricks-eng/vector/pull/419
91. expose file fingerprint and offset on each event https://github.com/databricks-eng/vector/pull/436
92. Removing default exclusion label for prometheus scrape. https://github.com/databricks-eng/vector/pull/435
93. Add sink request retry and failure rate metrics https://github.com/databricks-eng/vector/pull/425
94. Handle object/blob not found (404) in `aws_s3` and `azure_blob` sources by deleting the queue message instead of retrying indefinitely https://github.com/databricks-eng/vector/pull/433
95. Add cryptographic nonce file prefix functionality to cloud blob sinks - https://github.com/databricks-eng/vector/pull/414
96. Add config-driven ingestion callback component for object storage sources (aws_s3, azure_blob) to notify upstream services on file processing completion https://github.com/databricks-eng/vector/pull/429
97. Add required file_id field to direct ingest messages in aws_s3 and azure_blob sources for ingestion callback integration https://github.com/databricks-eng/vector/pull/430
98. Modifies termination behavior for kubernetes_logs to deliver logs up until the termination time - https://github.com/databricks-eng/vector/pull/402
99. Integrate ingestion callback component with aws_s3 and azure_blob sources to notify upstream services on direct-ingest file processing completion https://github.com/databricks-eng/vector/pull/431
100. Fix file source infinite retry loop when `remove_after_secs` is configured and the file is already deleted externally (e.g. kubelet cleaning up emptyDir volumes on pod termination) https://github.com/databricks-eng/vector/pull/443
101. Emit `VECTOR_PROCESS_COMPONENTS_CLOSED` VEL event during shutdown to signal that graceful shutdown completed successfully https://github.com/databricks-eng/vector/pull/438
102. add is_done in file checkpoints to represent archieved file has reached eof https://github.com/databricks-eng/vector/pull/448
103. Support reading from in-progress gzipped files https://github.com/databricks-eng/vector/pull/447
104. Stop overwriting event timestamps with Utc::now() in DatabricksParser for kubernetes_logs source https://github.com/databricks-eng/vector/pull/451
105. Add optional `host_key` metadata field to `kubernetes_logs` source to attach hostname to each event https://github.com/databricks-eng/vector/pull/453
106. Make `line_delimiter` configurable in the `kubernetes_logs` source to match file source behavior https://github.com/databricks-eng/vector/pull/454
107. Add encoding/charset transcoding support to kubernetes_logs source, allowing non-UTF-8 log files to be transcoded to UTF-8 on ingestion https://github.com/databricks-eng/vector/pull/455
108. Add static custom HTTP header support to ingestion callback endpoints (`CallbackEndpointConfig`), with startup validation and reserved-header protection for `content-type` and `authorization` https://github.com/databricks-eng/vector/pull/450
109. Add `gcp_gcs` source for ingesting logs from Google Cloud Storage via a Pub/Sub subscription, with shared compression detection/decoding across all object-storage sources https://github.com/databricks-eng/vector/pull/439 https://github.com/databricks-eng/vector/pull/440 https://github.com/databricks-eng/vector/pull/441 https://github.com/databricks-eng/vector/pull/442
110. Fix incorrect protobuf type mappings for Delta DATE and TIMESTAMP columns in the Zerobus sink https://github.com/databricks-eng/vector/pull/463
111. Add shared Kubernetes API watcher registry for `kubernetes_logs` source to deduplicate watch connections across multiple source instances with identical watcher parameters https://github.com/databricks-eng/vector/pull/467
112. Instrument `azure_blob` and native `kafka` sinks with VECTOR_LOG_DELIVERY_EVENT / VECTOR_FILE_SEND_EVENT emissions (via EventLoggingService wrapper)
113. Skip `emptyDir` path enumeration in `kubernetes_logs` source for pods that do not declare an `emptyDir` volume, eliminating ENOENT-triggered `reader_failed`/`command_failed` errors for infrastructure DaemonSets (e.g. `netmon`, `kube-proxy`) when `extract_databricks_logs` is enabled https://github.com/databricks-eng/vector/pull/475
114. Add host path key to kuberenetes log source for different host path
     mounths https://github.com/databricks-eng/vector/pull/473 
115. flush and ack before closing sink and buffer https://github.com/databricks-eng/vector/pull/480
116. avoid fingerprinting archieve file https://github.com/databricks-eng/vector/pull/481
117. Revert https://github.com/databricks-eng/vector/pull/475 and instead downgrade the `emptyDir` `read_dir` failure log in `get_databricks_pod_logs_directories` from `warn!` to `trace!` to silence ENOENT noise for pods without an `emptyDir` volume
118. Two-wave shutdown observability and responsiveness fix: (a) log deferred/non-deferred component classifications at wave 1 start and a components-still-active snapshot at wave 2 start in the two-wave shutdown path, and (b) race the `demo_logs` source's interval tick against its `ShutdownSignal` via `tokio::select!` so the source reacts to SIGTERM without waiting for the next tick (previously blocked for up to the configured interval, so long-interval `demo_logs` instances stalled wave 1 until force-terminated) https://github.com/databricks-eng/vector/pull/487
119. Drain buffered events in the `internal_logs` source on shutdown: replace `take_until(shutdown)` (which dropped events that were already sitting in the `tracing` broadcast channel) with a select loop that switches into non-blocking drain mode when shutdown fires. Also yield briefly in `topology::running::shutdown` between the `VECTOR_PROCESS_COMPONENTS_CLOSED` VEL emission and `deferred_shutdowns.shutdown_all()` so the event can reach downstream transforms and sinks before the source is cancelled. Closes the missing-VEL window at the wave 1 → wave 2 boundary. https://github.com/databricks-eng/vector/pull/488
120. Treat tokio task cancellation as a clean shutdown signal in the disk_v2 buffer reader and the file_server checkpoint-writer await. Both previously panicked on `JoinError::Cancelled` during topology teardown, which cascaded through `handle_errors` → `abort_tx` and forced unnecessary container restarts. https://github.com/databricks-eng/vector/pull/490
121. Revert https://github.com/databricks-eng/vector/pull/485 — restore opt-in hostpath discovery via the `use_hostpath_logging_annotation_override` boolean. Always-on hostpath inflated the kubernetes_logs file watch set on Nephos staging by ~14× (cons-s3-commit-style high-rotation hostpath logs), which combined with the `default_rotate_wait = u64::MAX/2` dead-watcher-reaping bottleneck produced an FD leak: held FDs to unlinked rotated `.gz` files at ~3/pod/hr, saturating ephemeral disk in ~24h on high-volume control-plane pods. Empirical isolation: 7 vector binaries deployed to staging-aws-uw2-nephos-0-cuv4b8; reverting just #485 dropped fd_avg 341 → 208 and deleted-FD count 1.5/pod → 0. Universe-side pair: revert databricks-eng/universe#1839285 to restore emitting the field.
122. Cherry-pick upstream Vector v0.55 fix for high CPU usage in `file` / `kubernetes_logs` sources after the async file server migration (vectordotdev/vector#25064). Adds exponential backoff (1ms → 250ms, doubling per consecutive EOF, reset on data) in `FileWatcher::should_read` so idle files no longer tight-loop on `read → 0 bytes → retry`. Async file server migration landed in upstream v0.50.0 (vectordotdev/vector#23612) and was missing this backoff; we have been carrying the regression since the v0.50 uplift. https://github.com/databricks-eng/vector/pull/501
123. Initialize `last_config_reload_success` to `1.0` on `VectorStarted` so the gauge is exposed from boot rather than only after the first reload, removing the ambiguity between "never reloaded" and a missing scrape https://github.com/databricks-eng/vector/pull/497
124. Add `discarded_internal_logs_total` metric to track when internal log events are dropped due to broadcast channel lag in the `internal_logs` source. Increase the default broadcast channel capacity from 99 to 512 and make it configurable via the `VECTOR_INTERNAL_LOG_BROADCAST_CAPACITY` environment variable. https://github.com/databricks-eng/vector/pull/498
