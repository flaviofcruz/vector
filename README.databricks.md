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
43. Add a Databricks ZeroBus sink to vector. https://github.com/databricks-eng/vector/pull/294 https://github.com/databricks-eng/vector/pull/305 https://github.com/databricks-eng/vector/pull/350
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
