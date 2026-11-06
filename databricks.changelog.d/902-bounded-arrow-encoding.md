Add optional bounded Arrow encoding workers to the Zerobus sink so concurrent requests can encode in parallel. Running jobs retain their worker slots after caller cancellation; encoding and queue-wait timings are exposed. Inline encoding remains the default.
https://github.com/databricks-eng/vector/pull/902
