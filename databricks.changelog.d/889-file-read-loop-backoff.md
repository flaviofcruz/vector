Add positive `read_loop_max_backoff_ms` configuration to the file and Kubernetes log sources, preserving the 2,048 ms default while allowing deployments to trade idle wake-up latency against polling overhead.
https://github.com/databricks-eng/vector/pull/889
