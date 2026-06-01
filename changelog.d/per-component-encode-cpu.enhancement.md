Added a `component_encode_cpu_seconds` histogram metric that records the CPU time each sink component spends encoding and compressing a batch, tagged by `component_id`. This makes per-sink encode and compression cost directly observable.

authors: srinidhisai-boorgu_data

