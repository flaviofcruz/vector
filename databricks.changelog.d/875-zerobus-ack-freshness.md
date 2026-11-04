Zerobus sink records per-row end-to-end freshness at ack as Prometheus histograms labeled by table (zerobus_ack_born_to_saved_seconds, zerobus_ack_lp_ingest_to_saved_seconds), plus counters for negative latency, missing born timestamps, and rows over the 30 min born-to-saved SLA (zerobus_ack_over_sla_total).
https://github.com/databricks-eng/vector/pull/875
