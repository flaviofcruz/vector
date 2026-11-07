Fix `drain_on_shutdown` persisting file checkpoints past lines downstream had not taken. The FileServer drain no longer writes checkpoints; only the consumer advances them, so a reload with a stalled downstream re-reads in-flight lines instead of skipping them.
https://github.com/databricks-eng/vector/pull/905
