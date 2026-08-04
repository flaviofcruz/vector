package metadata

generated: components: transforms: metric_batch: configuration: {
	batch_period_ms: {
		description: "Batch window in milliseconds. Each worker flushes its accumulated groups this often."
		required:    true
		type: uint: {}
	}
	exclude_tags: {
		description: """
			Tag keys excluded from the group key and the shard hash.

			Excluded tags are still emitted in `labels`; they just do not affect which group a metric
			falls into or which worker it is routed to.
			"""
		required: false
		type: array: {
			default: []
			items: type: string: {}
		}
	}
	group_cache_clean_interval_ms: {
		description: "How often, in milliseconds, each worker scans for and evicts idle groups."
		required:    false
		type: uint: default: 30000
	}
	group_cache_ttl_ms: {
		description: "Evict a group if no sample arrives for it within this long, in milliseconds."
		required:    false
		type: uint: default: 120000
	}
	output_shard_label: {
		description: """
			Log field to receive the output shard index (for example, `__output_shard__`).

			Required, and only used, when `shard_outputs` is `true`; ignored otherwise.
			"""
		required: false
		type: string: {}
	}
	output_shards: {
		description: """
			Number of output shards to spread groups across.

			Required, and only used, when `shard_outputs` is `true`; ignored otherwise.
			"""
		required: false
		type: uint: {}
	}
	shard_outputs: {
		description: """
			Stamp each output log with `output_shard_label = hash % output_shards`.

			The hash is the same one used for worker routing, so no extra hashing is done. This lets a
			downstream `route` transform fan the load across parallel derive transforms. When enabled,
			`output_shards` and `output_shard_label` are required.
			"""
		required: false
		type: bool: default: false
	}
	split_by_time_hour_truncation: {
		description: """
			Include the hour bucket in the group key.

			When enabled, the hour bucket (the sample timestamp truncated to the hour) is part of the
			group key, so samples that cross an hour boundary flush as separate groups (one log per
			series per hour). When disabled, all samples for a series in the window batch into a single
			group regardless of hour.
			"""
		required: false
		type: bool: default: false
	}
	worker_flush_offset_ms: {
		description: """
			Flush-stagger base, in milliseconds.

			All workers flush in lockstep when unset (or `0`); setting it to `batch_period_ms` spreads
			the workers' flushes evenly across one period. Worker `w` is offset earlier by
			`(worker_flush_offset_ms / worker_shards) * (w + 1)`.
			"""
		required: false
		type: uint: {}
	}
	worker_shards: {
		description: "Number of independent shard workers, each on its own task and flush timer."
		required:    false
		type: uint: default: 1
	}
}
