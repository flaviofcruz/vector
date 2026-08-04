package metadata

components: transforms: metric_batch: {
	title: "Metric Batch"

	description: """
		Batch-reduces multiple metric events into a single log event per series over a fixed
		time window. Each output log carries the metric identity plus every sample seen in the
		window as a `values` array, leaving any further shaping (for example, deriving additional
		columns) to a downstream transform. Work is sharded across independent workers for
		throughput.
		"""

	classes: {
		commonly_used: false
		development:   "stable"
		egress_method: "stream"
		stateful:      true
	}

	features: {
		reduce: {}
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	configuration: generated.components.transforms.metric_batch.configuration

	input: {
		logs: false
		metrics: {
			counter:      true
			distribution: false
			gauge:        true
			histogram:    false
			set:          false
			summary:      false
		}
		traces: false
	}

	output: {
		logs: "": {
			description: "A batched `log` event per group, carrying the metric identity and every sample seen in the window."
		}
	}

	examples: [
		{
			title: "Batch a counter over a window"

			configuration: {
				batch_period_ms: 120000
				worker_shards:   10
			}

			input: [
				{
					metric: {
						kind:      "absolute"
						name:      "requests_total"
						timestamp: "2020-08-01T21:15:00Z"
						tags: {
							host: "my.host.com"
						}
						counter: {
							value: 10.0
						}
					}
				},
				{
					metric: {
						kind:      "absolute"
						name:      "requests_total"
						timestamp: "2020-08-01T21:15:30Z"
						tags: {
							host: "my.host.com"
						}
						counter: {
							value: 11.0
						}
					}
				},
			]

			output: log: {
				name: "requests_total"
				labels: {
					host: "my.host.com"
				}
				start_ts: 1596316500000
				end_ts:   1596316530000
				values: [
					{ts: 1596316500000, v: 10.0},
					{ts: 1596316530000, v: 11.0},
				]
			}
		},
	]

	how_it_works: {
		grouping: {
			title: "Grouping and reduction"
			body: """
				Incoming metrics are grouped by their name and the set of tags not listed in
				`exclude_tags`. When `split_by_time_hour_truncation` is enabled, the hour bucket (the
				sample timestamp truncated to the hour) is also part of the group key, so samples that
				cross an hour boundary flush as separate groups. Each group accumulates every sample
				seen during the `batch_period_ms` window and, on flush, emits one log whose `values`
				array holds every `{ts, v}` observed, along with `start_ts` and `end_ts` (the minimum
				and maximum sample timestamps in the window). Only `counter` and `gauge` samples are
				reduced.
				"""
		}

		sharding: {
			title: "Sharding and flushing"
			body: """
				Events are hash-routed across `worker_shards` independent worker tasks, each owning its
				own group map and flush timer and sharing no state, so no worker blocks another. Flushes
				can be staggered across the window with `worker_flush_offset_ms` to spread the per-flush
				CPU spike. Idle groups are evicted after `group_cache_ttl_ms`, scanned every
				`group_cache_clean_interval_ms`.
				"""
		}

		output_sharding: {
			title: "Output sharding"
			body: """
				With `shard_outputs` enabled, each output log is stamped with
				`<output_shard_label> = hash % output_shards`, reusing the same hash already computed for
				worker routing. A downstream `route` transform can then fan the stream across that many
				parallel derive transforms. When `shard_outputs` is enabled, `output_shards` and
				`output_shard_label` are required.
				"""
		}
	}

	telemetry: metrics: {
		metric_batch_groups: components.sources.internal_metrics.output.metrics.metric_batch_groups
	}
}
