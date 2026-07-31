package metadata

generated: components: transforms: configuration: {
	graph: {
		description: """
			Extra graph configuration

			Configure output for component when generated with graph command
			"""
		required: false
		type: object: options: node_attributes: {
			description: """
				Node attributes to add to this component's node in resulting graph

				They are added to the node as provided
				"""
			required: false
			type: object: {
				examples: [{
					color: "red"
					name:  "Example Node"
					width: "5.0"
				}]
				options: "*": {
					description: "A single graph node attribute in graphviz DOT language."
					required:    true
					type: string: {}
				}
			}
		}
	}
	inputs: {
		description: """
			A list of upstream [source][sources] or [transform][transforms] IDs.

			Wildcards (`*`) are supported.

			See [configuration][configuration] for more info.

			[sources]: https://vector.dev/docs/reference/configuration/sources/
			[transforms]: https://vector.dev/docs/reference/configuration/transforms/
			[configuration]: https://vector.dev/docs/reference/configuration/
			"""
		required: true
		type: array: items: type: string: examples: ["my-source-or-transform-id", "prefix-*"]
	}
	measure_cpu_usage: {
		description: """
			Enable CPU usage metrics for this transform.

			When set to `true`, each poll of the transform task is timed using the OS thread CPU clock
			and the accumulated nanoseconds are reported as the `component_cpu_usage_ns_total` counter,
			tagged with `component_id`, `component_kind`, and `component_type`.

			Defaults to `false`. Enable only for transforms where CPU attribution is needed, as it
			adds a `clock_gettime` call on every future poll.
			"""
		required: false
		type: bool: default: false
	}
}
