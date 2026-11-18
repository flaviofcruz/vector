# Opt-in `hydra_remote_write` source

`hydra_remote_write` receives Prometheus remote write and writes each request's
samples as one pre-encoded payload per destination pod: NDJSON rows for S3 and
an Arrow IPC stream for Zerobus. It decodes the request with the same prost types
as `prometheus_remote_write` but does not build a `Metric` event per sample. Every sample is still its own
row, with the same fields, routing and timestamp checks as
`prometheus_remote_write` (`group_metrics: false`) followed by
`metric_to_log_hydra`.

This is unrelated to the existing VAM row-merging batching. It does not change
sink or buffer code, Vector's event model or runtime, but each event is now a
payload, which changes what event counts mean (see [Metrics](#metrics)). The existing source and
transform keep their current behavior.

The source is built with the `sources-hydra_remote_write` feature, which is part
of the normal `sources-metrics` aggregate and therefore of every `target-*`
production bundle. Configuration must still select it explicitly. The envelope
routing below also needs `transforms-route`, which the production bundles
include.

## Metrics

With this source, one Vector event is one payload: the rows of one
remote-write request for one destination. Every metric that counts events
therefore counts payloads, not samples. Row counts and the Databricks delivery
counters still count rows.

| Metric | Today (`prometheus_remote_write` + `metric_to_log_hydra`) | With `hydra_remote_write` | Use instead |
|---|---|---|---|
| `component_received_events_total`, `component_sent_events_total` (source, route, sinks) | Samples (one row each) | Payloads (one per request per destination) | `hydra_remote_write_encoded_rows_total{output}` for rows per destination; `delivery_events_total` for rows delivered by S3 |
| `component_received_event_bytes_total`, `component_sent_event_bytes_total` | Estimated JSON size of the row events | Estimated size of the payloads (different values) | `component_sent_bytes_total` on the sinks for bytes on the wire (within 9% of base) |
| `buffer_events`, `buffer_received_events_total`, `buffer_sent_events_total` | Rows | Payloads | `buffer_byte_size` for how full a buffer is |
| `buffer_byte_size` | Bytes held | Bytes held (unchanged meaning) | — |
| `component_discarded_events_total` | Rows lost (stale or future timestamp), emitted by the transform | Rows lost, same `reason`s, emitted by the source | Filter on the source's component ID |
| `metric_to_log_hydra_dropped_total{reason}` | Rows dropped, emitted by the transform | Rows dropped, same name and `reason`s, emitted by the source | Filter on the source's component ID |
| `delivery_events_total` (S3 sinks) | Rows | Rows (the source sets each payload's delivery count to its row count) | — |
| `delivery_event_bytes_total` (S3 sinks) | In-memory size of the row events | In-memory size of the payloads, about 2.7x lower | `component_sent_bytes_total` |
| `hydra_remote_write_encoded_rows_total{output}` | Not emitted | Rows written to each destination | — |
| `hydra_remote_write_received_samples_total` | Not emitted | Samples in accepted requests, before drops and routing; each is either dropped (`metric_to_log_hydra_dropped_total`) or written to at least one output | Use for samples/s and samples per CPU-second |
| `blob_sink_file_uploads_total` (S3) | Objects uploaded | Objects uploaded; fewer, larger objects unless `batch.max_bytes` is lowered (see below) | — |

Zerobus sinks emit no `delivery_events_total` with either pipeline.

Three behavior changes follow from "one event is one payload":

- Size memory buffers with `max_size` (bytes) instead of `max_events`. A
  `max_events` value sized for samples allows far more memory than intended.
  Lower byte-based batch limits too; see
  [Batch and buffer settings](#batch-and-buffer-settings).
- `when_full: drop_newest` drops a whole payload: every row of one request for
  that destination.
- `vector tap` shows encoded payloads, not metric rows.

Measured locally on the shared corpus (450,228 samples in 236 requests, eight
destinations, acknowledgements off, production sink-event-logging environment),
final values per pod0 S3 sink: base 438,189 received/sent events and 438,189
delivered rows; this source 236 received/sent events, 438,189 delivered rows and
`hydra_remote_write_encoded_rows_total{output="pod0"}` 438,189. Both dropped the
same 12,039 non-finite samples (`reason="non_finite_value"`, not a loss, so no
discard counter). No sample was lost on this corpus, so
`component_discarded_events_total` stayed at zero on both.

## Batch and buffer settings

Byte-based sink limits measure the in-memory size of events. A per-sample row
event is about 2.8x larger in memory than its NDJSON row, while a payload is
about its encoded size, so the same `max_bytes` holds 2-3x more rows with this
source. Use these settings to keep objects and requests the size they are today:

| Sink | Today | With `hydra_remote_write` | Why |
|---|---|---|---|
| `aws_s3` | `batch.max_bytes: 314572800` | `batch.max_bytes: 111000000` | 314572800 gives ~111 MB objects today and ~314 MB objects (2.8x the rows) with payloads |
| `databricks_zerobus` | default `batch.max_bytes` (10000000) | `batch.max_bytes: 4800000` | 10 MB of payloads is 3,700-5,500 rows per request instead of ~1,800 |
| all (memory buffers) | `max_events: 500000` | `max_size: 1000000000` | `max_events` counts payloads; 1 GB holds about the same 500,000 rows |

Keep the other batch settings (`timeout_secs`, `when_full`) as they are.

Measured on the shared corpus (acknowledgements off; acknowledgements on gives
the same rows and was IDENTICAL against base with these settings):

| Sink | Base | This source, today's settings | This source, settings above |
|---|---|---|---|
| S3 pod0 objects | 8, median 56,546 rows / 111 MB | 3, median 159,966 rows / 314 MB | 8, median 55,802 rows / 110 MB |
| Zerobus requests (pod_prime) | 244, mean 1,796 rows | 118, mean 3,713 rows | 236, mean 1,857 rows |
| Zerobus requests (VARIANT tables) | 244, mean 1,796 rows | 79, mean 5,547 rows | 236, mean 1,857 rows |

No Zerobus size limit is exceeded with either setting. The SDK splits each
request into Arrow Flight messages of at most about 2 MiB
(`GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES`), each with its own offset and ack, so the
messages the service sees are the same size with and without this source
(median about 175 KB, max 232 KB here). The SDK's 10 MiB ingest-payload limit
applies only to JSON and protobuf streams, and the sink rejects
`batch.max_bytes` above 10 MB. The smaller Zerobus `max_bytes` keeps each
request, and so each retry, the same size as today. These are byte-based, so
they hold whatever the senders' request sizes are; one payload larger than
`max_bytes` is still sent on its own.

## Configuration and rollback

Names are placeholders. Supply the existing sinks' transport, authentication,
table and batching settings separately.

```yaml
sources:
  metrics:
    type: hydra_remote_write
    address: "0.0.0.0:9090"
    pod_routing:
      systems:
        - system: "*"
          pods: [archive, table]
        - system: example-special
          pods: [table]
    outputs:
      archive: ndjson
      table: arrow_ipc_variant

transforms:
  destinations:
    type: route
    inputs: [metrics]
    reroute_unmatched: false
    route:
      archive: '.hydra_port == "archive"'
      table: '.hydra_port == "table"'
```

`pod_routing` has the same meaning as in `metric_to_log_hydra`, and every pod it
names must be listed in `outputs`. Routing is per metric, so a request with
several `system` labels sends each destination only its own samples.

- `ndjson`: connect to an `aws_s3` sink with `encoding.codec: text` and
  `framing.method: newline_delimited`. The payload already holds the rows; do
  not use the `json` codec on it.
- `arrow_ipc` (MAP `labels`) or `arrow_ipc_variant` (VARIANT `labels`): connect
  to a `databricks_zerobus` sink with `batch_encoding.codec: arrow_ipc`. The
  codec conforms the stream to the table schema the sink loaded, using the
  existing `arrow_stream` coercion when a column type differs. The sink still
  owns schema loading, authentication, transport and retries.
- The Arrow payload holds every top-level field of the per-sample
  `metric_to_log_hydra` row, with the same names, so a table can select any
  subset of them.
- Keep payloads intact between the source and its sink. `port_field` defaults to `hydra_port`; if
  you change it, update the route conditions.

To roll back, switch the source back to `prometheus_remote_write`
(`group_metrics: false`) with the per-sample `metric_to_log_hydra` transform,
and the sinks back to S3 `json` and Zerobus `arrow_stream`, all in the same
change. Do not switch only one side.

## Operational changes

- See [Metrics](#metrics) for counters, buffers, drops and `vector tap`. One
  payload can exceed a byte-based sink batch limit on its own.
- Acknowledgements cover payload delivery through the existing source and sink
  plumbing. They add no atomicity across destinations.
