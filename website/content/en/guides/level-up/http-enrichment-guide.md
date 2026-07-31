---
date: "2026-07-06"
title: Enrich data from an HTTP service
short: HTTP enrichment
description: Learn how to use the `http` enrichment table to enrich events from a REST API
authors: ["flaviofcruz"]
domain: enriching
weight: 6
tags: ["enrichment", "logs", "http", "level up", "guides", "guide"]
---

{{< requirement >}}
Before you begin, this guide assumes the following:

* You understand the [basic Vector concepts][concepts]
* You understand [how to set up a basic pipeline][pipeline]
* You are familiar with [enrichment tables][Enrichment tables] and the
  [`get_enrichment_table_record`][get_enrichment_table_record] /
  [`find_enrichment_table_records`][find_enrichment_table_records] VRL functions

[concepts]: /docs/introduction/concepts
[pipeline]: /docs/setup/quickstart
[Enrichment tables]: /docs/reference/glossary/#enrichment-tables
[get_enrichment_table_record]: /docs/reference/vrl/functions/#get_enrichment_table_record
[find_enrichment_table_records]: /docs/reference/vrl/functions/#find_enrichment_table_records
{{< /requirement >}}

The [`file`][Enrichment tables] enrichment table is a great fit when your
reference data lives in a CSV or JSON file on disk. But often the data you want
to enrich with lives behind an HTTP API — a service that owns the mapping,
allow-list, or dimension table you care about. The `http` enrichment table lets
you point Vector at such an endpoint and use its response for lookups, exactly
like any other enrichment table.

## How it works

The `http` table fetches a dataset from an HTTP endpoint into an in-memory,
indexed snapshot, and serves lookups from that snapshot. Crucially, **lookups
never make a network request** — they read the in-memory copy, so per-event
enrichment stays fast. A background task refreshes the snapshot on a configurable
interval and swaps in the new data atomically, so lookups always see a complete,
consistent dataset.

This design makes the `http` table a good fit for **small, relatively static
reference datasets** served over a REST-style API. It is *not* intended for
per-key lookups against a huge, high-cardinality dataset — the whole result set
is loaded into memory.

The endpoint is expected to return a **JSON array of objects**. Each object
becomes a row, and the union of object keys across all rows becomes the set of
columns. A key missing from a given row is treated as `null`.

## A basic example

Suppose you run a service that maps a `user_id` to organizational metadata, and
it serves the mapping at `https://directory.internal/api/users` as:

```json
[
  {"user_id": "u-100", "team": "logging", "tier": "gold"},
  {"user_id": "u-200", "team": "billing", "tier": "silver"}
]
```

Configure the enrichment table and a `remap` transform to look rows up by
`user_id`:

```yaml
enrichment_tables:
  user_directory:
    type: http
    url: https://directory.internal/api/users
    # Refresh every hour; omit to fetch once at startup and never refresh.
    refresh_interval_secs: 3600

transforms:
  enrich:
    type: remap
    inputs: ["my_source"]
    source: |
      row, err = get_enrichment_table_record(
        "user_directory",
        { "user_id": .user_id }
      )
      if err == null {
        .team = row.team
        .tier = row.tier
      }
```

Because the lookup condition uses an exact match on `user_id`, Vector builds an
index on that column at startup, so lookups are fast even for large datasets.

## Nested responses

Many APIs wrap the array in an envelope. Use `response.items_pointer` — a
[JSON Pointer][json_pointer] — to point at the array within the body. For a
response like:

```json
{"data": [ {"user_id": "u-100", "team": "logging"} ], "count": 1}
```

set:

```yaml
enrichment_tables:
  user_directory:
    type: http
    url: https://directory.internal/api/users
    response:
      items_pointer: /data
```

The default (empty pointer) treats the entire body as the array.

## Authentication and headers

The `http` table supports the standard Vector HTTP authentication strategies —
`bearer`, `basic`, and a custom `Authorization` header value — plus arbitrary
request headers and TLS settings:

```yaml
enrichment_tables:
  user_directory:
    type: http
    url: https://directory.internal/api/users
    auth:
      strategy: bearer
      token: "${DIRECTORY_API_TOKEN}"
    headers:
      accept: application/json
```

## Pagination

Real APIs rarely return everything in one response. The `http` table walks every
page in a single refresh and accumulates all rows before building indexes and
swapping in the new snapshot. Two strategies are supported.

**Cursor / token pagination** — the response carries a token pointing at the next
page, which Vector sends back as a query parameter. Paging stops when the token is
absent, `null`, or empty:

```yaml
    pagination:
      strategy: cursor
      token_pointer: /next_cursor   # JSON Pointer to the token in the response
      token_param: cursor           # query parameter to send it back in
```

**Offset / limit pagination** — Vector advances an offset by a fixed page size
each request. Paging stops when a page returns fewer rows than the page size:

```yaml
    pagination:
      strategy: offset
      offset_param: offset
      limit_param: limit
      page_size: 1000
```

To guard against runaway fetches, `limits.max_pages` and `limits.max_rows` bound
a single refresh. When either is hit, the refresh stops and Vector logs a warning
(the data is never silently truncated without notice).

## Surviving restarts

By default the `http` table persists each fetched snapshot to disk under the
global [`data_dir`][data_dir]. On restart, if a persisted snapshot exists (and is
younger than `max_cache_age_secs`, when set), it is loaded immediately so lookups
work right away, while a fresh fetch happens in the background. Set `persist:
false` to disable this.

If a refresh fails — the endpoint is down, returns an error, or times out — the
table keeps serving the last good snapshot rather than emptying itself, so a
flaky upstream never breaks enrichment.

## Type coercion

By default, cell types are inferred from the JSON values. Use `schema` to coerce
string values into a specific type, using the same syntax as the `file` table
(for example `integer`, `float`, `boolean`, or `timestamp|%+`):

```yaml
    schema:
      created_at: "timestamp|%+"
      active: boolean
```

These examples are intended as a basic guide to enriching your data from an HTTP
service. If you use the `http` enrichment table for other use cases, let us know
on our [Discord chat] or [Twitter], along with any feedback or requests you have
for the Vector team!

[json_pointer]: https://datatracker.ietf.org/doc/html/rfc6901
[data_dir]: /docs/reference/configuration/global-options/#data_dir
[Discord chat]: https://discord.com/invite/dX3bdkF
[Twitter]: https://twitter.com/vectordotdev
