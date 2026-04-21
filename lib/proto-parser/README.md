# proto-parser

Vendored from [`databricks-eng/universe`](https://sourcegraph.prod.databricks-corp.com/databricks-eng/universe/-/tree/shinkansen/core/proto-parser).

Used by the wire-to-Arrow batch serializer in `lib/codecs` to decode proto wire
bytes directly into Arrow column builders, bypassing the
`ProtobufDeserializer -> LogEvent -> ArrowStreamSerializer` chain.

## Sync policy

This is a copy, not a git submodule. If upstream changes, re-sync with:

```sh
cp ../universe/shinkansen/core/proto-parser/src/*.rs lib/proto-parser/src/
```

Record the source revision in the commit message.

## Long-term plan

Upstream plans to extract `proto-parser` into its own repository once the
zerobus in-prod rollout stabilizes (~early May 2026). At that point this
vendored copy should be replaced with a `git` dependency on the standalone
repo, matching the pattern used for `databricks-zerobus-ingest-sdk`.
