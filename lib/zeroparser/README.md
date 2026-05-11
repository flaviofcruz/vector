# zeroparser

Vendored from [`databricks-eng/universe`](https://sourcegraph.prod.databricks-corp.com/databricks-eng/universe/-/tree/shinkansen/core/zeroparser).

Used by the wire-to-Arrow batch serializer in `lib/codecs` to decode proto wire
bytes directly into Arrow column builders, bypassing the
`ProtobufDeserializer -> LogEvent -> ArrowStreamSerializer` chain.

> Originally vendored as `proto-parser`. Upstream was renamed to
> `zeroparser` (universe PR
> [#1876036](https://github.com/databricks-eng/universe/pull/1876036));
> this vendor refresh follows the rename to keep names aligned.

## Sync policy

This is a copy, not a git submodule. If upstream changes, re-sync with:

```sh
cp ../universe/shinkansen/core/zeroparser/src/*.rs lib/zeroparser/src/
```

Record the source revision in the commit message.

### Vector-side patches

After resyncing from upstream, re-apply:

- `src/lib.rs`: change `mod wire;` to `pub mod wire;` so downstream Arrow
  encoders can call `wire::try_parse_field` and `wire::decode_zigzag*`
  directly. Upstream equivalent will likely land when the crate is OSS'd.

## Long-term plan

Upstream plans to extract `zeroparser` into its own repository once the
zerobus in-prod rollout stabilizes. At that point this vendored copy should be
replaced with a `git` dependency on the standalone repo, matching the pattern
used for `databricks-zerobus-ingest-sdk`.
