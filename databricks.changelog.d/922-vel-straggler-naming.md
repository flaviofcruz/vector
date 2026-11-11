Graceful shutdown: the COMPONENTS_CLOSED event now names force-terminated
components from tokio task state instead of a peek-based snapshot, fixing the
empty-list non-graceful closes seen on CPU-starved agents; a missed deadline
with nothing nameable emits an `<unattributed>` entry so a non-graceful close
always stays queryable.
https://github.com/databricks-eng/vector/pull/922
