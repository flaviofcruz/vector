Added a new `http` enrichment table that loads a dataset from an HTTP endpoint and exposes it for lookups in the `remap` transform. The dataset is fetched into an in-memory, indexed snapshot and refreshed in the background on a configurable interval, so lookups never block on the network — making it a good fit for small, relatively static reference datasets served over a REST-style API.

The endpoint is expected to return a JSON array of objects (optionally nested within the body via a JSON Pointer). Cursor and offset pagination are supported, along with `none`/`bearer`/`basic` authentication, custom headers, TLS, per-column type coercion, and optional on-disk persistence of the fetched snapshot so a restart can serve data immediately.

authors: flaviofcruz
