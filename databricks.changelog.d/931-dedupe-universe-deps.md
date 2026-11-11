Moved shared dependencies (AWS SDK, deltalake, kube, async-nats, prost-reflect, opendal, reqwest, codespan-reporting) to Universe's vendored versions and the Rust toolchain to 1.95. Protobuf codec output for some repeated fields can differ byte-for-byte, and the Kubernetes source no longer reads `HTTP(S)_PROXY` from the environment.
https://github.com/databricks-eng/vector/pull/931
