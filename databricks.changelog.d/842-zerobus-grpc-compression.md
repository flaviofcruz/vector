Add optional `stream_options.grpc_compression` (`"zstd"`) to the `databricks_zerobus` sink, using the Zerobus SDK's whole-message gRPC zstd. It is mutually exclusive with `stream_options.compression`; the default is unchanged. Bumps the Zerobus SDK to 3.1.1 and Arrow to 60.1, which affects every `codecs-arrow` user.
https://github.com/databricks-eng/vector/pull/842
