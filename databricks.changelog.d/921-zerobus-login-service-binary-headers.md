Allow `login_service` auth in the `databricks_zerobus` sink: config validation no longer rejects it at startup, because the Zerobus SDK now sends `s2s-principal-context-sig-bin` as binary gRPC metadata (databricks/zerobus-sdk#1070). Pins the SDK to that change until 3.2.0 is published.
https://github.com/databricks-eng/vector/pull/921
