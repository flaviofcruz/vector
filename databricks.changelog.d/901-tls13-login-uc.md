Require TLS 1.3 on the token manager's Login and Unity Catalog gRPC connections (OpenSSL minimum protocol version); peers that only offer TLS 1.2 are now refused. Zerobus ingestion is unchanged because the SDK exposes no protocol-version setting.
https://github.com/databricks-eng/vector/pull/901
