Expose only the verified client leaf in TLS connection metadata. Check validity after session resumption and close verified client connections before certificate expiry so clients reconnect with renewed credentials.
https://github.com/databricks-eng/vector/pull/831
