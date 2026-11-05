Reject oversized protobuf field numbers before narrowing them to i32 so malformed tags cannot wrap into valid fields in zeroparser or the wire-to-Arrow VARIANT path. InvalidFieldNumber retains the rejected value as u64; valid field-number APIs remain i32.
https://github.com/databricks-eng/vector/pull/881
