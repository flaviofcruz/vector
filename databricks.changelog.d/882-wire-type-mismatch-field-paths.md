Include root-to-leaf protobuf field numbers in sampled wire-to-Arrow wire-type mismatch warnings, including nested structs and repeated messages, so operators can identify the failing field without logging payload data. Row validation and drop behavior are unchanged.
https://github.com/databricks-eng/vector/pull/882
