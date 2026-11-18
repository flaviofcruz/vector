`wire_to_arrow` now counts every wire-type-mismatched field skip in a row that needs duplicate or oneof normalization, and logs the producer-attribution warning for it. Previously a mismatch after the duplicate was dropped without being counted, and a mismatch inside a message-backed VARIANT payload was counted twice.
https://github.com/databricks-eng/vector/pull/993
