Add `lp_any_subtable`, a VRL function that keys a Lumberjack Prime union row's generic `google.protobuf.Any` payload by its sub-table name, as the classic variant union does through its `<Table>AnySubtable` meta message. Typed rows pass through unchanged and a row with no sub-table returns null. It is not called by default.
https://github.com/databricks-eng/vector/pull/978
