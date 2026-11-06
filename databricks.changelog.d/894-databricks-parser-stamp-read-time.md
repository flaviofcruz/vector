Stamp the read time on timestamp-less lines in the `kubernetes_logs` `DatabricksParser` (legacy log namespace). Raw-file lines without a date, such as JVM boot output, previously reached downstream with no timestamp; an existing timestamp is kept and the vector namespace is unchanged.
https://github.com/databricks-eng/vector/pull/894
