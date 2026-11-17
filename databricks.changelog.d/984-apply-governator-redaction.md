Add the `apply_governator_redaction!(message, plan_dir, log_topic, policy)` VRL function, which registers every `<POLICY>__<log-topic>.pb` plan in a literal directory at compile time and redacts each record with the plan for its log topic and policy, using the existing redactor C ABI.
https://github.com/databricks-eng/vector/pull/984
