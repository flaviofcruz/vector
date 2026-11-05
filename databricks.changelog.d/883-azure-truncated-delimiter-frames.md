Discard unterminated newline/character-delimited tails when the Azure Blob source tolerates a truncated archive, preventing torn records from being forwarded as valid shorter payloads. Preserve terminated prefix records, normal EOF behavior, and other framing modes.
https://github.com/databricks-eng/vector/pull/883
