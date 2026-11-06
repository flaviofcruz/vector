Hold the token manager's cached principal-context bytes in `secrecy` 0.10 `SecretSlice` secrets, so their `Debug` output is redacted and the bytes are zeroized on drop; they can no longer leak through logs or panic messages. Adds the `secrecy` 0.10.3 crate (only dependency `zeroize`, already locked).
https://github.com/databricks-eng/vector/pull/900
