#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "The checked-in redaction test fixture supports Linux x86_64 only." >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir="${repo_root}/lib/vector-redaction-executor/tests/fixtures/linux-x86_64"

cd "${repo_root}"
exec env DATABRICKS_VECTOR_REDACTOR_LIB_DIR="${fixture_dir}" \
  cargo test --locked \
    -p vector-redaction-executor \
    -p vector-vrl-functions \
    --features vector-redaction-executor/static-executor,vector-vrl-functions/databricks-redaction
