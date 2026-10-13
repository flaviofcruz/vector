#!/usr/bin/env bash
# Regenerates .github/clippy-grandfathered-exceptions.txt, the clippy lints that already existed when clippy was
# added to CI. The workspace crates set `#![deny(warnings)]`, so lints are capped to warnings here and CI fails only
# if the regenerated file differs from the committed one (`git diff --exit-code`), i.e. a lint was added or fixed.
# Run this script and commit the result to update the exceptions.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export DATABRICKS_VECTOR_REDACTOR_LIB_DIR="${DATABRICKS_VECTOR_REDACTOR_LIB_DIR:-$PWD/lib/vector-redaction-executor/tests/fixtures/linux-x86_64}"

# One line per lint: code, file, and the flagged source line. Line numbers are left out so moving code does not
# change the file, and the source line tells apart two lints of the same kind in one file. Diagnostics repeat per
# target (lib, lib test, ...), so they are deduped on location first.
cargo clippy --workspace --all-targets --locked --features databricks-redaction \
  --message-format=json -- --cap-lints warn \
  | jq -r 'select(.reason == "compiler-message") | .message
      | select(.code != null and (.level == "warning" or .level == "error"))
      | first(.spans[] | select(.is_primary)) as $s
      | select(($s.file_name | startswith("/") or startswith("..") or contains(".cargo/")) | not)
      | [.code.code, $s.file_name, $s.line_start, $s.column_start, ($s.text | map(.text) | join(" ") | gsub("\\s+"; " "))] | @tsv' \
  | LC_ALL=C sort -u | cut -f1,2,5 | LC_ALL=C sort > .github/clippy-grandfathered-exceptions.txt
