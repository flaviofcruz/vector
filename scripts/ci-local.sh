#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

usage() {
  cat <<'EOF'
Run the blocking Databricks Vector PR checks locally.

Usage: ./scripts/ci-local.sh [--fix]

  --fix  Run the supported Rust format and Clippy fixers before verification.
EOF
}

fix=false
for argument in "$@"; do
  case "$argument" in
    --fix)
      if [[ "$fix" == true ]]; then
        echo "error: --fix may only be specified once" >&2
        exit 2
      fi
      fix=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $argument" >&2
      usage >&2
      exit 2
      ;;
  esac
done

base_ref="origin/v0.53-custom"

missing_tools=()
required_tools=(cargo git jq python3 sha256sum)
for tool in "${required_tools[@]}"; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    missing_tools+=("$tool")
  fi
done
if ((${#missing_tools[@]} > 0)); then
  echo "error: missing required tools: ${missing_tools[*]}" >&2
  exit 2
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required (CI uses version 0.9.129)" >&2
  exit 2
fi

if ! git rev-parse --verify --quiet "${base_ref}^{commit}" >/dev/null; then
  echo "error: Databricks base ref is not available: $base_ref" >&2
  echo "Fetch it with: git fetch origin v0.53-custom" >&2
  exit 2
fi

export DATABRICKS_VECTOR_REDACTOR_LIB_DIR="${DATABRICKS_VECTOR_REDACTOR_LIB_DIR:-$PWD/lib/vector-redaction-executor/tests/fixtures/linux-x86_64}"
export CARGO_INCREMENTAL=0
export RUST_BACKTRACE=1
export CARGO_TERM_COLOR=always

names=()
results=()
durations=()
failures=0

run_step() {
  local name="$1"
  shift
  local started=$SECONDS

  echo
  echo "==> $name"
  if "$@"; then
    names+=("$name")
    results+=("PASS")
  else
    local status=$?
    names+=("$name")
    results+=("FAIL ($status)")
    failures=$((failures + 1))
  fi
  durations+=("$((SECONDS - started))s")
}

verify_redaction_fixture() {
  echo "2913e3b4e7253b7fd8fc13330927fd33441e15c928d8e99a85b2a6ad8f09219e  lib/vector-redaction-executor/tests/fixtures/linux-x86_64/libvector_redactor.a" | sha256sum --check -
}

verify_clippy_exceptions_unchanged() {
  git diff --exit-code -- .github/clippy-grandfathered-exceptions.txt
}

verify_upstream_workflows_disabled() {
  local workflow
  local status=0

  for workflow in .github/workflows/*.yml .github/workflows/*.yaml; do
    [[ -e "$workflow" ]] || continue
    case "$(basename "$workflow")" in
      databricks-*) continue ;;
    esac
    if grep -qE "^(on|\"on\"|'on')[[:space:]]*:" "$workflow"; then
      echo "Upstream workflow has an 'on:' trigger: $workflow" >&2
      status=1
    fi
  done
  return "$status"
}

if [[ "$fix" == true ]]; then
  run_step "Fix Clippy findings" \
    cargo clippy --workspace --all-targets --locked --features databricks-redaction \
    --fix --allow-dirty --allow-staged -- --cap-lints warn
  run_step "Fix Rust formatting" cargo fmt --all
fi

run_step "Verify redaction fixture" verify_redaction_fixture
run_step "Build workspace" cargo build --workspace --all-targets --locked --features databricks-redaction
run_step "Run unit tests" cargo nextest run --workspace --no-fail-fast --locked --features databricks-redaction
run_step "Run Databricks redaction tests" scripts/test-databricks-redaction.sh
run_step "Run Clippy" scripts/clippy-grandfathered-exceptions.sh
run_step "Verify Clippy exceptions are unchanged" verify_clippy_exceptions_unchanged
run_step "Check Rust formatting" cargo fmt --all -- --check
run_step "Verify upstream workflows are disabled" verify_upstream_workflows_disabled
run_step "Test changelog validator" python3 -m unittest scripts/tests/test_check_databricks_changelog.py
run_step "Validate Databricks changelog" \
  python3 scripts/check_databricks_changelog.py \
  --base-ref "$base_ref" \
  --head-ref HEAD \
  --repository databricks-eng/vector

echo
echo "Summary"
for index in "${!names[@]}"; do
  printf '  %-43s %-10s %s\n' "${names[$index]}" "${results[$index]}" "${durations[$index]}"
done

if ((failures > 0)); then
  echo
  echo "$failures check(s) failed."
  exit 1
fi

echo
echo "All blocking Databricks PR checks passed."
