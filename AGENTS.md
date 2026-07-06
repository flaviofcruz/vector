# Repository Instructions

This repository is Databricks' `v0.53-custom` Vector fork. Do not assume
`master` or upstream Vector is the correct base for Databricks work.

## Before Changing Code

- Fetch the latest Databricks base before implementation:

  ```bash
  git fetch origin v0.53-custom
  ```

- Start new work from, or rebase existing work onto, `origin/v0.53-custom`.
  If a branch has been open for more than a short session, fetch and rebase
  again before pushing.
- Check `git status --short --branch` before editing and before pushing.

## Databricks Changelog

- Do not add upstream `changelog.d/*` fragments for Databricks-only changes.
- Add a numbered entry to `README.databricks.md` for behavior changes,
  backports, operational fixes, and repo/process changes.
- Keep the numbering consecutive. If `v0.53-custom` moved while you were
  working, fetch/rebase first, then resolve the README entry at the new end of
  the list.

## Validation

- There is no reliable GitHub CI signal for this fork/branch. A PR can be open
  without the tests that would normally protect upstream Vector changes.
- Run comprehensive local validation before pushing. At minimum, run the
  relevant focused tests plus broader `cargo test` coverage when the change can
  affect shared topology, buffering, config, sources, sinks, or shutdown.
- For feature-gated components, also run compile-only or runtime tests with the
  relevant feature flags, for example:

  ```bash
  cargo test --no-run --features <component-integration-feature> <test_name>
  ```

- If an integration test needs an external service that is not running locally
  (for example LocalStack, Kafka, Redis, Azure storage), state that explicitly
  in the PR test plan and add or rely on hermetic coverage for the core behavior
  when possible.
