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

## Push and PR Creation Flow

When the user asks to push changes or open a PR (including requests such as
"commit and push"), complete this entire sequence before returning to the
user:

1. Make and validate the code or repository changes.
2. Commit the changes without the changelog fragment, then push the branch.
3. Open the PR against `v0.53-custom` so the final PR number and URL exist.
4. Add the changelog fragment `databricks.changelog.d/<pr-number>-<slug>.md`
   containing the PR's exact URL. Never commit a placeholder such as
   `pull/TODO`.
5. Commit the fragment as a separate commit and push again.
6. Verify that the PR head includes the fragment commit and that its file name
   and URL match the PR. Only then report the push or PR as complete.

Do not return immediately after the initial push or PR creation; the follow-up
fragment commit, second push, and verification are part of the requested
workflow.

## Databricks Changelog

- Do not add upstream `changelog.d/*` fragments for Databricks-only changes.
- Add a fragment to `databricks.changelog.d/` for behavior changes, backports,
  operational fixes, and repo/process changes. See
  `databricks.changelog.d/README.md` for the format.
- `README.databricks.md` is the frozen historical list. Do not add, edit, or
  renumber its entries.
- The `BLOCKING: Databricks Changelog Check` workflow enforces both rules.

## Validation

- There is no reliable GitHub CI signal for this fork/branch. A PR can be open
  without the tests that would normally protect upstream Vector changes.
- Run all blocking Databricks PR checks from any directory in the repository
  with:

  ```bash
  scripts/ci-local.sh
  ```

  The full check can take up to 10 minutes to run. Use `--fix` to run the
  supported Rust formatting and Clippy fixers before verification. It never
  edits the changelog, which is validated last.
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
