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
2. Commit the changes without the `README.databricks.md` entry, then push the
   branch.
3. Open the PR against `v0.53-custom` so the final PR number and URL exist.
4. Fetch `origin/v0.53-custom` again. If it moved, rebase the PR branch before
   choosing the changelog number. Read the refreshed end of
   `README.databricks.md` and use the next consecutive number.
5. Add the changelog entry with the PR's exact URL. Never commit a placeholder
   such as `pull/TODO`, and never reuse a number already present on the fresh
   base.
6. Commit the changelog update as a separate commit and push again. If step 4
   required a rebase, use `--force-with-lease` rather than an unguarded force
   push.
7. Verify that the PR head includes the changelog commit and that the entry's
   number and URL are correct. Only then report the push or PR as complete.

Do not return immediately after the initial push or PR creation; the follow-up
changelog commit, second push, and verification are part of the requested
workflow.

## Databricks Changelog

- Do not add upstream `changelog.d/*` fragments for Databricks-only changes.
- Add a numbered entry to `README.databricks.md` for behavior changes,
  backports, operational fixes, and repo/process changes.
- Include the PR link in each entry. If the PR has not been opened yet, add
  its link after creation and push that update to the same PR.
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
