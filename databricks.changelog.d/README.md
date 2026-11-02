# Databricks changelog fragments

Custom changes merged into the Databricks fork of Vector, one file per PR.
Entries before this directory existed are in the frozen
[`README.databricks.md`](../README.databricks.md).

Each PR adds a file named `<pr-number>-<slug>.md`, for example
`838-zerobus-create-principal-context.md`. The slug is lowercase letters,
digits and hyphens. The file holds one paragraph describing the change and
ends with the PR's URL:

```markdown
Short description of the behavior change, backport, or fix.
https://github.com/databricks-eng/vector/pull/838
```

Because each PR adds its own file, PRs never conflict over the changelog.
Since the PR number is only known after the PR is opened, commit the fragment
after opening the PR. A PR with several distinct changes may add more than one
fragment, each named with the same PR number.

The `BLOCKING: Databricks Changelog Check` workflow
(`scripts/check_databricks_changelog.py`) requires at least one new fragment
named for the PR and containing its exact URL, and rejects edits to the
entries in `README.databricks.md`.
