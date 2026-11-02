#!/usr/bin/env python3

"""Validate the Databricks changelog fragment added by a pull request.

Each PR adds one file per change to `databricks.changelog.d/`, named
`<pr-number>-<slug>.md`, containing the PR's URL. `README.databricks.md` holds
the frozen historical list; its numbered entries must not change.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass


FRAGMENT_DIR = "databricks.changelog.d"
FROZEN_CHANGELOG = "README.databricks.md"
FRAGMENT_NAME = re.compile(
    rf"^{re.escape(FRAGMENT_DIR)}/([0-9]+)-[a-z0-9][a-z0-9-]*\.md$"
)
# Files in the fragment directory that are not changelog fragments.
NON_FRAGMENT_FILES = {f"{FRAGMENT_DIR}/README.md"}
ENTRY_HEADING = re.compile(r"^([0-9]+)\.\s")


class ValidationError(Exception):
    """A deterministic changelog policy violation."""


@dataclass(frozen=True)
class Entry:
    number: int
    lines: tuple[str, ...]
    start_line: int

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


def parse_entries(contents: str) -> list[Entry]:
    """Parse numbered entries, including their continuation lines."""
    entries: list[Entry] = []
    current_number: int | None = None
    current_lines: list[str] = []
    current_start = 0

    for line_number, line in enumerate(contents.splitlines(), start=1):
        match = ENTRY_HEADING.match(line)
        if match:
            if current_number is not None:
                entries.append(
                    Entry(current_number, tuple(current_lines), current_start)
                )
            current_number = int(match.group(1))
            current_lines = [line]
            current_start = line_number
        elif current_number is not None:
            current_lines.append(line)

    if current_number is not None:
        entries.append(Entry(current_number, tuple(current_lines), current_start))

    return entries


def contains_exact_pr_url(text: str, pr_url: str) -> bool:
    """Accept the PR URL as a complete link, not a prefix of another URL."""
    boundary = r"(?=$|[\s)\]}>.,;:!?\"'])"
    return re.search(re.escape(pr_url) + boundary, text) is not None


def infer_pr_number(added_files: dict[str, str]) -> int:
    """Infer one PR number from valid fragment filenames for local validation."""
    numbers = {
        int(match.group(1))
        for path in added_files
        if path not in NON_FRAGMENT_FILES
        if (match := FRAGMENT_NAME.match(path)) is not None
    }
    if len(numbers) != 1:
        raise ValidationError(
            "Expected changelog fragments for exactly one PR when --pr-number is "
            f"omitted, found {len(numbers)} PR numbers."
        )
    return numbers.pop()


def validate_changelog(
    base_frozen: str,
    head_frozen: str,
    added_files: dict[str, str],
    pr_number: int,
    pr_url: str,
) -> list[str]:
    """Return the valid fragment paths added by the PR or raise a policy error.

    `added_files` maps each file the PR adds under the fragment directory to its
    contents.
    """
    # Compare text, not line positions, so the header above the list may change.
    def entry_texts(contents: str) -> list[tuple[int, tuple[str, ...]]]:
        return [(entry.number, entry.lines) for entry in parse_entries(contents)]

    if entry_texts(base_frozen) != entry_texts(head_frozen):
        raise ValidationError(
            f"{FROZEN_CHANGELOG} is frozen: do not add, edit or renumber its "
            f"entries. Add a file to {FRAGMENT_DIR}/ instead."
        )

    fragments = []
    for path, contents in sorted(added_files.items()):
        if path in NON_FRAGMENT_FILES:
            continue
        match = FRAGMENT_NAME.match(path)
        if not match:
            raise ValidationError(
                f"{path} is not a valid changelog fragment name; use "
                f"{FRAGMENT_DIR}/{pr_number}-<slug>.md (lowercase, digits, hyphens)."
            )
        if int(match.group(1)) != pr_number:
            raise ValidationError(
                f"{path} is named for PR {match.group(1)}, but this is PR {pr_number}."
            )
        if not contents.strip():
            raise ValidationError(f"{path} is empty.")
        if not contains_exact_pr_url(contents, pr_url):
            raise ValidationError(
                f"{path} must contain the exact current PR URL: {pr_url}"
            )
        fragments.append(path)

    if not fragments:
        raise ValidationError(
            f"No changelog fragment was added. Add "
            f"{FRAGMENT_DIR}/{pr_number}-<slug>.md containing {pr_url}"
        )

    return fragments


def git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def read_file_at_revision(revision: str, path: str) -> str:
    return git_output("show", f"{revision}:{path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--head-ref", default="HEAD")
    parser.add_argument("--repository", required=True)
    parser.add_argument(
        "--pr-number",
        type=int,
        help="Current PR number; if omitted, infer it from fragment filenames.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        merge_base = git_output("merge-base", args.base_ref, args.head_ref).strip()
        added_paths = git_output(
            "diff",
            "--name-only",
            "--diff-filter=A",
            merge_base,
            args.head_ref,
            "--",
            FRAGMENT_DIR,
        ).splitlines()
        added_files = {
            path: read_file_at_revision(args.head_ref, path) for path in added_paths
        }
        pr_number = (
            args.pr_number
            if args.pr_number is not None
            else infer_pr_number(added_files)
        )
        pr_url = f"https://github.com/{args.repository}/pull/{pr_number}"
        fragments = validate_changelog(
            read_file_at_revision(merge_base, FROZEN_CHANGELOG),
            read_file_at_revision(args.head_ref, FROZEN_CHANGELOG),
            added_files,
            pr_number,
            pr_url,
        )
    except ValidationError as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or str(error)
        print(f"::error::Unable to inspect the changelog: {detail}", file=sys.stderr)
        return 2

    print(f"Validated Databricks changelog fragment(s): {', '.join(fragments)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
