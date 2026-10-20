#!/usr/bin/env python3

"""Validate appended Databricks changelog entries for a pull request."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass


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


def validate_changelog(base_contents: str, head_contents: str, pr_url: str) -> list[Entry]:
    """Return valid appended entries or raise a policy error."""
    base_entries = parse_entries(base_contents)
    head_entries = parse_entries(head_contents)

    if not base_entries:
        raise ValidationError("The base changelog has no numbered entries.")

    base_numbers = [entry.number for entry in base_entries]
    existing_head_numbers = [
        entry.number for entry in head_entries[: len(base_entries)]
    ]
    if len(head_entries) < len(base_entries) or existing_head_numbers != base_numbers:
        raise ValidationError(
            "Existing changelog entry headings were inserted, removed, reordered, or "
            "renumbered. Preserve existing headings and append new entries at the end."
        )

    new_entries = head_entries[len(base_entries) :]
    if not new_entries:
        raise ValidationError(
            "No new numbered entry was appended to README.databricks.md."
        )

    expected_number = base_entries[-1].number + 1
    for entry in new_entries:
        if entry.number != expected_number:
            raise ValidationError(
                f"Expected appended changelog entry {expected_number}, found "
                f"{entry.number} on line {entry.start_line}."
            )
        if not contains_exact_pr_url(entry.text, pr_url):
            raise ValidationError(
                f"Changelog entry {entry.number} must contain the exact current PR "
                f"URL: {pr_url}"
            )
        expected_number += 1

    return new_entries


def git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def read_file_at_revision(revision: str, path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--head-ref", default="HEAD")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pr-number", required=True, type=int)
    parser.add_argument("--path", default="README.databricks.md")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        merge_base = git_output("merge-base", args.base_ref, args.head_ref)
        base_contents = read_file_at_revision(merge_base, args.path)
        head_contents = read_file_at_revision(args.head_ref, args.path)
        pr_url = f"https://github.com/{args.repository}/pull/{args.pr_number}"
        new_entries = validate_changelog(base_contents, head_contents, pr_url)
    except ValidationError as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or str(error)
        print(f"::error::Unable to inspect the changelog: {detail}", file=sys.stderr)
        return 2

    numbers = ", ".join(str(entry.number) for entry in new_entries)
    print(f"Validated Databricks changelog entry or entries: {numbers}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
