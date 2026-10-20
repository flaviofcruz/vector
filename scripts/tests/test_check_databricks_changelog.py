import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from check_databricks_changelog import ValidationError, validate_changelog  # noqa: E402


PR_URL = "https://github.com/databricks-eng/vector/pull/123"


def changelog(*entries: tuple[int, str]) -> str:
    rendered = ["# Databricks changelog", ""]
    rendered.extend(f"{number}. {body}" for number, body in entries)
    return "\n".join(rendered) + "\n"


class ValidateChangelogTest(unittest.TestCase):
    def test_accepts_valid_changes(self):
        cases = [
            (
                "one appended entry",
                changelog((10, "Existing entry.")),
                changelog((10, "Existing entry."), (11, f"New entry. {PR_URL}")),
                [11],
            ),
            (
                "multiple consecutive entries",
                changelog((10, "Existing entry.")),
                changelog(
                    (10, "Existing entry."),
                    (11, f"First change. {PR_URL}"),
                    (12, f"Second change. {PR_URL}"),
                ),
                [11, 12],
            ),
            (
                "link on a continuation line",
                changelog((10, "Existing entry.")),
                changelog(
                    (10, "Existing entry."),
                    (11, f"A multiline entry.\nAdditional context and {PR_URL}"),
                ),
                [11],
            ),
            (
                "historical link backfill with a new summary",
                changelog((10, "Old entry without a link.")),
                changelog(
                    (
                        10,
                        "Old entry with its historical link. "
                        "https://github.com/databricks-eng/vector/pull/42",
                    ),
                    (11, f"Backfill missing historical links. {PR_URL}"),
                ),
                [11],
            ),
            (
                "preserved historical duplicate numbers",
                changelog(
                    (10, "First."), (10, "Historical duplicate."), (11, "Last.")
                ),
                changelog(
                    (10, "First."),
                    (10, "Historical duplicate."),
                    (11, "Last."),
                    (12, f"New entry. {PR_URL}"),
                ),
                [12],
            ),
        ]

        for name, base, head, expected_numbers in cases:
            with self.subTest(name=name):
                entries = validate_changelog(base, head, PR_URL)
                self.assertEqual(expected_numbers, [entry.number for entry in entries])

    def test_rejects_invalid_changes(self):
        cases = [
            (
                "backfill without a new summary",
                changelog((10, "Old entry without a link.")),
                changelog(
                    (
                        10,
                        "Old entry with its historical link. "
                        "https://github.com/databricks-eng/vector/pull/42",
                    )
                ),
                "No new numbered entry",
            ),
            (
                "missing PR link",
                changelog((10, "Existing entry.")),
                changelog((10, "Existing entry."), (11, "Missing its link.")),
                "exact current PR URL",
            ),
            (
                "one missing link among multiple new entries",
                changelog((10, "Existing entry.")),
                changelog(
                    (10, "Existing entry."),
                    (11, f"Linked change. {PR_URL}"),
                    (12, "Unlinked change."),
                ),
                "entry 12",
            ),
            (
                "wrong PR link",
                changelog((10, "Existing entry.")),
                changelog(
                    (10, "Existing entry."),
                    (
                        11,
                        "Wrong link. https://github.com/databricks-eng/vector/pull/122",
                    ),
                ),
                "exact current PR URL",
            ),
            (
                "current PR URL is only a prefix",
                changelog((10, "Existing entry.")),
                changelog(
                    (10, "Existing entry."),
                    (11, f"Wrong link. {PR_URL}4"),
                ),
                "exact current PR URL",
            ),
            (
                "nonconsecutive new entry number",
                changelog((10, "Existing entry.")),
                changelog(
                    (10, "Existing entry."), (12, f"Skipped 11. {PR_URL}")
                ),
                "Expected.*11.*found 12",
            ),
            (
                "inserted numbered entry",
                changelog((10, "First."), (11, "Second.")),
                changelog(
                    (10, "First."),
                    (99, "Inserted."),
                    (11, "Second."),
                    (12, f"Appended. {PR_URL}"),
                ),
                "inserted, removed, reordered",
            ),
            (
                "renumbered existing entry",
                changelog((10, "First."), (11, "Second.")),
                changelog((10, "First."), (12, "Renumbered.")),
                "inserted, removed, reordered",
            ),
        ]

        for name, base, head, error_pattern in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValidationError, error_pattern):
                    validate_changelog(base, head, PR_URL)


if __name__ == "__main__":
    unittest.main()
