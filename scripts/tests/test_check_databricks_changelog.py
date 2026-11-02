import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from check_databricks_changelog import ValidationError, validate_changelog  # noqa: E402


PR_NUMBER = 123
PR_URL = "https://github.com/databricks-eng/vector/pull/123"
FROZEN = "This lists custom changes.\n10. Existing entry.\n11. Last entry.\n"


class ValidateChangelogTest(unittest.TestCase):
    def test_accepts_valid_changes(self):
        cases = [
            (
                "one fragment",
                FROZEN,
                {"databricks.changelog.d/123-fix-thing.md": f"Fix thing. {PR_URL}\n"},
                ["databricks.changelog.d/123-fix-thing.md"],
            ),
            (
                "multiple fragments for one PR",
                FROZEN,
                {
                    "databricks.changelog.d/123-first.md": f"First. {PR_URL}",
                    "databricks.changelog.d/123-second.md": f"Second.\nMore. {PR_URL}",
                },
                [
                    "databricks.changelog.d/123-first.md",
                    "databricks.changelog.d/123-second.md",
                ],
            ),
            (
                "frozen header text edited, entries unchanged",
                "Frozen; see databricks.changelog.d/.\n" + FROZEN,
                {"databricks.changelog.d/123-x.md": f"X. {PR_URL}"},
                ["databricks.changelog.d/123-x.md"],
            ),
            (
                "fragment directory README ignored",
                FROZEN,
                {
                    "databricks.changelog.d/README.md": "How to write fragments.",
                    "databricks.changelog.d/123-x.md": f"X. {PR_URL}",
                },
                ["databricks.changelog.d/123-x.md"],
            ),
        ]

        for name, head_frozen, added, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(
                    expected,
                    validate_changelog(FROZEN, head_frozen, added, PR_NUMBER, PR_URL),
                )

    def test_rejects_invalid_changes(self):
        cases = [
            (
                "no fragment",
                FROZEN,
                {},
                "No changelog fragment",
            ),
            (
                "only the directory README",
                FROZEN,
                {"databricks.changelog.d/README.md": "Docs."},
                "No changelog fragment",
            ),
            (
                "entry appended to frozen file",
                FROZEN + f"12. New entry. {PR_URL}\n",
                {"databricks.changelog.d/123-x.md": f"X. {PR_URL}"},
                "frozen",
            ),
            (
                "frozen entry edited",
                FROZEN.replace("Last entry.", "Edited entry."),
                {"databricks.changelog.d/123-x.md": f"X. {PR_URL}"},
                "frozen",
            ),
            (
                "fragment named for another PR",
                FROZEN,
                {"databricks.changelog.d/122-x.md": f"X. {PR_URL}"},
                "named for PR 122",
            ),
            (
                "fragment name without slug",
                FROZEN,
                {"databricks.changelog.d/123.md": f"X. {PR_URL}"},
                "not a valid changelog fragment name",
            ),
            (
                "fragment name with uppercase",
                FROZEN,
                {"databricks.changelog.d/123-Fix.md": f"X. {PR_URL}"},
                "not a valid changelog fragment name",
            ),
            (
                "empty fragment",
                FROZEN,
                {"databricks.changelog.d/123-x.md": "  \n"},
                "empty",
            ),
            (
                "missing PR link",
                FROZEN,
                {"databricks.changelog.d/123-x.md": "No link."},
                "exact current PR URL",
            ),
            (
                "wrong PR link",
                FROZEN,
                {
                    "databricks.changelog.d/123-x.md": (
                        "X. https://github.com/databricks-eng/vector/pull/122"
                    )
                },
                "exact current PR URL",
            ),
            (
                "current PR URL is only a prefix",
                FROZEN,
                {"databricks.changelog.d/123-x.md": f"X. {PR_URL}4"},
                "exact current PR URL",
            ),
        ]

        for name, head_frozen, added, error_pattern in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValidationError, error_pattern):
                    validate_changelog(FROZEN, head_frozen, added, PR_NUMBER, PR_URL)


if __name__ == "__main__":
    unittest.main()
