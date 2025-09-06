"""Test that all test fixtures comply with schema contracts from ADR."""

from __future__ import annotations

import re
from pathlib import Path


def test_journal_entries_fixtures_include_item_id() -> None:
    """Test that all CREATE TABLE journal_entries in test files include item_id column.

    Per ADR 1.5.1: journal_entries.item_id is part of the schema contract.
    All test fixtures must comply to prevent drift.
    """
    test_files = list(Path("tests").rglob("*.py"))
    violations = []

    for file_path in test_files:
        with file_path.open() as f:
            content = f.read()

        # Find all CREATE TABLE journal_entries statements
        create_patterns = re.finditer(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?journal_entries\s*\(",
            content,
            re.IGNORECASE | re.MULTILINE,
        )

        for match in create_patterns:
            # Extract the full CREATE TABLE statement
            start = match.start()
            paren_count = 0
            end = start
            in_create = False

            for i, char in enumerate(content[start:]):
                if char == "(":
                    paren_count += 1
                    in_create = True
                elif char == ")" and in_create:
                    paren_count -= 1
                    if paren_count == 0:
                        end = start + i + 1
                        break

            if end > start:
                table_def = content[start:end]

                # Check for exemption comment
                if "# SCHEMA-CONTRACT-EXEMPT:" in table_def:
                    continue

                # Check if item_id column exists as actual column definition
                if not re.search(
                    r"^\s*item_id\b", table_def, re.IGNORECASE | re.MULTILINE
                ):
                    violations.append({
                        "file": str(file_path),
                        "table_def": table_def[:200] + "..."
                        if len(table_def) > 200
                        else table_def,
                    })

    if violations:
        violation_msg = "\n".join([
            f"- {v['file']}: {v['table_def']}" for v in violations
        ])
        fix_hint = (
            "\nQuick fix: Add 'item_id TEXT' to each fixture's "
            "CREATE TABLE journal_entries statement."
        )
        msg = (
            f"Found journal_entries CREATE TABLE statements missing item_id "
            f"column:\n{violation_msg}{fix_hint}"
        )
        raise AssertionError(msg)


def test_all_test_schemas_are_discoverable() -> None:
    """Ensure our schema scanning logic can find CREATE TABLE statements correctly."""
    # This is a meta-test to verify our scanning works
    test_files = list(Path("tests").rglob("*.py"))
    found_any_journal_entries = False

    for file_path in test_files:
        with file_path.open() as f:
            content = f.read()

        if "CREATE TABLE journal_entries" in content:
            found_any_journal_entries = True
            break

    assert found_any_journal_entries, (
        "Schema scanner should find at least one journal_entries table"
    )
