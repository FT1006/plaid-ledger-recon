"""Schema guard tests to ensure production schema matches ADR requirements."""

from __future__ import annotations

import re
from pathlib import Path


def test_ingest_accounts_has_item_id_column() -> None:
    """Verify ingest_accounts table has item_id column per ADR.

    Post Step B migration: item_id should be NOT NULL and part of composite PK.
    This test prevents schema drift between tests and production.
    """
    schema_path = Path(__file__).parent.parent / "etl" / "schema.sql"
    schema_content = schema_path.read_text(encoding="utf-8")

    # Find the ingest_accounts table definition
    # Match from CREATE TABLE to the closing );
    pattern = r"CREATE TABLE IF NOT EXISTS ingest_accounts\s*\((.*?)\);"
    match = re.search(pattern, schema_content, re.DOTALL | re.IGNORECASE)

    assert match, "Could not find ingest_accounts table definition in schema.sql"

    table_def = match.group(1)

    # Check for item_id column with NOT NULL constraint
    item_id_pattern = r"\bitem_id\s+TEXT\s+NOT\s+NULL\b"
    has_item_id = bool(re.search(item_id_pattern, table_def, re.IGNORECASE))

    assert has_item_id, (
        "ingest_accounts table must have item_id TEXT NOT NULL per Step B migration. "
        "The composite PK (item_id, plaid_account_id) requires NOT NULL item_id."
    )


def test_ingest_accounts_has_composite_primary_key() -> None:
    """Verify ingest_accounts has composite PRIMARY KEY per Step B migration."""
    schema_path = Path(__file__).parent.parent / "etl" / "schema.sql"
    schema_content = schema_path.read_text(encoding="utf-8")

    # Find the ingest_accounts table definition
    pattern = r"CREATE TABLE IF NOT EXISTS ingest_accounts\s*\((.*?)\);"
    match = re.search(pattern, schema_content, re.DOTALL | re.IGNORECASE)

    assert match, "Could not find ingest_accounts table definition in schema.sql"

    table_def = match.group(1)

    # Check for composite PRIMARY KEY (item_id, plaid_account_id)
    pk_pattern = r"PRIMARY\s+KEY\s*\(\s*item_id\s*,\s*plaid_account_id\s*\)"
    has_composite_pk = bool(re.search(pk_pattern, table_def, re.IGNORECASE))

    assert has_composite_pk, (
        "ingest_accounts must have composite PRIMARY KEY (item_id, plaid_account_id) "
        "per Step B migration. This enables proper item-scoped account management."
    )


def test_ingest_accounts_has_item_id_index() -> None:
    """Verify ingest_accounts has an index on item_id for query performance."""
    schema_path = Path(__file__).parent.parent / "etl" / "schema.sql"
    schema_content = schema_path.read_text(encoding="utf-8")

    # Look for item_id index (composite PK provides one, but separate index is good too)
    index_patterns = [
        (
            r"CREATE\s+(?:UNIQUE\s+)?"
            r"INDEX.*idx_ingest_accounts_item_id.*ON\s+ingest_accounts.*\bitem_id\b"
        ),
        r"PRIMARY KEY\s*\(\s*item_id\s*,\s*plaid_account_id\s*\)",
        # Composite PK covers item_id
    ]

    has_index = any(
        re.search(pattern, schema_content, re.IGNORECASE | re.DOTALL)
        for pattern in index_patterns
    )

    assert has_index, (
        "ingest_accounts should have an index covering item_id for efficient queries. "
        "Either CREATE INDEX idx_ingest_accounts_item_id ON ingest_accounts(item_id) "
        "or composite PRIMARY KEY (item_id, plaid_account_id) provides this."
    )
