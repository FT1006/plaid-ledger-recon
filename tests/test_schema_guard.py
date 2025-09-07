"""Schema guard tests to ensure production schema matches ADR requirements."""

from __future__ import annotations

import re
from pathlib import Path


def test_ingest_accounts_has_item_id_column() -> None:
    """Verify ingest_accounts table has item_id column per ADR.
    
    The ADR and diagrams specify that ingest_accounts should have both
    item_id and plaid_account_id columns to support item-scoped queries.
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
    
    # Check for item_id column
    # Should match patterns like:
    # - item_id TEXT
    # - item_id TEXT NOT NULL
    # - item_id TEXT,
    item_id_pattern = r"\bitem_id\s+TEXT\b"
    has_item_id = bool(re.search(item_id_pattern, table_def, re.IGNORECASE))
    
    assert has_item_id, (
        "ingest_accounts table must have item_id column per ADR. "
        "The ADR requires (item_id, plaid_account_id) to support item-scoped queries. "
        "Add: item_id TEXT to the table definition."
    )


def test_ingest_accounts_has_item_id_index() -> None:
    """Verify ingest_accounts has an index on item_id for query performance."""
    schema_path = Path(__file__).parent.parent / "etl" / "schema.sql"
    schema_content = schema_path.read_text(encoding="utf-8")
    
    # Look for any index that includes item_id
    # Could be a single column index or composite index
    index_patterns = [
        r"CREATE\s+(?:UNIQUE\s+)?INDEX.*ON\s+ingest_accounts.*\bitem_id\b",
        r"PRIMARY KEY\s*\(\s*item_id",
        r"PRIMARY KEY\s*\([^)]*\bitem_id\b",
    ]
    
    has_index = any(
        re.search(pattern, schema_content, re.IGNORECASE | re.DOTALL)
        for pattern in index_patterns
    )
    
    assert has_index, (
        "ingest_accounts should have an index on item_id for efficient queries. "
        "Add either: CREATE INDEX ON ingest_accounts(item_id) or "
        "use composite PRIMARY KEY (item_id, plaid_account_id)"
    )