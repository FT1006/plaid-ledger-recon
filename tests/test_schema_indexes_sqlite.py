"""SQLite schema contract tests for performance indexes."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text


def test_journal_entries_item_date_index_exists_sqlite(tmp_path: Path) -> None:
    """Test that idx_journal_entries_item_date exists with correct columns in SQLite.

    Per performance contract: item-scoped reconciliation queries require this
    compound index to avoid full table scans on large datasets.
    """
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)

    with engine.begin() as conn:
        # Load production schema.sql
        # Note: Path is relative to test file.
        # Alternative: use CLI init-db in temp schema
        schema_path = Path(__file__).parent.parent / "etl" / "schema.sql"
        schema_sql = schema_path.read_text(encoding="utf-8")
        conn.execute(text(schema_sql))

        # Check if index exists
        idx_rows = conn.execute(text("PRAGMA index_list('journal_entries')")).fetchall()
        idx_names = {row[1] for row in idx_rows}  # row[1] = name

        assert "idx_journal_entries_item_date" in idx_names, (
            "Missing performance index idx_journal_entries_item_date. "
            "This index is required for efficient item-scoped reconciliation queries."
        )

        # Check column order is (item_id, txn_date)
        col_rows = conn.execute(
            text("PRAGMA index_info('idx_journal_entries_item_date')")
        ).fetchall()
        col_names = [row[2] for row in col_rows]  # row[2] = column name

        assert col_names == ["item_id", "txn_date"], (
            f"idx_journal_entries_item_date has wrong column order: {col_names}. "
            f"Expected: ['item_id', 'txn_date'] for optimal query performance."
        )
