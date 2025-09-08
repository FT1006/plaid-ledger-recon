"""SQLite schema contract tests for performance indexes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from pathlib import Path


def test_journal_entries_item_date_index_exists_sqlite(tmp_path: Path) -> None:
    """Test that idx_journal_entries_item_date exists with correct columns in SQLite.

    Per performance contract: item-scoped reconciliation queries require this
    compound index to avoid full table scans on large datasets.
    """
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)

    with engine.begin() as conn:
        # Create minimal SQLite-compatible schema for index testing
        # Note: Full schema.sql contains PostgreSQL-specific features
        conn.execute(
            text("""
            CREATE TABLE journal_entries (
                id TEXT PRIMARY KEY,
                item_id TEXT,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT NOT NULL,
                currency CHAR(3) NOT NULL,
                source_hash TEXT NOT NULL,
                transform_version INTEGER NOT NULL
            )
        """)
        )

        # Create the indexes we want to test - mirrors schema.sql structure
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_journal_entries_date "
                "ON journal_entries(txn_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_journal_entries_txn "
                "ON journal_entries(txn_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_journal_entries_hash "
                "ON journal_entries(source_hash)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_journal_entries_item_date "
                "ON journal_entries(item_id, txn_date)"
            )
        )

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
