"""PostgreSQL schema contract tests for performance indexes."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.utils.db_helper import create_test_engine

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="Requires PostgreSQL; set DATABASE_URL to run.",
)


@pytest.mark.integration
def test_journal_entries_item_date_index_exists_postgres() -> None:
    """Test idx_journal_entries_item_date exists with correct definition in PostgreSQL.

    Per performance contract: item-scoped reconciliation queries require this
    compound index to avoid full table scans on large datasets.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_test_engine(database_url)
    schema_name = f"test_idx_{uuid4().hex[:8]}"

    with engine.connect() as conn:
        # Create isolated test schema
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))
        conn.commit()

        try:
            # Load production schema.sql
            # Note: Path is relative to test file.
            # Alternative: use CLI init-db in temp schema
            schema_path = Path(__file__).parent.parent / "etl" / "schema.sql"
            schema_sql = schema_path.read_text(encoding="utf-8")
            conn.execute(text(schema_sql))
            conn.commit()

            # Check if index exists using pg_indexes system catalog
            row = conn.execute(text("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = :schema_name
                  AND tablename = 'journal_entries'
                  AND indexname = 'idx_journal_entries_item_date'
            """), {"schema_name": schema_name}).fetchone()

            assert row, (
                "Missing performance index idx_journal_entries_item_date. "
                "This index is required for efficient item-scoped reconciliation "
                "queries."
            )

            # Verify it's a btree index on correct columns
            assert "USING btree" in row.indexdef, (
                f"idx_journal_entries_item_date should be btree index. "
                f"Found: {row.indexdef}"
            )

            assert "(item_id, txn_date)" in row.indexdef, (
                f"idx_journal_entries_item_date should be on (item_id, txn_date). "
                f"Found: {row.indexdef}"
            )

        finally:
            # Cleanup
            with contextlib.suppress(Exception):
                conn.rollback()
            with contextlib.suppress(Exception):
                conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))
                conn.commit()


@pytest.mark.integration
def test_reconcile_query_uses_item_date_index_postgres() -> None:
    """Test reconciliation queries use performance index (optional runtime check).

    Seeds 1-2 rows to make index selection deterministic across PostgreSQL
    versions. Empty tables can have quirky query plans.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_test_engine(database_url)
    schema_name = f"test_plan_{uuid4().hex[:8]}"

    with engine.connect() as conn:
        # Create isolated test schema
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))
        conn.commit()

        try:
            # Load production schema.sql
            # Note: Path is relative to test file.
            # Alternative: use CLI init-db in temp schema
            schema_path = Path(__file__).parent.parent / "etl" / "schema.sql"
            schema_sql = schema_path.read_text(encoding="utf-8")
            conn.execute(text(schema_sql))
            conn.commit()

            # Seed minimal data to make planner behavior deterministic
            # Use integer PKs to avoid UUID extension dependency
            conn.execute(text("""
                INSERT INTO journal_entries(id, txn_id, txn_date, description,
                                           currency, source_hash, transform_version,
                                           item_id)
                SELECT gen_random_uuid(), 'seed', '2024-03-31', 'seed', 'USD',
                       'h', 1, 'test_item_123'
            """))

            # Get the entry_id and a valid account_id for the journal_lines FK
            entry_id = conn.execute(text(
                "SELECT id FROM journal_entries WHERE txn_id = 'seed'"
            )).scalar()

            account_id = conn.execute(text(
                "SELECT id FROM accounts WHERE code = 'Assets:Bank:Checking' LIMIT 1"
            )).scalar()

            conn.execute(text("""
                INSERT INTO journal_lines(id, entry_id, account_id, side, amount)
                VALUES (gen_random_uuid(), :entry_id, :account_id, 'debit', 0.00)
            """), {"entry_id": entry_id, "account_id": account_id})

            conn.commit()

            # Force index usage to avoid planner variance
            conn.execute(text("SET enable_seqscan = off"))

            # Test the critical reconciliation query pattern
            plan_rows = conn.execute(text("""
                EXPLAIN (FORMAT TEXT)
                SELECT je.id
                FROM journal_entries je
                JOIN journal_lines jl ON je.id = jl.entry_id
                WHERE je.item_id = :item_id AND je.txn_date <= :period_end
            """), {
                "item_id": "test_item_123",
                "period_end": "2024-03-31"
            }).fetchall()

            plan_text = "\n".join(row[0] for row in plan_rows)

            # Verify index is used (this would fail if index is missing)
            assert "Index Scan" in plan_text or "Bitmap Index Scan" in plan_text, (
                f"Query should use index scan. Plan: {plan_text}"
            )

            assert "idx_journal_entries_item_date" in plan_text, (
                f"Query should use idx_journal_entries_item_date index. "
                f"Plan: {plan_text}"
            )

        finally:
            # Cleanup
            with contextlib.suppress(Exception):
                conn.rollback()
            with contextlib.suppress(Exception):
                conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))
                conn.commit()
