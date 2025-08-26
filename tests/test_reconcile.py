"""Tests for reconciliation checks."""

import pytest
from sqlalchemy import create_engine, text

from etl.reconcile import run_reconciliation


def test_reconcile_success_zero_variance_and_balanced() -> None:
    """Test successful reconciliation with balanced entries and zero variance."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        # Create minimal schema
        conn.execute(
            text("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                is_cash BOOLEAN NOT NULL DEFAULT 0
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_entries (
                id INTEGER PRIMARY KEY,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT NOT NULL,
                currency TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                transform_version INTEGER NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency TEXT NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE account_links (
                id INTEGER PRIMARY KEY,
                plaid_account_id TEXT UNIQUE NOT NULL,
                account_id INTEGER NOT NULL REFERENCES accounts(id)
            )
        """)
        )

        # Seed data
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Bank Checking', 'asset', 1),
                (2, 'Expenses:Dining', 'Dining', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES ('plaid_checking', 'Chase Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES ('plaid_checking', 1)
        """)
        )

        # Balanced journal entry with proper lineage
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (1, 'test-001', '2024-01-15', 'Test transaction', 'USD',
                   'hash123', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 50.00),   -- Cash increases by 50
                (1, 2, 'credit', 50.00)   -- Expense offset
        """)
        )

        # Mock Plaid balances (matching GL exactly)
        plaid_balances = {
            "plaid_checking": 50.00  # Matches debit to cash account
        }

        result = run_reconciliation(
            conn, period="2024Q1", plaid_balances=plaid_balances
        )

        assert result["success"] is True
        assert result["checks"]["entry_balance"]["passed"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["checks"]["cash_variance"]["variance"] == pytest.approx(
            0.00, abs=1e-2
        )
        assert result["checks"]["lineage"]["passed"] is True


def test_reconcile_fails_on_unbalanced_entries() -> None:
    """Test reconciliation fails when entries have unbalanced debits/credits."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        # Create schema
        conn.execute(
            text("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                is_cash BOOLEAN NOT NULL DEFAULT 0
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_entries (
                id INTEGER PRIMARY KEY,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT NOT NULL,
                currency TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                transform_version INTEGER NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency TEXT NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE account_links (
                id INTEGER PRIMARY KEY,
                plaid_account_id TEXT UNIQUE NOT NULL,
                account_id INTEGER NOT NULL REFERENCES accounts(id)
            )
        """)
        )

        # Seed accounts
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type) VALUES
                (1, 'Assets:Bank:Checking', 'Bank Checking', 'asset'),
                (2, 'Expenses:Dining', 'Dining', 'expense')
        """)
        )

        # Unbalanced entry (debits != credits)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (1, 'unbalanced-001', '2024-01-15', 'Unbalanced transaction', 'USD',
                   'hash456', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'credit', 50.00),
                (1, 2, 'debit', 75.00)  -- Doesn't balance!
        """)
        )

        result = run_reconciliation(conn, period="2024Q1", plaid_balances={})

        assert result["success"] is False
        assert result["checks"]["entry_balance"]["passed"] is False
        assert len(result["checks"]["entry_balance"]["unbalanced_entries"]) == 1
        assert (
            "unbalanced-001" in result["checks"]["entry_balance"]["unbalanced_entries"]
        )


def test_reconcile_fails_on_cash_ledger_variance() -> None:
    """Test reconciliation fails when cash variance exceeds tolerance."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        # Create schema
        conn.execute(
            text("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                is_cash BOOLEAN NOT NULL DEFAULT 0
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_entries (
                id INTEGER PRIMARY KEY,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT NOT NULL,
                currency TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                transform_version INTEGER NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency TEXT NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE account_links (
                id INTEGER PRIMARY KEY,
                plaid_account_id TEXT UNIQUE NOT NULL,
                account_id INTEGER NOT NULL REFERENCES accounts(id)
            )
        """)
        )

        # Seed cash account
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Bank Checking', 'asset', 1),
                (2, 'Expenses:Dining', 'Dining', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES ('plaid_checking', 'Chase Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES ('plaid_checking', 1)
        """)
        )

        # Balanced entry
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (1, 'test-001', '2024-03-31', 'Test transaction', 'USD',
                   'hash789', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),  -- Cash increases
                (1, 2, 'credit', 100.00)
        """)
        )

        # Plaid balance doesn't match GL (variance > 0.01)
        plaid_balances = {
            "plaid_checking": 150.00  # GL shows 100, variance = 50.00
        }

        result = run_reconciliation(
            conn, period="2024Q1", plaid_balances=plaid_balances
        )

        assert result["success"] is False
        assert result["checks"]["cash_variance"]["passed"] is False
        assert result["checks"]["cash_variance"]["variance"] == pytest.approx(
            50.00, abs=1e-2
        )
        assert result["checks"]["cash_variance"]["tolerance"] == pytest.approx(
            0.01, abs=1e-2
        )


def test_lineage_presence_gate() -> None:
    """Test that missing source_hash or transform_version fails reconciliation."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        # Create schema (allowing NULLs for this test)
        conn.execute(
            text("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                is_cash BOOLEAN NOT NULL DEFAULT 0
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_entries (
                id INTEGER PRIMARY KEY,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT NOT NULL,
                currency TEXT NOT NULL,
                source_hash TEXT,  -- Allow NULL for test
                transform_version INTEGER  -- Allow NULL for test
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency TEXT NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE account_links (
                id INTEGER PRIMARY KEY,
                plaid_account_id TEXT UNIQUE NOT NULL,
                account_id INTEGER NOT NULL REFERENCES accounts(id)
            )
        """)
        )

        # Seed accounts
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type) VALUES
                (1, 'Assets:Bank:Checking', 'Bank Checking', 'asset'),
                (2, 'Expenses:Dining', 'Dining', 'expense')
        """)
        )

        # Entry without lineage information
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES
                (1, 'missing-lineage-001', '2024-01-15', 'No lineage', 'USD', NULL,
                 NULL),
                (2, 'partial-lineage-001', '2024-01-16', 'Partial lineage', 'USD',
                 'hash', NULL)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 50.00),
                (1, 2, 'credit', 50.00),
                (2, 1, 'debit', 25.00),
                (2, 2, 'credit', 25.00)
        """)
        )

        result = run_reconciliation(conn, period="2024Q1", plaid_balances={})

        assert result["success"] is False
        assert result["checks"]["lineage"]["passed"] is False
        assert result["checks"]["lineage"]["missing_lineage"] == 2
