"""Tests for period-based reconciliation filtering."""

import pytest
from sqlalchemy import create_engine, text

from etl.reconcile import run_reconciliation


def test_entry_balance_filters_by_period() -> None:
    """Test that entry balance check only validates entries within period."""
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
                transform_version INTEGER NOT NULL,
                item_id TEXT
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
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Bank Checking', 'asset', 1),
                (2, 'Expenses:Dining', 'Dining', 'expense', 0)
        """)
        )

        # Seed mapping for the cash account
        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES ('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES ('plaid_checking', 1)
        """)
        )

        # Q1 2024 balanced entry (mid-quarter)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (1, 'q1-balanced', '2024-02-15', 'Q1 balanced entry', 'USD',
                   'hash_q1', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Q1 2024 balanced entry on boundary (2024-03-31 - last day of Q1)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (2, 'q1-boundary-end', '2024-03-31',
                    'Q1 boundary end balanced', 'USD',
                   'hash_q1_boundary', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (2, 1, 'debit', 50.00),
                (2, 2, 'credit', 50.00)
        """)
        )

        # Q2 2024 unbalanced entry on boundary (2024-04-01 - first day of Q2)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (3, 'q2-boundary-start', '2024-04-01',
                    'Q2 boundary start unbalanced', 'USD',
                   'hash_q2_boundary', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (3, 1, 'debit', 30.00),
                (3, 2, 'credit', 40.00)  -- Intentionally unbalanced
        """)
        )

        # Q2 2024 unbalanced entry (mid-quarter)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (4, 'q2-unbalanced', '2024-05-15', 'Q2 unbalanced entry', 'USD',
                   'hash_q2', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (4, 1, 'debit', 50.00),
                (4, 2, 'credit', 75.00)  -- Intentionally unbalanced
        """)
        )

        # Test Q1 reconciliation - should pass (sees balanced entries, ignores Q2)
        result_q1 = run_reconciliation(conn, period="2024Q1", plaid_balances={})

        # Test Q2 reconciliation - should fail (includes unbalanced entries)
        result_q2 = run_reconciliation(conn, period="2024Q2", plaid_balances={})

        # Q1 should pass entry balance check (sees both balanced Q1 entries)
        assert result_q1["checks"]["entry_balance"]["passed"] is True
        assert len(result_q1["checks"]["entry_balance"]["unbalanced_entries"]) == 0

        # Q2 should fail entry balance check (includes 2 unbalanced entries)
        assert result_q2["checks"]["entry_balance"]["passed"] is False
        assert len(result_q2["checks"]["entry_balance"]["unbalanced_entries"]) == 2
        assert (
            "q2-boundary-start"
            in result_q2["checks"]["entry_balance"]["unbalanced_entries"]
        )
        assert (
            "q2-unbalanced"
            in result_q2["checks"]["entry_balance"]["unbalanced_entries"]
        )


def test_cash_variance_uses_asof_semantics() -> None:
    """Test that cash variance uses as-of (cumulative) GL balances through period."""
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
                transform_version INTEGER NOT NULL,
                item_id TEXT
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

        # Q1 2024: cash increases by $100 (mid-quarter)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (1, 'q1-cash', '2024-03-15', 'Q1 cash increase', 'USD',
                   'hash_q1_cash', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Q1 2024: cash increases by $50 on boundary (2024-03-31 - last day of Q1)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (2, 'q1-boundary-cash', '2024-03-31',
                    'Q1 boundary cash increase', 'USD',
                   'hash_q1_boundary_cash', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (2, 1, 'debit', 50.00),
                (2, 2, 'credit', 50.00)
        """)
        )

        # Q2 2024: cash increases by $75 on boundary (2024-04-01 - first day of Q2)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (3, 'q2-boundary-cash', '2024-04-01',
                    'Q2 boundary cash increase', 'USD',
                   'hash_q2_boundary_cash', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (3, 1, 'debit', 75.00),
                (3, 2, 'credit', 75.00)
        """)
        )

        # Q2 2024: cash increases by $125 more (mid-quarter)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (4, 'q2-cash', '2024-06-15', 'Q2 cash increase', 'USD',
                   'hash_q2_cash', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (4, 1, 'debit', 125.00),
                (4, 2, 'credit', 125.00)
        """)
        )

        # Test Q1 reconciliation with end-of-period balance
        # Q1 movements: $100 (mid-quarter) + $50 (boundary) = $150 total
        plaid_ending_balances_q1 = {"plaid_checking": 150.00}  # End of Q1: $150
        result_q1 = run_reconciliation(
            conn, period="2024Q1", plaid_balances=plaid_ending_balances_q1
        )

        # Test Q2 reconciliation with as-of end-of-period balance
        # Q2 movements: $75 (boundary) + $125 (mid-quarter) = $200 period total
        # End of Q2 cumulative (as-of): $150 (from Q1) + $200 (Q2) = $350 total
        plaid_ending_balances_q2 = {
            "plaid_checking": 350.00
        }  # As-of Q2 end: cumulative
        result_q2 = run_reconciliation(
            conn, period="2024Q2", plaid_balances=plaid_ending_balances_q2
        )

        # Q1 should pass variance check (GL end-of-Q1: $150, Plaid: $150)
        assert result_q1["checks"]["cash_variance"]["passed"] is True
        assert result_q1["checks"]["cash_variance"]["variance"] == pytest.approx(
            0.00, abs=1e-2
        )

        # Q2 should pass variance check (GL as-of Q2: $350, Plaid: $350)
        assert result_q2["checks"]["cash_variance"]["passed"] is True
        assert result_q2["checks"]["cash_variance"]["variance"] == pytest.approx(
            0.00, abs=1e-2
        )


def test_lineage_presence_filters_by_period() -> None:
    """Test that lineage check only validates entries within period."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        # Create schema (allowing NULLs for lineage test)
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
                source_hash TEXT,  -- Allow NULL
                transform_version INTEGER,  -- Allow NULL
                item_id TEXT
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

        # Seed mapping for the cash account
        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES ('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES ('plaid_checking', 1)
        """)
        )

        # Q1 2024: good lineage (mid-quarter)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (1, 'q1-good-lineage', '2024-01-15', 'Q1 good lineage', 'USD',
                   'hash_q1_good', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 50.00),
                (1, 2, 'credit', 50.00)
        """)
        )

        # Q1 2024: good lineage on boundary (2024-03-31 - last day of Q1)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (2, 'q1-boundary-lineage', '2024-03-31',
                    'Q1 boundary good lineage', 'USD',
                   'hash_q1_boundary_good', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (2, 1, 'debit', 30.00),
                (2, 2, 'credit', 30.00)
        """)
        )

        # Q2 2024: missing lineage on boundary (2024-04-01 - first day of Q2)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (3, 'q2-boundary-missing', '2024-04-01',
                    'Q2 boundary missing lineage', 'USD',
                   NULL, NULL)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (3, 1, 'debit', 20.00),
                (3, 2, 'credit', 20.00)
        """)
        )

        # Q2 2024: missing lineage (mid-quarter)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version)
            VALUES (4, 'q2-missing-lineage', '2024-04-15', 'Q2 missing lineage', 'USD',
                   NULL, NULL)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (4, 1, 'debit', 25.00),
                (4, 2, 'credit', 25.00)
        """)
        )

        # Test Q1 reconciliation - should pass (sees good lineage entries, ignores Q2)
        result_q1 = run_reconciliation(conn, period="2024Q1", plaid_balances={})

        # Test Q2 reconciliation - should fail (includes missing lineage entries)
        result_q2 = run_reconciliation(conn, period="2024Q2", plaid_balances={})

        # Q1 should pass lineage check (sees both good lineage Q1 entries)
        assert result_q1["checks"]["lineage"]["passed"] is True
        assert result_q1["checks"]["lineage"]["missing_lineage"] == 0

        # Q2 should fail lineage check (includes 2 missing lineage entries)
        assert result_q2["checks"]["lineage"]["passed"] is False
        assert result_q2["checks"]["lineage"]["missing_lineage"] == 2
