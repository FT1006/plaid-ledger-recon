"""M6: Tests for AS-OF reconciliation with item scoping and coverage rules."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

import pytest
from cli import app
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from etl.reconcile import run_reconciliation

runner = CliRunner()


def _create_test_schema(conn: Any) -> None:
    """Create test database schema with item_id support."""
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
            item_id TEXT  -- NEW: item scoping
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

    conn.execute(
        text("""
        CREATE TABLE etl_events (
            id INTEGER PRIMARY KEY,
            event_type TEXT NOT NULL,
            item_id TEXT,
            period TEXT,
            row_counts TEXT,
            started_at TEXT,
            finished_at TEXT,
            success BOOLEAN NOT NULL
        )
    """)
    )


def test_reconcile_coverage_rule_missing_account() -> None:
    """Test that reconciliation fails when a mapped cash account is missing."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Seed TWO cash accounts, both mapped
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Assets:Bank:Savings', 'Savings', 'asset', 1),
                (3, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES
                ('plaid_checking', 'Checking', 'depository', 'checking', 'USD'),
                ('plaid_savings', 'Savings', 'depository', 'savings', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES
                ('plaid_checking', 1),
                ('plaid_savings', 2)
        """)
        )

        # Create balanced entries for both accounts
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES
                (
                    1, 'txn-001', '2024-03-15', 'Checking deposit', 'USD',
                    'hash1', 1, 'item_A'
                ),
                (
                    2, 'txn-002', '2024-03-20', 'Savings deposit', 'USD',
                    'hash2', 1, 'item_A'
                )
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 3, 'credit', 100.00),
                (2, 2, 'debit', 50.00),
                (2, 3, 'credit', 50.00)
        """)
        )

        # JSON includes only ONE account (missing plaid_savings)
        plaid_balances = {
            "plaid_checking": 100.00
            # plaid_savings is MISSING - should cause failure
        }

        # This should fail with coverage rule violation
        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        assert result["success"] is False
        assert "checks" in result
        assert "coverage" in result["checks"]
        assert result["checks"]["coverage"]["passed"] is False
        assert "missing" in result["checks"]["coverage"]
        assert "plaid_savings" in result["checks"]["coverage"]["missing"]
        assert result["checks"]["coverage"]["extra"] == []  # No extra, only missing


def test_reconcile_item_scoped_filtering() -> None:
    """Test that reconciliation only includes entries for the specified item."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Seed one cash account
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES('plaid_checking', 1)
        """)
        )

        # Create entries for BOTH item_A and item_B
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES
                (
                    1, 'item-a-001', '2024-02-15', 'Item A transaction',
                    'USD', 'hash1', 1, 'item_A'
                ),
                (
                    2, 'item-b-001', '2024-02-20', 'Item B transaction',
                    'USD', 'hash2', 1, 'item_B'
                )
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 75.00),   -- Item A: +75 to checking
                (1, 2, 'credit', 75.00),
                (2, 1, 'debit', 125.00),  -- Item B: +125 to checking
                (2, 2, 'credit', 125.00)
        """)
        )

        # JSON matches only item_A's ending balance (75.00)
        plaid_balances = {
            "plaid_checking": 75.00  # Should match item_A only
        }

        # Reconcile for item_A only
        result = run_reconciliation(
            conn,
            period="2024Q1",
            item_id="item_A",  # Should only see 75.00
            plaid_balances=plaid_balances,
        )

        # Should pass because GL for item_A = 75.00 matches JSON
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["checks"]["cash_variance"]["total_variance"] == pytest.approx(
            0.00, abs=1e-2
        )
        assert result["checks"]["cash_variance"]["tolerance"] == pytest.approx(
            0.01, abs=1e-3
        )
        assert result["checks"]["coverage"]["passed"] is True


def test_reconcile_cash_only_filter() -> None:
    """Test that non-cash mapped accounts don't affect cash variance check."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Seed one cash and one non-cash account, BOTH mapped
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),  -- cash
                (2, 'Assets:Receivables', 'Receivables', 'asset', 0),  -- non-cash
                (3, 'Revenue:Sales', 'Sales', 'revenue', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES
                ('plaid_checking', 'Checking', 'depository', 'checking', 'USD'),
                ('plaid_receivables', 'Receivables', 'other', 'other', 'USD')
        """)
        )

        # Map BOTH accounts
        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES
                ('plaid_checking', 1),
                ('plaid_receivables', 2)
        """)
        )

        # Create entries affecting both accounts
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES
                (
                    1, 'cash-001', '2024-03-10', 'Cash receipt', 'USD',
                    'hash1', 1, 'item_A'
                ),
                (
                    2, 'ar-001', '2024-03-15', 'AR booking', 'USD',
                    'hash2', 1, 'item_A'
                )
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 200.00),   -- Cash: +200
                (1, 3, 'credit', 200.00),
                (2, 2, 'debit', 500.00),   -- Receivables: +500
                (2, 3, 'credit', 500.00)
        """)
        )

        # JSON includes ONLY the cash account (no receivables)
        plaid_balances = {
            "plaid_checking": 200.00  # Matches cash GL
            # plaid_receivables is missing but shouldn't matter (non-cash)
        }

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass - non-cash account not required in balances
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["checks"]["cash_variance"]["total_variance"] == pytest.approx(
            0.00, abs=1e-2
        )
        assert result["checks"]["coverage"]["passed"] is True


def test_reconcile_by_account_breakdown() -> None:
    """Test that recon.json includes by_account breakdown with individual variances."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Seed two cash accounts
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Assets:Bank:Savings', 'Savings', 'asset', 1),
                (3, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES
                ('plaid_checking', 'Checking', 'depository', 'checking', 'USD'),
                ('plaid_savings', 'Savings', 'depository', 'savings', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES
                ('plaid_checking', 1),
                ('plaid_savings', 2)
        """)
        )

        # Create entries with different amounts for each account
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES
                (
                    1, 'check-001', '2024-03-01', 'Checking activity',
                    'USD', 'hash1', 1, 'item_A'
                ),
                (
                    2, 'save-001', '2024-03-02', 'Savings activity',
                    'USD', 'hash2', 1, 'item_A'
                )
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 150.00),
                (1, 3, 'credit', 150.00),
                (2, 2, 'debit', 250.00),
                (2, 3, 'credit', 250.00)
        """)
        )

        # Exact matches for successful reconciliation
        plaid_balances = {"plaid_checking": 150.00, "plaid_savings": 250.00}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should have by_account breakdown
        assert "by_account" in result
        by_account = result["by_account"]
        assert len(by_account) == 2

        # Check structure of each account entry
        for account_detail in by_account:
            assert "plaid_account_id" in account_detail
            assert "gl_asof" in account_detail
            assert "ext_asof" in account_detail
            assert "variance" in account_detail

        # Verify specific values
        checking = next(
            a for a in by_account if a["plaid_account_id"] == "plaid_checking"
        )
        assert checking["gl_asof"] == pytest.approx(150.00, abs=1e-2)
        assert checking["ext_asof"] == pytest.approx(150.00, abs=1e-2)
        assert checking["variance"] == pytest.approx(0.00, abs=1e-2)

        savings = next(
            a for a in by_account if a["plaid_account_id"] == "plaid_savings"
        )
        assert savings["gl_asof"] == pytest.approx(250.00, abs=1e-2)
        assert savings["ext_asof"] == pytest.approx(250.00, abs=1e-2)
        assert savings["variance"] == pytest.approx(0.00, abs=1e-2)

        # Should also have total_variance and tolerance in cash_variance check
        assert result["checks"]["cash_variance"]["total_variance"] == pytest.approx(
            0.00, abs=1e-2
        )
        assert result["checks"]["cash_variance"]["tolerance"] == pytest.approx(
            0.01, abs=1e-3
        )
        assert result["total_variance"] == pytest.approx(0.00, abs=1e-2)


# NOTE: test_reconcile_etl_event_written removed per ADR v1.3.0
# run_reconciliation() is now pure (no side effects)
# ETL event writing is CLI responsibility - see tests/test_reconcile_cli.py


def test_reconcile_asof_ending_balance_cumulative() -> None:
    """Test AS-OF methodology uses cumulative balance up to period end."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup account structure
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Revenue:Sales', 'Sales', 'revenue', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES('plaid_checking', 1)
        """)
        )

        # Create 3 transactions across Q1: Jan +50, Feb +30, Mar +20 = 100 cumulative
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES
                (
                    1, 'jan-001', '2024-01-15', 'January deposit', 'USD',
                    'hash1', 1, 'item_A'
                ),
                (
                    2, 'feb-001', '2024-02-15', 'February deposit', 'USD',
                    'hash2', 1, 'item_A'
                ),
                (
                    3, 'mar-001', '2024-03-15', 'March deposit', 'USD',
                    'hash3', 1, 'item_A'
                ),
                (
                    4, 'apr-001', '2024-04-05', 'April deposit', 'USD',
                    'hash4', 1, 'item_A'
                )
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 50.00),   -- Jan: +50
                (1, 2, 'credit', 50.00),
                (2, 1, 'debit', 30.00),   -- Feb: +30
                (2, 2, 'credit', 30.00),
                (3, 1, 'debit', 20.00),   -- Mar: +20
                (3, 2, 'credit', 20.00),
                (4, 1, 'debit', 15.00),   -- Apr: +15 (should be excluded from Q1)
                (4, 2, 'credit', 15.00)
        """)
        )

        # AS-OF Q1 ending (2024-03-31) should be 100 (not 115)
        plaid_balances = {
            "plaid_checking": 100.00  # Cumulative through March
        }

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass with AS-OF balance of 100
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True

        # Verify by_account shows correct AS-OF balance
        if "by_account" in result:
            checking = next(
                (
                    a
                    for a in result["by_account"]
                    if a["plaid_account_id"] == "plaid_checking"
                ),
                None,
            )
            if checking:
                assert checking["gl_asof"] == pytest.approx(100.00, abs=1e-2)
                assert checking["ext_asof"] == pytest.approx(100.00, abs=1e-2)


def test_reconcile_cli_with_coverage_error(tmp_path: Path, monkeypatch: Any) -> None:
    """Test CLI fails with clear error when coverage rule violated."""
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url)
    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup two mapped cash accounts
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Assets:Bank:Savings', 'Savings', 'asset', 1),
                (3, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES
                ('plaid_checking', 'Checking', 'depository', 'checking', 'USD'),
                ('plaid_savings', 'Savings', 'depository', 'savings', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES
                ('plaid_checking', 1),
                ('plaid_savings', 2)
        """)
        )

        # Create entries
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES(1, 'txn-001', '2024-03-15', 'Test', 'USD', 'hash1', 1, 'item_CLI')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 3, 'credit', 100.00)
        """)
        )

    # Create balances JSON missing one account
    balances_file = tmp_path / "balances.json"
    balances_file.write_text(
        json.dumps({
            "plaid_checking": 100.00
            # Missing plaid_savings
        })
    )

    monkeypatch.setenv("DATABASE_URL", db_url)

    result = runner.invoke(
        app,
        [
            "reconcile",
            "--item-id",
            "item_CLI",
            "--period",
            "2024Q1",
            "--balances-json",
            str(balances_file),
            "--out",
            str(tmp_path / "recon.json"),
        ],
    )

    # Should fail with coverage error
    assert result.exit_code != 0
    assert (
        "missing balance" in result.output.lower()
        or "coverage" in result.output.lower()
    )


def test_reconcile_coverage_rule_extra_account() -> None:
    """Test that reconciliation fails when balances_json has unmapped accounts."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Seed ONE cash account
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES('plaid_checking', 1)
        """)
        )

        # Create entry
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES(1, 'txn-001', '2024-03-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # JSON includes extra unmapped account
        plaid_balances = {
            "plaid_checking": 100.00,
            "plaid_unmapped": 50.00,  # EXTRA - not mapped
        }

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        assert result["success"] is False
        assert result["checks"]["coverage"]["passed"] is False
        assert "extra" in result["checks"]["coverage"]
        assert "plaid_unmapped" in result["checks"]["coverage"]["extra"]
        assert result["checks"]["coverage"]["missing"] == []  # No missing, only extra


def test_reconcile_variance_within_tolerance() -> None:
    """Test that variance within tolerance passes reconciliation."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup account
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES('plaid_checking', 1)
        """)
        )

        # Create entry for 100.00
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES(1, 'txn-001', '2024-03-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Balance with tiny variance (within 0.01 tolerance)
        plaid_balances = {
            "plaid_checking": 100.005  # 0.005 variance < 0.01 tolerance
        }

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass due to tolerance
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert (
            result["checks"]["cash_variance"]["total_variance"]
            <= result["checks"]["cash_variance"]["tolerance"] + 1e-9
        )
        assert result["checks"]["cash_variance"]["tolerance"] == pytest.approx(
            0.01, abs=1e-3
        )
        assert result["checks"]["coverage"]["passed"] is True
        assert result["checks"]["coverage"]["missing"] == []
        assert result["checks"]["coverage"]["extra"] == []


def test_reconcile_coverage_all_good() -> None:
    """Test that coverage passes when JSON keys exactly match mapped cash accounts."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup two cash accounts
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Assets:Bank:Savings', 'Savings', 'asset', 1),
                (3, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES
                ('plaid_checking', 'Checking', 'depository', 'checking', 'USD'),
                ('plaid_savings', 'Savings', 'depository', 'savings', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES
                ('plaid_checking', 1),
                ('plaid_savings', 2)
        """)
        )

        # Create entries
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES
                (1, 'txn-001', '2024-03-15', 'Test 1', 'USD', 'hash1', 1, 'item_A'),
                (2, 'txn-002', '2024-03-20', 'Test 2', 'USD', 'hash2', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 150.00),
                (1, 3, 'credit', 150.00),
                (2, 2, 'debit', 75.00),
                (2, 3, 'credit', 75.00)
        """)
        )

        # JSON with EXACT match to mapped cash accounts
        plaid_balances = {"plaid_checking": 150.00, "plaid_savings": 75.00}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Coverage should pass perfectly
        assert result["checks"]["coverage"]["passed"] is True
        assert result["checks"]["coverage"]["missing"] == []
        assert result["checks"]["coverage"]["extra"] == []
        assert result["success"] is True


def test_reconcile_rounding_edge_case() -> None:
    """Test that rounding edge cases work correctly (100.004 rounds to 100.00)."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup account
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                (2, 'Expenses:Other', 'Other', 'expense', 0)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES('plaid_checking', 1)
        """)
        )

        # Create entry for exactly 100.00
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES(1, 'txn-001', '2024-03-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Balance that rounds to exactly match GL
        plaid_balances = {
            "plaid_checking": 100.004  # Should round to 100.00 and pass
        }

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass due to rounding
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["checks"]["cash_variance"]["total_variance"] == pytest.approx(
            0.00, abs=1e-2
        )
