"""Tests for pure run_reconciliation() function - no side effects allowed."""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, text

from etl.reconcile import run_reconciliation


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
            plaid_account_id TEXT UNIQUE NOT NULL
                REFERENCES plaid_accounts(plaid_account_id),
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
            success BOOLEAN NOT NULL,
            row_counts TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL
        )
    """)
    )


def test_reconcile_success_returns_structure() -> None:
    """Test successful reconciliation structure without writing ETL events."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup accounts and mappings
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

        # Create balanced entry
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_TEST')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Count ETL events before reconciliation
        event_count_before = conn.execute(
            text("SELECT COUNT(*) FROM etl_events")
        ).scalar()

        plaid_balances = {"plaid_checking": 100.00}

        # Run reconciliation (should be pure)
        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_TEST", plaid_balances=plaid_balances
        )

        # Count ETL events after reconciliation - should be unchanged
        event_count_after = conn.execute(
            text("SELECT COUNT(*) FROM etl_events")
        ).scalar()

        # Assert pure function behavior: no side effects
        assert event_count_before == event_count_after, (
            "run_reconciliation wrote ETL events (violates purity)"
        )

        # Assert proper structure returned
        assert result["success"] is True
        assert "checks" in result
        assert "by_account" in result
        assert "total_variance" in result

        # Assert checks structure
        assert result["checks"]["entry_balance"]["passed"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["checks"]["coverage"]["passed"] is True


def test_reconcile_failure_propagates() -> None:
    """Test failed reconciliation returns success=False without ETL events."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup accounts and mappings
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

        # Create balanced entry with GL balance of 100.00
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_TEST')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Count ETL events before reconciliation
        event_count_before = conn.execute(
            text("SELECT COUNT(*) FROM etl_events")
        ).scalar()

        # Plaid balance that will cause variance failure (>0.01 tolerance)
        plaid_balances = {"plaid_checking": 50.00}  # 50.00 variance

        # Run reconciliation (should fail but be pure)
        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_TEST", plaid_balances=plaid_balances
        )

        # Count ETL events after reconciliation - should be unchanged
        event_count_after = conn.execute(
            text("SELECT COUNT(*) FROM etl_events")
        ).scalar()

        # Assert pure function behavior: no side effects even on failure
        assert event_count_before == event_count_after, (
            "run_reconciliation wrote ETL events on failure (violates purity)"
        )

        # Assert failure propagated correctly
        assert result["success"] is False
        assert result["checks"]["cash_variance"]["passed"] is False
        assert result["total_variance"] == pytest.approx(50.00, abs=1e-2)

        # Run multiple times to verify consistent purity
        for _ in range(3):
            run_reconciliation(
                conn,
                period="2024Q1",
                item_id="item_TEST",
                plaid_balances=plaid_balances,
            )

        # Count should remain unchanged after multiple calls
        final_count = conn.execute(text("SELECT COUNT(*) FROM etl_events")).scalar()
        assert event_count_before == final_count, (
            f"Expected {event_count_before} ETL events, got {final_count} - "
            "run_reconciliation violates purity"
        )


def test_reconcile_item_scoping_isolation() -> None:
    """Test that run_reconciliation filters by item_id and ignores other items."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup accounts
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

        # Create entries for TWO different item_ids
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id) VALUES
                (1, 'item-A-txn', '2024-01-15', 'Item A', 'USD', 'hashA', 1, 'item_A'),
                (2, 'item-B-txn', '2024-01-20', 'Item B', 'USD', 'hashB', 1, 'item_B')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),   -- Item A: +100 to checking
                (1, 2, 'credit', 100.00),
                (2, 1, 'debit', 200.00),   -- Item B: +200 to checking
                (2, 2, 'credit', 200.00)
        """)
        )

        plaid_balances = {"plaid_checking": 100.00}  # Should match item A only

        # Run reconciliation scoped to item_A
        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should succeed because item_A GL balance = 100.00 matches plaid balance
        assert result["success"] is True
        assert result["total_variance"] == pytest.approx(0.00, abs=1e-2)

        # Verify item scoping worked - only one account processed
        assert len(result["by_account"]) == 1
        acct = result["by_account"][0]
        assert acct["variance"] == pytest.approx(0.00, abs=1e-2)


def test_reconcile_tolerance_boundary_pass() -> None:
    """Test that variance of 0.009 passes (within ±0.01 tolerance)."""
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
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Balance with 0.009 variance (within tolerance)
        plaid_balances = {"plaid_checking": 100.009}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass - 0.009 < 0.01 tolerance
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["total_variance"] == pytest.approx(0.009, abs=1e-3)


def test_reconcile_tolerance_boundary_fail() -> None:
    """Test that variance of 0.011 fails (exceeds ±0.01 tolerance)."""
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
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Balance with 0.015 variance (exceeds tolerance after rounding)
        plaid_balances = {"plaid_checking": 100.015}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should fail - 0.015 rounds to 0.02 > 0.01 tolerance
        assert result["success"] is False
        assert result["checks"]["cash_variance"]["passed"] is False
        assert result["total_variance"] == pytest.approx(0.015, abs=1e-3)


def test_reconcile_tolerance_boundary_equal_pass() -> None:
    """Test that variance of exactly 0.01 passes (inclusive boundary)."""
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
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Balance with exactly 0.01 variance (boundary case)
        plaid_balances = {"plaid_checking": 100.010}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass - tolerance is inclusive: abs(variance) <= 0.01
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["total_variance"] == pytest.approx(0.010, abs=1e-3)


def test_reconcile_tolerance_inclusive_positive() -> None:
    """Test that positive variance of +0.01 passes (inclusive boundary)."""
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
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Balance exactly +0.01 higher than GL
        plaid_balances = {"plaid_checking": 100.01}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass - tolerance is inclusive: abs(+0.01) <= 0.01
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["total_variance"] == pytest.approx(0.01, abs=1e-3)


def test_reconcile_tolerance_inclusive_negative() -> None:
    """Test that negative variance of -0.01 passes (inclusive boundary)."""
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
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # Balance exactly -0.01 lower than GL
        plaid_balances = {"plaid_checking": 99.99}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass - tolerance is inclusive: abs(-0.01) <= 0.01
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["total_variance"] == pytest.approx(0.01, abs=1e-3)


def test_reconcile_zero_gl_lines_counts_as_zero() -> None:
    """Test that mapped cash account with no lines in period has GL balance of 0.00."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup account
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1)
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

        # No journal lines for this account/period
        # GL balance should be 0.00

        # External balance is also 0.00
        plaid_balances = {"plaid_checking": 0.00}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should pass - both GL and external are 0.00, variance = 0.00
        assert result["success"] is True
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["total_variance"] == pytest.approx(0.00, abs=1e-3)

        # Verify by_account breakdown shows 0.00 GL balance
        assert len(result["by_account"]) == 1
        assert result["by_account"][0]["plaid_account_id"] == "plaid_checking"
        assert result["by_account"][0]["gl_asof"] == 0.00
        assert result["by_account"][0]["ext_asof"] == 0.00
        assert result["by_account"][0]["variance"] == 0.00


def test_reconcile_inclusive_period_window() -> None:
    """Test that period window [from, to] is inclusive of boundary dates."""
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

        # Create entries on FIRST and LAST day of Q1 (2024-01-01 to 2024-03-31)
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id) VALUES
                (
                    1, 'first-day', '2024-01-01', 'Q1 First Day', 'USD',
                    'hash1', 1, 'item_A'
                ),
                (
                    2, 'last-day', '2024-03-31', 'Q1 Last Day', 'USD',
                    'hash2', 1, 'item_A'
                )
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 50.00),    -- +50 on first day
                (1, 2, 'credit', 50.00),
                (2, 1, 'debit', 30.00),    -- +30 on last day = 80 total
                (2, 2, 'credit', 30.00)
        """)
        )

        plaid_balances = {"plaid_checking": 80.00}  # Should match both entries

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Should include both boundary date entries and pass reconciliation
        assert result["success"] is True
        assert result["total_variance"] == pytest.approx(0.00, abs=1e-2)


def test_coverage_ignores_extras_but_fails_on_missing() -> None:
    """Test that coverage check ignores extras but fails on missing mapped accounts."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup ONE mapped cash account
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

        # Test 1: Extra account in plaid_balances should be ignored (pass)
        plaid_balances_with_extra = {
            "plaid_checking": 0.00,  # Mapped
            "plaid_unmapped": 50.00,  # Extra - should be ignored
        }

        result = run_reconciliation(
            conn,
            period="2024Q1",
            item_id="item_A",
            plaid_balances=plaid_balances_with_extra,
        )

        assert result["checks"]["coverage"]["passed"] is True
        assert "extras_ignored" in result["checks"]["coverage"]
        assert "plaid_unmapped" in result["checks"]["coverage"]["extras_ignored"]

        # Test 2: Missing mapped account should fail coverage
        plaid_balances_missing_mapped = {
            "plaid_unmapped": 50.00,  # Only extra, missing the required mapped one
        }

        result = run_reconciliation(
            conn,
            period="2024Q1",
            item_id="item_A",
            plaid_balances=plaid_balances_missing_mapped,
        )

        assert result["checks"]["coverage"]["passed"] is False
        assert "missing" in result["checks"]["coverage"]
        assert "plaid_checking" in result["checks"]["coverage"]["missing"]
        assert result["success"] is False  # Overall failure due to coverage


def test_pure_function_never_writes_events() -> None:
    """Test that run_reconciliation() pure function never writes to etl_events table."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        _create_test_schema(conn)

        # Setup minimal data for a successful reconciliation
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1)
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

        # Run reconciliation (should be pure, no side effects)
        plaid_balances = {"plaid_checking": 0.00}
        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Verify reconciliation worked
        assert result["success"] is True

        # Verify NO events were written (pure function boundary)
        event_count = conn.execute(text("SELECT COUNT(*) FROM etl_events")).scalar()
        assert event_count == 0, (
            "Pure function run_reconciliation() must not write to etl_events"
        )


def test_tolerance_is_inclusive_at_boundary() -> None:
    """Test that tolerance check is inclusive: exactly 0.01 variance passes."""
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

        # Create entry for exact boundary case
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, txn_id, txn_date, description, currency,
                                        source_hash, transform_version, item_id)
            VALUES(1, 'txn-001', '2024-01-15', 'Test', 'USD', 'hash1', 1, 'item_A')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                (1, 1, 'debit', 100.00),
                (1, 2, 'credit', 100.00)
        """)
        )

        # External balance differs by exactly 0.01 (boundary case)
        plaid_balances = {"plaid_checking": 100.01}

        result = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances
        )

        # Exactly 0.01 variance should pass (inclusive boundary)
        assert result["checks"]["cash_variance"]["passed"] is True
        assert result["total_variance"] == pytest.approx(0.01, abs=1e-3)
        assert result["success"] is True

        # Test just over boundary (should fail)
        plaid_balances_over = {"plaid_checking": 100.011}

        result_over = run_reconciliation(
            conn, period="2024Q1", item_id="item_A", plaid_balances=plaid_balances_over
        )

        assert result_over["checks"]["cash_variance"]["passed"] is False
        assert result_over["success"] is False
