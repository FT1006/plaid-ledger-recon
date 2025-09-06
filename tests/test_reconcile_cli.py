"""Test CLI integration for reconcile command - ETL event writing responsibility."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

if TYPE_CHECKING:
    from pathlib import Path

from cli import app
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

runner = CliRunner()


def _create_test_schema_with_period(conn: Any) -> None:
    """Create test database schema including period column in etl_events."""
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

    # ETL events table with period column per ADR
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


def _seed_minimal_success_data(conn: Any) -> None:
    """Seed minimal data for successful reconciliation."""
    # Cash account
    conn.execute(
        text("""
        INSERT INTO accounts (id, code, name, type, is_cash) VALUES
            (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
            (2, 'Expenses:Other', 'Other', 'expense', 0)
    """)
    )

    # Plaid account
    conn.execute(
        text("""
        INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
        VALUES('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
    """)
    )

    # Account mapping
    conn.execute(
        text("""
        INSERT INTO account_links (plaid_account_id, account_id)
        VALUES('plaid_checking', 1)
    """)
    )

    # Balanced entry for 100.00
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


def test_cli_reconcile_writes_etl_event_success(tmp_path: Path) -> None:
    """Test that CLI writes ETL event on successful reconciliation."""
    # Create temp database
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"

    # Setup schema and data
    engine = create_engine(db_url)
    with engine.begin() as conn:
        _create_test_schema_with_period(conn)
        _seed_minimal_success_data(conn)

    # Create balances JSON file
    balances_json = tmp_path / "balances.json"
    balances_json.write_text(json.dumps({"plaid_checking": 100.00}))

    # Output file for reconciliation
    out_json = tmp_path / "recon.json"

    # Run CLI command
    with patch.dict("os.environ", {"DATABASE_URL": db_url}):
        result = runner.invoke(
            app,
            [
                "reconcile",
                "--item-id",
                "item_TEST",
                "--period",
                "2024Q1",
                "--balances-json",
                str(balances_json),
                "--out",
                str(out_json),
            ],
        )

    # Verify successful exit
    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert out_json.exists(), "recon.json not created"

    # Verify ETL event was written by CLI
    with engine.begin() as conn:
        event = conn.execute(
            text("""
            SELECT event_type, item_id, period, success, row_counts
            FROM etl_events
            WHERE event_type = 'reconcile'
        """)
        ).fetchone()

        assert event is not None, "ETL event not written by CLI"
        assert event[0] == "reconcile"  # event_type
        assert event[1] == "item_TEST"  # item_id
        assert event[2] == "2024Q1"  # period column
        assert bool(event[3]) is True  # success
        assert event[4] is not None  # row_counts populated

        # Verify row_counts is valid JSON (committed contract)
        row_counts = json.loads(event[4])
        assert "period" in row_counts  # Period must be in row_counts per ADR
        assert row_counts["period"] == "2024Q1"


def test_cli_reconcile_writes_etl_event_failure(tmp_path: Path) -> None:
    """Test that CLI writes ETL event on failed reconciliation with diagnostics."""
    # Create temp database
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"

    # Setup schema and data
    engine = create_engine(db_url)
    with engine.begin() as conn:
        _create_test_schema_with_period(conn)
        _seed_minimal_success_data(conn)

    # Create balances JSON with variance that will cause failure
    balances_json = tmp_path / "balances.json"
    balances_json.write_text(json.dumps({"plaid_checking": 50.00}))  # 50.00 variance

    # Output file for reconciliation
    out_json = tmp_path / "recon.json"

    # Run CLI command (should fail)
    with patch.dict("os.environ", {"DATABASE_URL": db_url}):
        result = runner.invoke(
            app,
            [
                "reconcile",
                "--item-id",
                "item_TEST",
                "--period",
                "2024Q1",
                "--balances-json",
                str(balances_json),
                "--out",
                str(out_json),
            ],
        )

    # Verify failure exit code
    assert result.exit_code == 1, f"CLI should have failed but got: {result.output}"
    assert out_json.exists(), "recon.json not created on failure"

    # Verify ETL event was written with failure status
    with engine.begin() as conn:
        event = conn.execute(
            text("""
            SELECT event_type, item_id, period, success, row_counts
            FROM etl_events
            WHERE event_type = 'reconcile'
        """)
        ).fetchone()

        assert event is not None, "ETL event not written by CLI on failure"
        assert event[0] == "reconcile"  # event_type
        assert event[1] == "item_TEST"  # item_id
        assert event[2] == "2024Q1"  # period column
        assert bool(event[3]) is False  # success=FALSE on failure
        assert event[4] is not None  # row_counts with diagnostics

        # Verify row_counts is valid JSON with required keys
        row_counts = json.loads(event[4])
        assert "period" in row_counts  # Period must be in row_counts per ADR
        assert row_counts["period"] == "2024Q1"


def test_cli_reconcile_includes_period_column(tmp_path: Path) -> None:
    """Test that CLI writes period to dedicated column, not just JSON."""
    # Create temp database
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"

    # Setup schema and data
    engine = create_engine(db_url)
    with engine.begin() as conn:
        _create_test_schema_with_period(conn)
        _seed_minimal_success_data(conn)

    # Create balances JSON file
    balances_json = tmp_path / "balances.json"
    balances_json.write_text(json.dumps({"plaid_checking": 100.00}))

    # Output file for reconciliation
    out_json = tmp_path / "recon.json"

    # Run CLI command
    with patch.dict("os.environ", {"DATABASE_URL": db_url}):
        result = runner.invoke(
            app,
            [
                "reconcile",
                "--item-id",
                "item_TEST",
                "--period",
                "2024Q2",  # Different period to verify
                "--balances-json",
                str(balances_json),
                "--out",
                str(out_json),
            ],
        )

    assert result.exit_code == 0

    # Verify period written to dedicated column for operator queries
    with engine.begin() as conn:
        period_from_column = conn.execute(
            text("SELECT period FROM etl_events WHERE event_type = 'reconcile'")
        ).scalar()

        assert period_from_column == "2024Q2", "Period not written to dedicated column"

        # Also verify it's in row_counts for completeness
        row_counts_json = conn.execute(
            text("SELECT row_counts FROM etl_events WHERE event_type = 'reconcile'")
        ).scalar()

        row_counts = json.loads(row_counts_json or "{}")
        assert row_counts["period"] == "2024Q2", "Period not in row_counts JSON"


def test_cli_reconcile_coverage_failure_records_event(tmp_path: Path) -> None:
    """Test that CLI records ETL event even when coverage validation fails."""
    # Create temp database
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"

    # Setup schema and minimal data (but no account mappings)
    engine = create_engine(db_url)
    with engine.begin() as conn:
        _create_test_schema_with_period(conn)

        # Only create accounts, no mappings
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1)
        """)
        )

    # Create balances JSON with unmapped account
    balances_json = tmp_path / "balances.json"
    balances_json.write_text(json.dumps({"plaid_unmapped": 100.00}))

    # Output file for reconciliation
    out_json = tmp_path / "recon.json"

    # Run CLI command (should fail due to coverage)
    with patch.dict("os.environ", {"DATABASE_URL": db_url}):
        result = runner.invoke(
            app,
            [
                "reconcile",
                "--item-id",
                "item_TEST",
                "--period",
                "2024Q1",
                "--balances-json",
                str(balances_json),
                "--out",
                str(out_json),
            ],
        )

    # Should fail but still record event
    assert result.exit_code == 1

    # Verify ETL event recorded with coverage failure
    with engine.begin() as conn:
        event = conn.execute(
            text(
                "SELECT success, row_counts FROM etl_events "
                "WHERE event_type = 'reconcile'"
            )
        ).fetchone()

        assert event is not None, (
            "ETL event should be recorded even on coverage failure"
        )
        assert bool(event[0]) is False  # success=FALSE

        # Verify row_counts is valid JSON (minimal check)
        row_counts = json.loads(event[1])
        assert isinstance(row_counts, dict)  # Just verify it's structured data


def test_cli_reconcile_exception_path_records_event(tmp_path: Path) -> None:
    """Test that CLI records ETL event even when run_reconciliation raises exception."""
    # Create temp database
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"

    # Setup schema and data
    engine = create_engine(db_url)
    with engine.begin() as conn:
        _create_test_schema_with_period(conn)
        _seed_minimal_success_data(conn)

    # Create balances JSON file
    balances_json = tmp_path / "balances.json"
    balances_json.write_text(json.dumps({"plaid_checking": 100.00}))

    # Output file for reconciliation
    out_json = tmp_path / "recon.json"

    # Patch run_reconciliation to raise an exception
    with patch("cli.run_reconciliation") as mock_reconcile:
        mock_reconcile.side_effect = RuntimeError("Simulated reconciliation failure")

        # Run CLI command with environment
        with patch.dict("os.environ", {"DATABASE_URL": db_url}):
            result = runner.invoke(
                app,
                [
                    "reconcile",
                    "--item-id",
                    "item_TEST",
                    "--period",
                    "2024Q1",
                    "--balances-json",
                    str(balances_json),
                    "--out",
                    str(out_json),
                ],
            )

    # Should fail due to exception
    assert result.exit_code == 1

    # Verify ETL event was still recorded with failure status
    with engine.begin() as conn:
        event = conn.execute(
            text("""
            SELECT event_type, item_id, period, success, row_counts
            FROM etl_events
            WHERE event_type = 'reconcile'
        """)
        ).fetchone()

        assert event is not None, "ETL event should be recorded even on exception"
        assert event[0] == "reconcile"  # event_type
        assert event[1] == "item_TEST"  # item_id
        assert event[2] == "2024Q1"  # period column
        assert bool(event[3]) is False  # success=FALSE on exception
        assert event[4] is not None  # row_counts populated

        # Verify row_counts contains period
        row_counts = json.loads(event[4])
        assert "period" in row_counts
        assert row_counts["period"] == "2024Q1"
