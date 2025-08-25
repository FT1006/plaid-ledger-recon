"""Tests for CLI interface and command validation."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_ingest_requires_item_and_window() -> None:
    """Missing required args should exit 2 with help text.

    Per ADR: CLI must validate arguments before processing.
    """
    # Test missing all arguments
    result = runner.invoke(app, ["ingest"])
    assert result.exit_code == 2
    assert "Missing option" in result.output or "Usage:" in result.output

    # Test missing --from and --to
    result = runner.invoke(app, ["ingest", "--item-id", "test_item"])
    assert result.exit_code == 2
    assert "Missing option" in result.output

    # Test missing --to
    result = runner.invoke(
        app,
        ["ingest", "--item-id", "test_item", "--from", "2024-01-01"],
    )
    assert result.exit_code == 2
    assert "Missing option" in result.output

    # Test missing --item-id
    result = runner.invoke(
        app,
        ["ingest", "--from", "2024-01-01", "--to", "2024-01-31"],
    )
    assert result.exit_code == 2
    assert "Missing option" in result.output


def test_ingest_validates_date_format() -> None:
    """Invalid date formats should exit with error.

    Dates must be YYYY-MM-DD format.
    """
    # Test invalid --from date
    result = runner.invoke(
        app,
        [
            "ingest",
            "--item-id",
            "test_item",
            "--from",
            "01/01/2024",
            "--to",
            "2024-01-31",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid date format" in result.output or "date" in result.output.lower()

    # Test invalid --to date
    result = runner.invoke(
        app,
        [
            "ingest",
            "--item-id",
            "test_item",
            "--from",
            "2024-01-01",
            "--to",
            "Jan 31 2024",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid date format" in result.output or "date" in result.output.lower()


def test_ingest_success_exit_zero() -> None:
    """Successful ingest should exit 0 with status message.

    Per ADR: Successful ETL operations must exit 0.
    """
    with (
        patch("cli.sync_transactions") as sync_txns,
        patch("cli.fetch_accounts") as fetch_accts,
        patch("cli.map_plaid_to_journal") as map_to_journal,
        patch("cli.load_accounts"),
        patch("cli.load_journal_entries"),
        patch("cli.load_dotenv"),
        patch("cli.os.getenv") as getenv,
    ):
        getenv.side_effect = lambda k: {
            "DATABASE_URL": "postgresql://test",
            "PLAID_ACCESS_TOKEN": "test_token",
        }.get(k)

        # Database connection not needed - loaders are mocked

        # Mock ETL pipeline
        sync_txns.return_value = iter([
            {
                "transaction_id": "txn_001",
                "account_id": "acc_001",
                "amount": 100.00,
                "date": "2024-01-15",
                "name": "Test Transaction",
                "pending": False,
            },
        ])

        fetch_accts.return_value = [
            {
                "account_id": "acc_001",
                "type": "depository",
                "subtype": "checking",
                "balances": {"current": 1000.00},
                "name": "Test Account",
                "iso_currency_code": "USD",
            },
        ]

        map_to_journal.return_value = [
            {
                "txn_id": "txn_001",
                "txn_date": date(2024, 1, 15),
                "description": "Test Transaction",
                "currency": "USD",
                "source_hash": "test_hash",
                "transform_version": 1,
                "lines": [
                    {
                        "account": "Expenses:Dining",
                        "side": "debit",
                        "amount": Decimal("100.00"),
                    },
                    {
                        "account": "Assets:Bank:Checking",
                        "side": "credit",
                        "amount": Decimal("100.00"),
                    },
                ],
            },
        ]

        result = runner.invoke(
            app,
            [
                "ingest",
                "--item-id",
                "test_item",
                "--from",
                "2024-01-01",
                "--to",
                "2024-01-31",
            ],
        )

        assert result.exit_code == 0
        assert "✅" in result.output or "success" in result.output.lower()


def test_ingest_database_connection_failure() -> None:
    """Database connection failure should exit 1 with error message.

    Per ADR: Infrastructure failures must be clearly reported.
    """
    with (
        patch("cli.os.getenv") as getenv,
        patch("cli.load_dotenv"),
        patch("cli.sync_transactions") as sync_txns,
        patch("cli.fetch_accounts") as fetch_accts,
        patch("cli.map_plaid_to_journal") as map_to_journal,
        patch("cli.load_accounts"),
        patch(
            "cli.load_journal_entries",
            side_effect=RuntimeError("DB connection failed"),
        ),
    ):
        getenv.side_effect = lambda k: {
            "DATABASE_URL": "postgresql://test",
            "PLAID_ACCESS_TOKEN": "test_token",
        }.get(k)

        # Set up successful ETL pipeline up to database
        sync_txns.return_value = iter([
            {
                "transaction_id": "txn_001",
                "account_id": "acc_001",
                "amount": 100.00,
                "date": "2024-01-15",
                "name": "Test",
                "pending": False,
            },
        ])
        fetch_accts.return_value = [
            {
                "account_id": "acc_001",
                "type": "depository",
                "subtype": "checking",
                "name": "Test",
                "iso_currency_code": "USD",
            },
        ]
        map_to_journal.return_value = [
            {
                "txn_id": "txn_001",
                "txn_date": date(2024, 1, 15),
                "description": "Test",
                "currency": "USD",
                "source_hash": "hash",
                "transform_version": 1,
                "lines": [],
            },
        ]

        result = runner.invoke(
            app,
            [
                "ingest",
                "--item-id",
                "test_item",
                "--from",
                "2024-01-01",
                "--to",
                "2024-01-31",
            ],
        )

        assert result.exit_code == 1
        assert (
            "DB connection failed" in result.output
            or "database" in result.output.lower()
        )


def test_ingest_plaid_api_failure() -> None:
    """Plaid API failures should exit 1 with error message.

    Per ADR: External API failures must be handled gracefully.
    """
    with (
        patch("cli.os.getenv") as getenv,
        patch("cli.load_dotenv"),
        patch(
            "cli.sync_transactions",
            side_effect=Exception("Plaid API error: Invalid credentials"),
        ),
    ):
        getenv.side_effect = lambda k: {
            "DATABASE_URL": "postgresql://test",
            "PLAID_ACCESS_TOKEN": "test_token",
        }.get(k)

        result = runner.invoke(
            app,
            [
                "ingest",
                "--item-id",
                "test_item",
                "--from",
                "2024-01-01",
                "--to",
                "2024-01-31",
            ],
        )

        assert result.exit_code == 1
        assert "Plaid" in result.output or "API" in result.output


def test_ingest_date_range_validation() -> None:
    """From date must be before or equal to To date.

    Business rule: Can't ingest future-to-past ranges.
    """
    result = runner.invoke(
        app,
        [
            "ingest",
            "--item-id",
            "test_item",
            "--from",
            "2024-01-31",
            "--to",
            "2024-01-01",
        ],
    )

    assert result.exit_code == 1
    assert "date range" in result.output.lower() or "invalid" in result.output.lower()


def test_ingest_empty_transaction_set() -> None:
    """Empty transaction set should succeed with appropriate message.

    Per ADR: No transactions is a valid state (exit 0).
    """
    with (
        patch("cli.sync_transactions") as sync_txns,
        patch("cli.fetch_accounts") as fetch_accts,
        patch("cli.map_plaid_to_journal") as map_to_journal,
        patch("cli.load_accounts"),
        patch("cli.load_journal_entries"),
        patch("cli.load_dotenv"),
        patch("cli.os.getenv") as getenv,
    ):
        getenv.side_effect = lambda k: {
            "DATABASE_URL": "postgresql://test",
            "PLAID_ACCESS_TOKEN": "test_token",
        }.get(k)

        # Database connection not needed - loaders are mocked

        # Mock empty result set
        sync_txns.return_value = iter([])  # No transactions
        fetch_accts.return_value = []  # No accounts
        map_to_journal.return_value = []  # No journal entries

        result = runner.invoke(
            app,
            [
                "ingest",
                "--item-id",
                "test_item",
                "--from",
                "2024-01-01",
                "--to",
                "2024-01-31",
            ],
        )

        assert result.exit_code == 0
        assert (
            "0 transactions" in result.output
            or "no transactions" in result.output.lower()
        )


def test_ingest_row_count_reporting() -> None:
    """Successful ingest should report row counts.

    Per ADR: ETL operations must report affected row counts.
    """
    with (
        patch("cli.sync_transactions") as sync_txns,
        patch("cli.fetch_accounts") as fetch_accts,
        patch("cli.map_plaid_to_journal") as map_to_journal,
        patch("cli.load_accounts"),
        patch("cli.load_journal_entries"),
        patch("cli.load_dotenv"),
        patch("cli.os.getenv") as getenv,
    ):
        getenv.side_effect = lambda k: {
            "DATABASE_URL": "postgresql://test",
            "PLAID_ACCESS_TOKEN": "test_token",
        }.get(k)

        # Database connection not needed - loaders are mocked

        # Mock 5 transactions
        sync_txns.return_value = iter([
            {
                "transaction_id": f"txn_{i}",
                "account_id": "acc_001",
                "amount": 10.00 * i,
                "date": f"2024-01-{i + 1:02d}",
                "name": f"Transaction {i}",
                "pending": False,
            }
            for i in range(5)
        ])

        fetch_accts.return_value = [
            {
                "account_id": "acc_001",
                "name": "Test Account",
                "type": "depository",
                "subtype": "checking",
                "balances": {"current": 1000.00},
                "iso_currency_code": "USD",
            },
        ]

        map_to_journal.return_value = [
            {
                "txn_id": f"txn_{i}",
                "txn_date": date(2024, 1, i + 1),
                "description": f"Transaction {i}",
                "currency": "USD",
                "source_hash": f"hash_{i}",
                "transform_version": 1,
                "lines": [
                    {
                        "account": "Expenses:Dining",
                        "side": "debit",
                        "amount": Decimal(f"{10.00 * i}"),
                    },
                    {
                        "account": "Assets:Bank:Checking",
                        "side": "credit",
                        "amount": Decimal(f"{10.00 * i}"),
                    },
                ],
            }
            for i in range(5)
        ]

        result = runner.invoke(
            app,
            [
                "ingest",
                "--item-id",
                "test_item",
                "--from",
                "2024-01-01",
                "--to",
                "2024-01-31",
            ],
        )

        assert result.exit_code == 0
        # Should report transaction count
        assert "5" in result.output or "transactions" in result.output
