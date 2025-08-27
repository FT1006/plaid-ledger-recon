"""Skeleton test for the `pfetl report` CLI.

This is intentionally skipped until the report command is implemented.
It documents the expected behavior and can be used as a starting point
for the pair-programming exercise.
"""

from __future__ import annotations

from pathlib import Path  # noqa: TC003

import pytest
from cli import app
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

runner = CliRunner()


def _seed_minimal_reporting_schema(engine_url: str) -> None:
    """Create a minimal schema and seed one balanced entry.

    Mirrors the structure used by tests/test_reports.py but simplified
    for end-to-end CLI invocation.
    """
    engine = create_engine(engine_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    is_cash BOOLEAN NOT NULL DEFAULT 0
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS journal_entries (
                    id TEXT PRIMARY KEY,
                    item_id TEXT,
                    txn_id TEXT UNIQUE NOT NULL,
                    txn_date DATE NOT NULL,
                    description TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    transform_version INTEGER NOT NULL
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS journal_lines (
                    id TEXT PRIMARY KEY,
                    entry_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount DECIMAL(18,2) NOT NULL
                )
                """
            )
        )

        # Seed minimal accounts and one balanced entry (2024Q1)
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO accounts (id, code, name, type, is_cash) VALUES
                    ('acc1', 'Assets:Bank:Checking', 'Bank Checking Account',
                     'asset', 1),
                    ('acc2', 'Expenses:Dining:Restaurants', 'Restaurant Expenses',
                     'expense', 0)
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO journal_entries (
                    id, item_id, txn_id, txn_date, description, currency,
                    source_hash, transform_version
                ) VALUES (
                    'entry1', 'test_item', 'test_txn_001', '2024-01-15',
                    'Test Restaurant Purchase', 'USD', 'abc123def456', 1
                )
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO journal_lines (
                    id, entry_id, account_id, side, amount
                ) VALUES
                    ('line1', 'entry1', 'acc2', 'debit', 25.00),
                    ('line2', 'entry1', 'acc1', 'credit', 25.00)
                """
            )
        )


@pytest.mark.skip(reason="report CLI not yet implemented — enable after implementation")
def test_report_cli_emits_deterministic_html(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Expected: report CLI writes 2 deterministic HTML files for the period."""
    # Create in-memory SQLite DB and seed data
    db_url = "sqlite:///:memory:"
    _seed_minimal_reporting_schema(db_url)

    # Point CLI to our in-memory DB
    monkeypatch.setenv("DATABASE_URL", db_url)

    out_dir = tmp_path / "build"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Run CLI to generate only HTML for speed
    result = runner.invoke(
        app,
        [
            "report",
            "--item-id",
            "test_item",
            "--period",
            "2024Q1",
            "--formats",
            "html",
            "--out",
            str(out_dir),
        ],
    )

    assert result.exit_code == 0, f"report command failed: {result.output}"

    # Expect deterministic filenames
    bs_html = out_dir / "bs_2024Q1.html"
    cf_html = out_dir / "cf_2024Q1.html"
    assert bs_html.exists(), "balance sheet HTML missing"
    assert cf_html.exists(), "cash flow HTML missing"

    # Optional: compute and compare hashes using tests.utils.hash.hash_html
    # from tests.utils.hash import hash_html
    # assert hash_html(bs_html.read_text()) == EXPECTED_BS_HASH
    # assert hash_html(cf_html.read_text()) == EXPECTED_CF_HASH
