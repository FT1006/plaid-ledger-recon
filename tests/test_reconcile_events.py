"""Test that reconcile command records an etl_events 'reconcile' row."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

from cli import app
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

runner = CliRunner()


def _seed_schema(engine_url: str) -> None:
    engine = create_engine(engine_url)
    with engine.begin() as conn:
        # Minimal canonical schema pieces used by reconciliation
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY,
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
                    id INTEGER PRIMARY KEY,
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
                    id INTEGER PRIMARY KEY,
                    entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                    amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS plaid_accounts (
                    plaid_account_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    subtype TEXT NOT NULL,
                    currency TEXT NOT NULL
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS account_links (
                    id INTEGER PRIMARY KEY,
                    plaid_account_id TEXT UNIQUE NOT NULL
                        REFERENCES plaid_accounts(plaid_account_id),
                    account_id INTEGER NOT NULL REFERENCES accounts(id)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS etl_events (
                    id INTEGER PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    item_id TEXT,
                    row_counts TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    success BOOLEAN NOT NULL
                )
                """
            )
        )

        # Seed minimal data: cash account, mapped plaid account, balanced entry in Q1
        conn.execute(
            text(
                """
                INSERT INTO accounts (id, code, name, type, is_cash)
                VALUES (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1)
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO plaid_accounts (
                    plaid_account_id, name, type, subtype, currency
                )
                VALUES ('plaid_checking', 'Checking', 'depository', 'checking', 'USD')
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO account_links (plaid_account_id, account_id)
                VALUES ('plaid_checking', 1)
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO journal_entries (
                    id, txn_id, txn_date, description, currency,
                    source_hash, transform_version
                ) VALUES (1, 'txn1', '2024-02-01', 'Test', 'USD', 'hash1', 1)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES (1, 1, 'debit', 100.00),
                       (1, 1, 'credit', 100.00)
                """
            )
        )


def test_reconcile_records_etl_event(tmp_path: Any, monkeypatch: Any) -> None:
    # Use file-based sqlite so the CLI sees the same DB
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"
    _seed_schema(db_url)

    # Point CLI to our DB and provide a fake Plaid token
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("PLAID_ACCESS_TOKEN", "fake-token")

    # Patch fetch_accounts to return matching plaid account with balance 0
    def _fake_fetch_accounts(_access_token: str) -> list[dict[str, Any]]:
        return [
            {
                "account_id": "plaid_checking",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "balances": {"current": 0.0},
            }
        ]

    # Run reconcile via CLI
    out_json = tmp_path / "recon.json"
    with patch("cli.fetch_accounts", _fake_fetch_accounts):
        result = runner.invoke(
            app,
            [
                "reconcile",
                "--item-id",
                "test-item",
                "--period",
                "2024Q1",
                "--out",
                str(out_json),
            ],
        )

    # Command should complete; verify event created and output file exists
    assert out_json.exists(), result.output

    # Check event recorded
    engine = create_engine(db_url)
    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM etl_events WHERE event_type = 'reconcile'")
        ).scalar()
        assert count is not None and count >= 1

        # Optional: verify JSON shape
        row = conn.execute(
            text(
                """
                SELECT row_counts, success FROM etl_events
                WHERE event_type='reconcile' ORDER BY id DESC LIMIT 1
                """
            )
        ).fetchone()
        if row and row[0]:
            data = json.loads(row[0])
            assert data.get("period") == "2024Q1"
