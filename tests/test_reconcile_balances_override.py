"""Test reconcile --balances-json override for demos/CI (no Plaid required)."""

from __future__ import annotations

import json
from typing import Any

from cli import app
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

runner = CliRunner()


def _seed_minimal_gl(db_url: str) -> None:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE accounts (
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
                CREATE TABLE journal_entries (
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
                CREATE TABLE journal_lines (
                    id INTEGER PRIMARY KEY,
                    entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    side TEXT NOT NULL,
                    amount NUMERIC(18,2) NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE plaid_accounts (
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
                CREATE TABLE account_links (
                    id INTEGER PRIMARY KEY,
                    plaid_account_id TEXT UNIQUE NOT NULL,
                    account_id INTEGER NOT NULL REFERENCES accounts(id)
                )
                """
            )
        )

        # Seed one cash account and one expense account; map only the cash account
        conn.execute(
            text(
                """
                INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                    (1, 'Assets:Bank:Checking', 'Checking', 'asset', 1),
                    (2, 'Expenses:Dining', 'Dining', 'expense', 0)
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

        # Seed balanced entry in Q1 with +150 cash movement
        conn.execute(
            text(
                """
                INSERT INTO journal_entries (
                    id, txn_id, txn_date, description, currency,
                    source_hash, transform_version
                ) VALUES (
                    1, 'q1-1', '2024-03-31', 'End of Q1 cash in', 'USD', 'hash', 1
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
                    (1, 1, 'debit', 150.00),   -- cash increases by 150
                    (1, 2, 'credit', 150.00)  -- expense offset
                """
            )
        )

        # Optional: create etl_events to avoid logging errors
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


def test_reconcile_uses_balances_json_override(tmp_path: Any, monkeypatch: Any) -> None:
    # Arrange: DB and balances file
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    _seed_minimal_gl(db_url)

    balances_path = tmp_path / "balances.json"
    # Provide period movement balance 150.00 matching GL
    balances_path.write_text(json.dumps({"plaid_checking": 150.00}))

    # Point CLI to our SQLite DB and run without PLAID creds
    monkeypatch.setenv("DATABASE_URL", db_url)
    out_json = tmp_path / "recon.json"
    result = runner.invoke(
        app,
        [
            "reconcile",
            "--item-id",
            "demo-item",
            "--period",
            "2024Q1",
            "--out",
            str(out_json),
            "--balances-json",
            str(balances_path),
        ],
    )

    # Assert: exit 0 and JSON written
    assert result.exit_code == 0, result.output
    assert out_json.exists()
    data = json.loads(out_json.read_text())
    assert data["success"] is True
    assert data["checks"]["cash_variance"]["variance"] == 0.0
