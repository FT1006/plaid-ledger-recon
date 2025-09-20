"""Demo data loader for offline/deterministic demonstrations."""

import contextlib
import json
import os
import sqlite3
from decimal import Decimal
from pathlib import Path

from sqlalchemy import Connection, create_engine, text
from sqlalchemy.engine import Engine


def setup_sqlite_determinism() -> None:
    """Configure SQLite for deterministic behavior matching Postgres."""
    # Register Decimal adapter to prevent float rounding
    sqlite3.register_adapter(Decimal, str)

    # Set deterministic environment
    os.environ["LC_ALL"] = "C.UTF-8"
    os.environ["TZ"] = "UTC"


def create_demo_engine() -> Engine:
    """Create an in-memory SQLite engine configured for demos."""
    setup_sqlite_determinism()

    # Use in-memory database for speed and isolation
    engine = create_engine("sqlite:///:memory:", echo=False)

    # Apply SQLite configuration using direct SQL execution
    with engine.begin() as conn:
        # Set SQLite pragmas first
        conn.execute(text("PRAGMA foreign_keys = ON"))
        conn.execute(text("PRAGMA journal_mode = WAL"))
        conn.execute(text("PRAGMA synchronous = NORMAL"))

        # Create tables manually - simple and reliable approach
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL CHECK (
                    type IN ('asset','liability','equity','revenue','expense')
                ),
                subtype TEXT,
                currency TEXT NOT NULL DEFAULT 'USD',
                is_cash BOOLEAN NOT NULL DEFAULT 0,
                active BOOLEAN NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'USD',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS account_links (
                id TEXT PRIMARY KEY,
                plaid_account_id TEXT UNIQUE NOT NULL,
                account_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (plaid_account_id)
                    REFERENCES plaid_accounts(plaid_account_id) ON DELETE CASCADE,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS ingest_accounts (
              item_id TEXT NOT NULL,
              plaid_account_id TEXT NOT NULL,
              name TEXT NOT NULL,
              type TEXT NOT NULL,
              subtype TEXT NOT NULL,
              currency TEXT NOT NULL DEFAULT 'USD',
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              PRIMARY KEY (item_id, plaid_account_id)
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS journal_entries (
                id TEXT PRIMARY KEY,
                item_id TEXT,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'USD',
                source_hash TEXT NOT NULL,
                transform_version INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS journal_lines (
                id TEXT PRIMARY KEY,
                entry_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('debit', 'credit')),
                amount DECIMAL(15,2) NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (entry_id) REFERENCES journal_entries(id) ON DELETE CASCADE,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS raw_transactions (
                id TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                txn_id TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS etl_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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

        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_accounts_code ON accounts(code)",
            "CREATE INDEX IF NOT EXISTS idx_accounts_type ON accounts(type)",
            "CREATE INDEX IF NOT EXISTS idx_accounts_is_cash ON accounts(is_cash)",
            "CREATE INDEX IF NOT EXISTS idx_journal_entries_item_date"
            " ON journal_entries(item_id, txn_date)",
            "CREATE INDEX IF NOT EXISTS idx_journal_entries_txn_date"
            " ON journal_entries(txn_date)",
            "CREATE INDEX IF NOT EXISTS idx_journal_lines_entry_id"
            " ON journal_lines(entry_id)",
            "CREATE INDEX IF NOT EXISTS idx_journal_lines_account_id"
            " ON journal_lines(account_id),",
        ]

        for index_sql in indexes:
            with contextlib.suppress(Exception):
                conn.execute(text(index_sql))

    return engine


def load_demo_fixtures(conn: Connection) -> None:
    """Load demo fixture data into the database."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures" / "demo"

    # Load accounts first (dependencies)
    accounts_file = fixtures_dir / "accounts.json"
    if accounts_file.exists():
        accounts = json.loads(accounts_file.read_text(encoding="utf-8"))
        for account in accounts:
            conn.execute(
                text("""
                INSERT INTO accounts (id, code, name, type, is_cash)
                VALUES (:id, :code, :name, :type, :is_cash)
                """),
                account,
            )

    # Load Plaid accounts
    plaid_accounts_file = fixtures_dir / "plaid_accounts.json"
    if plaid_accounts_file.exists():
        plaid_accounts = json.loads(plaid_accounts_file.read_text(encoding="utf-8"))
        for account in plaid_accounts:
            conn.execute(
                text("""
                INSERT INTO plaid_accounts
                (plaid_account_id, name, type, subtype)
                VALUES (:plaid_account_id, :name, :type, :subtype)
                """),
                account,
            )

    # Load account links
    links_file = fixtures_dir / "account_links.json"
    if links_file.exists():
        links = json.loads(links_file.read_text(encoding="utf-8"))
        for link in links:
            conn.execute(
                text("""
                INSERT INTO account_links (plaid_account_id, account_id)
                VALUES (:plaid_account_id, :account_id)
                """),
                link,
            )

    # Load journal entries
    entries_file = fixtures_dir / "journal_entries.json"
    if entries_file.exists():
        entries = json.loads(entries_file.read_text(encoding="utf-8"))
        for entry in entries:
            conn.execute(
                text("""
                INSERT INTO journal_entries
                (id, item_id, txn_id, txn_date, description, currency,
                 source_hash, transform_version)
                VALUES (:id, :item_id, :txn_id, :txn_date, :description,
                        :currency, :source_hash, :transform_version)
                """),
                entry,
            )

    # Load journal lines
    lines_file = fixtures_dir / "journal_lines.json"
    if lines_file.exists():
        lines = json.loads(lines_file.read_text(encoding="utf-8"))
        for line in lines:
            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES (:entry_id, :account_id, :side, :amount)
                """),
                line,
            )

    # Load ingest_accounts for scoping
    ingest_file = fixtures_dir / "ingest_accounts.json"
    if ingest_file.exists():
        ingest_accounts = json.loads(ingest_file.read_text(encoding="utf-8"))
        for ingest_account in ingest_accounts:
            conn.execute(
                text("""
                INSERT INTO ingest_accounts
                (item_id, plaid_account_id, name, type, subtype)
                VALUES (:item_id, :plaid_account_id, :name, :type, :subtype)
                """),
                ingest_account,
            )

    conn.commit()


def get_demo_balances() -> dict[str, float]:
    """Get demo balances for reconciliation."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures" / "demo"
    balances_file = fixtures_dir / "balances_2024q1.json"

    if balances_file.exists():
        return json.loads(balances_file.read_text(encoding="utf-8"))  # type: ignore[no-any-return]

    # Fallback to empty balances
    return {}
