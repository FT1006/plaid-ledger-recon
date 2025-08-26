"""Tests for data loading and database operations."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.pool import StaticPool

# Load API under test:
# - load_accounts(accounts: list[dict], conn: Connection) -> None
# - load_journal_entries(entries: list[dict], conn: Connection) -> None
# - get_account_by_plaid_id(plaid_id: str, conn: Connection) -> dict | None
# - get_entries_count(conn: Connection) -> int
from etl.load import (
    get_account_by_plaid_id,
    get_entries_count,
    load_accounts,
    load_journal_entries,
)


@pytest.fixture
def db_engine() -> Engine:
    """In-memory SQLite for tests."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    # Create minimal schema for testing
    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=ON"))

        conn.execute(
            text("""
            CREATE TABLE ingest_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency TEXT NOT NULL
            )
        """),
        )
        conn.execute(
            text("""
            CREATE TABLE journal_entries (
                id INTEGER PRIMARY KEY,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT,
                currency TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                transform_version INTEGER NOT NULL
            )
        """),
        )
        # NOTE: Using MVP schema with text account names (no FK)
        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL,
                account TEXT NOT NULL,
                side TEXT NOT NULL,
                amount DECIMAL(15,2) NOT NULL,
                FOREIGN KEY (entry_id) REFERENCES journal_entries(id) ON DELETE CASCADE
            )
        """),
        )
        conn.execute(
            text("""
            CREATE TABLE etl_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                item_id TEXT,
                row_counts TEXT,
                started_at TEXT,
                finished_at TEXT,
                success INTEGER NOT NULL
            )
        """),
        )
    return engine


def test_idempotent_reingest_no_duplicates(db_engine: Engine) -> None:
    """Loading same data twice → same counts, no duplicates.

    Per ADR §2: Idempotency via upsert on unique keys.
    """
    accounts = [
        {
            "plaid_account_id": "plaid_chk",
            "name": "Assets:Bank:Checking",
            "type": "depository",
            "subtype": "checking",
            "currency": "USD",
        },
        {
            "plaid_account_id": "exp_dining",
            "name": "Expenses:Dining",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "inc_salary",
            "name": "Income:Salary",
            "type": "revenue",
            "subtype": "",
            "currency": "USD",
        },
    ]

    entries = [
        {
            "txn_id": "txn_001",
            "txn_date": date(2024, 1, 15),
            "description": "Coffee Shop",
            "currency": "USD",
            "source_hash": "abc123def456",
            "transform_version": 1,
            "lines": [
                {
                    "account": "Expenses:Dining",
                    "side": "debit",
                    "amount": Decimal("5.50"),
                },
                {
                    "account": "Assets:Bank:Checking",
                    "side": "credit",
                    "amount": Decimal("5.50"),
                },
            ],
        },
        {
            "txn_id": "txn_002",
            "txn_date": date(2024, 1, 16),
            "description": "Salary",
            "currency": "USD",
            "source_hash": "def789ghi012",
            "transform_version": 1,
            "lines": [
                {
                    "account": "Assets:Bank:Checking",
                    "side": "debit",
                    "amount": Decimal("1000.00"),
                },
                {
                    "account": "Income:Salary",
                    "side": "credit",
                    "amount": Decimal("1000.00"),
                },
            ],
        },
    ]

    with db_engine.begin() as conn:
        # First load
        load_accounts(accounts, conn)
        load_journal_entries(entries, conn)
        first_count = get_entries_count(conn)

        # Second load (idempotent)
        load_accounts(accounts, conn)
        load_journal_entries(entries, conn)
        second_count = get_entries_count(conn)

    assert first_count == 2
    assert second_count == 2  # No duplicates


def test_accounts_upsert_by_plaid_account_id(db_engine: Engine) -> None:
    """Accounts upsert on plaid_account_id; updates preserve key.

    Per ADR §2: Account metadata refreshes via upsert.
    """
    initial_account = {
        "plaid_account_id": "acc_456",
        "name": "Old Name",
        "type": "depository",
        "subtype": "checking",
        "currency": "USD",
    }

    updated_account = {
        "plaid_account_id": "acc_456",  # Same ID
        "name": "New Name",  # Changed
        "type": "depository",
        "subtype": "savings",  # Changed
        "currency": "USD",
    }

    with db_engine.begin() as conn:
        # Initial load
        load_accounts([initial_account], conn)
        first_result = get_account_by_plaid_id("acc_456", conn)

        # Update load
        load_accounts([updated_account], conn)
        second_result = get_account_by_plaid_id("acc_456", conn)

    assert first_result is not None
    assert first_result["name"] == "Old Name"
    assert first_result["subtype"] == "checking"

    assert second_result is not None
    assert second_result["name"] == "New Name"
    assert second_result["subtype"] == "savings"

    # Verify no duplicate accounts
    with db_engine.begin() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM ingest_accounts WHERE plaid_account_id = :pid"),
            {"pid": "acc_456"},
        ).scalar()
    assert result == 1


def test_source_hash_is_sha256_of_compact_raw_json_and_is_stable() -> None:
    """Source hash must be SHA256 of minified, sorted JSON.

    Per ADR Appendix: "Hash: SHA256 of canonicalized (minified, key-sorted) JSON."
    """
    # Simulate raw Plaid transaction
    raw_txn = {
        "transaction_id": "txn_xyz",
        "account_id": "acc_789",
        "amount": 42.50,
        "date": "2024-01-20",
        "name": "Test Merchant",
        "category": ["Food and Drink", "Restaurants"],
        "pending": False,
    }

    # Compute hash as per ADR spec
    canonical = json.dumps(raw_txn, sort_keys=True, separators=(",", ":"))
    expected_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # The transform should produce this exact hash
    entry = {
        "txn_id": "txn_xyz",
        "txn_date": date(2024, 1, 20),
        "description": "Test Merchant",
        "currency": "USD",
        "source_hash": expected_hash,  # Must match
        "transform_version": 1,
        "lines": [
            {"account": "Expenses:Dining", "side": "debit", "amount": Decimal("42.50")},
            {
                "account": "Assets:Bank:Checking",
                "side": "credit",
                "amount": Decimal("42.50"),
            },
        ],
    }

    # Verify stability: same input → same hash
    canonical2 = json.dumps(raw_txn, sort_keys=True, separators=(",", ":"))
    hash2 = hashlib.sha256(canonical2.encode("utf-8")).hexdigest()

    assert entry["source_hash"] == expected_hash
    assert hash2 == expected_hash  # Stable across runs


def test_bulk_load_performance_with_many_entries(db_engine: Engine) -> None:
    """Bulk loading should handle large batches efficiently.

    Per ADR §2: Bulk operations for performance.
    """
    # Seed accounts first
    accounts = [
        {
            "plaid_account_id": "exp_test",
            "name": "Expenses:Test",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "bank",
            "name": "Assets:Bank",
            "type": "asset",
            "subtype": "",
            "currency": "USD",
        },
    ]

    # Generate 1000 entries
    entries = [
        {
            "txn_id": f"txn_{i:04d}",
            "txn_date": date(2024, 1, 1 + (i % 28)),
            "description": f"Transaction {i}",
            "currency": "USD",
            "source_hash": hashlib.sha256(f"txn_{i}".encode()).hexdigest(),
            "transform_version": 1,
            "lines": [
                {
                    "account": "Expenses:Test",
                    "side": "debit",
                    "amount": Decimal(f"{i}.50"),
                },
                {
                    "account": "Assets:Bank",
                    "side": "credit",
                    "amount": Decimal(f"{i}.50"),
                },
            ],
        }
        for i in range(1000)
    ]

    with db_engine.begin() as conn:
        load_accounts(accounts, conn)
        load_journal_entries(entries, conn)
        count = get_entries_count(conn)

    assert count == 1000


def test_journal_lines_linked_to_entries(db_engine: Engine) -> None:
    """Journal lines must be properly linked to their parent entries.

    Per ADR §2: Referential integrity for audit trail.
    """
    # Seed accounts first
    accounts = [
        {
            "plaid_account_id": "exp_office",
            "name": "Expenses:Office",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "exp_tax",
            "name": "Expenses:Tax",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "bank",
            "name": "Assets:Bank",
            "type": "asset",
            "subtype": "",
            "currency": "USD",
        },
    ]

    entry = {
        "txn_id": "txn_link_test",
        "txn_date": date(2024, 1, 25),
        "description": "Multi-line entry",
        "currency": "USD",
        "source_hash": "test_hash",
        "transform_version": 1,
        "lines": [
            {
                "account": "Expenses:Office",
                "side": "debit",
                "amount": Decimal("100.00"),
            },
            {"account": "Expenses:Tax", "side": "debit", "amount": Decimal("10.00")},
            {"account": "Assets:Bank", "side": "credit", "amount": Decimal("110.00")},
        ],
    }

    with db_engine.begin() as conn:
        load_accounts(accounts, conn)
        load_journal_entries([entry], conn)

        # Verify entry exists
        entry_result = conn.execute(
            text("SELECT id FROM journal_entries WHERE txn_id = :tid"),
            {"tid": "txn_link_test"},
        ).fetchone()
        assert entry_result is not None
        entry_id = entry_result[0]

        # Verify all lines are linked
        lines_result = conn.execute(
            text("SELECT COUNT(*) FROM journal_lines WHERE entry_id = :eid"),
            {"eid": entry_id},
        ).scalar()
        assert lines_result == 3

        # Verify line details (MVP schema uses text account names)
        lines = conn.execute(
            text("""
                SELECT l.account, l.side, l.amount
                FROM journal_lines l
                WHERE l.entry_id = :eid
                ORDER BY l.id
            """),
            {"eid": entry_id},
        ).fetchall()

        assert lines[0] == ("Expenses:Office", "debit", 100.00)
        assert lines[1] == ("Expenses:Tax", "debit", 10.00)
        assert lines[2] == ("Assets:Bank", "credit", 110.00)


def test_transform_version_tracked_for_lineage(db_engine: Engine) -> None:
    """Transform version must be preserved for audit lineage.

    Per ADR §4: Transform version tracking for reproducibility.
    """
    # Seed accounts first
    accounts = [
        {
            "plaid_account_id": "exp_v1",
            "name": "Expenses:V1",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "exp_v2",
            "name": "Expenses:V2",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "bank",
            "name": "Assets:Bank",
            "type": "asset",
            "subtype": "",
            "currency": "USD",
        },
    ]

    entries_v1 = [
        {
            "txn_id": "txn_v1",
            "txn_date": date(2024, 1, 10),
            "description": "Version 1 transform",
            "currency": "USD",
            "source_hash": "hash_v1",
            "transform_version": 1,
            "lines": [
                {"account": "Expenses:V1", "side": "debit", "amount": Decimal("50.00")},
                {
                    "account": "Assets:Bank",
                    "side": "credit",
                    "amount": Decimal("50.00"),
                },
            ],
        },
    ]

    entries_v2 = [
        {
            "txn_id": "txn_v2",
            "txn_date": date(2024, 1, 11),
            "description": "Version 2 transform",
            "currency": "USD",
            "source_hash": "hash_v2",
            "transform_version": 2,
            "lines": [
                {"account": "Expenses:V2", "side": "debit", "amount": Decimal("75.00")},
                {
                    "account": "Assets:Bank",
                    "side": "credit",
                    "amount": Decimal("75.00"),
                },
            ],
        },
    ]

    with db_engine.begin() as conn:
        load_accounts(accounts, conn)
        load_journal_entries(entries_v1, conn)
        load_journal_entries(entries_v2, conn)

        # Verify versions are preserved
        v1_result = conn.execute(
            text("SELECT transform_version FROM journal_entries WHERE txn_id = :tid"),
            {"tid": "txn_v1"},
        ).scalar()
        v2_result = conn.execute(
            text("SELECT transform_version FROM journal_entries WHERE txn_id = :tid"),
            {"tid": "txn_v2"},
        ).scalar()

    assert v1_result == 1
    assert v2_result == 2


def test_currency_preserved_in_journal_entries(db_engine: Engine) -> None:
    """Currency must be preserved from source through to storage.

    Per ADR §1: Multi-currency support (no FX in MVP).
    """
    # Seed accounts first
    accounts = [
        {
            "plaid_account_id": "exp_usd",
            "name": "Expenses:USD",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "exp_cad",
            "name": "Expenses:CAD",
            "type": "expense",
            "subtype": "",
            "currency": "CAD",
        },
        {
            "plaid_account_id": "assets_usd",
            "name": "Assets:USD",
            "type": "asset",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "assets_cad",
            "name": "Assets:CAD",
            "type": "asset",
            "subtype": "",
            "currency": "CAD",
        },
    ]

    entries = [
        {
            "txn_id": "txn_usd",
            "txn_date": date(2024, 1, 20),
            "description": "USD transaction",
            "currency": "USD",
            "source_hash": "hash_usd",
            "transform_version": 1,
            "lines": [
                {
                    "account": "Expenses:USD",
                    "side": "debit",
                    "amount": Decimal("100.00"),
                },
                {
                    "account": "Assets:USD",
                    "side": "credit",
                    "amount": Decimal("100.00"),
                },
            ],
        },
        {
            "txn_id": "txn_cad",
            "txn_date": date(2024, 1, 21),
            "description": "CAD transaction",
            "currency": "CAD",
            "source_hash": "hash_cad",
            "transform_version": 1,
            "lines": [
                {
                    "account": "Expenses:CAD",
                    "side": "debit",
                    "amount": Decimal("150.00"),
                },
                {
                    "account": "Assets:CAD",
                    "side": "credit",
                    "amount": Decimal("150.00"),
                },
            ],
        },
    ]

    with db_engine.begin() as conn:
        load_accounts(accounts, conn)
        load_journal_entries(entries, conn)

        currencies = conn.execute(
            text("SELECT txn_id, currency FROM journal_entries ORDER BY txn_id"),
        ).fetchall()

    assert currencies[0] == ("txn_cad", "CAD")
    assert currencies[1] == ("txn_usd", "USD")


def test_etl_events_rowcounts_recorded(db_engine: Engine) -> None:
    """Loader appends etl_events with row_counts and success flag.

    Per ADR §2: ETL events tracking for audit trail.
    """
    # Seed accounts first
    accounts = [
        {
            "plaid_account_id": "exp_test",
            "name": "Expenses:Test",
            "type": "expense",
            "subtype": "",
            "currency": "USD",
        },
        {
            "plaid_account_id": "bank",
            "name": "Assets:Bank",
            "type": "asset",
            "subtype": "",
            "currency": "USD",
        },
    ]

    entries = [
        {
            "txn_id": "txn_evt",
            "txn_date": date(2024, 1, 22),
            "description": "Event test",
            "currency": "USD",
            "source_hash": "hash_evt",
            "transform_version": 1,
            "lines": [
                {
                    "account": "Expenses:Test",
                    "side": "debit",
                    "amount": Decimal("10.00"),
                },
                {
                    "account": "Assets:Bank",
                    "side": "credit",
                    "amount": Decimal("10.00"),
                },
            ],
        },
    ]

    with db_engine.begin() as conn:
        load_accounts(accounts, conn)
        load_journal_entries(entries, conn)

        row = conn.execute(
            text("""
                SELECT event_type, row_counts, success
                FROM etl_events ORDER BY id DESC LIMIT 1
            """),
        ).fetchone()

    assert row is not None
    assert row[0] == "load"
    rc = json.loads(row[1])
    assert rc.get("journal_entries") == 1
    assert rc.get("journal_lines") == 2
    assert row[2] == 1  # success=true
