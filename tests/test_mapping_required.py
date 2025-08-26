"""Test that missing GL account codes fail fast with clear error messages."""

import pytest
from sqlalchemy import create_engine, text

from etl.load import load_journal_entries


def test_missing_gl_account_fails_fast() -> None:
    """Test that missing GL account code fails with clear error message."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create canonical GL schema
        conn.execute(
            text("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL CHECK (type IN (
                    'asset','liability','equity','revenue','expense'
                )),
                is_cash BOOLEAN NOT NULL DEFAULT 0
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
                    REFERENCES plaid_accounts(plaid_account_id) ON DELETE CASCADE,
                account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT
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
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            CREATE TABLE etl_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                row_counts TEXT,
                started_at TEXT,
                finished_at TEXT,
                success BOOLEAN NOT NULL
            )
        """)
        )

        # Seed GL accounts
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Bank Checking Account', 'asset', 1),
                (2, 'Expenses:Dining:Restaurants', 'Restaurant Expenses', 'expense', 0)
        """)
        )

        # Seed plaid account but NO account_links mapping
        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES (
                'unmapped_plaid_account', 'Unmapped Chase Checking',
                'depository', 'checking', 'USD'
            )
        """)
        )

        # Entry that references unmapped GL account
        entries = [
            {
                "txn_id": "test_txn_unmapped",
                "txn_date": "2024-01-01",
                "description": "Transaction with unmapped account",
                "currency": "USD",
                "source_hash": "unmapped123",
                "transform_version": 1,
                "lines": [
                    {
                        "account": "Assets:Bank:Checking",
                        "side": "debit",
                        "amount": 100.00,
                    },
                    {
                        "account": "Expenses:Unmapped:Category",
                        "side": "credit",
                        "amount": 100.00,
                    },
                ],
            }
        ]

        # Should fail fast with crisp error message
        with pytest.raises(ValueError) as exc_info:
            load_journal_entries(entries, conn)

        error_msg = str(exc_info.value)
        assert "No GL account found for code: Expenses:Unmapped:Category" in error_msg
        assert (
            "Set PFETL_AUTO_CREATE_ACCOUNTS=true to allow creation "
            "(disabled by default)" in error_msg
        )


def test_plaid_account_missing_link_scenario() -> None:
    """Test scenario where plaid account exists but has no GL mapping."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create schema
        conn.execute(
            text("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL CHECK (type IN (
                    'asset','liability','equity','revenue','expense'
                )),
                is_cash BOOLEAN NOT NULL DEFAULT 0
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
            CREATE TABLE journal_entries (
                id INTEGER PRIMARY KEY,
                txn_id TEXT UNIQUE NOT NULL,
                txn_date DATE NOT NULL,
                description TEXT NOT NULL,
                currency TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                transform_version INTEGER NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL,
                amount NUMERIC(18,2) NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE etl_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                row_counts TEXT,
                started_at TEXT,
                finished_at TEXT,
                success BOOLEAN NOT NULL
            )
        """)
        )

        # Seed GL accounts
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                (1, 'Assets:Bank:Checking', 'Bank Checking', 'asset', 1),
                (2, 'Assets:Bank:Savings', 'Bank Savings', 'asset', 1)
        """)
        )

        # Seed plaid accounts
        conn.execute(
            text("""
            INSERT INTO plaid_accounts (
                plaid_account_id, name, type, subtype, currency
            ) VALUES
                (
                    'plaid_checking_mapped', 'Chase Checking',
                    'depository', 'checking', 'USD'
                ),
                (
                    'plaid_savings_unmapped', 'Chase Savings',
                    'depository', 'savings', 'USD'
                )
        """)
        )

        # Only map one of the plaid accounts
        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES ('plaid_checking_mapped', 1)
        """)
        )

        # Entry that should work (uses mapped GL account)
        good_entries = [
            {
                "txn_id": "good_txn",
                "txn_date": "2024-01-01",
                "description": "Good transaction",
                "currency": "USD",
                "source_hash": "good123",
                "transform_version": 1,
                "lines": [
                    {
                        "account": "Assets:Bank:Checking",
                        "side": "debit",
                        "amount": 50.00,
                    },
                    {
                        "account": "Assets:Bank:Checking",
                        "side": "credit",
                        "amount": 50.00,
                    },
                ],
            }
        ]

        # This should work
        load_journal_entries(good_entries, conn)

        # Verify it worked
        count = conn.execute(text("SELECT COUNT(*) FROM journal_lines")).scalar()
        assert count == 2

        # Entry that should fail (GL account doesn't exist)
        bad_entries = [
            {
                "txn_id": "bad_txn",
                "txn_date": "2024-01-01",
                "description": "Bad transaction",
                "currency": "USD",
                "source_hash": "bad123",
                "transform_version": 1,
                "lines": [
                    {
                        "account": "Assets:Bank:Checking",
                        "side": "debit",
                        "amount": 50.00,
                    },
                    {
                        "account": "Assets:Bank:NonExistent",
                        "side": "credit",
                        "amount": 50.00,
                    },
                ],
            }
        ]

        # Should fail with clear error
        with pytest.raises(
            ValueError, match="No GL account found for code: Assets:Bank:NonExistent"
        ):
            load_journal_entries(bad_entries, conn)


def test_error_message_includes_env_hint() -> None:
    """Test error message includes helpful hint about PFETL_AUTO_CREATE_ACCOUNTS."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Minimal schema
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
                transform_version INTEGER NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL,
                amount NUMERIC(18,2) NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE etl_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                row_counts TEXT,
                started_at TEXT,
                finished_at TEXT,
                success BOOLEAN NOT NULL
            )
        """)
        )

        # No accounts seeded - everything will be unmapped

        entries = [
            {
                "txn_id": "test_txn",
                "txn_date": "2024-01-01",
                "description": "Test",
                "currency": "USD",
                "source_hash": "test123",
                "transform_version": 1,
                "lines": [
                    {
                        "account": "Assets:Bank:NonExistent",
                        "side": "debit",
                        "amount": 100.00,
                    },
                    {
                        "account": "Expenses:Also:NonExistent",
                        "side": "credit",
                        "amount": 100.00,
                    },
                ],
            }
        ]

        with pytest.raises(ValueError) as exc_info:
            load_journal_entries(entries, conn)

        error_msg = str(exc_info.value)

        # Should mention the missing account
        assert "Assets:Bank:NonExistent" in error_msg

        # Should include env variable hint
        assert "PFETL_AUTO_CREATE_ACCOUNTS" in error_msg


def test_multiple_missing_accounts_first_error_wins() -> None:
    """Test that we get the first missing account error, not all of them."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Schema setup
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
                transform_version INTEGER NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL,
                amount NUMERIC(18,2) NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE etl_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                row_counts TEXT,
                started_at TEXT,
                finished_at TEXT,
                success BOOLEAN NOT NULL
            )
        """)
        )

        # Entry with multiple unmapped accounts
        entries = [
            {
                "txn_id": "test_multi_fail",
                "txn_date": "2024-01-01",
                "description": "Multiple unmapped accounts",
                "currency": "USD",
                "source_hash": "multi123",
                "transform_version": 1,
                "lines": [
                    {
                        "account": "Assets:First:Missing",
                        "side": "debit",
                        "amount": 100.00,
                    },
                    {
                        "account": "Expenses:Second:Missing",
                        "side": "credit",
                        "amount": 100.00,
                    },
                ],
            }
        ]

        with pytest.raises(ValueError) as exc_info:
            load_journal_entries(entries, conn)

        error_msg = str(exc_info.value)

        # Should mention at least one of the missing accounts
        # (Implementation detail: which one fails first depends on processing order)
        assert ("Assets:First:Missing" in error_msg) or (
            "Expenses:Second:Missing" in error_msg
        )

        # Should still include the env hint
        assert "PFETL_AUTO_CREATE_ACCOUNTS" in error_msg


@pytest.mark.parametrize("source_hash,transform_version,expected_error", [
    (None, 1, "source_hash is required"),
    ("", 1, "source_hash cannot be empty"),
    ("   ", 1, "source_hash cannot be empty"),
    ("hash123", None, "transform_version is required"),
    ("hash123", 0, "transform_version must be positive"),
    ("hash123", -1, "transform_version must be positive"),
])
def test_missing_lineage_fails_load(source_hash: str | None, transform_version: int | None, expected_error: str) -> None:
    """Test that entries without valid source_hash or transform_version fail validation."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Minimal schema - allow NULLs so loader validates first
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
                source_hash TEXT,  -- Allow NULL for loader validation
                transform_version INTEGER  -- Allow NULL for loader validation
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY,
                entry_id INTEGER NOT NULL REFERENCES journal_entries(id),
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                side TEXT NOT NULL,
                amount NUMERIC(18,2) NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE etl_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                row_counts TEXT,
                started_at TEXT,
                finished_at TEXT,
                success BOOLEAN NOT NULL
            )
        """)
        )

        # Seed account
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash)
            VALUES (1, 'Assets:Bank:Checking', 'Bank Checking Account', 'asset', 1)
        """)
        )

        # Entry with invalid lineage
        entries = [
            {
                "txn_id": f"test_txn_{source_hash}_{transform_version}",
                "txn_date": "2024-01-01",
                "description": "Test lineage validation",
                "currency": "USD",
                "source_hash": source_hash,
                "transform_version": transform_version,
                "lines": [
                    {
                        "account": "Assets:Bank:Checking",
                        "side": "debit",
                        "amount": 100.00,
                    },
                ],
            }
        ]

        with pytest.raises(ValueError, match=expected_error):
            load_journal_entries(entries, conn)
