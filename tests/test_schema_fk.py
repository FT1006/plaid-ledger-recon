"""Test foreign key enforcement in the canonical GL schema."""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


def test_journal_lines_fk_enforced() -> None:
    """Test that journal_lines.account_id must reference a valid accounts.id."""
    # Use in-memory SQLite for isolated test
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        # Create minimal schema with FK enforcement
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create accounts table
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

        # Create journal_entries table
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

        # Create journal_lines table with FK
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

        # Insert test data
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash)
            VALUES(1, 'Assets:Bank:Checking', 'Bank Checking', 'asset', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_entries (
                id, txn_id, txn_date, description, currency,
                source_hash, transform_version
            )
            VALUES(1, 'test-txn-1', '2024-01-01', 'Test entry', 'USD', 'abc123', 1)
        """)
        )

        # This should work - valid account_id
        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount)
            VALUES(1, 1, 'debit', 100.00)
        """)
        )

        # This should fail - non-existent account_id
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES(1, 999, 'credit', 100.00)
            """)
            )


def test_account_links_fk_cascade() -> None:
    """Test that account_links properly cascades deletes from plaid_accounts."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create tables
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

        # Insert test data
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type)
            VALUES(1, 'Assets:Bank:Checking', 'Checking', 'asset')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES('plaid_123', 'Chase Checking', 'depository', 'checking', 'USD')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES('plaid_123', 1)
        """)
        )

        # Verify link exists
        result = conn.execute(text("SELECT COUNT(*) FROM account_links")).scalar()
        assert result == 1

        # Delete plaid_account should cascade delete the link
        conn.execute(
            text("DELETE FROM plaid_accounts WHERE plaid_account_id = 'plaid_123'")
        )

        # Link should be gone
        result = conn.execute(text("SELECT COUNT(*) FROM account_links")).scalar()
        assert result == 0

        # But GL account should still exist
        result = conn.execute(text("SELECT COUNT(*) FROM accounts")).scalar()
        assert result == 1


def test_accounts_code_unique_constraint() -> None:
    """Test that accounts.code must be unique."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
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

        # First insert should work
        conn.execute(
            text("""
            INSERT INTO accounts (code, name, type)
            VALUES('Assets:Bank:Checking', 'Checking Account', 'asset')
        """)
        )

        # Duplicate code should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO accounts (code, name, type)
                VALUES('Assets:Bank:Checking', 'Another Checking', 'asset')
            """)
            )


def test_journal_lines_amount_check_constraint() -> None:
    """Test that journal_lines.amount must be >= 0."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create minimal schema
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
                side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
            )
        """)
        )

        # Setup test data
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type)
            VALUES(1, 'Assets:Bank:Checking', 'Checking', 'asset')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_entries (
                id, txn_id, txn_date, description, currency,
                source_hash, transform_version
            )
            VALUES(1, 'test-1', '2024-01-01', 'Test', 'USD', 'hash1', 1)
        """)
        )

        # Positive amount should work
        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount)
            VALUES(1, 1, 'debit', 100.00)
        """)
        )

        # Zero amount should work
        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount)
            VALUES(1, 1, 'credit', 0.00)
        """)
        )

        # Negative amount should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES(1, 1, 'debit', -50.00)
            """)
            )


def test_journal_lines_invalid_side_value() -> None:
    """Test that journal_lines.side must be 'debit' or 'credit'."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create minimal schema
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
                side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
            )
        """)
        )

        # Setup test data
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type)
            VALUES(1, 'Assets:Bank:Checking', 'Checking', 'asset')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_entries (
                id, txn_id, txn_date, description, currency,
                source_hash, transform_version
            )
            VALUES(1, 'test-1', '2024-01-01', 'Test', 'USD', 'hash1', 1)
        """)
        )

        # Valid sides should work
        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount)
            VALUES(1, 1, 'debit', 100.00)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount)
            VALUES(1, 1, 'credit', 100.00)
        """)
        )

        # Invalid side should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES(1, 1, 'both', 50.00)
            """)
            )


def test_journal_entries_txn_id_uniqueness() -> None:
    """Test that journal_entries.txn_id must be unique (ADR §2 deduplication)."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
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

        # First entry should work
        conn.execute(
            text("""
            INSERT INTO journal_entries (
                txn_id, txn_date, description, currency, source_hash, transform_version
            )
            VALUES('txn-123', '2024-01-01', 'First entry', 'USD', 'hash123', 1)
        """)
        )

        # Duplicate txn_id should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO journal_entries (
                txn_id, txn_date, description, currency, source_hash, transform_version
            )
                VALUES('txn-123', '2024-01-02', 'Duplicate entry', 'USD', 'hash456', 1)
            """)
            )


def test_journal_lines_entry_id_fk_enforced() -> None:
    """Test that journal_lines.entry_id must reference a valid journal_entries.id."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create minimal schema
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
                side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
            )
        """)
        )

        # Setup test data
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type)
            VALUES(1, 'Assets:Bank:Checking', 'Checking', 'asset')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_entries (
                id, txn_id, txn_date, description, currency,
                source_hash, transform_version
            )
            VALUES(1, 'test-1', '2024-01-01', 'Test', 'USD', 'hash1', 1)
        """)
        )

        # Valid entry_id should work
        conn.execute(
            text("""
            INSERT INTO journal_lines (entry_id, account_id, side, amount)
            VALUES(1, 1, 'debit', 100.00)
        """)
        )

        # Invalid entry_id should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES(999, 1, 'credit', 100.00)
            """)
            )


def test_account_links_plaid_account_id_uniqueness() -> None:
    """Test that account_links.plaid_account_id must be unique."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))

        # Create tables
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

        # Insert test data
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type)
            VALUES
                (1, 'Assets:Bank:Checking', 'Checking', 'asset'),
                (2, 'Assets:Bank:Savings', 'Savings', 'asset')
        """)
        )

        conn.execute(
            text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype, currency)
            VALUES('plaid_123', 'Chase Checking', 'depository', 'checking', 'USD')
        """)
        )

        # First mapping should work
        conn.execute(
            text("""
            INSERT INTO account_links (plaid_account_id, account_id)
            VALUES('plaid_123', 1)
        """)
        )

        # Second mapping to same plaid_account_id should fail
        # (even to different GL account)
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO account_links (plaid_account_id, account_id)
                VALUES('plaid_123', 2)
            """)
            )


def test_accounts_type_not_null() -> None:
    """Test that accounts.type cannot be NULL."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
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

        # Valid type should work
        conn.execute(
            text("""
            INSERT INTO accounts (code, name, type)
            VALUES('Assets:Bank:Checking', 'Checking Account', 'asset')
        """)
        )

        # NULL type should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO accounts (code, name, type)
                VALUES('Assets:Bank:Savings', 'Savings Account', NULL)
            """)
            )

        # Invalid type should also fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO accounts (code, name, type)
                VALUES('Invalid:Account', 'Invalid Type', 'invalid')
            """)
            )


def test_journal_entries_required_fields() -> None:
    """Test that journal_entries enforces required source_hash and transform_version."""
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
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

        # Valid entry with all required fields
        conn.execute(
            text("""
            INSERT INTO journal_entries (
                txn_id, txn_date, description, currency, source_hash, transform_version
            )
            VALUES('test-1', '2024-01-01', 'Valid entry', 'USD', 'hash123', 1)
        """)
        )

        # Missing source_hash should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO journal_entries (
                    txn_id, txn_date, description, currency, transform_version
                )
                VALUES('test-2', '2024-01-01', 'Missing hash', 'USD', 1)
            """)
            )

        # Missing transform_version should fail
        with pytest.raises(IntegrityError):
            conn.execute(
                text("""
                INSERT INTO journal_entries (
                    txn_id, txn_date, description, currency, source_hash
                )
                VALUES('test-3', '2024-01-01', 'Missing version', 'USD', 'hash456')
            """)
            )
