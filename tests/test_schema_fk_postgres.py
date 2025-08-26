"""PostgreSQL-specific FK constraint integration tests."""

import contextlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


@pytest.mark.integration
def test_postgres_journal_lines_fk_enforced() -> None:
    """Test FK enforcement with PostgreSQL-specific behavior."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_engine(database_url)
    schema_name = f"test_fk_{uuid4().hex[:8]}"

    with engine.connect() as conn:
        # Create isolated test schema
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))
        conn.commit()

        try:
            # Create tables with PostgreSQL-specific types
            conn.execute(
                text("""
                CREATE TABLE accounts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL CHECK (type IN (
                        'asset','liability','equity','revenue','expense'
                    )),
                    is_cash BOOLEAN NOT NULL DEFAULT false
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE journal_entries (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    txn_id TEXT UNIQUE NOT NULL,
                    txn_date DATE NOT NULL,
                    description TEXT NOT NULL,
                    currency CHAR(3) NOT NULL,
                    source_hash TEXT NOT NULL,
                    transform_version INTEGER NOT NULL,
                    ingested_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE journal_lines (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    entry_id UUID NOT NULL
                        REFERENCES journal_entries(id) ON DELETE CASCADE,
                    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
                    side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                    amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
                )
            """)
            )

            # Insert test data
            account_id = conn.execute(
                text("""
                INSERT INTO accounts (code, name, type, is_cash)
                VALUES ('Assets:Bank:Checking', 'Bank Checking', 'asset', true)
                RETURNING id
            """)
            ).scalar()

            entry_id = conn.execute(
                text("""
                INSERT INTO journal_entries (
                    txn_id, txn_date, description, currency,
                    source_hash, transform_version
                )
                VALUES ('test-txn-1', '2024-01-01', 'Test entry', 'USD', 'abc123', 1)
                RETURNING id
            """)
            ).scalar()

            # Valid FK should work
            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES (:entry_id, :account_id, 'debit', 100.00)
            """),
                {"entry_id": entry_id, "account_id": account_id},
            )

            # Invalid FK should fail with IntegrityError
            fake_uuid = "00000000-0000-0000-0000-000000000000"
            with pytest.raises(IntegrityError, match="violates foreign key constraint"):
                conn.execute(
                    text("""
                    INSERT INTO journal_lines (entry_id, account_id, side, amount)
                    VALUES (:entry_id, :fake_uuid, 'credit', 100.00)
                """),
                    {"entry_id": entry_id, "fake_uuid": fake_uuid},
                )

        finally:
            # Cleanup - rollback any failed transaction first
            with contextlib.suppress(Exception):
                conn.rollback()
            conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))
            conn.commit()


@pytest.mark.integration
def test_postgres_account_links_cascade_behavior() -> None:
    """Test CASCADE vs RESTRICT FK behavior in PostgreSQL."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_engine(database_url)
    schema_name = f"test_cascade_{uuid4().hex[:8]}"

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))

        try:
            # Create full canonical schema
            conn.execute(
                text("""
                CREATE TABLE accounts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL CHECK (type IN (
                        'asset','liability','equity','revenue','expense'
                    )),
                    is_cash BOOLEAN NOT NULL DEFAULT false
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
                    currency CHAR(3) NOT NULL
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE account_links (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    plaid_account_id TEXT UNIQUE NOT NULL
                    REFERENCES plaid_accounts(plaid_account_id) ON DELETE CASCADE,
                    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE journal_entries (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    txn_id TEXT UNIQUE NOT NULL,
                    txn_date DATE NOT NULL,
                    description TEXT NOT NULL,
                    currency CHAR(3) NOT NULL,
                    source_hash TEXT NOT NULL,
                    transform_version INTEGER NOT NULL
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE journal_lines (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    entry_id UUID NOT NULL
                        REFERENCES journal_entries(id) ON DELETE CASCADE,
                    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
                    side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                    amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
                )
            """)
            )

            # Insert test data
            gl_account_id = conn.execute(
                text("""
                INSERT INTO accounts (code, name, type)
                VALUES ('Assets:Bank:Checking', 'Checking', 'asset')
                RETURNING id
            """)
            ).scalar()

            conn.execute(
                text("""
                INSERT INTO plaid_accounts (
                    plaid_account_id, name, type, subtype, currency
                )
                VALUES ('plaid_123', 'Chase Checking', 'depository', 'checking', 'USD')
            """)
            )

            conn.execute(
                text("""
                INSERT INTO account_links (plaid_account_id, account_id)
                VALUES ('plaid_123', :account_id)
            """),
                {"account_id": gl_account_id},
            )

            # Add journal line referencing the GL account
            entry_id = conn.execute(
                text("""
                INSERT INTO journal_entries (
                    txn_id, txn_date, description, currency,
                    source_hash, transform_version
                )
                VALUES ('test-entry', '2024-01-01', 'Test', 'USD', 'hash1', 1)
                RETURNING id
            """)
            ).scalar()

            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES (:entry_id, :account_id, 'debit', 100.00)
            """),
                {"entry_id": entry_id, "account_id": gl_account_id},
            )

            # Verify links exist
            link_count = conn.execute(
                text("SELECT COUNT(*) FROM account_links")
            ).scalar()
            assert link_count == 1

            # Delete plaid account should CASCADE the link
            conn.execute(
                text("DELETE FROM plaid_accounts WHERE plaid_account_id = 'plaid_123'")
            )

            link_count = conn.execute(
                text("SELECT COUNT(*) FROM account_links")
            ).scalar()
            assert link_count == 0  # Link was cascaded away

            # But GL account should still exist (RESTRICT protected by journal_lines)
            account_count = conn.execute(text("SELECT COUNT(*) FROM accounts")).scalar()
            assert account_count == 1

            # Trying to delete GL account should fail due to RESTRICT + journal_lines FK
            with pytest.raises(IntegrityError, match="violates foreign key constraint"):
                conn.execute(
                    text("DELETE FROM accounts WHERE id = :account_id"),
                    {"account_id": gl_account_id},
                )

        finally:
            conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))


@pytest.mark.integration
def test_postgres_check_constraints_and_enums() -> None:
    """Test PostgreSQL-specific constraint behavior."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_engine(database_url)
    schema_name = f"test_check_{uuid4().hex[:8]}"

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))

        try:
            # Create table with strict constraints
            conn.execute(
                text("""
                CREATE TABLE accounts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL CHECK (type IN (
                        'asset','liability','equity','revenue','expense'
                    )),
                    is_cash BOOLEAN NOT NULL DEFAULT false
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE journal_lines (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    entry_id UUID NOT NULL,
                    account_id UUID NOT NULL REFERENCES accounts(id),
                    side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                    amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
                )
            """)
            )

            # Valid constraint values should work
            account_id = conn.execute(
                text("""
                INSERT INTO accounts (code, name, type, is_cash)
                VALUES ('Assets:Test', 'Test Account', 'asset', true)
                RETURNING id
            """)
            ).scalar()

            conn.execute(
                text("""
                INSERT INTO journal_lines (entry_id, account_id, side, amount)
                VALUES (gen_random_uuid(), :account_id, 'debit', 100.50)
            """),
                {"account_id": account_id},
            )

            # Invalid account type should fail
            with pytest.raises(IntegrityError, match="violates check constraint"):
                conn.execute(
                    text("""
                    INSERT INTO accounts (code, name, type)
                    VALUES ('Bad:Account', 'Bad Type', 'invalid_type')
                """)
                )

            # Invalid side should fail
            with pytest.raises(IntegrityError, match="violates check constraint"):
                conn.execute(
                    text("""
                    INSERT INTO journal_lines (entry_id, account_id, side, amount)
                    VALUES (gen_random_uuid(), :account_id, 'both', 50.00)
                """),
                    {"account_id": account_id},
                )

            # Negative amount should fail
            with pytest.raises(IntegrityError, match="violates check constraint"):
                conn.execute(
                    text("""
                    INSERT INTO journal_lines (entry_id, account_id, side, amount)
                    VALUES (gen_random_uuid(), :account_id, 'credit', -25.00)
                """),
                    {"account_id": account_id},
                )

        finally:
            conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))


@pytest.mark.integration
def test_postgres_uuid_and_timestamptz() -> None:
    """Test PostgreSQL UUID and TIMESTAMPTZ behavior."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_engine(database_url)
    schema_name = f"test_uuid_{uuid4().hex[:8]}"

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))

        try:
            conn.execute(
                text("""
                CREATE TABLE journal_entries (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    txn_id TEXT UNIQUE NOT NULL,
                    txn_date DATE NOT NULL,
                    description TEXT NOT NULL,
                    currency CHAR(3) NOT NULL,
                    source_hash TEXT NOT NULL,
                    transform_version INTEGER NOT NULL,
                    ingested_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            )

            # Insert with auto-generated UUID and timestamp
            entry_id = conn.execute(
                text("""
                INSERT INTO journal_entries (
                    txn_id, txn_date, description, currency,
                    source_hash, transform_version
                )
                VALUES ('test-uuid', '2024-01-01', 'Test', 'USD', 'hash1', 1)
                RETURNING id, ingested_at
            """)
            ).fetchone()

            assert entry_id is not None
            assert entry_id[0] is not None  # UUID generated
            assert entry_id[1] is not None  # TIMESTAMPTZ generated

            # Verify UUID format (36 chars with dashes)
            uuid_str = str(entry_id[0])
            assert len(uuid_str) == 36
            assert uuid_str.count("-") == 4

            # Verify duplicate txn_id fails with specific error
            with pytest.raises(
                IntegrityError, match="duplicate key value violates unique constraint"
            ):
                conn.execute(
                    text("""
                    INSERT INTO journal_entries (
                    txn_id, txn_date, description, currency,
                    source_hash, transform_version
                )
                    VALUES ('test-uuid', '2024-01-02', 'Duplicate', 'USD', 'hash2', 1)
                """)
                )

        finally:
            conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))
