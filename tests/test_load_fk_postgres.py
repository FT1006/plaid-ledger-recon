"""PostgreSQL integration tests for loader FK resolution."""

import os
from uuid import uuid4

import pytest
from sqlalchemy import text

from etl.load import load_journal_entries
from tests.utils.db_helper import create_test_engine

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="Requires Postgres; set DATABASE_URL to run.",
)


@pytest.mark.integration
def test_postgres_loader_resolves_account_fk() -> None:
    """Test that loader resolves GL account codes to FK IDs in PostgreSQL."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_test_engine(database_url)
    schema_name = f"test_load_{uuid4().hex[:8]}"

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))

        try:
            # Create canonical GL schema with PostgreSQL types
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

            conn.execute(
                text("""
                CREATE TABLE etl_events (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    event_type TEXT NOT NULL,
                    row_counts JSONB,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    success BOOLEAN NOT NULL
                )
            """)
            )

            # Seed GL accounts - loader must resolve by code, not name
            checking_id, restaurant_id = conn.execute(
                text("""
                INSERT INTO accounts (code, name, type, is_cash) VALUES
                    ('Assets:Bank:Checking', 'Bank Checking Account', 'asset', true),
                    (
                        'Expenses:Dining:Restaurants', 'Restaurant Expenses',
                        'expense', false
                    )
                RETURNING id
            """)
            ).fetchall()

            # Seed plaid account and link
            conn.execute(
                text("""
                INSERT INTO plaid_accounts (
                    plaid_account_id, name, type, subtype, currency
                )
                VALUES (
                    'plaid_checking_123', 'Chase Checking',
                    'depository', 'checking', 'USD'
                )
            """)
            )

            conn.execute(
                text("""
                INSERT INTO account_links (plaid_account_id, account_id)
                VALUES ('plaid_checking_123', :account_id)
            """),
                {"account_id": checking_id[0]},
            )

            # Journal entry with GL account codes (transform output format)
            entries = [
                {
                    "txn_id": "postgres_test_txn_1",
                    "txn_date": "2024-01-01",
                    "description": "PostgreSQL test transaction",
                    "currency": "USD",
                    "source_hash": "postgres_abc123hash",
                    "transform_version": 1,
                    "lines": [
                        # Loader must resolve these codes to UUIDs
                        {
                            "account": "Expenses:Dining:Restaurants",
                            "side": "debit",
                            "amount": 75.50,
                        },
                        {
                            "account": "Assets:Bank:Checking",
                            "side": "credit",
                            "amount": 75.50,
                        },
                    ],
                }
            ]

            # Load should resolve account codes to UUID FKs
            load_journal_entries(entries, conn)

            # Verify entries were loaded with proper UUID FKs
            lines = conn.execute(
                text("""
                SELECT l.account_id, a.code, l.side, l.amount
                FROM journal_lines l
                JOIN accounts a ON l.account_id = a.id
                ORDER BY l.side DESC
            """)
            ).fetchall()

            assert len(lines) == 2
            # Verify account_id is a valid UUID
            assert lines[0][0] is not None  # UUID not null
            assert lines[1][0] is not None  # UUID not null
            # Verify correct account codes were resolved
            assert lines[0] == (
                restaurant_id[0],
                "Expenses:Dining:Restaurants",
                "debit",
                75.50,
            )
            assert lines[1] == (checking_id[0], "Assets:Bank:Checking", "credit", 75.50)

            # Verify ETL event was recorded with JSONB
            event = conn.execute(
                text("""
                SELECT row_counts FROM etl_events WHERE event_type = 'load'
            """)
            ).scalar()
            assert event is not None
            assert event["journal_entries"] == 1
            assert event["journal_lines"] == 2

        finally:
            conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))


@pytest.mark.integration
def test_postgres_loader_fails_on_unmapped_code() -> None:
    """Test that loader fails fast when GL code has no accounts entry."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_test_engine(database_url)
    schema_name = f"test_unmapped_{uuid4().hex[:8]}"

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))

        try:
            # Minimal schema
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

            conn.execute(
                text("""
                CREATE TABLE etl_events (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    event_type TEXT NOT NULL,
                    row_counts JSONB,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    success BOOLEAN NOT NULL
                )
            """)
            )

            # Seed only one GL account
            conn.execute(
                text("""
                INSERT INTO accounts (code, name, type, is_cash)
                VALUES ('Assets:Bank:Checking', 'Bank Checking Account', 'asset', true)
            """)
            )

            # Entry references unmapped GL code
            entries = [
                {
                    "txn_id": "unmapped_test_txn",
                    "txn_date": "2024-01-01",
                    "description": "Test transaction with unmapped code",
                    "currency": "USD",
                    "source_hash": "unmapped_hash123",
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

            # Should fail fast with clear message including account code
            with pytest.raises(ValueError) as exc_info:
                load_journal_entries(entries, conn)

            error_msg = str(exc_info.value)
            assert (
                "No GL account found for code: Expenses:Unmapped:Category" in error_msg
            )
            assert "PFETL_AUTO_CREATE_ACCOUNTS" in error_msg

        finally:
            conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))


@pytest.mark.integration
def test_postgres_loader_env_var_behavior() -> None:
    """Test PFETL_AUTO_CREATE_ACCOUNTS environment variable with PostgreSQL."""

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not configured for integration test")

    engine = create_test_engine(database_url)
    schema_name = f"test_env_{uuid4().hex[:8]}"

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
        conn.execute(text(f"SET search_path TO {schema_name}"))

        try:
            # Minimal schema
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
                    transform_version INTEGER NOT NULL
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE journal_lines (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    entry_id UUID NOT NULL REFERENCES journal_entries(id),
                    account_id UUID NOT NULL REFERENCES accounts(id),
                    side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                    amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
                )
            """)
            )

            conn.execute(
                text("""
                CREATE TABLE etl_events (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    event_type TEXT NOT NULL,
                    row_counts JSONB,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    success BOOLEAN NOT NULL
                )
            """)
            )

            # Only seed one account
            conn.execute(
                text("""
                INSERT INTO accounts (code, name, type, is_cash)
                VALUES ('Assets:Bank:Checking', 'Bank Checking Account', 'asset', true)
            """)
            )

            # Entry with unmapped account code
            entries = [
                {
                    "txn_id": "env_test_txn",
                    "txn_date": "2024-01-01",
                    "description": "Environment variable test",
                    "currency": "USD",
                    "source_hash": "env_test_hash",
                    "transform_version": 1,
                    "lines": [
                        {
                            "account": "Assets:Bank:Checking",
                            "side": "debit",
                            "amount": 50.00,
                        },
                        {
                            "account": f"Expenses:Unmapped:{uuid4()}",
                            "side": "credit",
                            "amount": 50.00,
                        },
                    ],
                }
            ]

            # Test default behavior (should fail)
            old_env = os.environ.get("PFETL_AUTO_CREATE_ACCOUNTS")
            try:
                # Remove env var - default is false
                os.environ.pop("PFETL_AUTO_CREATE_ACCOUNTS", None)

                with pytest.raises(
                    ValueError,
                    match=r"No GL account found for code: Expenses:Unmapped:",
                ):
                    load_journal_entries(entries, conn)

                # Explicitly set to false
                os.environ["PFETL_AUTO_CREATE_ACCOUNTS"] = "false"
                with pytest.raises(
                    ValueError,
                    match=r"No GL account found for code: Expenses:Unmapped:",
                ):
                    load_journal_entries(entries, conn)

                # Set to "0" (should also be false)
                os.environ["PFETL_AUTO_CREATE_ACCOUNTS"] = "0"
                with pytest.raises(
                    ValueError,
                    match=r"No GL account found for code: Expenses:Unmapped:",
                ):
                    load_journal_entries(entries, conn)

            finally:
                # Restore original env
                if old_env is not None:
                    os.environ["PFETL_AUTO_CREATE_ACCOUNTS"] = old_env
                else:
                    os.environ.pop("PFETL_AUTO_CREATE_ACCOUNTS", None)

        finally:
            conn.execute(text(f"DROP SCHEMA {schema_name} CASCADE"))
