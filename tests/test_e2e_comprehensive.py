"""Comprehensive E2E test that follows the actual user flow from README.

This test addresses the gaps identified:
1. Tests the complete flow (not just ingest in isolation)
2. Uses real database with proper schema
3. Tests all CLI commands in sequence
4. Verifies composite PK functionality throughout
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from dotenv import load_dotenv
from sqlalchemy import text

from tests.utils.db_helper import create_test_engine

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.mark.e2e
@pytest.mark.integration
def test_complete_quick_start_flow(compose_services: Any, tmp_path: Path) -> None:  # noqa: PLR0912, PLR0915, ARG001
    """Test the complete Quick Start flow from README.

    This comprehensive test covers:
    1. Database initialization (pfetl init-db)
    2. Seed Chart of Accounts (make seed-coa)
    3. Onboard sandbox item (pfetl onboard)
    4. Ingest transactions with item-id scoping
    5. List Plaid accounts (item-scoped)
    6. Map accounts to GL codes
    7. Generate demo balances
    8. Run reconciliation
    9. Generate reports

    Verifies composite PK (item_id, plaid_account_id) throughout.
    """
    load_dotenv()

    # Use existing credentials or skip (require real credentials, not mock)
    mock_values = {"mock_client_id", "mock_secret", "fake_client_id", "fake_secret"}
    cid, sec = os.getenv("PLAID_CLIENT_ID"), os.getenv("PLAID_SECRET")
    if not cid or not sec or cid in mock_values or sec in mock_values:
        pytest.skip("Real Plaid sandbox credentials required for comprehensive E2E")

    database_url = os.getenv(
        "DATABASE_URL", "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl"
    )

    # Step 1: Initialize database schema
    result = subprocess.run(  # noqa: S603
        [sys.executable, "cli.py", "init-db"],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ,
    )
    assert result.returncode == 0, f"init-db failed: {result.stderr}"
    assert "Database schema initialized" in result.stdout

    # Step 2: Seed Chart of Accounts
    seed_sql_path = Path(__file__).parent.parent / "etl" / "seed_coa.sql"
    if seed_sql_path.exists():
        engine = create_test_engine(database_url)
        with engine.begin() as conn:
            conn.execute(text(seed_sql_path.read_text()))
    else:
        # Create minimal COA for testing
        engine = create_test_engine(database_url)
        with engine.begin() as conn:
            conn.execute(
                text("""
                INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                ('checking', 'Assets:Bank:Checking', 'Checking Account', 'asset', true),
                ('savings', 'Assets:Bank:Savings', 'Savings Account', 'asset', true),
                ('mm', 'Assets:Bank:MoneyMarket', 'Money Market', 'asset', true),
                ('dining', 'Expenses:Dining', 'Dining', 'expense', false),
                ('shopping', 'Expenses:Shopping', 'Shopping', 'expense', false),
                ('income', 'Income:Salary', 'Salary', 'income', false)
                ON CONFLICT (id) DO NOTHING
            """)
            )

    # Step 3: Use existing item or onboard new one
    item_id = os.getenv("PLAID_ITEM_ID")
    if not item_id or not os.getenv("PLAID_ACCESS_TOKEN"):
        # Try to onboard sandbox item
        result = subprocess.run(  # noqa: S603
            [sys.executable, "cli.py", "onboard", "--sandbox", "--write-env"],
            check=False,
            capture_output=True,
            text=True,
            env=os.environ,
        )
        if result.returncode != 0 and (
            "Sandbox onboarding" in result.stderr or "unavailable" in result.stderr
        ):
            pytest.skip("Sandbox onboarding not available in CI")

        # Parse item_id from output
        for line in result.stdout.split("\n"):
            if "PLAID_ITEM_ID=" in line:
                item_id = line.split("=")[1].strip()
                break

        if not item_id:
            # Try to reload from .env if it was written
            load_dotenv(override=True)
            item_id = os.getenv("PLAID_ITEM_ID")

    assert item_id, "No item_id available for testing"

    # Step 4: Ingest transactions for Q1 2024
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "cli.py",
            "ingest",
            "--item-id",
            item_id,
            "--from",
            "2024-01-01",
            "--to",
            "2024-03-31",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ,
    )
    assert result.returncode == 0, f"ingest failed: {result.stderr}"
    assert "Ingested" in result.stdout or "transactions" in result.stdout

    # Verify composite PK data in database
    engine = create_test_engine(database_url)
    with engine.connect() as conn:
        # Check ingest_accounts has item-scoped rows with composite PK
        ingest_rows = conn.execute(
            text("""
                SELECT COUNT(*) FROM ingest_accounts
                WHERE item_id = :item_id
            """),
            {"item_id": item_id},
        ).scalar()
        assert ingest_rows and ingest_rows > 0, "No item-scoped rows in ingest_accounts"

        # Verify composite PK is enforced (no duplicates)
        pk_check = conn.execute(
            text("""
                SELECT item_id, plaid_account_id, COUNT(*) as cnt
                FROM ingest_accounts
                WHERE item_id = :item_id
                GROUP BY item_id, plaid_account_id
                HAVING COUNT(*) > 1
            """),
            {"item_id": item_id},
        ).fetchall()
        assert len(pk_check) == 0, f"Duplicate composite keys found: {pk_check}"

        # Check journal entries created
        journal_count = conn.execute(
            text("SELECT COUNT(*) FROM journal_entries")
        ).scalar()
        assert journal_count and journal_count > 0, "No journal entries created"

        # Verify double-entry bookkeeping
        unbalanced = conn.execute(
            text("""
                SELECT e.txn_id,
                       SUM(CASE WHEN l.side = 'debit' THEN l.amount ELSE 0 END)
                           as debits,
                       SUM(CASE WHEN l.side = 'credit' THEN l.amount ELSE 0 END)
                           as credits
                FROM journal_entries e
                JOIN journal_lines l ON e.id = l.entry_id
                GROUP BY e.txn_id
                HAVING SUM(CASE WHEN l.side = 'debit' THEN l.amount ELSE 0 END) !=
                       SUM(CASE WHEN l.side = 'credit' THEN l.amount ELSE 0 END)
            """)
        ).fetchall()
        assert len(unbalanced) == 0, f"Unbalanced entries found: {unbalanced}"

    # Step 5: List Plaid accounts (item-scoped)
    result = subprocess.run(  # noqa: S603
        [sys.executable, "cli.py", "list-plaid-accounts", "--item-id", item_id],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ,
    )
    assert result.returncode == 0, f"list-plaid-accounts failed: {result.stderr}"
    assert result.stdout, "No accounts listed"

    # Parse account IDs for mapping
    account_mappings = []
    for line in result.stdout.split("\n"):
        if "|" in line and "depository" in line.lower():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 3:
                plaid_id = parts[0]
                type_subtype = parts[2].lower()
                if "checking" in type_subtype:
                    account_mappings.append((plaid_id, "Assets:Bank:Checking"))
                elif "savings" in type_subtype:
                    account_mappings.append((plaid_id, "Assets:Bank:Savings"))
                elif "money" in type_subtype:
                    account_mappings.append((plaid_id, "Assets:Bank:MoneyMarket"))

    # Step 6: Map at least one account
    if account_mappings:
        plaid_id, gl_code = account_mappings[0]
        result = subprocess.run(  # noqa: S603
            [
                sys.executable,
                "cli.py",
                "map-account",
                "--plaid-account-id",
                plaid_id,
                "--gl-code",
                gl_code,
            ],
            check=False,
            capture_output=True,
            text=True,
            env=os.environ,
        )
        assert result.returncode == 0, f"map-account failed: {result.stderr}"

        # Verify mapping created
        with engine.connect() as conn:
            link_count = conn.execute(
                text("""
                    SELECT COUNT(*) FROM account_links
                    WHERE plaid_account_id = :pid
                """),
                {"pid": plaid_id},
            ).scalar()
            assert link_count == 1, "Account link not created"

    # Step 7: Generate demo balances
    balances_file = tmp_path / "demo_balances.json"
    with engine.connect() as conn:
        # Generate GL balances as-of period end
        balances_result = conn.execute(
            text("""
                WITH cash_accounts AS (
                    SELECT a.id, al.plaid_account_id
                    FROM accounts a
                    JOIN account_links al ON al.account_id = a.id
                    WHERE a.is_cash = TRUE
                ),
                gl_balances AS (
                    SELECT
                        ca.plaid_account_id,
                        COALESCE(SUM(
                            CASE WHEN jl.side = 'debit'
                            THEN jl.amount
                            ELSE -jl.amount
                            END
                        ), 0.00) AS balance
                    FROM cash_accounts ca
                    LEFT JOIN journal_lines jl ON jl.account_id = ca.id
                    LEFT JOIN journal_entries je ON je.id = jl.entry_id
                    WHERE je.txn_date <= '2024-03-31' OR je.txn_date IS NULL
                    GROUP BY ca.plaid_account_id
                )
                SELECT COALESCE(json_object_agg(plaid_account_id, balance), '{}')::text
                FROM gl_balances
            """)
        ).scalar()

        balances_data = balances_result if balances_result else "{}"
        balances_file.write_text(str(balances_data))

    # Step 8: Run reconciliation (may have variances but shouldn't error)
    recon_file = tmp_path / "recon.json"
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "cli.py",
            "reconcile",
            "--item-id",
            item_id,
            "--period",
            "2024Q1",
            "--balances-json",
            str(balances_file),
            "--out",
            str(recon_file),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ,
    )
    # Allow reconciliation to have variances but check it runs
    assert result.returncode == 0, f"reconcile failed: {result.stderr}"
    assert recon_file.exists(), "Reconciliation output not created"

    # Step 9: Generate reports
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "cli.py",
            "report",
            "--item-id",
            item_id,
            "--period",
            "2024Q1",
            "--formats",
            "html",
            "--out",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ,
    )
    assert result.returncode == 0, f"report failed: {result.stderr}"

    # Verify report files created
    assert (tmp_path / "bs_2024Q1.html").exists(), "Balance sheet not generated"
    assert (tmp_path / "cf_2024Q1.html").exists(), "Cash flow not generated"

    # Step 10: Test idempotency - run ingest again
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "cli.py",
            "ingest",
            "--item-id",
            item_id,
            "--from",
            "2024-01-01",
            "--to",
            "2024-03-31",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ,
    )
    assert result.returncode == 0, f"Second ingest failed: {result.stderr}"

    # Verify no duplicates created
    with engine.connect() as conn:
        new_journal_count = conn.execute(
            text("SELECT COUNT(*) FROM journal_entries")
        ).scalar()
        assert new_journal_count == journal_count, "Duplicate entries created on re-run"

        # Check ingest_accounts still has same count (composite PK prevents duplicates)
        new_ingest_rows = conn.execute(
            text("""
                SELECT COUNT(*) FROM ingest_accounts
                WHERE item_id = :item_id
            """),
            {"item_id": item_id},
        ).scalar()
        assert new_ingest_rows == ingest_rows, "Duplicate ingest_accounts created"


@pytest.fixture(scope="module")
def compose_services() -> Generator[None, None, None]:
    """Start Docker Compose services for testing."""
    import shutil
    import subprocess
    import time

    # Skip if DATABASE_URL is already set (use existing DB)
    if os.getenv("DATABASE_URL"):
        database_url = os.getenv("DATABASE_URL")
        assert database_url  # For mypy
        # Wait for DB to be ready
        for _ in range(30):
            try:
                engine = create_test_engine(database_url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                break
            except Exception:
                time.sleep(1)
        yield
        return

    # Check for docker compose
    if not shutil.which("docker"):
        pytest.skip("Docker not available")

    # Find compose file
    compose_file = Path(__file__).parent.parent / "docker-compose.yml"
    if not compose_file.exists():
        pytest.skip("No docker-compose.yml found")

    # Start services
    subprocess.run(  # noqa: S603
        ["docker", "compose", "-f", str(compose_file), "up", "-d", "postgres"],  # noqa: S607
        check=True,
        capture_output=True,
    )

    # Wait for database
    test_db_url = "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl"
    for _ in range(30):
        try:
            engine = create_test_engine(test_db_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            break
        except Exception:
            time.sleep(1)

    yield

    # Teardown
    subprocess.run(  # noqa: S603
        ["docker", "compose", "-f", str(compose_file), "down", "-v"],  # noqa: S607
        check=True,
        capture_output=True,
    )
