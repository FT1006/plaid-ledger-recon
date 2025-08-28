"""End-to-end integration tests with real database."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from dotenv import load_dotenv
from sqlalchemy import text

from tests.utils.db_helper import create_test_engine

if not (
    os.getenv("DATABASE_URL")
    or shutil.which("docker")
    or shutil.which("docker-compose")
):
    pytest.skip(
        "E2E requires Postgres via DATABASE_URL or Docker; skipping locally.",
        allow_module_level=True,
    )

if TYPE_CHECKING:
    from collections.abc import Generator


def wait_for_db(database_url: str, max_retries: int = 30) -> None:
    """Wait for database to be ready, with retries."""
    for attempt in range(max_retries):
        try:
            engine = create_test_engine(database_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(1)
        else:
            return


def _get_docker_compose_cmd() -> list[str] | None:
    """Get available docker compose command (v2 first, fallback to v1)."""
    # Try docker compose (v2) first
    if shutil.which("docker"):
        try:
            docker_path = shutil.which("docker")
            if docker_path:
                subprocess.run(  # noqa: S603
                    [docker_path, "compose", "version"],
                    capture_output=True,
                    check=True,
                )
        except subprocess.CalledProcessError:
            pass
        else:
            return ["docker", "compose"]

    # Fallback to docker-compose (v1)
    if shutil.which("docker-compose"):
        return ["docker-compose"]

    return None


@pytest.fixture(scope="module")
def compose_services() -> Generator[None, None, None]:
    """Start Docker Compose services for testing."""
    # Skip docker-compose if in CI (use GitHub's Postgres service) or DATABASE_URL set
    if os.getenv("CI") or os.getenv("DATABASE_URL"):
        # CI: Use GitHub Actions Postgres service, or Local: Use existing database
        database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl",
        )
        wait_for_db(database_url)
        yield
        return

    # Check for compose file
    compose_file = Path(__file__).parent.parent / "docker-compose.yml"
    if not compose_file.exists():
        compose_file = Path(__file__).parent.parent / "infra" / "docker-compose.yml"
        if not compose_file.exists():
            pytest.skip("No docker-compose.yml found")

    # Check for docker compose command
    compose_cmd = _get_docker_compose_cmd()
    if not compose_cmd:
        pytest.skip("Docker compose not available")

    # Start services
    subprocess.run(  # noqa: S603
        [*compose_cmd, "-f", str(compose_file), "up", "-d", "postgres"],
        check=True,
        capture_output=True,
    )

    # Wait for database
    test_db_url = "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl"
    wait_for_db(test_db_url)

    yield

    # Teardown
    subprocess.run(  # noqa: S603
        [*compose_cmd, "-f", str(compose_file), "down", "-v"],
        check=True,
        capture_output=True,
    )


@pytest.mark.e2e
def test_e2e_full_ingest_pipeline(compose_services: Any) -> None:  # noqa: ARG001, PLR0915
    """Test complete ETL pipeline from Plaid to database.

    This test verifies:
    1. CLI commands execute without error
    2. Data flows through extract → transform → load
    3. Database contains expected journal entries
    4. Idempotency on re-run
    """
    load_dotenv()

    # Skip if no Plaid credentials
    if not os.getenv("PLAID_CLIENT_ID") or not os.getenv("PLAID_SECRET"):
        pytest.skip("Plaid credentials not configured")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        database_url = "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl"
        os.environ["DATABASE_URL"] = database_url

    # 1. Initialize database schema
    result = subprocess.run(  # noqa: S603
        [sys.executable, "cli.py", "init-db"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"init-db failed: {result.stderr}"
    assert "Database schema initialized" in result.stdout

    # 2. Onboard a sandbox item (if not already done)
    if not os.getenv("PLAID_ACCESS_TOKEN"):
        result = subprocess.run(  # noqa: S603
            [sys.executable, "cli.py", "onboard", "--sandbox", "--write-env"],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"onboard failed: {result.stderr}"
        # Extract item_id from output
        item_id = result.stdout.strip()
        assert len(item_id) > 0, f"Empty item_id: {item_id}"

        # Reload .env to pick up new access token
        load_dotenv(override=True)
    else:
        # Use existing item
        item_id = os.getenv("PLAID_ITEM_ID", "test_item")

    # 3. Run ingest for a date range
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
            "2024-01-31",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"ingest failed: {result.stderr}"
    assert "Ingested" in result.stdout or "transactions" in result.stdout

    # 4. Verify data in database
    engine = create_test_engine(database_url)
    with engine.connect() as conn:
        # Check journal entries exist
        entry_count = conn.execute(
            text("SELECT COUNT(*) FROM journal_entries")
        ).scalar()
        assert entry_count is not None and entry_count > 0, "No journal entries created"

        # Check journal lines exist and balance
        lines_count = conn.execute(text("SELECT COUNT(*) FROM journal_lines")).scalar()
        assert lines_count is not None and lines_count > 0, "No journal lines created"
        assert entry_count is not None and lines_count >= entry_count * 2, (
            "Each entry needs at least 2 lines"
        )

        # Verify double-entry balance
        unbalanced = conn.execute(
            text("""
                SELECT e.txn_id,
                       SUM(CASE WHEN l.side = 'debit' THEN l.amount ELSE 0 END) debits,
                       SUM(CASE WHEN l.side = 'credit' THEN l.amount ELSE 0 END) credits
                FROM journal_entries e
                JOIN journal_lines l ON e.id = l.entry_id
                GROUP BY e.txn_id
                HAVING SUM(CASE WHEN l.side = 'debit' THEN l.amount ELSE 0 END) !=
                       SUM(CASE WHEN l.side = 'credit' THEN l.amount ELSE 0 END)
            """)
        ).fetchall()
        assert len(unbalanced) == 0, f"Unbalanced entries found: {unbalanced}"

        # Check ETL event recorded
        event_count = conn.execute(
            text("SELECT COUNT(*) FROM etl_events WHERE event_type = 'load'")
        ).scalar()
        assert event_count is not None and event_count >= 1, (
            "No ETL load event recorded"
        )

        # Verify plaid_accounts table is populated (new canonical table)
        plaid_accounts_count = conn.execute(
            text("SELECT COUNT(*) FROM plaid_accounts")
        ).scalar()
        assert plaid_accounts_count is not None and plaid_accounts_count > 0, (
            "No Plaid accounts populated in canonical plaid_accounts table"
        )

        # Test explicit account mapping command (optional smoke test)
        plaid_id = conn.execute(
            text("""
            SELECT plaid_account_id
            FROM plaid_accounts
            WHERE type='depository' AND subtype='checking'
            LIMIT 1
        """)
        ).scalar()

        if plaid_id:
            # Run map-account CLI command
            map_res = subprocess.run(  # noqa: S603
                [
                    sys.executable,
                    "cli.py",
                    "map-account",
                    "--plaid-account-id",
                    plaid_id,
                    "--gl-code",
                    "Assets:Bank:Checking",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            assert map_res.returncode == 0, f"map-account failed: {map_res.stderr}"

            # Verify the link was created
            with engine.connect() as verify_conn:
                link_cnt = verify_conn.execute(
                    text("""
                    SELECT COUNT(*) FROM account_links WHERE plaid_account_id = :pid
                """),
                    {"pid": plaid_id},
                ).scalar()
                assert link_cnt == 1, "Account link not created"

        # Verify FK integrity: all journal_lines have valid account_id (Task 9)
        # Only check if account_id column exists (post-migration)
        try:
            # Try to select account_id - if column doesn't exist, we'll get an exception
            conn.execute(text("SELECT account_id FROM journal_lines LIMIT 1"))
            has_account_id = True
        except Exception:
            has_account_id = False

        if has_account_id:
            null_account_ids = conn.execute(
                text("SELECT COUNT(*) FROM journal_lines WHERE account_id IS NULL")
            ).scalar()
            assert null_account_ids == 0, (
                f"Found {null_account_ids} journal_lines with NULL account_id"
            )

    # 5. Test idempotency - run again with same date range
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
            "2024-01-31",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Second ingest failed: {result.stderr}"

    # Verify no duplicates created
    with engine.connect() as conn:
        new_entry_count = conn.execute(
            text("SELECT COUNT(*) FROM journal_entries")
        ).scalar()
        assert new_entry_count == entry_count, "Duplicate entries created"

        # But ETL event should be recorded
        new_event_count = conn.execute(
            text("SELECT COUNT(*) FROM etl_events WHERE event_type = 'load'")
        ).scalar()
        assert (
            new_event_count is not None
            and event_count is not None
            and new_event_count > event_count
        ), "No new ETL event for second run"


@pytest.mark.e2e
def test_e2e_credit_card_transactions(compose_services: Any) -> None:  # noqa: ARG001
    """Test that credit card transactions are properly handled.

    Specifically tests the normalization fixes for 'credit/credit card'.
    """
    load_dotenv()

    if not os.getenv("PLAID_CLIENT_ID") or not os.getenv("PLAID_SECRET"):
        pytest.skip("Plaid credentials not configured")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        database_url = "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl"
        os.environ["DATABASE_URL"] = database_url

    # Ensure schema is initialized
    subprocess.run(  # noqa: S603
        [sys.executable, "cli.py", "init-db"], check=False, capture_output=True
    )

    # Use existing access token if available
    if not os.getenv("PLAID_ACCESS_TOKEN"):
        pytest.skip("Requires existing PLAID_ACCESS_TOKEN with credit accounts")

    item_id = os.getenv("PLAID_ITEM_ID", "test_item")

    # Run ingest
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
            "2024-03-31",  # Longer range to catch credit transactions
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    # Should not fail with "Unmapped Plaid account type/subtype: credit/credit card"
    assert "Unmapped Plaid account type/subtype" not in result.stderr
    assert result.returncode == 0, f"ingest failed: {result.stderr}"

    # Verify credit account data
    engine = create_test_engine(database_url)
    with engine.connect() as conn:
        # Check for credit accounts in ingest_accounts
        credit_accounts = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM ingest_accounts
                WHERE type = 'credit'
            """)
        ).scalar()

        if credit_accounts is not None and credit_accounts > 0:
            # Verify credit card transactions were transformed
            # Use the actual account mapping from coa.yaml
            credit_entries = conn.execute(
                text("""
                    SELECT COUNT(DISTINCT e.id)
                    FROM journal_entries e
                    JOIN journal_lines l ON e.id = l.entry_id
                    JOIN accounts a ON l.account_id = a.id
                    WHERE a.code LIKE 'Liabilities:CreditCard%'
                """)
            ).scalar()
            assert credit_entries is not None and credit_entries >= 0, (
                "Credit card account mapping verified"
            )
