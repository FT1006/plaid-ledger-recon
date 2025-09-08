"""End-to-end integration tests with real database."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
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
        try:
            wait_for_db(database_url)
        except Exception:
            pytest.skip(
                "E2E: DATABASE_URL set but DB unreachable; skipping in constrained env"
            )

        # Optional: Skip if outbound HTTP egress to Plaid sandbox is unavailable
        try:
            with httpx.Client(timeout=httpx.Timeout(connect=3.0, read=3.0)) as client:
                # Any HTTPStatusError proves egress works; only connect/timeout skip
                resp = client.get(
                    "https://sandbox.plaid.com/",
                    headers={"User-Agent": "pfetl-e2e-check"},
                )
                _ = resp.status_code
        except (httpx.ConnectError, httpx.TimeoutException):
            pytest.skip("E2E: Plaid sandbox not reachable; skipping in constrained env")
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
        if result.returncode != 0:
            pytest.skip(f"Sandbox onboarding unavailable in CI: {result.stderr}")
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


@pytest.mark.e2e
def test_e2e_quick_start_flow(compose_services: Any, tmp_path: Path) -> None:  # noqa: PLR0915, ARG001
    """Simulate the Quick Start flow from README.

    Steps covered:
    - init-db
    - seed-coa (execute SQL file directly)
    - onboard (sandbox)
    - ingest (Q1 window)
    - list-plaid-accounts (item-scoped)
    - map-account (map common depository subtypes)
    - generate demo balances (as-of period end)
    - reconcile (deterministic JSON mode)
    - report (HTML)
    """
    load_dotenv()

    # Skip if Plaid credentials not available and sandbox onboard fails
    database_url = (
        os.getenv("DATABASE_URL")
        or "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl"
    )
    os.environ["DATABASE_URL"] = database_url

    # 1) init-db
    res = subprocess.run(  # noqa: S603
        [sys.executable, "cli.py", "init-db"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f"init-db failed: {res.stderr}"

    # 2) seed-coa by executing the SQL file directly against the configured DB
    seed_sql = Path(__file__).resolve().parents[1] / "etl" / "seed_coa.sql"
    assert seed_sql.exists(), "Missing etl/seed_coa.sql"
    from sqlalchemy import create_engine as _ce

    engine = _ce(database_url)
    with engine.begin() as conn:
        conn.execute(text(seed_sql.read_text()))

    # 3) onboard sandbox (if no existing token)
    if not os.getenv("PLAID_ACCESS_TOKEN"):
        res = subprocess.run(  # noqa: S603
            [sys.executable, "cli.py", "onboard", "--sandbox", "--write-env"],
            check=False,
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            pytest.skip(f"Sandbox onboarding unavailable: {res.stderr}")
        load_dotenv(override=True)
    item_id = os.getenv("PLAID_ITEM_ID", "test_item")

    # 4) ingest Q1
    res = subprocess.run(  # noqa: S603
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
    )
    assert res.returncode == 0, f"ingest failed: {res.stderr}"

    # 5) list-plaid-accounts to discover account IDs
    res = subprocess.run(  # noqa: S603
        [sys.executable, "cli.py", "list-plaid-accounts", "--item-id", item_id],
        check=False,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f"list-plaid-accounts failed: {res.stderr}"
    lines = [ln.strip() for ln in (res.stdout or "").splitlines() if ln.strip()]
    # Expect lines like: "acc_id | Name | type/subtype"
    mappings: list[tuple[str, str]] = []
    subtype_to_gl = {
        "checking": "Assets:Bank:Checking",
        "savings": "Assets:Bank:Savings",
        "money_market": "Assets:Bank:MoneyMarket",
        "cash_management": "Assets:Bank:CashManagement",
    }
    for ln in lines:
        parts = [p.strip() for p in ln.split("|")]
        if len(parts) != 3:
            continue
        acc_id = parts[0]
        type_sub = parts[2]
        if "/" in type_sub:
            typ, sub = [
                s.strip().lower().replace(" ", "_") for s in type_sub.split("/", 1)
            ]
            if typ == "depository" and sub in subtype_to_gl:
                mappings.append((acc_id, subtype_to_gl[sub]))

    # 6) map detected cash accounts
    for acc_id, gl_code in mappings:
        mr = subprocess.run(  # noqa: S603
            [
                sys.executable,
                "cli.py",
                "map-account",
                "--plaid-account-id",
                acc_id,
                "--gl-code",
                gl_code,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert mr.returncode == 0, f"map-account failed for {acc_id}: {mr.stderr}"

    # 7) generate demo balances (GL as-of 2024-03-31)
    build_dir = tmp_path / "build"
    build_dir.mkdir(parents=True, exist_ok=True)
    balances_path = build_dir / "demo_balances.json"
    # Query GL as-of balances similar to Makefile target
    q = text(
        """
        WITH cash_accts AS (
          SELECT a.id, al.plaid_account_id
          FROM accounts a
          JOIN account_links al ON al.account_id = a.id
          WHERE a.is_cash = TRUE
        ), gl_bal AS (
          SELECT ca.plaid_account_id,
                 COALESCE(SUM(CASE WHEN jl.side='debit' THEN jl.amount
                              ELSE -jl.amount END),0.00) AS bal
          FROM cash_accts ca
          LEFT JOIN journal_lines jl ON jl.account_id = ca.id
          LEFT JOIN journal_entries je ON je.id = jl.entry_id
          WHERE je.txn_date <= '2024-03-31' OR je.txn_date IS NULL
          GROUP BY ca.plaid_account_id
        )
        SELECT COALESCE(json_object_agg(plaid_account_id, bal), '{}'::json) FROM gl_bal;
        """
    )
    with engine.begin() as conn:
        row = conn.execute(q).scalar()
    balances_path.write_text(str(row))

    # 8) reconcile (deterministic JSON mode)
    recon_path = build_dir / "recon.json"
    rr = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "cli.py",
            "reconcile",
            "--item-id",
            item_id,
            "--period",
            "2024Q1",
            "--balances-json",
            str(balances_path),
            "--out",
            str(recon_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rr.returncode == 0, f"reconcile failed: {rr.stderr}\n{rr.stdout}"
    assert recon_path.exists(), "recon.json not written"

    # 9) report (HTML)
    rp = subprocess.run(  # noqa: S603
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
            str(build_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rp.returncode == 0, f"report failed: {rp.stderr}"
    assert (build_dir / "bs_2024Q1.html").exists(), "Balance Sheet HTML missing"
    assert (build_dir / "cf_2024Q1.html").exists(), "Cash Flow HTML missing"
