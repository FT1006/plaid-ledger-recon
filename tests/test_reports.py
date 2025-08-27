"""RED tests for deterministic HTML + PDF reports."""

import re
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from tests.utils.hash import hash_html


@pytest.fixture
def db_engine() -> Engine:
    """In-memory SQLite for tests."""
    engine = create_engine("sqlite:///:memory:")

    # Create schema (simplified for testing)
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
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
                id TEXT PRIMARY KEY,
                item_id TEXT,
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
                id TEXT PRIMARY KEY,
                entry_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                side TEXT NOT NULL,
                amount DECIMAL(18,2) NOT NULL,
                FOREIGN KEY (entry_id) REFERENCES journal_entries(id),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE account_links (
                id TEXT PRIMARY KEY,
                plaid_account_id TEXT UNIQUE NOT NULL,
                account_id TEXT NOT NULL,
                FOREIGN KEY (plaid_account_id)
                    REFERENCES plaid_accounts(plaid_account_id),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            )
        """)
        )

        # Insert seed data
        seed_sql = Path("tests/fixtures/report_seed.sql").read_text()
        # Remove postgres-specific parts for sqlite
        seed_sql = seed_sql.replace("DELETE FROM", "DELETE FROM").replace(
            "gen_random_uuid()", "'test-uuid'"
        )

        # Insert accounts first
        conn.execute(
            text("""
            INSERT INTO accounts (id, code, name, type, is_cash) VALUES
                ('acc1', 'Assets:Bank:Checking', 'Bank Checking Account', 'asset', 1),
                ('acc2', 'Expenses:Dining:Restaurants',
                 'Restaurant Expenses', 'expense', 0)
        """)
        )

        # Insert test data
        conn.execute(
            text("""
            INSERT INTO journal_entries (id, item_id, txn_id, txn_date,
                description, currency, source_hash, transform_version) VALUES
                ('entry1', 'test_item', 'test_txn_001', '2024-01-15',
                 'Test Restaurant Purchase', 'USD', 'abc123def456', 1)
        """)
        )

        conn.execute(
            text("""
            INSERT INTO journal_lines (id, entry_id, account_id, side, amount) VALUES
                ('line1', 'entry1', 'acc2', 'debit', 25.00),
                ('line2', 'entry1', 'acc1', 'credit', 25.00)
        """)
        )

    return engine


def test_balance_sheet_html_matches_golden_snapshot(db_engine: Engine) -> None:
    """Balance sheet HTML must match golden snapshot hash.

    Per ADR: HTML reports must be deterministic for audit compliance.
    """
    from etl.reports.render import render_balance_sheet  # noqa: PLC0415

    html = render_balance_sheet("2024Q1", db_engine)
    actual_hash = hash_html(html)

    # Expected hash from golden snapshot
    expected_hash = "5a2bfedaa1fb5a80d65c25afa174862a1967e95ad08138f0508b573b84c81464"

    # Ensure no timestamps in output
    assert "2025-" not in html and "Generated at" not in html, (
        "HTML must not contain timestamps for deterministic output"
    )

    # Verify 2-decimal formatting for amounts
    amounts = re.findall(r"\d+\.\d{2}", html)
    assert len(amounts) > 0, (
        "HTML must contain properly formatted amounts (2 decimal places)"
    )

    assert actual_hash == expected_hash, (
        f"Balance sheet HTML hash mismatch.\n"
        f"Expected: {expected_hash}\n"
        f"Actual:   {actual_hash}\n"
        f"This indicates non-deterministic output or business rule change."
    )


def test_cash_flow_html_matches_golden_snapshot(db_engine: Engine) -> None:
    """Cash flow HTML must match golden snapshot hash.

    Per ADR: HTML reports must be deterministic for audit compliance.
    """
    from etl.reports.render import render_cash_flow  # noqa: PLC0415

    html = render_cash_flow("2024Q1", db_engine)
    actual_hash = hash_html(html)

    # Expected hash from golden snapshot
    expected_hash = "f8e5fb6550a82a7a1ae060a0e050697ad63ebbb4f236189f763ef385d1811e8f"

    # Ensure no timestamps in output
    assert "2025-" not in html and "Generated at" not in html, (
        "HTML must not contain timestamps for deterministic output"
    )

    # Verify 2-decimal formatting for amounts
    amounts = re.findall(r"\d+\.\d{2}", html)
    assert len(amounts) > 0, (
        "HTML must contain properly formatted amounts (2 decimal places)"
    )

    assert actual_hash == expected_hash, (
        f"Cash flow HTML hash mismatch.\n"
        f"Expected: {expected_hash}\n"
        f"Actual:   {actual_hash}\n"
        f"This indicates non-deterministic output or business rule change."
    )


def test_pdf_is_emitted_but_not_snapshotted(db_engine: Engine) -> None:
    """PDF must be generated and have non-zero size.

    Per ADR: PDFs are tested for existence only, not content.
    Binary format varies by environment and WeasyPrint version.
    """
    from etl.reports.render import (  # noqa: PLC0415
        render_balance_sheet,
        write_pdf,
    )

    html = render_balance_sheet("2024Q1", db_engine)

    with tempfile.TemporaryDirectory() as tmp_dir:
        pdf_path = write_pdf(html, Path(tmp_dir) / "test_report.pdf")

        assert pdf_path.exists(), "PDF file must be created"
        assert pdf_path.stat().st_size > 0, "PDF file must be non-empty"

        # Verify it's actually a PDF (basic header check)
        with pdf_path.open("rb") as f:
            header = f.read(4)
            assert header == b"%PDF", "File must be valid PDF format"


def test_running_report_twice_produces_identical_html_hash(db_engine: Engine) -> None:
    """Running same report twice must produce identical HTML hash.

    Per ADR: Reports must be deterministic for audit reproducibility.
    No timestamps, random IDs, or env-dependent values allowed.
    """
    from etl.reports.render import render_balance_sheet  # noqa: PLC0415

    # Run report twice with identical inputs
    html1 = render_balance_sheet("2024Q1", db_engine)
    html2 = render_balance_sheet("2024Q1", db_engine)

    # Verify 2-decimal formatting for amounts in both runs
    amounts1 = re.findall(r"\d+\.\d{2}", html1)
    amounts2 = re.findall(r"\d+\.\d{2}", html2)
    assert len(amounts1) > 0 and len(amounts2) > 0, (
        "Both runs must contain properly formatted amounts"
    )

    hash1 = hash_html(html1)
    hash2 = hash_html(html2)

    assert hash1 == hash2, (
        f"Report output must be deterministic.\n"
        f"Run 1 hash: {hash1}\n"
        f"Run 2 hash: {hash2}\n"
        f"Check for timestamps, random IDs, or env dependencies."
    )
