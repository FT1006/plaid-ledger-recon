# etl/load.py
"""Loader with idempotent upserts, audit trail, and ETL event tracking."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def load_accounts(
    accounts: list[dict[str, Any]],
    conn: Connection | None,
    *,
    item_id: str,
) -> None:
    """Upsert accounts by plaid_account_id into ingest_accounts shim table.

    Uses INSERT ... ON CONFLICT for PostgreSQL, fallback for SQLite tests.
    Post Step B migration: item_id is required for composite PK.

    Args:
        accounts: List of account dictionaries from Plaid
        conn: Database connection
        item_id: Required Plaid item ID for composite PK (item_id, plaid_account_id)
    """
    if not accounts:
        return

    if conn is None:
        return

    # Check if we're using PostgreSQL or SQLite
    dialect = conn.dialect.name

    if dialect == "postgresql":
        # PostgreSQL: Use raw SQL INSERT ... ON CONFLICT DO UPDATE
        for account in accounts:
            account_data = {
                **account,
                "item_id": item_id,
            }

            conn.execute(
                text("""
                    INSERT INTO ingest_accounts (
                        plaid_account_id, name, type, subtype, currency, item_id
                    )
                    VALUES (
                        :plaid_account_id, :name, :type, :subtype, :currency, :item_id
                    )
                    ON CONFLICT (item_id, plaid_account_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        subtype = EXCLUDED.subtype,
                        currency = EXCLUDED.currency
                """),
                account_data,
            )
    else:
        # SQLite (for tests): Manual upsert with composite PK
        for account in accounts:
            account_data = {
                **account,
                "item_id": item_id,
            }

            # Check if exists using composite key
            existing = conn.execute(
                text(
                    "SELECT 1 FROM ingest_accounts "
                    "WHERE item_id = :item_id AND plaid_account_id = :pid"
                ),
                {"item_id": item_id, "pid": account["plaid_account_id"]},
            ).fetchone()

            if existing:
                # Update existing record
                conn.execute(
                    text("""
                        UPDATE ingest_accounts
                        SET name = :name, type = :type, subtype = :subtype,
                            currency = :currency
                        WHERE item_id = :item_id
                            AND plaid_account_id = :plaid_account_id
                    """),
                    account_data,
                )
            else:
                # Insert new record
                conn.execute(
                    text("""
                        INSERT INTO ingest_accounts (
                            plaid_account_id, name, type, subtype, currency, item_id
                        )
                        VALUES (
                            :plaid_account_id, :name, :type, :subtype,
                            :currency, :item_id
                        )
                    """),
                    account_data,
                )


def _validate_lineage(entry: dict[str, Any]) -> None:
    """Validate that entry has proper lineage information.

    Args:
        entry: Journal entry dictionary

    Raises:
        ValueError: If source_hash or transform_version invalid
    """
    source_hash = entry.get("source_hash")
    transform_version = entry.get("transform_version")

    # Validate source_hash
    if source_hash is None:
        msg = "source_hash is required"
        raise ValueError(msg)
    if isinstance(source_hash, str) and not source_hash.strip():
        msg = "source_hash cannot be empty"
        raise ValueError(msg)

    # Validate transform_version
    if transform_version is None:
        msg = "transform_version is required"
        raise ValueError(msg)
    if not isinstance(transform_version, int) or transform_version <= 0:
        msg = "transform_version must be positive"
        raise ValueError(msg)


def _resolve_account_id(account_code: str, conn: Connection) -> str:
    """Resolve GL account code to UUID, with fail-fast on missing accounts.

    Args:
        account_code: GL account code like 'Assets:Bank:Checking'
        conn: Database connection

    Returns:
        Account UUID as string

    Raises:
        ValueError: If account code not found and auto-creation disabled
    """
    # Look up by code (canonical contract)
    result = conn.execute(
        text("SELECT id FROM accounts WHERE code = :code"), {"code": account_code}
    ).scalar()

    if result is not None:
        return str(result)

    # Check if auto-creation is enabled
    auto_create = os.environ.get("PFETL_AUTO_CREATE_ACCOUNTS", "false").lower()
    if auto_create in ("true", "1", "yes"):
        # TODO(M3): Implement auto-creation logic here - https://github.com/FT1006/plaid-ledger-recon/issues/new
        # For now, still fail - auto-creation will be added later
        pass

    # Fail fast with clear message and environment hint
    error_msg = (
        f"No GL account found for code: {account_code}. "
        "Set PFETL_AUTO_CREATE_ACCOUNTS=true to allow creation (disabled by default)."
    )
    raise ValueError(error_msg)


def load_journal_entries(
    entries: list[dict[str, Any]],
    conn: Connection | None,
) -> None:
    """Load journal entries with lines, tracking ETL events.

    - Idempotent insert (skip duplicates by txn_id)
    - Resolve account codes to UUIDs for FK integrity
    - Record row counts in etl_events
    - Fail fast on unmapped GL account codes
    """
    if not entries:
        return

    if conn is None:
        return

    started_at = datetime.now(UTC).isoformat()
    entries_inserted = 0
    lines_inserted = 0

    for entry in entries:
        # Validate lineage before processing
        _validate_lineage(entry)

        # Check if entry already exists (idempotency)
        existing = conn.execute(
            text("SELECT id FROM journal_entries WHERE txn_id = :tid"),
            {"tid": entry["txn_id"]},
        ).fetchone()

        if existing:
            continue  # Skip duplicate

        # Insert journal entry
        conn.execute(
            text("""
                INSERT INTO journal_entries
                (txn_id, txn_date, description, currency, source_hash,
                 transform_version)
                VALUES (:txn_id, :txn_date, :description, :currency, :source_hash,
                        :transform_version)
            """),
            {
                "txn_id": entry["txn_id"],
                "txn_date": entry["txn_date"],
                "description": entry.get("description", ""),
                "currency": entry["currency"],
                "source_hash": entry["source_hash"],
                "transform_version": entry["transform_version"],
            },
        )
        entries_inserted += 1

        # Get the inserted entry ID
        entry_id = conn.execute(
            text("SELECT id FROM journal_entries WHERE txn_id = :tid"),
            {"tid": entry["txn_id"]},
        ).scalar()

        # Insert journal lines (resolve GL codes to FK account_id)
        for line in entry["lines"]:
            # Resolve account code to UUID with fail-fast
            account_id = _resolve_account_id(line["account"], conn)

            conn.execute(
                text("""
                    INSERT INTO journal_lines (entry_id, account_id, side, amount)
                    VALUES (:entry_id, :account_id, :side, :amount)
                """),
                {
                    "entry_id": entry_id,
                    "account_id": account_id,  # FK to canonical GL account
                    "side": line["side"],
                    "amount": line["amount"],  # Keep as Decimal for precision
                },
            )
            lines_inserted += 1

    # Record ETL event
    finished_at = datetime.now(UTC).isoformat()
    row_counts = json.dumps({
        "journal_entries": entries_inserted,
        "journal_lines": lines_inserted,
    })

    conn.execute(
        text("""
            INSERT INTO etl_events (
                event_type, row_counts, started_at, finished_at, success
            )
            VALUES (:event_type, :row_counts, :started_at, :finished_at, :success)
        """),
        {
            "event_type": "load",
            "row_counts": row_counts,
            "started_at": started_at,
            "finished_at": finished_at,
            "success": True,
        },
    )


def get_account_by_plaid_id(plaid_id: str, conn: Connection) -> dict[str, Any] | None:
    """Fetch account row by plaid_account_id from ingest_accounts shim."""
    result = conn.execute(
        text("""
            SELECT plaid_account_id, name, type, subtype, currency
            FROM ingest_accounts
            WHERE plaid_account_id = :pid
        """),
        {"pid": plaid_id},
    ).fetchone()

    if result:
        return {
            "plaid_account_id": result[0],
            "name": result[1],
            "type": result[2],
            "subtype": result[3],
            "currency": result[4],
        }
    return None


def get_entries_count(conn: Connection) -> int:
    """Return count of journal entries."""
    result = conn.execute(text("SELECT COUNT(*) FROM journal_entries")).scalar()
    return int(result) if result else 0


def upsert_plaid_accounts(accounts: list[dict[str, Any]], conn: Connection) -> None:
    """Upsert Plaid accounts into plaid_accounts table."""
    if not accounts:
        return

    # Check if we're using PostgreSQL or SQLite
    dialect = conn.dialect.name

    if dialect == "postgresql":
        # PostgreSQL: Use INSERT ... ON CONFLICT DO UPDATE
        for account in accounts:
            conn.execute(
                text("""
                    INSERT INTO plaid_accounts (
                        plaid_account_id, name, type, subtype, currency
                    )
                    VALUES (:plaid_account_id, :name, :type, :subtype, :currency)
                    ON CONFLICT (plaid_account_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        subtype = EXCLUDED.subtype,
                        currency = EXCLUDED.currency
                """),
                account,
            )
    else:
        # SQLite (for tests): Manual upsert
        for account in accounts:
            # Check if exists
            existing = conn.execute(
                text(
                    "SELECT plaid_account_id FROM plaid_accounts "
                    "WHERE plaid_account_id = :pid"
                ),
                {"pid": account["plaid_account_id"]},
            ).fetchone()

            if existing:
                # Update
                conn.execute(
                    text("""
                        UPDATE plaid_accounts
                        SET name = :name, type = :type, subtype = :subtype,
                            currency = :currency
                        WHERE plaid_account_id = :plaid_account_id
                    """),
                    account,
                )
            else:
                # Insert
                conn.execute(
                    text("""
                        INSERT INTO plaid_accounts (
                            plaid_account_id, name, type, subtype, currency
                        )
                        VALUES (:plaid_account_id, :name, :type, :subtype, :currency)
                    """),
                    account,
                )


def link_plaid_to_account(
    plaid_account_id: str, account_code: str, conn: Connection
) -> None:
    """Link a Plaid account to a GL account via account_links."""
    # Pre-check: Verify Plaid account exists
    plaid_exists = conn.execute(
        text("SELECT 1 FROM plaid_accounts WHERE plaid_account_id = :pid"),
        {"pid": plaid_account_id},
    ).scalar()
    if not plaid_exists:
        msg = f"Plaid account not found: {plaid_account_id}"
        raise ValueError(msg)

    # Resolve GL account code to UUID
    account_id = conn.execute(
        text("SELECT id FROM accounts WHERE code = :code"), {"code": account_code}
    ).scalar()

    if not account_id:
        msg = f"GL account not found: {account_code}"
        raise ValueError(msg)

    # Check if we're using PostgreSQL or SQLite
    dialect = conn.dialect.name

    if dialect == "postgresql":
        # PostgreSQL: Use INSERT ... ON CONFLICT DO UPDATE
        conn.execute(
            text("""
                INSERT INTO account_links (plaid_account_id, account_id)
                VALUES (:plaid_account_id, :account_id)
                ON CONFLICT (plaid_account_id) DO UPDATE SET
                    account_id = EXCLUDED.account_id
            """),
            {"plaid_account_id": plaid_account_id, "account_id": account_id},
        )
    else:
        # SQLite (for tests): Manual upsert
        existing = conn.execute(
            text("SELECT id FROM account_links WHERE plaid_account_id = :pid"),
            {"pid": plaid_account_id},
        ).fetchone()

        if existing:
            # Update
            conn.execute(
                text("""
                    UPDATE account_links
                    SET account_id = :account_id
                    WHERE plaid_account_id = :plaid_account_id
                """),
                {"plaid_account_id": plaid_account_id, "account_id": account_id},
            )
        else:
            # Insert
            conn.execute(
                text("""
                    INSERT INTO account_links (plaid_account_id, account_id)
                    VALUES (:plaid_account_id, :account_id)
                """),
                {"plaid_account_id": plaid_account_id, "account_id": account_id},
            )
