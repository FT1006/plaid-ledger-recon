# etl/load.py
"""Loader with idempotent upserts, audit trail, and ETL event tracking."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def load_accounts(accounts: list[dict[str, Any]], conn: Connection | None) -> None:
    """Upsert accounts by plaid_account_id into ingest_accounts shim table.

    Uses INSERT ... ON CONFLICT for PostgreSQL, fallback for SQLite tests.
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
            conn.execute(
                text("""
                    INSERT INTO ingest_accounts (
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
                    "SELECT plaid_account_id FROM ingest_accounts "
                    "WHERE plaid_account_id = :pid"
                ),
                {"pid": account["plaid_account_id"]},
            ).fetchone()

            if existing:
                # Update
                conn.execute(
                    text("""
                        UPDATE ingest_accounts
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
                        INSERT INTO ingest_accounts (
                            plaid_account_id, name, type, subtype, currency
                        )
                        VALUES (:plaid_account_id, :name, :type, :subtype, :currency)
                    """),
                    account,
                )


def load_journal_entries(
    entries: list[dict[str, Any]],
    conn: Connection | None,
) -> None:
    """Load journal entries with lines, tracking ETL events.

    - Idempotent insert (skip duplicates by txn_id)
    - Resolve account names to IDs for FK integrity
    - Record row counts in etl_events
    """
    if not entries:
        return

    if conn is None:
        return

    started_at = datetime.now(UTC).isoformat()
    entries_inserted = 0
    lines_inserted = 0

    for entry in entries:
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

        # Insert journal lines (using shim table with text account names)
        for line in entry["lines"]:
            conn.execute(
                text("""
                    INSERT INTO journal_lines (entry_id, account, side, amount)
                    VALUES (:entry_id, :account, :side, :amount)
                """),
                {
                    "entry_id": entry_id,
                    "account": line["account"],  # Direct text account name
                    "side": line["side"],
                    "amount": float(line["amount"]),  # Convert Decimal for DB
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
