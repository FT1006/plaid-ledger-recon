# etl/load.py
"""Loader stubs for RED phase: idempotency, upsert, hashing to be implemented in GREEN."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def load_accounts(accounts: list[dict], conn: "Connection") -> None:
    """Upsert accounts by plaid_account_id (to be implemented in GREEN)."""
    raise NotImplementedError("load_accounts not implemented (RED)")


def load_journal_entries(entries: list[dict], conn: "Connection") -> None:
    """Insert journal_entries and journal_lines; record etl_events (GREEN later)."""
    raise NotImplementedError("load_journal_entries not implemented (RED)")


def get_account_by_plaid_id(plaid_id: str, conn: "Connection") -> dict | None:
    """Fetch account row by plaid_account_id (GREEN later)."""
    raise NotImplementedError("get_account_by_plaid_id not implemented (RED)")


def get_entries_count(conn: "Connection") -> int:
    """Return count(*) from journal_entries (GREEN later)."""
    raise NotImplementedError("get_entries_count not implemented (RED)")
