"""Test configuration and fixtures."""

import sqlite3
from decimal import Decimal

from sqlalchemy import Connection, text

# Register Decimal adapter for SQLite tests (exact decimal text, no float rounding)
sqlite3.register_adapter(Decimal, str)


def seed_account(
    conn: Connection,
    code: str,
    type_: str = "asset",
    is_cash: int = 0,
    id_: str | None = None,
) -> str:
    """Add a single account to the test database."""
    if id_ is None:
        id_ = code.replace(":", "_").lower()
    name = code.split(":")[-1] or code

    conn.execute(
        text("""
        INSERT INTO accounts (id, code, name, type, is_cash)
        VALUES (:id, :code, :name, :type, :is_cash)
        """),
        {"id": id_, "code": code, "name": name, "type": type_, "is_cash": is_cash},
    )
    return id_
