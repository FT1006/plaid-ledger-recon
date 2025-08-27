"""Reconciliation checks for audit-ready financial data integrity."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def parse_period(period: str) -> tuple[str, str]:
    """Parse period string to date range.

    Args:
        period: Period like "2024Q1"

    Returns:
        Tuple of (start_date, end_date) as ISO strings
    """
    if period.endswith("Q1"):
        year = period[:4]
        return f"{year}-01-01", f"{year}-03-31"
    if period.endswith("Q2"):
        year = period[:4]
        return f"{year}-04-01", f"{year}-06-30"
    if period.endswith("Q3"):
        year = period[:4]
        return f"{year}-07-01", f"{year}-09-30"
    if period.endswith("Q4"):
        year = period[:4]
        return f"{year}-10-01", f"{year}-12-31"
    msg = f"Unsupported period format: {period}"
    raise ValueError(msg)


def check_entry_balance(conn: Connection, _period: str) -> dict[str, Any]:
    """Check that all journal entries have balanced debits and credits.

    Args:
        conn: Database connection
        period: Period to check (e.g., "2024Q1")

    Returns:
        Check result with passed status and unbalanced entries list
    """
    # TODO(M4): https://github.com/user/repo/issues/M4 Filter by period using
    # journal_entries.txn_date once from/to params added
    # Query for unbalanced entries (where debits != credits)
    query = text("""
        SELECT je.txn_id,
               SUM(CASE WHEN jl.side = 'debit' THEN jl.amount ELSE 0 END) as debits,
               SUM(CASE WHEN jl.side = 'credit' THEN jl.amount ELSE 0 END) as credits
        FROM journal_entries je
        JOIN journal_lines jl ON je.id = jl.entry_id
        GROUP BY je.id, je.txn_id
        HAVING SUM(CASE WHEN jl.side = 'debit' THEN jl.amount ELSE 0 END) !=
               SUM(CASE WHEN jl.side = 'credit' THEN jl.amount ELSE 0 END)
    """)

    unbalanced = conn.execute(query).fetchall()

    return {
        "passed": len(unbalanced) == 0,
        "unbalanced_entries": [row[0] for row in unbalanced],
    }


def check_cash_variance(
    conn: Connection, _period: str, plaid_balances: dict[str, float]
) -> dict[str, Any]:
    """Check variance between GL cash accounts and Plaid balances.

    Args:
        conn: Database connection
        period: Period to check (e.g., "2024Q1")
        plaid_balances: Dict of plaid_account_id to balance

    Returns:
        Check result with passed status, variance, and tolerance
    """
    # TODO(M4): https://github.com/user/repo/issues/M4 Filter by period using
    # journal_entries.txn_date once from/to params added
    # Calculate total GL cash balance (sum of all cash account movements)
    query = text("""
        SELECT al.plaid_account_id,
               SUM(CASE
                   WHEN jl.side = 'debit' THEN jl.amount
                   ELSE -jl.amount
               END) as gl_balance
        FROM journal_lines jl
        JOIN accounts a ON jl.account_id = a.id
        JOIN account_links al ON a.id = al.account_id
        WHERE a.is_cash = 1 OR a.is_cash = true
        GROUP BY al.plaid_account_id
    """)

    gl_balances = {
        row[0]: Decimal(str(row[1])) for row in conn.execute(query).fetchall()
    }

    # Calculate total variance using absolute values and Decimal precision
    total_variance = Decimal("0.00")
    tolerance = Decimal("0.01")

    # Compare each mapped account (MVP: use magnitude comparison,
    # assumes zero opening balance)
    for plaid_id, plaid_balance in plaid_balances.items():
        gl_balance = gl_balances.get(plaid_id, Decimal("0.00"))
        plaid_decimal = Decimal(str(plaid_balance))

        # Use absolute values for magnitude comparison
        variance = abs(abs(gl_balance) - abs(plaid_decimal))
        total_variance += variance

    # Round to 2 decimal places for stable comparison
    total_variance = total_variance.quantize(Decimal("0.01"))

    return {
        "passed": total_variance <= tolerance,
        "variance": float(total_variance),
        "tolerance": float(tolerance),
    }


def check_lineage_presence(conn: Connection, _period: str) -> dict[str, Any]:
    """Check that all journal entries have source_hash and transform_version.

    Args:
        conn: Database connection
        period: Period to check (e.g., "2024Q1")

    Returns:
        Check result with passed status and count of missing lineage
    """
    # TODO(M4): https://github.com/user/repo/issues/M4 Filter by period using
    # journal_entries.txn_date once from/to params added
    query = text("""
        SELECT COUNT(*)
        FROM journal_entries
        WHERE source_hash IS NULL
           OR transform_version IS NULL
           OR source_hash = ''
           OR transform_version <= 0
    """)

    missing_count = conn.execute(query).scalar() or 0

    return {"passed": missing_count == 0, "missing_lineage": missing_count}


def run_reconciliation(
    conn: Connection, period: str, plaid_balances: dict[str, float] | None = None
) -> dict[str, Any]:
    """Run all reconciliation checks and return consolidated results.

    Args:
        conn: Database connection
        period: Period to reconcile (e.g., "2024Q1")
        plaid_balances: Optional dict of plaid_account_id to balance

    Returns:
        Reconciliation result with all check statuses
    """
    if plaid_balances is None:
        plaid_balances = {}

    # Run all checks
    entry_balance = check_entry_balance(conn, period)
    cash_variance = check_cash_variance(conn, period, plaid_balances)
    lineage = check_lineage_presence(conn, period)

    # Determine overall success
    success = entry_balance["passed"] and cash_variance["passed"] and lineage["passed"]

    return {
        "period": period,
        "success": success,
        "checks": {
            "entry_balance": entry_balance,
            "cash_variance": cash_variance,
            "lineage": lineage,
        },
    }
