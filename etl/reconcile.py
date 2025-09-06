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


def check_entry_balance(
    conn: Connection, period: str, item_id: str | None = None
) -> dict[str, Any]:
    """Check that all journal entries have balanced debits and credits.

    Args:
        conn: Database connection
        period: Period to check (e.g., "2024Q1")
        item_id: Optional item ID to scope the check

    Returns:
        Check result with passed status and unbalanced entries list
    """
    from_date, to_date = parse_period(period)

    # Query for unbalanced entries (where debits != credits) within period
    query = text("""
        SELECT je.txn_id,
               SUM(CASE WHEN jl.side = 'debit' THEN jl.amount ELSE 0 END) as debits,
               SUM(CASE WHEN jl.side = 'credit' THEN jl.amount ELSE 0 END) as credits
        FROM journal_entries je
        JOIN journal_lines jl ON je.id = jl.entry_id
        WHERE je.txn_date >= :from_date AND je.txn_date <= :to_date
          AND (:item_id IS NULL OR je.item_id = :item_id)
        GROUP BY je.id, je.txn_id
        HAVING SUM(CASE WHEN jl.side = 'debit' THEN jl.amount ELSE 0 END) !=
               SUM(CASE WHEN jl.side = 'credit' THEN jl.amount ELSE 0 END)
    """)

    unbalanced = conn.execute(
        query, {"from_date": from_date, "to_date": to_date, "item_id": item_id}
    ).fetchall()

    return {
        "passed": len(unbalanced) == 0,
        "unbalanced_entries": [row[0] for row in unbalanced],
    }


def check_cash_variance(
    conn: Connection, period: str, item_id: str | None, plaid_balances: dict[str, float]
) -> dict[str, Any]:
    """Check variance between GL cash accounts and Plaid balances.

    Args:
        conn: Database connection
        period: Period to check (e.g., "2024Q1")
        item_id: Optional item ID to scope the check
        plaid_balances: Dict of plaid_account_id to balance

    Returns:
        Check result with passed status, variance, and tolerance
    """
    _, period_end = parse_period(period)

    # AS-OF GL ending balance per mapped cash account (item-scoped)
    query = text("""
        SELECT al.plaid_account_id,
               COALESCE(SUM(CASE
                   WHEN jl.side = 'debit' THEN jl.amount
                   ELSE -jl.amount
               END), 0) as gl_asof
        FROM account_links al
        JOIN accounts a ON a.id = al.account_id AND a.is_cash = true
        LEFT JOIN journal_lines jl ON jl.account_id = a.id
        LEFT JOIN journal_entries je ON je.id = jl.entry_id
        WHERE (:item_id IS NULL OR je.item_id = :item_id)
          AND (je.txn_date IS NULL OR je.txn_date <= :period_end)
        GROUP BY al.plaid_account_id
    """)

    gl_results = conn.execute(
        query, {"item_id": item_id, "period_end": period_end}
    ).fetchall()

    gl_balances = {row[0]: Decimal(str(row[1])) for row in gl_results}

    # Build by_account breakdown and calculate total variance
    total_variance = Decimal("0.00")
    tolerance = Decimal("0.01")
    by_account = []

    # Compare GL vs external balances for each account in plaid_balances
    for plaid_id, plaid_balance in plaid_balances.items():
        gl_asof = gl_balances.get(plaid_id, Decimal("0.00"))
        ext_asof = Decimal(str(plaid_balance))

        # Calculate account-level variance (absolute difference)
        account_variance = abs(gl_asof - ext_asof)
        total_variance += account_variance

        by_account.append({
            "plaid_account_id": plaid_id,
            "gl_asof": float(gl_asof),
            "ext_asof": float(ext_asof),
            "variance": float(account_variance),
        })

    # Round to 2 decimal places for stable comparison
    total_variance = total_variance.quantize(Decimal("0.01"))

    return {
        "passed": total_variance <= tolerance,
        "total_variance": float(total_variance),
        "tolerance": float(tolerance),
        "by_account": by_account,
    }


def check_lineage_presence(
    conn: Connection, period: str, item_id: str | None = None
) -> dict[str, Any]:
    """Check that all journal entries have source_hash and transform_version.

    Args:
        conn: Database connection
        period: Period to check (e.g., "2024Q1")
        item_id: Optional item ID to scope the check

    Returns:
        Check result with passed status and count of missing lineage
    """
    from_date, to_date = parse_period(period)

    query = text("""
        SELECT COUNT(*)
        FROM journal_entries
        WHERE (source_hash IS NULL
           OR transform_version IS NULL
           OR source_hash = ''
           OR transform_version <= 0)
        AND txn_date >= :from_date AND txn_date <= :to_date
        AND (:item_id IS NULL OR item_id = :item_id)
    """)

    missing_count = (
        conn.execute(
            query, {"from_date": from_date, "to_date": to_date, "item_id": item_id}
        ).scalar()
        or 0
    )

    return {"passed": missing_count == 0, "missing_lineage": missing_count}


def check_coverage(
    conn: Connection, _item_id: str | None, plaid_balances: dict[str, float]
) -> dict[str, Any]:
    """Validate coverage of mapped cash accounts against provided balances.

    Args:
        conn: Database connection
        item_id: Optional item ID (not used for coverage, but for consistency)
        plaid_balances: Dict of plaid_account_id to balance

    Returns:
        Check result with passed status, missing accounts, and extra accounts
    """
    # Get all mapped cash accounts
    query = text("""
        SELECT DISTINCT al.plaid_account_id
        FROM account_links al
        JOIN accounts a ON a.id = al.account_id
        WHERE a.is_cash = true
    """)

    mapped_accounts = {row[0] for row in conn.execute(query).fetchall()}

    provided_accounts = set(plaid_balances.keys())

    # Find missing and extra accounts
    missing = list(mapped_accounts - provided_accounts)
    extra = list(provided_accounts - mapped_accounts)

    return {
        "passed": len(missing) == 0 and len(extra) == 0,
        "missing": missing,
        "extra": extra,
    }


def run_reconciliation(
    conn: Connection,
    *,
    period: str,
    item_id: str | None = None,
    plaid_balances: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Run all reconciliation checks and return consolidated results.

    Args:
        conn: Database connection
        period: Period to reconcile (e.g., "2024Q1")
        item_id: Optional item ID to scope reconciliation
        plaid_balances: Optional dict of plaid_account_id to balance

    Returns:
        Reconciliation result with all check statuses
    """
    if plaid_balances is None:
        plaid_balances = {}

    # Run all checks with item scoping
    coverage = check_coverage(conn, item_id, plaid_balances)
    entry_balance = check_entry_balance(conn, period, item_id)
    cash_variance = check_cash_variance(conn, period, item_id, plaid_balances)
    lineage = check_lineage_presence(conn, period, item_id)

    # Determine overall success
    success = (
        coverage["passed"]
        and entry_balance["passed"]
        and cash_variance["passed"]
        and lineage["passed"]
    )

    # Extract by_account data from cash_variance
    by_account = cash_variance.pop("by_account", [])
    total_variance = cash_variance.get("total_variance", 0.0)

    return {
        "period": period,
        "item_id": item_id,
        "success": success,
        "checks": {
            "coverage": coverage,
            "entry_balance": entry_balance,
            "cash_variance": cash_variance,
            "lineage": lineage,
        },
        "by_account": by_account,
        "total_variance": total_variance,
    }
