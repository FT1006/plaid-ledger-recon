"""Deterministic HTML/PDF report rendering with Jinja2 templates."""

from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import text

from etl.reconcile import parse_period

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def _get_template_env() -> Environment:
    """Get Jinja2 environment with deterministic settings."""
    template_dir = Path(__file__).parent / "templates"
    return Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )


def _format_amount(amount: Decimal | float | int) -> str:
    """Format amount to exactly 2 decimal places for deterministic output."""
    if isinstance(amount, float | int):
        amount = Decimal(str(amount))
    return f"{amount.quantize(Decimal('0.01')):.2f}"


def render_balance_sheet(period: str, engine: "Engine") -> str:
    """Generate deterministic HTML for balance sheet.

    Args:
        period: Period string (e.g., "2024Q1")
        engine: Database engine

    Returns:
        HTML string with deterministic formatting
    """
    start_date, end_date = parse_period(period)

    # Query for account balances with deterministic ordering
    query = text("""
        SELECT
            a.code,
            a.name,
            a.type,
            COALESCE(SUM(
                CASE
                    WHEN jl.side = 'debit' AND a.type IN ('asset', 'expense')
                        THEN jl.amount
                    WHEN jl.side = 'credit' AND a.type IN (
                        'liability', 'equity', 'revenue'
                    ) THEN jl.amount
                    WHEN jl.side = 'debit' AND a.type IN (
                        'liability', 'equity', 'revenue'
                    ) THEN -jl.amount
                    WHEN jl.side = 'credit' AND a.type IN ('asset', 'expense')
                        THEN -jl.amount
                    ELSE 0
                END
            ), 0) as balance
        FROM accounts a
        LEFT JOIN journal_lines jl ON a.id = jl.account_id
        LEFT JOIN journal_entries je ON jl.entry_id = je.id
        WHERE je.txn_date IS NULL OR (
            je.txn_date >= :start_date AND je.txn_date <= :end_date
        )
        GROUP BY a.id, a.code, a.name, a.type
        ORDER BY a.type, a.code
    """)

    with engine.begin() as conn:
        rows = conn.execute(
            query, {"start_date": start_date, "end_date": end_date}
        ).fetchall()

    # Group accounts by type with deterministic ordering
    assets = []
    liabilities = []
    equity = []

    for row in rows:
        account_data = {
            "code": row.code,
            "name": row.name,
            "balance": _format_amount(row.balance),
        }

        if row.type == "asset":
            assets.append(account_data)
        elif row.type == "liability":
            liabilities.append(account_data)
        elif row.type == "equity":
            equity.append(account_data)

    # Calculate totals with fixed precision
    total_assets = sum(Decimal(acc["balance"]) for acc in assets)
    total_liabilities = sum(Decimal(acc["balance"]) for acc in liabilities)
    total_equity = sum(Decimal(acc["balance"]) for acc in equity)
    total_liabilities_equity = total_liabilities + total_equity

    # Render template with deterministic data
    env = _get_template_env()
    template = env.get_template("balance_sheet.html.j2")

    return template.render(
        period=period,
        assets=assets,
        liabilities=liabilities,
        equity=equity,
        total_assets=_format_amount(total_assets),
        total_liabilities=_format_amount(total_liabilities),
        total_equity=_format_amount(total_equity),
        total_liabilities_equity=_format_amount(total_liabilities_equity),
    )


def render_cash_flow(period: str, engine: "Engine") -> str:
    """Generate deterministic HTML for cash flow statement.

    Args:
        period: Period string (e.g., "2024Q1")
        engine: Database engine

    Returns:
        HTML string with deterministic formatting
    """
    start_date, end_date = parse_period(period)

    # Query for cash flow data with deterministic ordering
    # For now, classify all non-cash transactions as operating activities
    query = text("""
        SELECT
            je.txn_date,
            je.txn_id,
            je.description,
            jl.side,
            jl.amount,
            a.code as account_code,
            a.name as account_name,
            a.type as account_type,
            a.is_cash
        FROM journal_entries je
        JOIN journal_lines jl ON je.id = jl.entry_id
        JOIN accounts a ON jl.account_id = a.id
        WHERE je.txn_date >= :start_date AND je.txn_date <= :end_date
        ORDER BY je.txn_date, je.txn_id, a.code, jl.side
    """)

    with engine.begin() as conn:
        rows = conn.execute(
            query, {"start_date": start_date, "end_date": end_date}
        ).fetchall()

    # Process transactions to calculate cash flows
    operating_activities: list[dict[str, str]] = []
    investing_activities: list[dict[str, str]] = []  # Empty for now
    financing_activities: list[dict[str, str]] = []  # Empty for now

    # Group by transaction and calculate net cash impact
    transactions: dict[str, dict[str, Any]] = {}

    for row in rows:
        txn_id = row.txn_id
        if txn_id not in transactions:
            transactions[txn_id] = {
                "description": row.description,
                "txn_date": row.txn_date,
                "cash_impact": Decimal(0),
                "non_cash_accounts": [],
            }

        # Track cash impact (debit increases cash, credit decreases cash)
        if row.is_cash:
            if row.side == "debit":
                transactions[txn_id]["cash_impact"] += Decimal(str(row.amount))
            else:  # credit
                transactions[txn_id]["cash_impact"] -= Decimal(str(row.amount))
        else:
            transactions[txn_id]["non_cash_accounts"].append(row.account_name)

    # Convert to operating activities (simplified classification)
    for _txn_id, txn_data in sorted(transactions.items()):
        if txn_data["cash_impact"] != 0:
            operating_activities.append({
                "description": txn_data["description"],
                "amount": _format_amount(txn_data["cash_impact"]),
            })

    # Calculate net cash flows
    net_operating = sum(Decimal(item["amount"]) for item in operating_activities)
    net_investing = Decimal(0)  # No investing activities yet
    net_financing = Decimal(0)  # No financing activities yet
    net_cash_change = net_operating + net_investing + net_financing

    # For now, assume zero beginning cash (TODO: calculate from previous periods)
    cash_beginning = Decimal(0)
    cash_ending = cash_beginning + net_cash_change

    # Render template with deterministic data
    env = _get_template_env()
    template = env.get_template("cash_flow.html.j2")

    return template.render(
        period=period,
        operating_activities=operating_activities,
        investing_activities=investing_activities,
        financing_activities=financing_activities,
        net_operating=_format_amount(net_operating),
        net_investing=_format_amount(net_investing),
        net_financing=_format_amount(net_financing),
        net_cash_change=_format_amount(net_cash_change),
        cash_beginning=_format_amount(cash_beginning),
        cash_ending=_format_amount(cash_ending),
    )


def write_pdf(html: str, out_path: Path) -> Path:
    """Convert HTML to PDF using WeasyPrint.

    Args:
        html: HTML string to convert
        out_path: Output PDF path

    Returns:
        Path to created PDF file
    """
    from weasyprint import HTML  # noqa: PLC0415

    # Ensure parent directory exists
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert HTML to PDF using WeasyPrint
    html_doc = HTML(string=html)
    html_doc.write_pdf(out_path)

    return out_path
