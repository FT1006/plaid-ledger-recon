#!/usr/bin/env python3
"""CLI interface for Plaid Financial ETL pipeline."""

import json
import os
from datetime import date
from pathlib import Path
from typing import Annotated

import psycopg
import typer
from dotenv import load_dotenv
from sqlalchemy import create_engine

from etl.connectors.plaid_client import create_plaid_client_from_env
from etl.extract import fetch_accounts, sync_transactions
from etl.load import load_accounts, load_journal_entries
from etl.reconcile import run_reconciliation
from etl.reports.render import render_balance_sheet, render_cash_flow, write_pdf
from etl.transform import map_plaid_to_journal

app = typer.Typer(
    name="pfetl",
    help="Plaid Financial ETL - Audit-ready pipeline: Sandbox → Postgres → Reports",
    no_args_is_help=True,
)


@app.callback()
def _load_env() -> None:
    load_dotenv()


def _parse_date(value: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        typer.echo(f"❌ Invalid date format: {value}. Use YYYY-MM-DD", err=True)
        raise typer.Exit(1) from None


@app.command("init-db")
def init_db() -> None:
    """Initialize database schema from etl/schema.sql."""
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo("❌ DATABASE_URL not found in environment", err=True)
        raise typer.Exit(2)

    schema_path = Path(__file__).parent / "etl" / "schema.sql"
    if not schema_path.exists():
        typer.echo(f"❌ Schema file not found: {schema_path}", err=True)
        raise typer.Exit(2)

    try:
        with psycopg.connect(database_url) as conn, conn.cursor() as cur:
            schema_sql = schema_path.read_text()
            cur.execute(schema_sql)
            conn.commit()

        typer.echo("✅ Database schema initialized successfully")
    except psycopg.Error as e:
        typer.echo(f"❌ Database error: {e}", err=True)
        raise typer.Exit(1) from e
    except Exception as e:
        typer.echo(f"❌ Unexpected error: {e}", err=True)
        raise typer.Exit(1) from e


@app.command("onboard")
def onboard(
    sandbox: Annotated[
        bool,
        typer.Option("--sandbox", help="Use Plaid sandbox environment"),
    ] = False,
    write_env: Annotated[
        bool,
        typer.Option("--write-env", help="Append credentials to .env file"),
    ] = False,
    env_path: Annotated[
        str,
        typer.Option("--env-path", help="Path to .env file"),
    ] = ".env",
) -> None:
    """Onboard a Plaid item and obtain access token."""
    try:
        with create_plaid_client_from_env() as client:
            if not (sandbox and client.base_url.endswith("sandbox.plaid.com")):
                typer.echo("Non-sandbox onboard not supported in MVP", err=True)
                raise typer.Exit(code=1)  # noqa: TRY301

            public_token = client.create_sandbox_public_token()
            access_token, item_id = client.exchange_public_token(public_token)

            typer.echo(item_id)
            if write_env:
                # append/dedupe
                lines = {}
                env_file = Path(env_path)
                try:
                    with env_file.open() as f:
                        for line in f:
                            if "=" in line:
                                k, v = line.strip().split("=", 1)
                                lines[k] = v
                except FileNotFoundError:
                    pass
                lines["PLAID_ACCESS_TOKEN"] = access_token
                lines["PLAID_ITEM_ID"] = item_id
                with env_file.open("w") as f:
                    for k, v in lines.items():
                        f.write(f"{k}={v}\n")
    except Exception as e:
        typer.echo(f"onboard failed: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command("ingest")
def ingest(
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid item ID")],
    from_date: Annotated[str, typer.Option("--from", help="Start date (YYYY-MM-DD)")],
    to_date: Annotated[str, typer.Option("--to", help="End date (YYYY-MM-DD)")],
) -> None:
    """Ingest transactions from Plaid for the specified date range."""
    # Validate dates
    start = _parse_date(from_date)
    end = _parse_date(to_date)
    if start > end:
        typer.echo("❌ Invalid date range: --from must be <= --to", err=True)
        raise typer.Exit(1)

    # Check environment
    load_dotenv()
    access_token = os.getenv("PLAID_ACCESS_TOKEN")
    if not access_token:
        typer.echo("❌ PLAID_ACCESS_TOKEN not set in environment", err=True)
        raise typer.Exit(1)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo("❌ DATABASE_URL not found in environment", err=True)
        raise typer.Exit(2)

    try:
        # Extract
        txns = list(sync_transactions(access_token, start.isoformat(), end.isoformat()))
        accts = fetch_accounts(access_token)

        if not txns:
            typer.echo("No transactions to ingest (0 transactions).")
            return

        # Transform
        acct_map = {
            a["account_id"]: {
                "type": a["type"],
                "subtype": a["subtype"],
                "currency": a.get("iso_currency_code") or "USD",
                "name": a["name"],
            }
            for a in accts
        }
        entries = map_plaid_to_journal(txns, acct_map)

        # Load
        load_accts = [
            {
                "plaid_account_id": a["account_id"],
                "name": a["name"],
                "type": a["type"],
                "subtype": a["subtype"],
                "currency": a.get("iso_currency_code") or "USD",
            }
            for a in accts
        ]

        # Connect to database and load data
        engine = create_engine(database_url)
        with engine.begin() as conn:
            load_accounts(load_accts, conn)
            load_journal_entries(entries, conn)

        typer.echo(f"✅ Ingested {len(txns)} transactions.")

    except Exception as e:
        typer.echo(f"❌ Error during ingest: {e}", err=True)
        raise typer.Exit(1) from e


@app.command("reconcile")
def reconcile(
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid item ID")],
    period: Annotated[str, typer.Option("--period", help="Period (e.g., 2024Q1)")],
    out: Annotated[str, typer.Option("--out", help="Output file for recon.json")],
) -> None:
    """Run reconciliation checks and generate recon.json."""
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo("❌ DATABASE_URL not found in environment", err=True)
        raise typer.Exit(2)

    access_token = os.getenv("PLAID_ACCESS_TOKEN")
    if not access_token:
        typer.echo("❌ PLAID_ACCESS_TOKEN not set in environment", err=True)
        raise typer.Exit(1)

    def _handle_success() -> None:
        typer.echo(f"✅ Reconciliation passed for {period}")
        typer.echo(f"📄 Results written to {out}")
        raise typer.Exit(0)

    def _handle_failure() -> None:
        typer.echo(f"❌ Reconciliation failed for {period}", err=True)
        typer.echo(f"📄 Details written to {out}", err=True)
        raise typer.Exit(1)

    try:
        # Fetch current Plaid balances for comparison
        accounts = fetch_accounts(access_token)
        plaid_balances = {
            a["account_id"]: a.get("balances", {}).get("current", 0.0) for a in accounts
        }

        # Connect to database and run reconciliation
        engine = create_engine(database_url)
        with engine.begin() as conn:
            result = run_reconciliation(conn, period, plaid_balances)

        # Write result to output file
        out_path = Path(out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w") as f:
            json.dump(result, f, indent=2, default=str)

        if result["success"]:
            _handle_success()
        _handle_failure()

    except Exception as e:
        typer.echo(f"❌ Error during reconciliation: {e}", err=True)
        raise typer.Exit(1) from e


def _validate_report_formats(formats: str) -> list[str]:
    """Validate and parse report formats."""
    requested_formats = [f.strip().lower() for f in formats.split(",")]
    if not all(f in ["html", "pdf"] for f in requested_formats):
        typer.echo("❌ Invalid format. Use: html,pdf or html or pdf", err=True)
        raise typer.Exit(1)
    return requested_formats


@app.command("report")
def report(
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid item ID")],
    period: Annotated[str, typer.Option("--period", help="Period (e.g., 2024Q1)")],
    formats: Annotated[
        str,
        typer.Option("--formats", help="Comma-separated formats (html,pdf)"),
    ] = "html,pdf",
    out: Annotated[str, typer.Option("--out", help="Output directory")] = "./build",
) -> None:
    """Generate Balance Sheet and Cash Flow reports."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo("❌ DATABASE_URL not found in environment", err=True)
        raise typer.Exit(2)

    try:
        # Parse requested formats
        requested_formats = _validate_report_formats(formats)

        # Create output directory
        out_path = Path(out)
        out_path.mkdir(parents=True, exist_ok=True)

        # Connect to database and generate reports
        engine = create_engine(database_url)

        # Generate Balance Sheet
        bs_html = render_balance_sheet(period, engine)
        if "html" in requested_formats:
            bs_html_path = out_path / f"balance_sheet_{period.lower()}.html"
            bs_html_path.write_text(bs_html)
            typer.echo(f"✅ Generated: {bs_html_path}")

        if "pdf" in requested_formats:
            bs_pdf_path = out_path / f"balance_sheet_{period.lower()}.pdf"
            write_pdf(bs_html, bs_pdf_path)
            typer.echo(f"✅ Generated: {bs_pdf_path}")

        # Generate Cash Flow
        cf_html = render_cash_flow(period, engine)
        if "html" in requested_formats:
            cf_html_path = out_path / f"cash_flow_{period.lower()}.html"
            cf_html_path.write_text(cf_html)
            typer.echo(f"✅ Generated: {cf_html_path}")

        if "pdf" in requested_formats:
            cf_pdf_path = out_path / f"cash_flow_{period.lower()}.pdf"
            write_pdf(cf_html, cf_pdf_path)
            typer.echo(f"✅ Generated: {cf_pdf_path}")

        typer.echo(f"🎉 Reports generated for {period} in {out_path}")

    except Exception as e:
        typer.echo(f"❌ Error generating reports: {e}", err=True)
        raise typer.Exit(1) from e


if __name__ == "__main__":
    app()
