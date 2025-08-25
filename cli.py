#!/usr/bin/env python3

from typing import Annotated

import typer

app = typer.Typer(
    name="pfetl",
    help="Plaid Financial ETL - Audit-ready pipeline: Sandbox → Postgres → Reports",
    no_args_is_help=True,
)


@app.command("init-db")
def init_db() -> None:
    """Initialize database schema from etl/schema.sql."""
    typer.echo("🚧 init-db: Not yet implemented")
    raise typer.Exit(1)


@app.command("onboard")
def onboard(
    sandbox: Annotated[
        bool, typer.Option("--sandbox", help="Use Plaid sandbox environment")
    ] = False,
    write_env: Annotated[
        bool, typer.Option("--write-env", help="Append credentials to .env file")
    ] = False,
) -> None:
    """Onboard a Plaid item and obtain access token."""
    if not sandbox:
        typer.echo("❌ Only sandbox mode is supported in MVP", err=True)
        raise typer.Exit(2)

    typer.echo("🚧 onboard: Not yet implemented")
    raise typer.Exit(1)


@app.command("ingest")
def ingest(
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid item ID")],
    from_date: Annotated[str, typer.Option("--from", help="Start date (YYYY-MM-DD)")],
    to_date: Annotated[str, typer.Option("--to", help="End date (YYYY-MM-DD)")],
) -> None:
    """Ingest transactions from Plaid for the specified date range."""
    typer.echo("🚧 ingest: Not yet implemented")
    raise typer.Exit(1)


@app.command("reconcile")
def reconcile(
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid item ID")],
    period: Annotated[str, typer.Option("--period", help="Period (e.g., 2024Q1)")],
    out: Annotated[str, typer.Option("--out", help="Output file for recon.json")],
) -> None:
    """Run reconciliation checks and generate recon.json."""
    typer.echo("🚧 reconcile: Not yet implemented")
    raise typer.Exit(1)


@app.command("report")
def report(
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid item ID")],
    period: Annotated[str, typer.Option("--period", help="Period (e.g., 2024Q1)")],
    formats: Annotated[
        str, typer.Option("--formats", help="Comma-separated formats (html,pdf)")
    ] = "html,pdf",
    out: Annotated[str, typer.Option("--out", help="Output directory")] = "./build",
) -> None:
    """Generate Balance Sheet and Cash Flow reports."""
    typer.echo("🚧 report: Not yet implemented")
    raise typer.Exit(1)


if __name__ == "__main__":
    app()
