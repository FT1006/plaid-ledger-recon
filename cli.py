#!/usr/bin/env python3

import os
from pathlib import Path
from typing import Annotated

import psycopg
import typer
from dotenv import load_dotenv

from etl.connectors.plaid_client import create_plaid_client_from_env

app = typer.Typer(
    name="pfetl",
    help="Plaid Financial ETL - Audit-ready pipeline: Sandbox → Postgres → Reports",
    no_args_is_help=True,
)


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
        bool, typer.Option("--sandbox", help="Use Plaid sandbox environment")
    ] = False,
    write_env: Annotated[
        bool, typer.Option("--write-env", help="Append credentials to .env file")
    ] = False,
    env_path: Annotated[
        str, typer.Option("--env-path", help="Path to .env file")
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
