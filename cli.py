#!/usr/bin/env python3
"""CLI interface for Plaid Financial ETL pipeline."""

import importlib.util
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any

import psycopg
import typer
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from etl.connectors.plaid_client import create_plaid_client_from_env
from etl.demo import create_demo_engine, get_demo_balances, load_demo_fixtures
from etl.extract import fetch_accounts, sync_transactions
from etl.load import (
    link_plaid_to_account,
    load_accounts,
    load_journal_entries,
    upsert_plaid_accounts,
)
from etl.reconcile import run_reconciliation
from etl.reports.render import render_balance_sheet, render_cash_flow, write_pdf
from etl.transform import map_plaid_to_journal

app = typer.Typer(
    name="pfetl",
    help="Plaid Financial ETL - Audit-ready pipeline: Sandbox → Postgres → Reports",
    no_args_is_help=True,
)


def _mark_success() -> str:
    """Return success indicator (emoji or plain text based on PFETL_PLAIN env var)."""
    return "" if os.getenv("PFETL_PLAIN") == "1" else "✅"


def _mark_error() -> str:
    """Return error indicator (emoji or plain text based on PFETL_PLAIN env var)."""
    return "" if os.getenv("PFETL_PLAIN") == "1" else "❌"


@app.callback()
def _load_env() -> None:
    # Skip dotenv loading in tests/CI for hermetic environments
    if os.getenv("PFETL_SKIP_DOTENV") != "1":
        load_dotenv(override=False)  # Never override already-set env in CI/tests


def _parse_date(value: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        typer.echo(
            f"{_mark_error()} Invalid date format: {value}. Use YYYY-MM-DD", err=True
        )
        raise typer.Exit(1) from None


@app.command("init-db")
def init_db() -> None:
    """Initialize database schema from etl/schema.sql."""
    _load_env()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo(
            "Error: DATABASE_URL not set. Please set it via environment or .env file.",
            err=True,
        )
        raise typer.Exit(2)

    schema_path = Path(__file__).parent / "etl" / "schema.sql"
    if not schema_path.exists():
        typer.echo(f"{_mark_error()} Schema file not found: {schema_path}", err=True)
        raise typer.Exit(2)

    try:
        with psycopg.connect(database_url) as conn, conn.cursor() as cur:
            schema_sql = schema_path.read_text()
            cur.execute(schema_sql)
            conn.commit()

        typer.echo(f"{_mark_success()} Database schema initialized successfully")
    except psycopg.Error as e:
        typer.echo(f"{_mark_error()} Database error: {e}", err=True)
        raise typer.Exit(1) from e
    except Exception as e:
        typer.echo(f"{_mark_error()} Unexpected error: {e}", err=True)
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
        typer.echo(
            f"{_mark_error()} Invalid date range: --from must be <= --to", err=True
        )
        raise typer.Exit(1)

    # Check environment
    _load_env()
    access_token = os.getenv("PLAID_ACCESS_TOKEN")
    if not access_token:
        typer.echo(
            f"{_mark_error()} PLAID_ACCESS_TOKEN not set in environment", err=True
        )
        raise typer.Exit(1)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo(f"{_mark_error()} DATABASE_URL not found in environment", err=True)
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

        plaid_accts = [
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
            upsert_plaid_accounts(plaid_accts, conn)  # New canonical table
            load_accounts(
                load_accts, conn, item_id=item_id
            )  # Legacy shim (kept for now)
            load_journal_entries(entries, conn)

        typer.echo(f"{_mark_success()} Ingested {len(txns)} transactions.")

    except Exception as e:
        typer.echo(f"{_mark_error()} Error during ingest: {e}", err=True)
        raise typer.Exit(1) from e


def _load_balances_from_json(balances_json: str) -> dict[str, float]:
    """Load balances from JSON file with validation."""
    try:
        data = json.loads(Path(balances_json).read_text())
    except Exception as e:
        typer.echo(f"{_mark_error()} Failed to read --balances-json: {e}", err=True)
        raise typer.Exit(1) from e

    if not isinstance(data, dict):
        typer.echo(
            f"{_mark_error()} --balances-json must be JSON object "
            "{{plaid_account_id: balance}}",
            err=True,
        )
        raise typer.Exit(1)
    return {str(k): float(v) for k, v in data.items()}


def _load_live_plaid_balances(access_token: str | None) -> dict[str, float]:
    """Load live balances from Plaid API."""
    if not access_token:
        typer.echo(
            f"{_mark_error()} PLAID_ACCESS_TOKEN not set in environment", err=True
        )
        raise typer.Exit(1)
    accounts = fetch_accounts(access_token)
    return {
        a["account_id"]: a.get("balances", {}).get("current", 0.0) for a in accounts
    }


def _determine_balances(
    balances_json: str | None, use_plaid_live: bool, access_token: str | None
) -> dict[str, float]:
    """Determine balances source: override file (demo/CI) or live Plaid."""
    if balances_json:
        return _load_balances_from_json(balances_json)
    if use_plaid_live:
        return _load_live_plaid_balances(access_token)
    msg = "No balance source specified"  # Should never reach here due to validation
    raise RuntimeError(msg)


def _run_reconciliation_with_db(
    database_url: str,
    period: str,
    item_id: str,
    plaid_balances: dict[str, float],
    out: str,
) -> dict[str, Any]:
    """Connect to database, run reconciliation and write results."""
    engine = create_engine(database_url)
    with engine.begin() as conn:
        result = run_reconciliation(
            conn, period=period, item_id=item_id, plaid_balances=plaid_balances
        )

        # Write result to output file
        out_path = Path(out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w") as f:
            json.dump(result, f, indent=2, default=str)

    return result


def _log_etl_event(
    database_url: str,
    event_data: dict[str, str | dict[str, Any] | None],
    timestamps: dict[str, str],
) -> None:
    """Log ETL event for audit trail."""
    period = event_data["period"]
    item_id = event_data["item_id"]
    result = event_data["result"]

    if result is not None and isinstance(result, dict):
        # Success case - include reconciliation results
        row_counts = json.dumps({
            "period": period,
            "item_id": item_id,
            "checks": result.get("checks", {}),
            "accounts": len(result.get("by_account", [])),
            "total_variance": result.get("total_variance", 0.0),
        })
        success = bool(result.get("success"))
    else:
        # Exception case - minimal error info
        row_counts = json.dumps({
            "period": period,
            "item_id": item_id,
            "error": "Exception during reconciliation",
        })
        success = False

    engine = create_engine(database_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_events (
                    event_type, item_id, period, row_counts,
                    started_at, finished_at, success
                )
                VALUES (
                    :event_type, :item_id, :period, :row_counts,
                    :started_at, :finished_at, :success
                )
                """
            ),
            {
                "event_type": "reconcile",
                "item_id": item_id,
                "period": period,
                "row_counts": row_counts,
                "started_at": timestamps["started_at"],
                "finished_at": timestamps["finished_at"],
                "success": success,
            },
        )


@app.command("reconcile")
def reconcile(
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid item ID")],
    period: Annotated[str, typer.Option("--period", help="Period (e.g., 2024Q1)")],
    out: Annotated[str, typer.Option("--out", help="Output file for recon.json")],
    balances_json: Annotated[
        str | None,
        typer.Option(
            "--balances-json",
            help=(
                "Path to JSON file with balances {plaid_account_id: balance} "
                "to override Plaid live balances (for demos/CI)"
            ),
        ),
    ] = None,
    use_plaid_live: Annotated[
        bool,
        typer.Option(
            "--use-plaid-live",
            help="Use live Plaid API balances (production mode)",
        ),
    ] = False,
) -> None:
    """Run reconciliation checks and generate recon.json."""
    _load_env()

    # Validate one-of rule: exactly one balance source required
    has_json = balances_json is not None
    has_live = use_plaid_live

    if not has_json and not has_live:
        typer.echo("Usage: pfetl reconcile [OPTIONS]", err=True)
        typer.echo(
            "Provide exactly one of --balances-json or --use-plaid-live.", err=True
        )
        raise typer.Exit(2)

    if has_json and has_live:
        typer.echo("Usage: pfetl reconcile [OPTIONS]", err=True)
        typer.echo(
            "Provide exactly one of --balances-json or --use-plaid-live.", err=True
        )
        raise typer.Exit(2)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo(f"{_mark_error()} DATABASE_URL not found in environment", err=True)
        raise typer.Exit(2)

    access_token = os.getenv("PLAID_ACCESS_TOKEN")

    def _handle_success() -> None:
        typer.echo(f"{_mark_success()} Reconciliation passed for {period}")
        typer.echo(f"Results written to {out}")
        raise typer.Exit(0)

    def _handle_failure(result: dict[str, Any]) -> None:
        typer.echo(f"{_mark_error()} Reconciliation failed for {period}", err=True)

        # Provide specific failure details for common issues
        if result and "checks" in result:
            checks = result["checks"]
            if not checks.get("coverage", {}).get("passed", True):
                coverage = checks["coverage"]
                if coverage.get("missing"):
                    missing_accounts = ", ".join(coverage["missing"])
                    typer.echo(
                        f"Missing balance data for accounts: {missing_accounts}",
                        err=True,
                    )

        typer.echo(f"Details written to {out}", err=True)
        raise typer.Exit(1)

    started_at = datetime.now(UTC).isoformat()
    result = None

    try:
        # Determine balances source and run reconciliation
        plaid_balances = _determine_balances(
            balances_json, use_plaid_live, access_token
        )
        result = _run_reconciliation_with_db(
            database_url, period, item_id, plaid_balances, out
        )

        if result["success"]:
            _handle_success()
        _handle_failure(result)

    except typer.Exit:
        # Propagate intended exit codes (0 or 1) without wrapping
        raise
    except Exception as e:
        typer.echo(f"{_mark_error()} Error during reconciliation: {e}", err=True)
        raise typer.Exit(1) from e
    finally:
        # Always record ETL event (success or failure) for audit trail
        try:
            finished_at = datetime.now(UTC).isoformat()
            _log_etl_event(
                database_url,
                {"period": period, "item_id": item_id, "result": result},
                {"started_at": started_at, "finished_at": finished_at},
            )
        except Exception as log_error:
            # Do not fail the reconcile command if event logging fails
            typer.echo(f"WARNING: ETL event logging failed: {log_error}", err=True)


def _validate_report_formats(formats: str) -> list[str]:
    """Validate and parse report formats."""
    requested_formats = [f.strip().lower() for f in formats.split(",")]
    if not all(f in ["html", "pdf"] for f in requested_formats):
        typer.echo(
            f"{_mark_error()} Invalid format. Use: html,pdf or html or pdf", err=True
        )
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
        typer.echo(f"{_mark_error()} DATABASE_URL not found in environment", err=True)
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
            bs_html_path = out_path / f"bs_{period}.html"
            bs_html_path.write_text(bs_html)
            typer.echo(f"{_mark_success()} Generated: {bs_html_path}")

        if "pdf" in requested_formats:
            try:
                bs_pdf_path = out_path / f"bs_{period}.pdf"
                write_pdf(bs_html, bs_pdf_path)
                typer.echo(f"{_mark_success()} Generated: {bs_pdf_path}")
            except Exception as e:
                typer.echo(f"WARNING:  PDF generation not available: {e}")
                typer.echo("   (HTML report was generated successfully)")

        # Generate Cash Flow
        cf_html = render_cash_flow(period, engine)
        if "html" in requested_formats:
            cf_html_path = out_path / f"cf_{period}.html"
            cf_html_path.write_text(cf_html)
            typer.echo(f"{_mark_success()} Generated: {cf_html_path}")

        if "pdf" in requested_formats:
            try:
                cf_pdf_path = out_path / f"cf_{period}.pdf"
                write_pdf(cf_html, cf_pdf_path)
                typer.echo(f"{_mark_success()} Generated: {cf_pdf_path}")
            except Exception as e:
                typer.echo(f"WARNING:  PDF generation not available: {e}")
                typer.echo("   (HTML report was generated successfully)")

        typer.echo(f"Reports generated for {period} in {out_path}")

    except Exception as e:
        typer.echo(f"{_mark_error()} Error generating reports: {e}", err=True)
        raise typer.Exit(1) from e


@app.command("map-account")
def map_account(
    plaid_account_id: Annotated[str, typer.Option("--plaid-account-id")],
    gl_code: Annotated[str, typer.Option("--gl-code")],
) -> None:
    """Map a Plaid account to a GL account."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo(f"{_mark_error()} DATABASE_URL not found in environment", err=True)
        raise typer.Exit(2)

    try:
        engine = create_engine(database_url)
        with engine.begin() as conn:
            link_plaid_to_account(plaid_account_id, gl_code, conn)
        typer.echo(f"{_mark_success()} Linked {plaid_account_id} → {gl_code}")
    except Exception as e:
        typer.echo(f"{_mark_error()} Mapping failed: {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
def list_plaid_accounts(  # noqa: PLR0912
    item_id: Annotated[str, typer.Option("--item-id", help="Plaid ITEM_ID")],
    json_out: Annotated[bool, typer.Option("--json", help="Output JSON")] = False,
) -> None:
    """List Plaid accounts for an item (shows IDs for mapping)."""

    def _exit_no_accounts_found(message: str | None = None) -> None:
        """Exit with error when no accounts found."""
        if message is None:
            message = f"No Plaid accounts found for item_id: {item_id}"
        typer.echo(f"{_mark_error()} {message}", err=True)
        raise typer.Exit(1)

    try:
        rows = []
        engine = create_engine(os.environ["DATABASE_URL"])

        # 1) Try API first (if access token available)
        try:
            access_token = os.getenv("PLAID_ACCESS_TOKEN")
            if access_token:
                accounts = fetch_accounts(access_token)
                rows = [
                    {
                        "plaid_account_id": acc["account_id"],
                        "name": acc["name"],
                        "type": acc["type"],
                        "subtype": acc["subtype"],
                    }
                    for acc in accounts
                ]
        except Exception as e:
            logging.debug("API call failed, falling back to DB: %s", e)

        # 2) Try DB join via ingest_accounts (preferred), portable
        if not rows:
            with engine.begin() as conn:
                try:
                    insp = inspect(conn)
                    if not insp.has_table("ingest_accounts"):
                        _exit_no_accounts_found(
                            "Cannot scope by item_id yet. Ingest this item first."
                        )

                    query = text("""
                        SELECT DISTINCT
                            pa.plaid_account_id,
                            pa.name,
                            pa.type,
                            pa.subtype
                        FROM ingest_accounts ia
                        JOIN plaid_accounts pa
                            ON pa.plaid_account_id = ia.plaid_account_id
                        WHERE ia.item_id = :item_id
                        ORDER BY pa.name
                    """)
                    rows = [
                        {
                            "plaid_account_id": r[0],
                            "name": r[1],
                            "type": r[2],
                            "subtype": r[3],
                        }
                        for r in conn.execute(query, {"item_id": item_id}).fetchall()
                    ]

                    if not rows:
                        # Is the table empty entirely, or just no rows for this item?
                        has_any_data = conn.execute(
                            text("SELECT 1 FROM ingest_accounts LIMIT 1")
                        ).fetchone()
                        if not has_any_data:
                            _exit_no_accounts_found(
                                "Cannot scope by item_id yet. Ingest this item first."
                            )
                        else:
                            _exit_no_accounts_found(
                                f"No Plaid accounts found for item_id: {item_id}"
                            )

                except SQLAlchemyError:
                    # Generic DB error → treat as scoping unavailable for now
                    _exit_no_accounts_found(
                        "Cannot scope by item_id yet. Ingest this item first."
                    )

        if not rows:
            _exit_no_accounts_found()

        if json_out:
            typer.echo(json.dumps(rows, indent=2))
        else:
            for r in rows:
                typer.echo(
                    f"{r['plaid_account_id']} | {r['name']} | "
                    f"{r['type']}/{r['subtype']}"
                )

    except Exception as e:
        typer.echo(f"{_mark_error()} Failed to list accounts: {e}", err=True)
        raise typer.Exit(1) from e


@app.command("demo")
def demo(
    offline: Annotated[
        bool,
        typer.Option("--offline", help="Use SQLite in-memory database (fastest)"),
    ] = False,
    docker: Annotated[
        bool,
        typer.Option("--docker", help="Use Postgres via Docker (full stack)"),
    ] = False,
    out: Annotated[
        str,
        typer.Option("--out", help="Output directory for reports"),
    ] = "build",
) -> None:
    """Run offline demo with fixture data (no Plaid credentials required)."""
    # Set deterministic environment
    os.environ["LC_ALL"] = "C.UTF-8"
    os.environ["TZ"] = "UTC"
    os.environ["PFETL_NO_EGRESS"] = "1"  # Block all HTTP calls

    # Validate mode selection
    if offline and docker:
        typer.echo("Error: Cannot use both --offline and --docker modes", err=True)
        raise typer.Exit(2)

    if not offline and not docker:
        offline = True  # Default to offline mode

    def _raise_database_url_error() -> None:
        typer.echo(
            f"{_mark_error()} Docker mode requires DATABASE_URL environment variable",
            err=True,
        )
        raise typer.Exit(2)

    def _raise_reconciliation_failed() -> None:
        typer.echo("❌ Reconciliation: FAILED")
        raise typer.Exit(1)

    try:
        if offline:
            typer.echo("🚀 Starting offline demo (SQLite + fixtures)...")

            # Create in-memory SQLite database with fixtures
            engine = create_demo_engine()

            with engine.begin() as conn:
                load_demo_fixtures(conn)
                typer.echo(f"{_mark_success()} Demo database initialized with fixtures")

        else:  # docker mode
            typer.echo("🚀 Starting Docker demo (Postgres + fixtures)...")

            # Check for DATABASE_URL since we need it for docker mode
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                typer.echo(
                    f"{_mark_error()} Docker mode requires DATABASE_URL",
                    err=True,
                )
                _raise_database_url_error()

            # mypy: database_url is guaranteed to be str here due to check above
            engine = create_engine(database_url)  # type: ignore[arg-type]

            with engine.begin() as conn:
                load_demo_fixtures(conn)
                typer.echo(f"{_mark_success()} Demo fixtures loaded into Postgres")

        # Run reconciliation with demo data
        demo_balances = get_demo_balances()
        item_id = "demo_item_2024q1"
        period = "2024Q1"

        with engine.begin() as conn:
            result = run_reconciliation(
                conn, period=period, item_id=item_id, plaid_balances=demo_balances
            )

        # Write reconciliation results
        out_path = Path(out)
        out_path.mkdir(parents=True, exist_ok=True)

        recon_file = out_path / "demo_recon.json"
        with recon_file.open("w") as f:
            json.dump(result, f, indent=2, default=str)

        typer.echo(f"{_mark_success()} Reconciliation: {recon_file}")

        # Generate reports
        # Balance Sheet
        bs_html = render_balance_sheet(period, engine)
        bs_file = out_path / f"demo_bs_{period.lower()}.html"
        bs_file.write_text(bs_html, encoding="utf-8")
        typer.echo(f"{_mark_success()} Balance Sheet: {bs_file}")

        # Cash Flow
        cf_html = render_cash_flow(period, engine)
        cf_file = out_path / f"demo_cf_{period.lower()}.html"
        cf_file.write_text(cf_html, encoding="utf-8")
        typer.echo(f"{_mark_success()} Cash Flow: {cf_file}")

        # Success summary
        mode_desc = "SQLite (offline)" if offline else "Postgres (Docker)"
        typer.echo(f"\n{_mark_success()} Demo completed successfully using {mode_desc}")
        typer.echo(f"📂 Generated files in: {out_path.absolute()}")

        # Show reconciliation result
        if result.get("success"):
            typer.echo(
                f"✅ Reconciliation: PASSED (variance: {result.get('total_variance', 0):.2f})"  # noqa: E501
            )
        else:
            _raise_reconciliation_failed()

    except Exception as e:
        typer.echo(f"{_mark_error()} Demo failed: {e}", err=True)
        raise typer.Exit(1) from e


@app.command("doctor")
def _check_dependency(module_name: str) -> bool:
    """Check if a module is available without importing it."""
    return importlib.util.find_spec(module_name) is not None


def _check_python_version() -> bool:
    """Check Python version requirement."""
    py_version = sys.version_info
    typer.echo(
        f"Python version: {py_version.major}.{py_version.minor}.{py_version.micro}"
    )
    if py_version < (3, 11):
        version_str = f"{py_version.major}.{py_version.minor}.{py_version.micro}"
        typer.echo(f"{_mark_error()} Python 3.11+ required, found {version_str}")
        return False
    typer.echo(f"{_mark_success()} Python version OK")
    return True


def _check_docker() -> bool:
    """Check Docker availability and status."""
    docker_path = shutil.which("docker")
    if docker_path is None:
        typer.echo(f"{_mark_error()} Docker not found in PATH")
        return False

    try:
        result = subprocess.run(  # noqa: S603
            [docker_path, "info"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            typer.echo(f"{_mark_success()} Docker is running")
            return True
        typer.echo(f"{_mark_error()} Docker daemon not running")
        return False  # noqa: TRY300
    except (subprocess.TimeoutExpired, FileNotFoundError):
        typer.echo(f"{_mark_error()} Docker not responding")
        return False


def _check_database() -> bool:
    """Check database connection if configured."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        typer.echo("i  DATABASE_URL not set (OK for offline demo)")
        return True

    typer.echo(f"Database URL: {database_url[:20]}...")
    try:
        engine = create_engine(database_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        typer.echo(f"{_mark_error()} Database connection failed: {e}")
        return False
    else:
        typer.echo(f"{_mark_success()} Database connection OK")
        return True


def _check_plaid_credentials() -> None:
    """Check Plaid credentials (informational only)."""
    plaid_client_id = os.getenv("PLAID_CLIENT_ID")
    plaid_secret = os.getenv("PLAID_SECRET")
    if plaid_client_id and plaid_secret:
        typer.echo(f"{_mark_success()} Plaid credentials found")
    else:
        typer.echo("i  Plaid credentials not set (OK for offline demo)")


def _check_dependencies() -> bool:
    """Check Python package dependencies."""
    if (
        _check_dependency("httpx")
        and _check_dependency("jinja2")
        and _check_dependency("sqlalchemy")
    ):
        typer.echo(f"{_mark_success()} Core dependencies available")
        success = True
    else:
        typer.echo(f"{_mark_error()} Missing core dependencies")
        success = False

    if _check_dependency("weasyprint"):
        typer.echo(f"{_mark_success()} WeasyPrint available (PDF support)")
    else:
        typer.echo("i  WeasyPrint not available (PDF disabled, HTML reports only)")

    return success


def doctor() -> None:
    """Run preflight checks for system dependencies and configuration."""
    typer.echo("🔍 Running system preflight checks...\n")

    # Platform info
    typer.echo(f"Platform: {platform.system()} {platform.release()}")

    # Run all checks
    checks = [
        _check_python_version(),
        _check_docker(),
        _check_database(),
        _check_dependencies(),
    ]
    _check_plaid_credentials()  # Informational only

    all_good = all(checks)

    # Summary
    typer.echo()
    if all_good:
        typer.echo(f"{_mark_success()} All checks passed! System ready for pfetl")
        typer.echo("\nRecommended next steps:")
        typer.echo("  make demo-offline    # Quick start (no dependencies)")
        typer.echo("  make demo-docker     # Full stack demo")
        typer.echo("  make demo-sandbox    # Plaid sandbox (requires credentials)")
    else:
        typer.echo(f"{_mark_error()} Some checks failed. See errors above.")
        typer.echo("\nFor offline demo only:")
        typer.echo("  make demo-offline    # Works without Docker/DB")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
