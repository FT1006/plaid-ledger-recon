"""Tests for Plaid onboarding flow and credential management."""

from pathlib import Path
from typing import Any

import httpx
import respx
from cli import app  # Typer app
from typer.testing import CliRunner

runner = CliRunner()

# NOTE: Plaid's sandbox public token endpoint is /sandbox/public_token/create
# (hyphen vs underscore varies by SDK docs).
# Use exactly what your client uses. If you implement with the slash form, adjust here:
SANDBOX_PUBLIC_TOKEN_URL = "https://sandbox.plaid.com/sandbox/public_token/create"  # noqa: S105

EXCHANGE_URL = "https://sandbox.plaid.com/item/public_token/exchange"


@respx.mock
def test_onboard_sandbox_flow_success_prints_item_id_and_exits_zero(
    monkeypatch: Any,
) -> None:
    """ADR: Onboard performs sandbox public_token/create then exchange.

    Prints ITEM_ID, exits 0.
    """
    # Minimal env required by CLI (values are not used by mocks but mirror real usage)
    monkeypatch.setenv("PLAID_CLIENT_ID", "id_sandbox_x")
    monkeypatch.setenv("PLAID_SECRET", "secret_sandbox_x")
    monkeypatch.setenv("PLAID_ENV", "sandbox")

    calls: list[str] = []

    def record_request(request: httpx.Request) -> bool:
        calls.append(str(request.url))
        return True  # allow match to proceed

    respx.post(SANDBOX_PUBLIC_TOKEN_URL).side_effect = record_request  # type: ignore[assignment]
    respx.post(SANDBOX_PUBLIC_TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"public_token": "public-sandbox-token"}),
    )
    respx.post(EXCHANGE_URL).side_effect = record_request  # type: ignore[assignment]
    respx.post(EXCHANGE_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "access-sandbox-test-token",
                "item_id": "test-item-id-12345",
            },
        ),
    )

    result = runner.invoke(app, ["onboard", "--sandbox"])

    # RED expectation: this should be GREEN only after implementation
    assert result.exit_code == 0, result.stdout
    assert "test-item-id-12345" in result.stdout

    # Both endpoints called exactly once
    assert respx.calls.call_count == 2
    # Optional: simple order check (public_token before exchange)
    if len(calls) >= 2:
        assert calls[0].endswith("/sandbox/public_token/create")
        assert calls[1].endswith("/item/public_token/exchange")


@respx.mock
def test_onboard_failure_returns_nonzero_and_message(monkeypatch: Any) -> None:
    """If exchange fails (e.g., 500), CLI should exit 1 and show a helpful message."""
    monkeypatch.setenv("PLAID_CLIENT_ID", "id_sandbox_x")
    monkeypatch.setenv("PLAID_SECRET", "secret_sandbox_x")
    monkeypatch.setenv("PLAID_ENV", "sandbox")

    respx.post(SANDBOX_PUBLIC_TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"public_token": "public-sandbox-token"}),
    )
    respx.post(EXCHANGE_URL).mock(
        return_value=httpx.Response(500, json={"error": "boom"}),
    )

    result = runner.invoke(app, ["onboard", "--sandbox"])

    assert result.exit_code != 0
    # Adjust to your real error text later
    assert "onboard failed" in result.stderr.lower() or "error" in result.stderr.lower()


@respx.mock
def test_onboard_write_env_appends_and_dedupes(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """Optional (still RED): with --write-env, CLI appends PLAID_ACCESS_TOKEN.

    And PLAID_ITEM_ID to the specified .env path, without duplicating keys.
    """
    monkeypatch.setenv("PLAID_CLIENT_ID", "id_sandbox_x")
    monkeypatch.setenv("PLAID_SECRET", "secret_sandbox_x")
    monkeypatch.setenv("PLAID_ENV", "sandbox")

    env_path = tmp_path / ".env"
    env_path.write_text("PLAID_CLIENT_ID=id_sandbox_x\nPLAID_SECRET=secret_sandbox_x\n")

    respx.post(SANDBOX_PUBLIC_TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"public_token": "public-sandbox-token"}),
    )
    respx.post(EXCHANGE_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "access-sandbox-test-token",
                "item_id": "test-item-id-12345",
            },
        ),
    )

    # Expect your CLI to support --env-path, or default to ./ .env if omitted.
    result = runner.invoke(
        app,
        ["onboard", "--sandbox", "--write-env", f"--env-path={env_path}"],
    )
    assert result.exit_code == 0

    text = env_path.read_text()
    assert "PLAID_ACCESS_TOKEN=access-sandbox-test-token" in text
    assert "PLAID_ITEM_ID=test-item-id-12345" in text

    # Re-run should not duplicate
    result2 = runner.invoke(
        app,
        ["onboard", "--sandbox", "--write-env", f"--env-path={env_path}"],
    )
    assert result2.exit_code == 0
    text2 = env_path.read_text()
    assert text2.count("PLAID_ACCESS_TOKEN=") == 1
    assert text2.count("PLAID_ITEM_ID=") == 1
