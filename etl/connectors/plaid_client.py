import os
from types import TracebackType
from typing import Self

import httpx
from pydantic import BaseModel

__all__ = ["PlaidClient", "PlaidCredentials", "create_plaid_client_from_env"]


class PlaidCredentials(BaseModel):
    client_id: str
    secret: str
    env: str


class PlaidClient:
    """Plaid API client with ADR-specified timeouts: 5s connect, 15s read."""

    def __init__(self, credentials: PlaidCredentials) -> None:
        self.credentials = credentials
        self.base_url = self._get_base_url(credentials.env)
        self.client = httpx.Client(
            timeout=httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=10.0),
            headers={
                "Content-Type": "application/json",
            },
        )

    def _get_base_url(self, env: str) -> str:
        if env == "sandbox":
            return "https://sandbox.plaid.com"
        if env == "development":
            return "https://development.plaid.com"
        if env == "production":
            return "https://production.plaid.com"
        msg = f"Invalid Plaid environment: {env}"
        raise ValueError(msg)

    def create_sandbox_public_token(self) -> str:
        """Create sandbox public token for testing."""
        url = f"{self.base_url}/sandbox/public_token/create"
        payload = {
            "client_id": self.credentials.client_id,
            "secret": self.credentials.secret,
            "institution_id": "ins_109508",  # Chase sandbox institution
            "initial_products": ["transactions"],
        }

        response = self.client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        return str(data["public_token"])

    def exchange_public_token(self, public_token: str) -> tuple[str, str]:
        """Exchange public token for access token and item ID."""
        url = f"{self.base_url}/item/public_token/exchange"
        payload = {
            "client_id": self.credentials.client_id,
            "secret": self.credentials.secret,
            "public_token": public_token,
        }

        response = self.client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        return str(data["access_token"]), str(data["item_id"])

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


def create_plaid_client_from_env() -> PlaidClient:
    """Create Plaid client from environment variables."""
    credentials = PlaidCredentials(
        client_id=os.environ["PLAID_CLIENT_ID"],
        secret=os.environ["PLAID_SECRET"],
        env=os.environ["PLAID_ENV"],
    )
    return PlaidClient(credentials)
