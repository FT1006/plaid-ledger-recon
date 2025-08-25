"""Extract module - Plaid transaction extraction with pagination and retry."""

from __future__ import annotations

import json
import os
import random
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
import psycopg

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

from etl.connectors.plaid_client import create_plaid_client_from_env


def canonicalize_json(obj: dict[str, Any]) -> bytes:
    """Deterministic, minified, key-sorted JSON bytes (ADR §Hashing)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _default_backoff(attempt: int) -> float:
    """ADR backoff: ~0.5s, 1s, 2s with ±20% jitter; tests may override to 0."""
    base = 0.5 * (2**attempt)  # 0.5, 1.0, 2.0
    jitter: float = random.uniform(-0.2, 0.2)  # noqa: S311
    return base * (1 + jitter)  # type: ignore[no-any-return]


def sync_transactions(
    access_token: str,
    date_from: str,  # kept for signature parity  # noqa: ARG001
    date_to: str,  # noqa: ARG001
    *,
    backoff_fn: Callable[[int], float] | None = None,
) -> Iterable[dict[str, Any]]:
    """
    Sync transactions via /transactions/sync with pagination + bounded retry.

    ADR: See §Connector for design rationale.
    """
    if backoff_fn is None:
        backoff_fn = _default_backoff

    with create_plaid_client_from_env() as client:
        cursor: str | None = None
        has_more = True
        url = f"{client.base_url}/transactions/sync"

        while has_more:
            payload: dict[str, Any] = {
                "client_id": client.credentials.client_id,
                "secret": client.credentials.secret,
                "access_token": access_token,
            }
            if cursor:
                payload["cursor"] = cursor

            # Retry only on 429/5xx + connect/timeout, max 3 attempts
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    resp = client.client.post(url, json=payload)
                    resp.raise_for_status()
                    break
                except httpx.HTTPStatusError as e:
                    code = e.response.status_code
                    if code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                        time.sleep(backoff_fn(attempt))
                        continue
                    raise
                except (httpx.ConnectError, httpx.TimeoutException):
                    if attempt < max_retries - 1:
                        time.sleep(backoff_fn(attempt))
                        continue
                    raise

            data = resp.json()
            yield from data.get("added", [])

            has_more = bool(data.get("has_more"))
            cursor = data.get("next_cursor")


def land_raw(item_id: str, transactions: list[dict[str, Any]]) -> int:
    """
    Insert one row per transaction into raw_transactions with canonical JSON.

    ADR: See §Raw landing for design rationale.
    """
    # Match Compose/test defaults if env var absent
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl",
    )

    inserted = 0
    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        now = datetime.now(UTC)
        for txn in transactions:
            canon = canonicalize_json(txn)
            # Store as JSONB using psycopg.sql for proper type handling
            cur.execute(
                """
                INSERT INTO raw_transactions (item_id, txn_id, as_json, fetched_at)
                VALUES (%s, %s, %s::jsonb, %s)
                ON CONFLICT (txn_id) DO NOTHING
                """,
                (item_id, txn["transaction_id"], canon.decode(), now),
            )
            if cur.rowcount > 0:
                inserted += 1
        conn.commit()
    return inserted
