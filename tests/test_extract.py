"""Tests for data extraction from Plaid API."""

import json
import os
from datetime import datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest
import respx
from psycopg import connect

from etl.extract import canonicalize_json, land_raw, sync_transactions


class TestExtract:
    """Test suite for transaction extraction functionality."""

    def test_pagination_across_multiple_pages(
        self,
        monkeypatch: Any,
        database_url: str,
    ) -> None:
        """Extract paginates across multiple pages and lands all transactions."""
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_client")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("PLAID_ENV", "sandbox")

        tid1 = f"txn_{uuid4().hex[:8]}"
        tid2 = f"txn_{uuid4().hex[:8]}"
        item_id = f"item_{uuid4().hex[:8]}"

        page1_response = {
            "added": [
                {
                    "transaction_id": tid1,
                    "account_id": "acc_x",
                    "amount": 12.34,
                    "iso_currency_code": "USD",
                    "date": "2024-01-10",
                    "pending": False,
                    "merchant_name": "ACME",
                    "payment_channel": "in store",
                },
            ],
            "has_more": True,
            "next_cursor": "cursor_1",
        }

        page2_response = {
            "added": [
                {
                    "transaction_id": tid2,
                    "account_id": "acc_x",
                    "amount": 5.67,
                    "iso_currency_code": "USD",
                    "date": "2024-01-11",
                    "pending": False,
                    "merchant_name": "Store",
                    "payment_channel": "online",
                },
            ],
            "has_more": False,
            "next_cursor": None,
        }

        call_count = 0

        def mock_pagination(_request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            # First call has no cursor, second call has cursor
            if call_count == 1:
                return httpx.Response(200, json=page1_response)
            return httpx.Response(200, json=page2_response)

        with respx.mock:
            respx.post("https://sandbox.plaid.com/transactions/sync").mock(
                side_effect=mock_pagination,
            )

            # Get all transactions
            transactions = list(
                sync_transactions("test_access_token", "2024-01-01", "2024-01-31"),
            )

            # Should have both transactions
            assert len(transactions) == 2
            assert transactions[0]["transaction_id"] == tid1
            assert transactions[1]["transaction_id"] == tid2

            # Two HTTP calls for two pages
            assert respx.calls.call_count == 2

            # Land them in database
            count = land_raw(item_id, transactions)
            assert count == 2

            # Verify they're in the database
            with connect(database_url) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM raw_transactions WHERE item_id = %s",
                    (item_id,),
                )
                row = cur.fetchone()
                assert row is not None
                db_count = row[0]
                assert db_count == 2

    def test_retries_on_429_and_5xx_then_success(self, monkeypatch: Any) -> None:
        """Extract retries on 429/5xx with bounded attempts."""
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_client")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("PLAID_ENV", "sandbox")

        tid = f"txn_{uuid4().hex[:8]}"
        success_response = {
            "added": [
                {
                    "transaction_id": tid,
                    "account_id": "acc_x",
                    "amount": 12.34,
                    "iso_currency_code": "USD",
                    "date": "2024-01-10",
                    "pending": False,
                },
            ],
            "has_more": False,
            "next_cursor": None,
        }

        call_count = 0

        def mock_request(_request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(429, json={"error": "rate_limit"})
            if call_count == 2:
                return httpx.Response(500, json={"error": "server_error"})
            return httpx.Response(200, json=success_response)

        with respx.mock:
            respx.post("https://sandbox.plaid.com/transactions/sync").mock(
                side_effect=mock_request,
            )

            # Should succeed after 3 attempts with backoff_fn=lambda _: 0
            transactions = list(
                sync_transactions(
                    "test_access_token",
                    "2024-01-01",
                    "2024-01-31",
                    backoff_fn=lambda _: 0,
                ),
            )

            # Should have succeeded
            assert len(transactions) == 1
            assert transactions[0]["transaction_id"] == tid

            # Should have made exactly 3 attempts
            assert call_count == 3

    def test_raw_json_persisted_exactly_with_fetched_at(
        self,
        database_url: str,
    ) -> None:
        """Raw JSON is canonicalized and stored with fetched_at timestamp."""
        tid = f"txn_{uuid4().hex[:8]}"
        mock_txn = {
            "transaction_id": tid,
            "account_id": "acc_x",
            "amount": 12.34,
            "date": "2024-01-10",
            "merchant_name": "ACME",
        }

        # Canonicalize the JSON
        canonical_bytes = canonicalize_json(mock_txn)

        # Land the transaction
        count = land_raw("test_item_id", [mock_txn])
        assert count == 1

        # Verify stored data
        with connect(database_url) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT txn_id, as_json, fetched_at FROM raw_transactions "
                "WHERE txn_id = %s",
                (tid,),
            )
            row = cur.fetchone()

            assert row is not None
            assert row[0] == tid

            # Compare canonicalized JSON byte-for-byte
            stored_canonical = canonicalize_json(row[1])
            assert stored_canonical == canonical_bytes

            # fetched_at should be a datetime
            assert isinstance(row[2], datetime)


# Test the canonicalize_json helper separately
def test_canonicalize_json_is_stable() -> None:
    """canonicalize_json produces stable, minified, key-sorted output."""
    obj1 = {"b": 2, "a": 1, "c": {"z": 3, "y": 4}}
    obj2 = {"a": 1, "c": {"y": 4, "z": 3}, "b": 2}  # Same data, different order

    canonical1 = canonicalize_json(obj1)
    canonical2 = canonicalize_json(obj2)

    assert canonical1 == canonical2
    # Should be minified (no spaces)
    assert b" " not in canonical1
    # Should be JSON-parseable
    parsed = json.loads(canonical1.decode())
    assert parsed == {"a": 1, "b": 2, "c": {"y": 4, "z": 3}}


class TestExtractErrorHandling:
    """Test suite for extraction error handling and retry logic."""

    def test_no_retry_on_400(self, monkeypatch: Any) -> None:
        """Extract does not retry on 400 (non-retryable 4xx)."""
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_client")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("PLAID_ENV", "sandbox")

        with respx.mock:
            respx.post("https://sandbox.plaid.com/transactions/sync").mock(
                return_value=httpx.Response(400, json={"error": "bad_request"}),
            )

            with pytest.raises(httpx.HTTPStatusError):
                list(
                    sync_transactions(
                        "test_access_token",
                        "2024-01-01",
                        "2024-01-31",
                        backoff_fn=lambda _: 0,
                    ),
                )

            # Only one attempt - no retry on 400
            assert respx.calls.call_count == 1


@pytest.fixture
def database_url() -> str:
    """Get database URL from environment or use test default."""
    return os.getenv(
        "DATABASE_URL",
        "postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl",
    )
