"""Extract module - stub implementation to enable RED tests."""
import json
from typing import Any, Callable, Iterable


def canonicalize_json(obj: Any) -> bytes:
    """Canonicalize JSON to stable, minified, key-sorted bytes."""
    # Stub - will implement properly
    raise NotImplementedError("canonicalize_json not implemented")


def sync_transactions(
    access_token: str, 
    start_date: str, 
    end_date: str,
    backoff_fn: Callable[[int], float] | None = None
) -> Iterable[dict[str, Any]]:
    """Sync transactions from Plaid with pagination and retry."""
    # Stub - will implement properly
    raise NotImplementedError("sync_transactions not implemented")


def land_raw(item_id: str, transactions: list[dict[str, Any]]) -> int:
    """Land raw transactions in database."""
    # Stub - will implement properly
    raise NotImplementedError("land_raw not implemented")