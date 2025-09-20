"""HTTP client guard for demo mode egress control."""

import os
from typing import Any

import httpx


def create_guarded_client(**kwargs: Any) -> httpx.Client:  # noqa: ANN401
    """Create an httpx client that respects PFETL_NO_EGRESS."""
    if os.getenv("PFETL_NO_EGRESS") == "1":
        msg = "External API calls blocked in demo mode (PFETL_NO_EGRESS=1)"
        raise RuntimeError(msg)
    return httpx.Client(**kwargs)


def create_guarded_async_client(**kwargs: Any) -> httpx.AsyncClient:  # noqa: ANN401
    """Create an async httpx client that respects PFETL_NO_EGRESS."""
    if os.getenv("PFETL_NO_EGRESS") == "1":
        msg = "External API calls blocked in demo mode (PFETL_NO_EGRESS=1)"
        raise RuntimeError(msg)
    return httpx.AsyncClient(**kwargs)
