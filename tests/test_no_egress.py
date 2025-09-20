"""Test that PFETL_NO_EGRESS properly blocks HTTP calls in demo mode."""

import os

import pytest

from etl.http_guard import create_guarded_async_client, create_guarded_client


def test_http_guard_blocks_when_no_egress_set() -> None:
    """Verify HTTP guard raises when PFETL_NO_EGRESS=1."""
    with pytest.MonkeyPatch().context() as m:
        m.setenv("PFETL_NO_EGRESS", "1")

        with pytest.raises(
            RuntimeError, match="External API calls blocked in demo mode"
        ):
            create_guarded_client()


def test_async_http_guard_blocks_when_no_egress_set() -> None:
    """Verify async HTTP guard raises when PFETL_NO_EGRESS=1."""
    with pytest.MonkeyPatch().context() as m:
        m.setenv("PFETL_NO_EGRESS", "1")

        with pytest.raises(
            RuntimeError, match="External API calls blocked in demo mode"
        ):
            create_guarded_async_client()


def test_http_guard_allows_when_no_egress_unset() -> None:
    """Verify HTTP guard works normally when PFETL_NO_EGRESS is not set."""
    with pytest.MonkeyPatch().context() as m:
        m.delenv("PFETL_NO_EGRESS", raising=False)

        # Should not raise
        client = create_guarded_client()
        assert client is not None
        client.close()


def test_http_guard_allows_when_no_egress_false() -> None:
    """Verify HTTP guard works normally when PFETL_NO_EGRESS=0."""
    with pytest.MonkeyPatch().context() as m:
        m.setenv("PFETL_NO_EGRESS", "0")

        # Should not raise
        client = create_guarded_client()
        assert client is not None
        client.close()


def test_demo_command_sets_no_egress(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that demo command automatically sets PFETL_NO_EGRESS."""
    import tempfile
    from unittest.mock import Mock

    # Clear environment
    monkeypatch.delenv("PFETL_NO_EGRESS", raising=False)

    # Create a mock engine that supports context manager protocol
    mock_engine = Mock()
    mock_conn = Mock()
    mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = Mock(return_value=None)

    # Mock all the dependencies to avoid actual execution
    monkeypatch.setattr("etl.demo.create_demo_engine", lambda: mock_engine)
    monkeypatch.setattr("etl.demo.load_demo_fixtures", lambda _: None)
    monkeypatch.setattr("etl.demo.get_demo_balances", dict)
    monkeypatch.setattr(
        "etl.reconcile.run_reconciliation",
        lambda *_args, **_kwargs: {"success": True, "total_variance": 0},
    )
    monkeypatch.setattr(
        "etl.reports.render.render_balance_sheet", lambda *_args: "<html></html>"
    )
    monkeypatch.setattr(
        "etl.reports.render.render_cash_flow", lambda *_args: "<html></html>"
    )

    # Use temporary directory for output
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Import here to avoid import-time side effects
        from cli import demo

        # The demo function should set PFETL_NO_EGRESS=1 and run successfully
        demo(offline=True, docker=False, out=tmp_dir)

        # Verify the environment variable was set
        assert os.getenv("PFETL_NO_EGRESS") == "1"
