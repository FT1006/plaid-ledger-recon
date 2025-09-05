"""Tests for list-plaid-accounts CLI command (fail-fast, item-scoped)."""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from cli import app

runner = CliRunner()


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db_url = f"sqlite:///{path}"
    
    # Initialize schema
    engine = create_engine(db_url)
    with engine.begin() as conn:
        # Create minimal tables needed for testing
        conn.execute(text("""
            CREATE TABLE plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency CHAR(3) NOT NULL DEFAULT 'USD'
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE raw_transactions (
                item_id TEXT NOT NULL,
                txn_id TEXT PRIMARY KEY,
                as_json TEXT NOT NULL,
                fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE ingest_accounts (
                item_id TEXT NOT NULL,
                plaid_account_id TEXT NOT NULL,
                PRIMARY KEY (item_id, plaid_account_id)
            )
        """))
    
    yield db_url
    
    # Cleanup
    os.unlink(path)


@pytest.fixture
def temp_db_no_ingest_table():
    """Create a temporary SQLite database without ingest_accounts table."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db_url = f"sqlite:///{path}"
    
    # Initialize minimal schema - missing ingest_accounts table
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE plaid_accounts (
                plaid_account_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                subtype TEXT NOT NULL,
                currency CHAR(3) NOT NULL DEFAULT 'USD'
            )
        """))
        # Intentionally NOT creating ingest_accounts table
    
    yield db_url
    
    # Cleanup
    os.unlink(path)


def test_filters_by_item_via_db_join(temp_db):
    """list-plaid-accounts filters by item via DB join.
    
    Seed SQLite with plaid_accounts and raw_transactions.
    Insert 3 accounts; link 2 to item_A, 1 to item_B.
    Run: pfetl list-plaid-accounts --item-id item_A.
    Expect: exit 0; output shows only the 2 for item_A.
    """
    engine = create_engine(temp_db)
    
    # Seed test data
    with engine.begin() as conn:
        # Insert 3 plaid accounts
        conn.execute(text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype)
            VALUES 
                ('acc_1', 'Checking Account', 'depository', 'checking'),
                ('acc_2', 'Savings Account', 'depository', 'savings'),
                ('acc_3', 'Credit Card', 'credit', 'credit_card')
        """))
        
        # Insert ingest_accounts linking acc_1 and acc_2 to item_A, acc_3 to item_B
        conn.execute(text("""
            INSERT INTO ingest_accounts (item_id, plaid_account_id)
            VALUES 
                ('item_A', 'acc_1'),
                ('item_A', 'acc_2'),
                ('item_B', 'acc_3')
        """))
    
    # Test the command
    with patch.dict(os.environ, {"DATABASE_URL": temp_db}):
        with patch("cli.create_plaid_client_from_env") as mock_client:
            # Make API call fail so it falls back to DB
            mock_client.side_effect = Exception("No API access")
            
            result = runner.invoke(app, ["list-plaid-accounts", "--item-id", "item_A"])
    
    assert result.exit_code == 0
    output_lines = result.output.strip().split('\n')
    # Should show 2 accounts for item_A
    assert len(output_lines) == 2
    assert "acc_1" in result.output
    assert "acc_2" in result.output
    assert "acc_3" not in result.output


def test_fails_fast_when_no_accounts_for_item(temp_db):
    """list-plaid-accounts fails fast when no accounts found for item.
    
    Same schema; link none to item_Z.
    Run: pfetl list-plaid-accounts --item-id item_Z.
    Expect: exit 1; message: No Plaid accounts found for item_id: item_Z.
    """
    engine = create_engine(temp_db)
    
    # Seed test data with accounts but no transactions for item_Z
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype)
            VALUES ('acc_1', 'Checking Account', 'depository', 'checking')
        """))
        
        # No ingest_accounts for item_Z
    
    # Test the command
    with patch.dict(os.environ, {"DATABASE_URL": temp_db}):
        with patch("cli.create_plaid_client_from_env") as mock_client:
            # Make API call fail so it falls back to DB
            mock_client.side_effect = Exception("No API access")
            
            result = runner.invoke(app, ["list-plaid-accounts", "--item-id", "item_Z"])
    
    assert result.exit_code == 1
    assert "No Plaid accounts found for item_id: item_Z" in result.output


def test_fails_fast_when_item_scoping_unavailable(temp_db):
    """list-plaid-accounts fails fast when item scoping unavailable.
    
    Schema has plaid_accounts but ingest_accounts is empty (pre-ingest).
    Run: pfetl list-plaid-accounts --item-id item_A.
    Expect: exit 1; message: Cannot scope by item_id yet. Ingest this item first.
    """
    engine = create_engine(temp_db)
    
    # Seed test data with accounts but no ingest_accounts at all
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype)
            VALUES ('acc_1', 'Checking Account', 'depository', 'checking')
        """))
        # ingest_accounts table exists but is empty
    
    # Test the command  
    with patch.dict(os.environ, {"DATABASE_URL": temp_db}):
        with patch("cli.create_plaid_client_from_env") as mock_client:
            # Make API call fail so it falls back to DB
            mock_client.side_effect = Exception("No API access")
            
            result = runner.invoke(app, ["list-plaid-accounts", "--item-id", "item_A"])
    
    assert result.exit_code == 1
    assert "Cannot scope by item_id yet. Ingest this item first" in result.output


def test_fails_fast_when_ingest_table_missing(temp_db_no_ingest_table):
    """list-plaid-accounts fails fast when ingest_accounts table doesn't exist.
    
    Schema only has plaid_accounts (no ingest_accounts table).
    Run: pfetl list-plaid-accounts --item-id item_A.
    Expect: exit 1; message: Cannot scope by item_id yet. Ingest this item first.
    """
    engine = create_engine(temp_db_no_ingest_table)
    
    # Seed test data with plaid_accounts only
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype)
            VALUES ('acc_1', 'Checking Account', 'depository', 'checking')
        """))
    
    # Test the command  
    with patch.dict(os.environ, {"DATABASE_URL": temp_db_no_ingest_table}):
        with patch("cli.create_plaid_client_from_env") as mock_client:
            # Make API call fail so it falls back to DB
            mock_client.side_effect = Exception("No API access")
            
            result = runner.invoke(app, ["list-plaid-accounts", "--item-id", "item_A"])
    
    assert result.exit_code == 1
    assert "Cannot scope by item_id yet. Ingest this item first" in result.output


def test_api_path_succeeds(temp_db):
    """list-plaid-accounts API path succeeds when PLAID_ACCESS_TOKEN available.
    
    Mock create_plaid_client_from_env() to return stub with 2 accounts.
    Expect: exit 0; prints those 2 accounts.
    """
    # Mock Plaid account objects
    mock_account_1 = MagicMock()
    mock_account_1.account_id = "api_acc_1"
    mock_account_1.name = "API Checking"
    mock_account_1.type = "depository"
    mock_account_1.subtype = "checking"
    
    mock_account_2 = MagicMock()
    mock_account_2.account_id = "api_acc_2"
    mock_account_2.name = "API Savings"
    mock_account_2.type = "depository"
    mock_account_2.subtype = "savings"
    
    mock_accounts = [mock_account_1, mock_account_2]
    
    # Mock the Plaid client
    mock_client = MagicMock()
    mock_client.get_accounts_for_item.return_value = mock_accounts
    
    # Test the command
    with patch.dict(os.environ, {"DATABASE_URL": temp_db, "PLAID_ACCESS_TOKEN": "fake_token"}):
        with patch("cli.create_plaid_client_from_env") as mock_create_client:
            mock_create_client.return_value.__enter__.return_value = mock_client
            
            result = runner.invoke(app, ["list-plaid-accounts", "--item-id", "test_item"])
    
    assert result.exit_code == 0
    assert "api_acc_1" in result.output
    assert "api_acc_2" in result.output
    assert "API Checking" in result.output
    assert "API Savings" in result.output


def test_json_output_format(temp_db):
    """list-plaid-accounts --json outputs valid JSON format."""
    # Mock API response
    mock_account = MagicMock()
    mock_account.account_id = "json_acc_1"
    mock_account.name = "JSON Test Account"
    mock_account.type = "depository"
    mock_account.subtype = "checking"
    
    mock_client = MagicMock()
    mock_client.get_accounts_for_item.return_value = [mock_account]
    
    # Test the command with --json flag
    with patch.dict(os.environ, {"DATABASE_URL": temp_db, "PLAID_ACCESS_TOKEN": "fake_token"}):
        with patch("cli.create_plaid_client_from_env") as mock_create_client:
            mock_create_client.return_value.__enter__.return_value = mock_client
            
            result = runner.invoke(app, ["list-plaid-accounts", "--item-id", "test_item", "--json"])
    
    assert result.exit_code == 0
    
    # Parse and validate JSON output
    output_json = json.loads(result.output)
    assert len(output_json) == 1
    assert output_json[0]["plaid_account_id"] == "json_acc_1"
    assert output_json[0]["name"] == "JSON Test Account"
    assert output_json[0]["type"] == "depository"
    assert output_json[0]["subtype"] == "checking"