-- SQLite-compatible schema for demo mode
-- Based on main schema.sql but adapted for SQLite syntax

-- Canonical GL chart of accounts
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('asset','liability','equity','revenue','expense')),
    subtype TEXT,
    currency TEXT NOT NULL DEFAULT 'USD',
    is_cash BOOLEAN NOT NULL DEFAULT 0,
    active BOOLEAN NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)

CREATE TABLE IF NOT EXISTS plaid_accounts (
    plaid_account_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)

CREATE TABLE IF NOT EXISTS account_links (
    id TEXT PRIMARY KEY,
    plaid_account_id TEXT UNIQUE NOT NULL,
    account_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (plaid_account_id) REFERENCES plaid_accounts(plaid_account_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
)

CREATE TABLE IF NOT EXISTS ingest_accounts (
  item_id TEXT NOT NULL,
  plaid_account_id TEXT NOT NULL,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  subtype TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'USD',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (item_id, plaid_account_id)
)

CREATE TABLE IF NOT EXISTS journal_entries (
    id TEXT PRIMARY KEY,
    item_id TEXT,
    txn_id TEXT UNIQUE NOT NULL,
    txn_date DATE NOT NULL,
    description TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    source_hash TEXT NOT NULL,
    transform_version INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)

CREATE TABLE IF NOT EXISTS journal_lines (
    id TEXT PRIMARY KEY,
    entry_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('debit', 'credit')),
    amount DECIMAL(15,2) NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (entry_id) REFERENCES journal_entries(id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
)

CREATE TABLE IF NOT EXISTS raw_transactions (
    id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    txn_id TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)

CREATE TABLE IF NOT EXISTS etl_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    item_id TEXT,
    period TEXT,
    success BOOLEAN NOT NULL,
    row_counts TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL
)