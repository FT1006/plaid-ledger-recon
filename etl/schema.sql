-- pfetl M3 schema — Canonical GL with FK integrity (Task 9)
-- Postgres 16+. UUIDs via pgcrypto, audit via raw landing + events.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- CANONICAL TABLES for production GL system

-- Canonical GL chart of accounts
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('asset','liability','equity','revenue','expense')),
    subtype TEXT,
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    is_cash BOOLEAN NOT NULL DEFAULT false,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounts_code ON accounts(code);
CREATE INDEX IF NOT EXISTS idx_accounts_type ON accounts(type);
CREATE INDEX IF NOT EXISTS idx_accounts_is_cash ON accounts(is_cash);

-- Raw Plaid account metadata
CREATE TABLE IF NOT EXISTS plaid_accounts (
    plaid_account_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,        -- depository/credit/... (Plaid domain)
    subtype TEXT NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Mapping from Plaid accounts to GL accounts (1:1)
CREATE TABLE IF NOT EXISTS account_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plaid_account_id TEXT UNIQUE NOT NULL REFERENCES plaid_accounts(plaid_account_id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Legacy shim table for backward compatibility (will be deprecated)
-- TODO: Remove after migration complete
-- Step B: Composite PRIMARY KEY for proper item-scoped account management
CREATE TABLE IF NOT EXISTS ingest_accounts (
  item_id TEXT NOT NULL,
  plaid_account_id TEXT NOT NULL,
  name TEXT NOT NULL,
  type TEXT NOT NULL,        -- depository/credit/... (Plaid domain)
  subtype TEXT NOT NULL,
  currency TEXT NOT NULL,
  PRIMARY KEY (item_id, plaid_account_id)
);

CREATE INDEX IF NOT EXISTS idx_ingest_accounts_item_id ON ingest_accounts(item_id);

-- 1) Raw landing (for audit + hashing determinism)
CREATE TABLE IF NOT EXISTS raw_transactions (
  item_id       TEXT                 NOT NULL,
  txn_id        TEXT PRIMARY KEY,                       -- Plaid transaction_id
  as_json       JSONB                NOT NULL,          -- exact, compacted payload
  fetched_at    TIMESTAMPTZ          NOT NULL DEFAULT NOW()
);

-- Legacy accounts table (now superseded by canonical accounts above)
-- TODO: Remove after migration complete  
CREATE TABLE IF NOT EXISTS old_accounts (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  item_id          TEXT,
  plaid_account_id TEXT UNIQUE,                         -- idempotent upsert key
  name             TEXT                NOT NULL,        -- e.g. "Assets:Bank:Checking"
  type             TEXT                NOT NULL CHECK (type IN ('asset','liability','equity','revenue','expense')),
  subtype          TEXT,
  currency         CHAR(3)             NOT NULL,
  active           BOOLEAN             NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_old_accounts_plaid ON old_accounts(plaid_account_id);
CREATE INDEX IF NOT EXISTS idx_old_accounts_type  ON old_accounts(type);

-- 3) Journal entries (double-entry header) - Updated with required constraints
CREATE TABLE IF NOT EXISTS journal_entries (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  item_id           TEXT,
  txn_id            TEXT UNIQUE NOT NULL,               -- REQUIRED: ties back to source
  txn_date          DATE                NOT NULL,
  description       TEXT                NOT NULL,
  currency          CHAR(3)             NOT NULL,
  source_hash       TEXT                NOT NULL,       -- REQUIRED: SHA256(compact(raw_json)) as hex
  ingested_at       TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
  transform_version INTEGER             NOT NULL        -- REQUIRED: ADR §4 compliance
);

CREATE INDEX IF NOT EXISTS idx_journal_entries_date ON journal_entries(txn_date);
CREATE INDEX IF NOT EXISTS idx_journal_entries_txn  ON journal_entries(txn_id);
CREATE INDEX IF NOT EXISTS idx_journal_entries_hash ON journal_entries(source_hash);
-- Performance index for item-scoped reconciliation queries
CREATE INDEX IF NOT EXISTS idx_journal_entries_item_date ON journal_entries(item_id, txn_date);

-- 4) Journal lines - Updated with FK to canonical GL accounts
CREATE TABLE IF NOT EXISTS journal_lines (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entry_id UUID NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
  account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,  -- FK to canonical GL
  side TEXT NOT NULL CHECK (side IN ('debit','credit')),
  amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0),  -- Increased precision per spec
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_journal_lines_entry ON journal_lines(entry_id);
CREATE INDEX IF NOT EXISTS idx_journal_lines_account ON journal_lines(account_id);

-- Legacy journal_lines for migration (will be dropped)
-- TODO: Remove after migration complete
CREATE TABLE IF NOT EXISTS old_journal_lines (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entry_id UUID NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
  account TEXT NOT NULL,      -- free-form "Expenses:Dining:Restaurants"
  side TEXT NOT NULL CHECK (side IN ('debit','credit')),
  amount NUMERIC(15,2) NOT NULL CHECK (amount >= 0)
);

CREATE INDEX IF NOT EXISTS idx_old_journal_lines_entry ON old_journal_lines(entry_id);

-- 5) ETL events (append-only audit of pipeline runs)
CREATE TABLE IF NOT EXISTS etl_events (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type   TEXT        NOT NULL,         -- extract|transform|load|reconcile
  item_id      TEXT,
  period       TEXT,                         -- nullable for non-period ops (e.g., 2024Q1)
  input_hash   BYTEA,
  row_counts   JSONB,
  started_at   TIMESTAMPTZ,
  finished_at  TIMESTAMPTZ,
  success      BOOLEAN     NOT NULL
);

-- SEED DATA: Minimal deterministic GL chart for MVP
-- These accounts must exist for transform/load to work

INSERT INTO accounts (code, name, type, is_cash) VALUES
    ('Assets:Bank:Checking', 'Bank Checking Account', 'asset', true),
    ('Liabilities:CreditCard', 'Credit Card Liability', 'liability', false),
    ('Expenses:Dining:Restaurants', 'Restaurant Expenses', 'expense', false),
    ('Expenses:Miscellaneous', 'Miscellaneous Expenses', 'expense', false),
    ('Income:Salary', 'Salary Income', 'revenue', false),
    ('Income:Miscellaneous', 'Miscellaneous Income', 'revenue', false)
ON CONFLICT (code) DO NOTHING;

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_etl_events_type ON etl_events(event_type);
CREATE INDEX IF NOT EXISTS idx_etl_events_started ON etl_events(started_at);