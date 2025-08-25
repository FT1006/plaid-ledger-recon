-- pfetl MVP schema — aligned to ADR-LIVING
-- Postgres 16+. UUIDs via pgcrypto, audit via raw landing + events.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1) Raw landing (for audit + hashing determinism)
CREATE TABLE IF NOT EXISTS raw_transactions (
  item_id       TEXT                 NOT NULL,
  txn_id        TEXT PRIMARY KEY,                       -- Plaid transaction_id
  as_json       JSONB                NOT NULL,          -- exact, compacted payload
  fetched_at    TIMESTAMPTZ          NOT NULL DEFAULT NOW()
);

-- 2) Canonical accounts (ledger accounts, with optional Plaid link)
CREATE TABLE IF NOT EXISTS accounts (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  item_id          TEXT,
  plaid_account_id TEXT UNIQUE,                         -- idempotent upsert key
  name             TEXT                NOT NULL,        -- e.g. "Assets:Bank:Checking"
  type             TEXT                NOT NULL CHECK (type IN ('asset','liability','equity','revenue','expense')),
  subtype          TEXT,
  currency         CHAR(3)             NOT NULL,
  active           BOOLEAN             NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_accounts_plaid ON accounts(plaid_account_id);
CREATE INDEX IF NOT EXISTS idx_accounts_type  ON accounts(type);

-- 3) Journal entries (double-entry header)
CREATE TABLE IF NOT EXISTS journal_entries (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  item_id           TEXT,
  txn_id            TEXT UNIQUE,                        -- ties back to source
  txn_date          DATE                NOT NULL,
  description       TEXT                NOT NULL,
  currency          CHAR(3)             NOT NULL,
  source_hash       BYTEA               NOT NULL,       -- SHA256(compact(raw_json))
  ingested_at       TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
  transform_version INT                 NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_journal_entries_date ON journal_entries(txn_date);
CREATE INDEX IF NOT EXISTS idx_journal_entries_txn  ON journal_entries(txn_id);

-- 4) Journal lines (double-entry lines; single amount + side)
CREATE TABLE IF NOT EXISTS journal_lines (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entry_id    UUID NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
  account_id  UUID NOT NULL REFERENCES accounts(id)       ON DELETE RESTRICT,
  side        TEXT NOT NULL CHECK (side IN ('debit','credit')),
  amount      NUMERIC(18,2) NOT NULL CHECK (amount > 0)
);

CREATE INDEX IF NOT EXISTS idx_journal_lines_entry   ON journal_lines(entry_id);
CREATE INDEX IF NOT EXISTS idx_journal_lines_account ON journal_lines(account_id);

-- 5) ETL events (append-only audit of pipeline runs)
CREATE TABLE IF NOT EXISTS etl_events (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type   TEXT        NOT NULL,         -- extract|transform|load|reconcile
  item_id      TEXT,
  input_hash   BYTEA,
  row_counts   JSONB,
  started_at   TIMESTAMPTZ,
  finished_at  TIMESTAMPTZ,
  success      BOOLEAN     NOT NULL
);