# Schema Overview (canonical)

This project uses a canonical GL with FK integrity and explicit Plaid→GL mappings. A small shim table remains for ingestion compatibility.

## Tables

### `accounts` (canonical GL chart)
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `code` TEXT UNIQUE NOT NULL
- `name` TEXT NOT NULL
- `type` TEXT NOT NULL CHECK (type IN ('asset','liability','equity','revenue','expense'))
- `subtype` TEXT
- `currency` CHAR(3) NOT NULL DEFAULT 'USD'
- `is_cash` BOOLEAN NOT NULL DEFAULT false
- `active` BOOLEAN NOT NULL DEFAULT true

Indexes: `idx_accounts_code`, `idx_accounts_type`, `idx_accounts_is_cash`.

### `plaid_accounts` (raw Plaid metadata)
- `plaid_account_id` TEXT PRIMARY KEY
- `name` TEXT NOT NULL
- `type` TEXT NOT NULL
- `subtype` TEXT NOT NULL
- `currency` CHAR(3) NOT NULL DEFAULT 'USD'

### `account_links` (explicit 1:1 Plaid→GL mapping)
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `plaid_account_id` TEXT UNIQUE NOT NULL REFERENCES `plaid_accounts`(plaid_account_id) ON DELETE CASCADE
- `account_id` UUID NOT NULL REFERENCES `accounts`(id) ON DELETE RESTRICT
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW()

### `ingest_accounts` (shim; legacy staging)
- `plaid_account_id` TEXT PRIMARY KEY
- `name` TEXT NOT NULL
- `type` TEXT NOT NULL
- `subtype` TEXT NOT NULL
- `currency` TEXT NOT NULL

### `journal_entries`
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `item_id` TEXT
- `txn_id` TEXT UNIQUE NOT NULL
- `txn_date` DATE NOT NULL
- `description` TEXT NOT NULL
- `currency` CHAR(3) NOT NULL
- `source_hash` TEXT NOT NULL (SHA256 of compact, key-sorted raw JSON)
- `ingested_at` TIMESTAMPTZ NOT NULL DEFAULT NOW()
- `transform_version` INTEGER NOT NULL

Indexes: `idx_journal_entries_date`, `idx_journal_entries_txn`, `idx_journal_entries_hash`.

### `journal_lines`
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `entry_id` UUID NOT NULL REFERENCES `journal_entries`(id) ON DELETE CASCADE
- `account_id` UUID NOT NULL REFERENCES `accounts`(id) ON DELETE RESTRICT
- `side` TEXT NOT NULL CHECK (side IN ('debit','credit'))
- `amount` NUMERIC(18,2) NOT NULL CHECK (amount >= 0)

Indexes: `idx_journal_lines_entry`, `idx_journal_lines_account`.

### `raw_transactions`
- `item_id` TEXT NOT NULL
- `txn_id` TEXT PRIMARY KEY
- `as_json` JSONB NOT NULL (compacted JSON)
- `fetched_at` TIMESTAMPTZ NOT NULL DEFAULT NOW()

### `etl_events` (append-only audit)
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `event_type` TEXT NOT NULL (e.g., extract|transform|load|reconcile)
- `item_id` TEXT
- `input_hash` BYTEA
- `row_counts` JSONB
- `started_at` TIMESTAMPTZ
- `finished_at` TIMESTAMPTZ
- `success` BOOLEAN NOT NULL

Indexes: `idx_etl_events_type`, `idx_etl_events_started`.

## Invariants
- Double-entry: each journal entry balances (∑debits == ∑credits).
- Idempotency: re-ingesting the same window does not increase `journal_entries` (dedupe by `txn_id`).
- FK integrity: `journal_lines.account_id → accounts.id`; Plaid mappings via `account_links`.
- Lineage: every `journal_entry` has non-empty `source_hash` and positive `transform_version`.
