# Schema Overview (current)

> This project uses **shim tables** for journal lines to keep ingestion simple and idempotent.

## Tables

### `ingest_accounts` (shim for Plaid)
- `plaid_account_id` TEXT PRIMARY KEY
- `name` TEXT NOT NULL
- `type` TEXT NOT NULL
- `subtype` TEXT NOT NULL
- `currency` TEXT NOT NULL

### `accounts` (ledger accounts)
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `item_id` TEXT
- `plaid_account_id` TEXT UNIQUE
- `name` TEXT NOT NULL
- `type` TEXT NOT NULL CHECK (type IN ('asset','liability','equity','revenue','expense'))
- `subtype` TEXT
- `currency` CHAR(3) NOT NULL
- `active` BOOLEAN NOT NULL DEFAULT TRUE

**Indexes:**
- `idx_accounts_plaid` ON (plaid_account_id)
- `idx_accounts_type` ON (type)

### `journal_entries`
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `item_id` TEXT
- `txn_id` TEXT UNIQUE
- `txn_date` DATE NOT NULL
- `description` TEXT NOT NULL
- `currency` CHAR(3) NOT NULL
- `source_hash` TEXT NOT NULL (SHA256 of compact raw JSON as hex)
- `ingested_at` TIMESTAMPTZ NOT NULL DEFAULT NOW()
- `transform_version` INT NOT NULL

**Indexes:**
- `idx_journal_entries_date` ON (txn_date)
- `idx_journal_entries_txn` ON (txn_id)

### `journal_lines`
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `entry_id` UUID NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE
- `account` TEXT NOT NULL
- `side` TEXT NOT NULL CHECK (side IN ('debit','credit'))
- `amount` NUMERIC(15,2) NOT NULL CHECK (amount >= 0)

**Indexes:**
- `idx_journal_lines_entry` ON (entry_id)

### `raw_transactions`
- `item_id` TEXT NOT NULL
- `txn_id` TEXT PRIMARY KEY
- `as_json` JSONB NOT NULL
- `fetched_at` TIMESTAMPTZ NOT NULL DEFAULT NOW()

### `etl_events`
- `id` UUID PRIMARY KEY DEFAULT gen_random_uuid()
- `event_type` TEXT NOT NULL
- `item_id` TEXT
- `input_hash` BYTEA
- `row_counts` JSONB
- `started_at` TIMESTAMPTZ
- `finished_at` TIMESTAMPTZ
- `success` BOOLEAN NOT NULL

## Invariants
- Every `journal_entry` balances (sum debits == sum credits)
- Re-ingest of the same window does not increase `journal_entries` count
- All foreign key relationships are enforced with CASCADE deletes