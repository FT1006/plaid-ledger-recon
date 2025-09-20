-- SQLite configuration to mirror Postgres behavior
-- Enforces FK constraints and sets up deterministic environment

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = memory;
PRAGMA mmap_size = 268435456;

-- Mirror Postgres index names for compatibility
-- (SQLite will create indexes even if columns don't exist yet)
CREATE INDEX IF NOT EXISTS idx_journal_entries_item_date ON journal_entries(item_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_journal_entries_txn_date ON journal_entries(txn_date);
CREATE INDEX IF NOT EXISTS idx_journal_entries_txn_id ON journal_entries(txn_id);
CREATE INDEX IF NOT EXISTS idx_journal_entries_source_hash ON journal_entries(source_hash);
CREATE INDEX IF NOT EXISTS idx_journal_lines_entry_id ON journal_lines(entry_id);
CREATE INDEX IF NOT EXISTS idx_journal_lines_account_id ON journal_lines(account_id);
CREATE INDEX IF NOT EXISTS idx_accounts_code ON accounts(code);
CREATE INDEX IF NOT EXISTS idx_plaid_accounts_plaid_account_id ON plaid_accounts(plaid_account_id);
CREATE INDEX IF NOT EXISTS idx_account_links_plaid_account_id ON account_links(plaid_account_id);
CREATE INDEX IF NOT EXISTS idx_account_links_account_id ON account_links(account_id);
CREATE INDEX IF NOT EXISTS idx_etl_events_event_type ON etl_events(event_type);
CREATE INDEX IF NOT EXISTS idx_etl_events_item_id ON etl_events(item_id);
CREATE INDEX IF NOT EXISTS idx_etl_events_period ON etl_events(period);