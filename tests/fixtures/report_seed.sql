-- Minimal seed data for report testing
-- Creates a complete double-entry scenario for 2024Q1

-- Clear existing data
DELETE FROM journal_lines;
DELETE FROM journal_entries;
DELETE FROM plaid_accounts;
DELETE FROM account_links;

-- Insert test journal entry
INSERT INTO journal_entries (id, item_id, txn_id, txn_date, description, currency, source_hash, transform_version) VALUES
    ('11111111-1111-1111-1111-111111111111', 'test_item', 'test_txn_001', '2024-01-15', 'Test Restaurant Purchase', 'USD', 'abc123def456', 1);

-- Insert journal lines (balanced: $25 expense, $25 cash decrease)
INSERT INTO journal_lines (entry_id, account_id, side, amount) VALUES
    ('11111111-1111-1111-1111-111111111111', 
     (SELECT id FROM accounts WHERE code = 'Expenses:Dining:Restaurants'), 
     'debit', 25.00),
    ('11111111-1111-1111-1111-111111111111', 
     (SELECT id FROM accounts WHERE code = 'Assets:Bank:Checking'), 
     'credit', 25.00);

-- Insert Plaid account for reconciliation
INSERT INTO plaid_accounts (plaid_account_id, name, type, subtype) VALUES
    ('plaid_checking_123', 'Test Checking Account', 'depository', 'checking');

-- Link Plaid account to GL cash account
INSERT INTO account_links (plaid_account_id, account_id) VALUES
    ('plaid_checking_123', (SELECT id FROM accounts WHERE code = 'Assets:Bank:Checking'));