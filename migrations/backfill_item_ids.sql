-- Step B Migration: Backfill item_id from raw_transactions lineage
-- This script derives item_id values from raw_transactions to populate
-- the currently NULL item_id columns in ingest_accounts

BEGIN;

-- Verification: Check current state before backfill
SELECT 
    'Before backfill' as phase,
    COUNT(*) as total_ingest_accounts,
    COUNT(item_id) as accounts_with_item_id,
    COUNT(*) - COUNT(item_id) as accounts_missing_item_id
FROM ingest_accounts;

-- Backfill Strategy: Join raw_transactions to ingest_accounts via account_id
-- raw_transactions.as_json->>'account_id' = ingest_accounts.plaid_account_id

UPDATE ingest_accounts 
SET item_id = rt.item_id
FROM (
    SELECT DISTINCT 
        rt.item_id,
        rt.as_json->>'account_id' as account_id
    FROM raw_transactions rt
    WHERE rt.as_json->>'account_id' IS NOT NULL
) rt
WHERE ingest_accounts.plaid_account_id = rt.account_id
  AND ingest_accounts.item_id IS NULL;

-- Verification: Check state after backfill
SELECT 
    'After backfill' as phase,
    COUNT(*) as total_ingest_accounts,
    COUNT(item_id) as accounts_with_item_id,
    COUNT(*) - COUNT(item_id) as accounts_missing_item_id
FROM ingest_accounts;

-- Safety check: Ensure no duplicate (item_id, plaid_account_id) pairs
SELECT 
    item_id, 
    plaid_account_id, 
    COUNT(*) as duplicate_count
FROM ingest_accounts 
WHERE item_id IS NOT NULL
GROUP BY item_id, plaid_account_id 
HAVING COUNT(*) > 1;

-- Final verification: Show any remaining NULL item_ids for manual review
SELECT 
    plaid_account_id,
    name,
    'Manual review required' as status
FROM ingest_accounts 
WHERE item_id IS NULL;

COMMIT;