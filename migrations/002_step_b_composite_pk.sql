-- Step B Migration: Enforce composite PRIMARY KEY on ingest_accounts
-- This migration completes the transition to composite (item_id, plaid_account_id) PK
-- Prerequisites: 
-- 1. Run backfill_item_ids.sql first to populate item_id values
-- 2. Verify no NULL item_ids remain

BEGIN;

-- Pre-migration verification: Ensure data is ready
DO $$
DECLARE
    null_count INTEGER;
    duplicate_count INTEGER;
BEGIN
    -- Check for NULL item_ids
    SELECT COUNT(*) INTO null_count 
    FROM ingest_accounts 
    WHERE item_id IS NULL;
    
    IF null_count > 0 THEN
        RAISE EXCEPTION 'Cannot proceed: % rows have NULL item_id. Run backfill_item_ids.sql first.', null_count;
    END IF;
    
    -- Check for duplicates that would violate composite PK
    SELECT COUNT(*) INTO duplicate_count 
    FROM (
        SELECT item_id, plaid_account_id, COUNT(*) 
        FROM ingest_accounts 
        GROUP BY item_id, plaid_account_id 
        HAVING COUNT(*) > 1
    ) duplicates;
    
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Cannot proceed: % duplicate (item_id, plaid_account_id) pairs exist.', duplicate_count;
    END IF;
    
    RAISE NOTICE 'Pre-migration checks passed: % rows ready for composite PK', 
        (SELECT COUNT(*) FROM ingest_accounts);
END $$;

-- Step 1: Drop existing primary key constraint
ALTER TABLE ingest_accounts DROP CONSTRAINT IF EXISTS ingest_accounts_pkey;

-- Step 2: Set item_id to NOT NULL (now safe after backfill)
ALTER TABLE ingest_accounts ALTER COLUMN item_id SET NOT NULL;

-- Step 3: Add composite primary key
ALTER TABLE ingest_accounts ADD CONSTRAINT ingest_accounts_pkey 
    PRIMARY KEY (item_id, plaid_account_id);

-- Step 4: Update index strategy for new composite PK
-- The composite PK automatically creates an index on (item_id, plaid_account_id)
-- Keep the existing single-column index for item_id-only queries
-- (idx_ingest_accounts_item_id already exists from Step A)

-- Verification: Confirm new constraints
SELECT 
    'Migration complete' as status,
    COUNT(*) as total_rows,
    COUNT(DISTINCT (item_id, plaid_account_id)) as unique_composite_keys
FROM ingest_accounts;

-- Show the new primary key constraint
SELECT 
    conname as constraint_name,
    contype as constraint_type,
    array_agg(attname ORDER BY array_position(conkey, attnum)) as columns
FROM pg_constraint c
JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
WHERE c.conrelid = 'ingest_accounts'::regclass 
  AND c.contype = 'p'
GROUP BY conname, contype;

COMMIT;