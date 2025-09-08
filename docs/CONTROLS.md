# Controls & Auditability

## Invariants (enforced)
- **Double-entry balance:** each `journal_entry` balances (∑debits = ∑credits)
- **Idempotency:** re-ingest same dates → no new `journal_entries` (dedupe by `txn_id`)
- **Source identity:** `source_hash = SHA256(compact, key-sorted raw JSON)`
- **FK integrity:** `journal_lines.account_id → accounts.id` enforced
- **Lineage required:** all entries have `source_hash` + positive `transform_version`

## Reconciliation gates
- **Cash variance:** mapped accounts must match Plaid balances within ±$0.01 (inclusive: `abs(variance) <= 0.01` passes)
- **Item scoping:** only entries with `journal_entries.item_id == <item_id>` are considered in calculations
- **Period filtering:** uses inclusive `[from, to]` window on `txn_date`; AS-OF calculations include all entries where `txn_date <= period_end`
- **Coverage semantics:**
  - Coverage check requires all mapped `is_cash=TRUE` accounts to be present in provided balances
  - Extra balances (for unknown or non-cash accounts) are **ignored** - they do not cause coverage failure  
  - Variance is computed **only** over the intersection of (mapped cash accounts ∩ provided balances)
  - Missing mapped cash accounts → coverage failure → exit 1
- **Exit policy:** any breach → non-zero exit with structured JSON
- **Side-effect boundary:** `run_reconciliation()` is pure (no DB writes); the CLI command `pfetl reconcile` writes `etl_events` and persists `recon.json`.
- **Two-mode operation:**
  - **Deterministic mode:** `pfetl reconcile --balances-json <path>` uses curated balances (demos/CI)
  - **Operational mode:** `pfetl reconcile --use-plaid-live` uses live Plaid API balances (production)
  - **One-of rule:** exactly one data source required; command fails if both or neither specified (exit 2)

## Required Indexes (Performance Contract)
- **`journal_entries(item_id, txn_date)`** → `idx_journal_entries_item_date` (non-unique, btree)
  - **Contract:** Item-scoped reconciliation queries must use this compound index
  - **Query:** `WHERE je.item_id = :item_id AND je.txn_date <= :period_end`
  - **Without this:** Full table scan on large datasets → unacceptable performance
- **Single-column indexes maintained:** `txn_date`, `txn_id`, `source_hash` (functional requirements)

## Determinism
- **Report hashes:** same input produces identical HTML output
- **Transform stability:** same raw data → same journal entries
- **ETL events:** all operations logged with timestamps + row counts

## Example Reconciliation JSON

```json
{
  "period": "2024Q1",
  "success": true,
  "checks": {
    "entry_balance": {"passed": true, "unbalanced_entries": []},
    "cash_variance": {"passed": true, "variance": 0.0, "tolerance": 0.01},
    "lineage": {"passed": true, "missing_lineage": 0}
  }
}
```

## Exit Code Standards
- **0**: Success - operation completed without issues
- **1**: Gate/operational failure - reconciliation failed, no accounts found, missing token, etc.
- **2**: Usage/contract violation - missing required args, invalid one-of combinations, malformed input
- **>2**: Unexpected system errors - database unreachable, file I/O errors, etc.

## Operational gates
- Non-zero CLI exit → control failure (reconcile/mapping/config errors)  
- `pfetl reconcile` validates all gates → exit 0 (pass), 1 (fail), or 2 (invalid usage)
- Inspect `etl_events` and `build/recon.json` for audit trail

### Common failure scenarios (with frozen error messages):
- **Missing data source:** `"Provide exactly one of --balances-json or --use-plaid-live."` → exit 2 (usage error)
- **Missing credentials:** `"PLAID_ACCESS_TOKEN not set in environment"` → exit 1 (operational failure)
- **Coverage failure:** `"Missing balance data for accounts: [plaid_account_xyz]"` → JSON file incomplete or live API missing mapped accounts → exit 1
- **Scoping unavailable:** `"Cannot scope by item_id yet. Ingest this item first."` → Run `pfetl ingest` → exit 1
- **No accounts for item:** `"No Plaid accounts found for item_id: <id>"` → Check item exists → exit 1
- **Environment pollution:** Tests fail due to ambient `.env` credentials → Set `PFETL_SKIP_DOTENV=1` in test environments

### Currency scope:
- MVP assumes **single-currency (USD)**
- If multi-currency support added later, balances must match account currency or be converted at reconciliation time

### List-accounts scoping:
- Uses `ingest_accounts` table JOIN to `plaid_accounts` for item filtering
- If `ingest_accounts` missing/empty → fail-fast with scoping error (no "show all" fallback)

## Evidence
- `journal_entries.source_hash` links to raw transactions
- `account_links` shows explicit Plaid→GL mappings
- `etl_events` captures operation history
