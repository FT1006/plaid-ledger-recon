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
- **Exit policy:** any breach → non-zero exit with structured JSON
- **Two-mode operation:**
  - **Deterministic mode:** `pfetl reconcile --balances-json <path>` uses curated balances (demos/CI)
  - **Operational mode:** `pfetl reconcile --use-plaid-live` uses live Plaid API balances (production)
  - **One-of rule:** exactly one data source required; command fails if both or neither specified (exit 2)

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

## Operational gates
- Non-zero CLI exit → control failure (reconcile/mapping/config errors)
- `pfetl reconcile` validates all gates → exit 0 (pass), 1 (fail), or 2 (invalid usage)
- Inspect `etl_events` and `build/recon.json` for audit trail

### Common failure scenarios:
- **Missing data source:** `Must specify exactly one: --balances-json OR --use-plaid-live`
- **Coverage failure:** `Missing balance data for accounts: [plaid_account_xyz]` → JSON file incomplete
- **Item scope failure:** `Cannot scope by item_id yet. Ingest this item first` → Run `pfetl ingest`

## Evidence
- `journal_entries.source_hash` links to raw transactions
- `account_links` shows explicit Plaid→GL mappings
- `etl_events` captures operation history
