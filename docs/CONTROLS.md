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
- **Demo override:** for demos/CI, `pfetl reconcile --balances-json balances.json` can supply
  curated period balances instead of live Plaid current balances (prod behavior unchanged).

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
- `pfetl reconcile` validates all gates → exit 0 (pass) or 1 (fail)
- Inspect `etl_events` and `build/recon.json` for audit trail

## Evidence
- `journal_entries.source_hash` links to raw transactions
- `account_links` shows explicit Plaid→GL mappings
- `etl_events` captures operation history
