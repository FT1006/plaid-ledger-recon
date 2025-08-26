# Controls & Auditability

## Invariants (enforced by tests and runbook)
- **Double-entry balance:** each `journal_entry` balances.
- **Idempotency:** re-ingest same dates → no new `journal_entries`.
- **Source identity:** `source_hash = SHA256(minified, key-sorted raw JSON)`.

## Versioning
- Each journal entry records a `transform_version` (currently = 1).
- This allows reproducibility across future transform logic changes.

## Operational gates (manual for now)
- Non-zero CLI exit → treat as failure.
- After ingest, run balance query (see RUNBOOK) → expect zero unbalanced rows.
- Inspect `etl_events` for counts and timestamps.

**Note**: `pfetl reconcile` command is planned but returns "🚧 Not yet implemented" and exits with code 1. Use the SQL query in RUNBOOK.md for manual balance verification until reconcile is fully implemented.

## Evidence
- `journal_entries.source_hash` is deterministic.
- `etl_events` captures inserted counts for traceability.