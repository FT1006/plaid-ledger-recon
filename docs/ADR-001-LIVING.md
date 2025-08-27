# ADR-001-LIVING: Canonical GL, Mapping Policy, Lineage, and Period Gates

Version: 1.1.0

Status: Accepted (Living)

Owners: Data/ETL

## Context

We ingest Plaid Sandbox transactions and produce an audit-ready GL in Postgres. Controls require:

- Canonical GL with FK integrity
- Explicit Plaid→GL mapping
- Deterministic transformation and reporting
- Reconciliation gates enforced per period

## Decisions

1) Canonical GL with FK Integrity

- `journal_lines.account_id` is a required FK to `accounts.id`.
- GL accounts are referenced by `code` (e.g., `Assets:Bank:Checking`).
- FK enforcement is tested on SQLite and PostgreSQL.

2) Explicit Plaid→GL Mapping (1:1)

- `plaid_accounts` stores raw Plaid account metadata.
- `account_links (plaid_account_id UNIQUE) → accounts.id` defines a 1:1 mapping.
- Mapping is created via CLI: `pfetl map-account --plaid-account-id ... --gl-code ...`.
- Auto-create GL accounts is intentionally disabled for MVP. The loader fails fast with a helpful hint.
  - Hint: set `PFETL_AUTO_CREATE_ACCOUNTS=true` to permit creation in a future milestone (not implemented yet).

3) Lineage Gate

- Each `journal_entry` records `source_hash` = SHA256(canonicalized raw JSON) and positive `transform_version` (INT).
- Missing or invalid lineage causes reconciliation failure.

4) Period Filtering for Reconciliation

- Periods use inclusive date windows by quarter: `2024Q1 = [2024-01-01, 2024-03-31]`, etc.
- Gates:
  - Entry balance: all entries balanced within period.
  - Cash variance: GL cash movement vs Plaid balances within ≤ $0.01 for the period.
  - Lineage: zero entries missing lineage fields in the period.
- Exit policy: success=0; any breach=1.

Demo/CI Override:
- To avoid live Plaid point-in-time balance drift in demos, the CLI accepts
  `--balances-json` to provide curated period balances. When supplied, Plaid
  API is not called for balances. Production deployments should rely on Plaid
  live balances (default behavior).

5) Determinism

- Transform skips pending transactions and uses stable sorting where applicable.
- Report HTML is deterministic (no timestamps/nonces). PDFs are generated and checked for existence only.

6) Observability

- `etl_events` records `load` events (row counts, timestamps, success).
- Reconciliation adds `reconcile` events with `period`, checks summary, timestamps, and success.

## Consequences

- Developers and auditors have a clear model, explicit mappings, and traceable lineage.
- Fail-fast policy on unmapped accounts protects audit integrity.
- Deterministic outputs enable snapshot testing and audit reproducibility.

## Change Log

- 1.0: Initial living ADR capturing FK move to `journal_lines.account_id`, Plaid mapping policy, lineage gate, period filtering, reconcile etl_event.
- 1.1.0: Confirmed implementation of reconcile ETL events persisted, period-filtering in all checks, canonical mapping policy via plaid_accounts + account_links tables with ingest_accounts marked as legacy shim.

## Amendment v1.1 — Demo/CI Balances Override

- To support reproducible demos and CI without Plaid credentials,
  the CLI now accepts `--balances-json <file>` on `pfetl reconcile`.
- When provided, Plaid balances API is bypassed, and the override JSON
  is treated as the authoritative source.
- Production deployments must continue to use live Plaid balances.
