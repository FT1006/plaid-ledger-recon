# Architecture

PFETL is a small, auditable ETL with MVP architecture:
1) **Extract** from Plaid sandbox (pagination, retry)
2) **Transform** → balanced double-entry (bypasses raw landing for MVP)
3) **Load** → Postgres with idempotency + ETL events

*Note: Raw transaction landing is available but not used in current CLI workflow.*

```mermaid
flowchart LR
  U[Plaid Sandbox] -->|httpx| X[Extract]
  X -->|plaid txns| T[Transform]
  T -->|journal entries + lines| L[Load]
  L -->|INSERT/UPSERT| DB[(PostgreSQL)]
  L -->|row counts| EVT[etl_events]
  X -->|account metadata| L
  X -.->|raw JSON available| RAW[raw_transactions]

  subgraph "Database (Current MVP)"
  DB --- IACC[(ingest_accounts)]
  DB --- JE[(journal_entries)]
  DB --- JL[(journal_lines)]
  DB --- EVT[etl_events]
  end
  
  subgraph "Future Architecture"
  ACC[(accounts)]
  RAW
  end
```

## Key properties

* **Pagination & Retry:** bounded attempts on 429/5xx with jittered backoff
* **Determinism:** stable ordering `(posted_date, txn_id)`; canonical JSON hashing
* **Idempotency:** dedupe by `txn_id`; account upsert by `plaid_account_id` in `ingest_accounts`
* **Chart of Accounts:** YAML-based mapping from Plaid types to GL account names
* **CLI Interface:** `init-db`, `onboard`, `ingest` commands with sandbox-only operation
* **Shim Tables:** Uses `ingest_accounts` for MVP, bypassing full GL constraints