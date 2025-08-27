# Architecture

PFETL is a small, auditable ETL with MVP architecture:

1. **Extract** Plaid sandbox data (pagination, retry, optional raw landing)
2. **Transform** Plaid transactions into **balanced double-entry** journal entries
3. **Load** into Postgres with **FK integrity**, **idempotency**, and **ETL events**
4. **Reconcile** period results against Plaid balances with strict gates
5. **Report** deterministic HTML (+ PDF existence)

*Note: Raw transaction landing provides audit lineage - compacted JSON stored with `source_hash = SHA256(compact JSON)` carried forward to journal entries for traceability.*

```mermaid
flowchart TB
  U[Plaid Sandbox] --> E[Extract]

  subgraph DB [PostgreSQL]
    RAW[(raw_transactions)]
    ACC[(accounts)]
    JE[(journal_entries)]
    JL[(journal_lines)]
    EVT[(etl_events)]
  end
  
  subgraph MAP [Account Mapping]
    PA[(plaid_accounts)]
    AL[(account_links)]
    IACC[(ingest_accounts)]
  end
  
  E --> PA
  E --> IACC
  E --> RAW


  E --> T[Transform] 
  T --> L[Load]
  
  T --> JE

  L --> ACC
  L --> JE
  L --> JL
  L --> EVT
  
  RAW --> T
  
  ACC --> AL
  CLI[map-account] --> AL
  PA --> AL
  
  subgraph CTRL [Controls & Reports]
    R[Reconcile]
    REP[Reports]
  end
  
  JE --> R
  JL --> R
  AL --> R
  U --> R
  
  JE --> REP
  JL --> REP
  ACC --> REP
  
  R --> OUT1[(recon.json)]
  REP --> OUT2[(HTML/PDF)]
```

## Key properties

* **Pagination & Retry:** bounded attempts on 429/5xx with jittered backoff.
* **Determinism:** stable ordering; canonicalized JSON hashing for `source_hash`.
* **Idempotency:**

  * Entries deduped by `txn_id`.
  * Plaid accounts cached in `ingest_accounts` (staging), **canonical** metadata in `plaid_accounts`.
* **Canonical GL & FKs:**

  * `journal_lines.account_id → accounts.id` (FK enforced).
  * **Mappings** via `account_links` (Plaid → GL 1:1).
  * **Fail fast** on unmapped GL codes, with `PFETL_AUTO_CREATE_ACCOUNTS` hint (auto-create intentionally disabled in MVP).
* **Lineage (gate):** `source_hash` (SHA256 of compact raw JSON) and positive `transform_version` **required** on all entries, with optional `raw_transactions` audit trail.
* **Reconciliation gates (period-filtered by `txn_date`, inclusive):**

  * All entries balanced (`∑debits == ∑credits`).
  * Cash variance across **mapped cash accounts** ≤ **0.01** vs Plaid balances.
  * Missing lineage count must be zero.
  * Any breach → non-zero exit.
* **Reports:** Deterministic HTML (`bs_2024Q1.html`, `cf_2024Q1.html`) with graceful PDF fallback when WeasyPrint unavailable.
* **CLI Interface:** `init-db`, `onboard`, `ingest`, `map-account`, `reconcile`, `report`. Sandbox-only onboarding in MVP.
* **Dual Population:** `ingest` populates both `ingest_accounts` (legacy shim) and `plaid_accounts` (canonical) for backward compatibility.
* **Account Mapping:** User-driven via `map-account` CLI command creates explicit `account_links` for audit-ready reconciliation.
