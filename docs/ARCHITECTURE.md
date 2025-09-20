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

### 1) Ledger (ETL + Canonical GL + Idempotency)

```mermaid
flowchart TD
  A["Plaid API (sandbox)"] --> E[Extract]
  E --> RAW[(raw_transactions)]
  E --> PA[(plaid_accounts)]
  E --> IACC[(ingest_accounts)]

  E --> T["Transform (COA rules → GL codes)"]
  T --> M{Mapping exists in<br/>account_links?}
  M -- No --> X1[[Fail-fast: unmapped plaid_account_id]]:::fail
  M -- Yes --> L[Load]

  L --> D{txn_id already seen?}
  D -- Yes --> U["Upsert/Skip (idempotent)"]
  D -- No --> JE[(journal_entries)] & JL[(journal_lines)]

  JL --> FK{accounts.id FK ok?}
  FK -- No --> X2[[Fail-fast: FK violation]]:::fail
  FK -- Yes --> EVT[(etl_events: ingest)]:::event

  classDef fail fill:#ffe6e6,stroke:#ff5a5a,color:#b30000,stroke-width:1.5px;
  classDef event fill:#eef7ff,stroke:#5aa9ff,color:#0b3d91,stroke-width:1.5px;

```

**What this highlights (for reviewers):** explicit mapping, FK-enforced ledger, duplicate protection via `txn_id`, and fail-fast philosophy.

---

### 2) Reconcile (AS-OF Ending Balance, Coverage, Item Scoping)

```mermaid
sequenceDiagram
    participant U as CLI: pfetl reconcile
    participant FS as Filesystem
    participant P as Plaid API (optional)
    participant DB as Postgres
    participant R as ReconcileCore (pure)

    U->>U: Parse args
    Note over U: Enforce one-of: balances-json XOR use-plaid-live

    alt Deterministic JSON
        U->>FS: Read balances.json ({plaid_account_id: ending_balance})
        FS-->>U: External balances
    else Live Plaid
        U->>P: /accounts/balance/get (item access token)
        P-->>U: External balances
    end

    U->>DB: SELECT M = mapped cash accounts for item_id
    DB-->>U: M

    U->>R: run_reconciliation(M, balances, period)
    R->>R: Check coverage: balances cover ALL accounts in M?

    alt Missing coverage
        R-->>U: Error: list missing plaid_account_ids
        U->>FS: write recon.json (failed checks)
        U->>DB: INSERT etl_events(reconcile, success=false, ...)
        U-->>U: exit 1
    else Coverage OK
        R->>DB: GL_asof per account (Σdebits − Σcredits) where txn_date ≤ period_end AND item_id=...
        DB-->>R: GL amounts
        R-->>U: Result {by_account, total_variance, gates}
        U->>FS: write recon.json
        U->>DB: INSERT etl_events(reconcile, success=passed, ...)
        U-->>U: exit 0/1
    end
```

*Drop a one-line caption right below:*
**Formula:** `GL_asof = Σ(debits) − Σ(credits)` over **mapped `is_cash=TRUE`** accounts with `txn_date ≤ period_end`, filtered by `item_id`.

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
* **Dual Population:** `ingest` populates both `ingest_accounts` (item-scoped mapping with composite PK) and `plaid_accounts` (canonical) for backward compatibility.
* **Item-Scoped Accounts:** `ingest_accounts` uses composite PRIMARY KEY (item_id, plaid_account_id) to enable multi-item management and prevent account ID collisions.
* **Account Mapping:** User-driven via `map-account` CLI command creates explicit `account_links` for audit-ready reconciliation.
