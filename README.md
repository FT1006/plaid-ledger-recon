
# Plaid Ledger Recon

**Plaid → Postgres → Reconciled Balance Sheet & Cash Flow**
*A minimal, audit-ready financial automation demo.*

## 🚀 Quick Start Success = 2 Commands, ≤90s, Produces recon.json + HTML

```bash
# Tier 0: Offline Demo (SQLite + fixtures, no dependencies, ≤90s)
make demo-offline

# Tier 1: Docker Demo (Postgres + fixtures, deterministic output)
make demo-docker
```

**Results**: `build/demo_recon.json` (reconciliation), `build/demo_bs_2024q1.html` (balance sheet), `build/demo_cf_2024q1.html` (cash flow)

---

`plaid-ledger-recon` is a **CLI tool** that ingests bank data from the **Plaid Sandbox**, transforms it into a **double-entry ledger**, enforces **reconciliation gates**, and generates **deterministic reports** (Balance Sheet, Cash Flow) in HTML and PDF.

It's designed to feel like **"one-command audit automation"**: simple to onboard, transparent in failure, and reproducible.

## Executive Engineering Summary

* **Contract:** Reconcile compares **GL cash ending balances (AS-OF period end)** to **external balances** (Plaid or JSON) across **all mapped `is_cash=TRUE` accounts**. Pass if `abs(total_variance) ≤ 0.01`.
* **Scope:** Plaid Sandbox → Postgres → Canonical GL (FKs) → Reconcile → Deterministic HTML/PDF.
* **Integrity:** `journal_lines.account_id` is an FK into `accounts`; every `journal_entry` is double-entry balanced; lineage (`source_hash`, `transform_version`) required.
* **Idempotency:** Re-ingest same window does not create duplicates (dedupe by `txn_id`); `source_hash` = SHA256 of canonical raw JSON.
* **Mapping policy:** 1:1 `plaid_account_id` → `accounts.id` via `account_links`; explicit mapping required; fail-fast if missing.
* **Item scoping:** Reconcile reads only `journal_entries` with the requested `item_id`.
* **Two reconcile modes (strict one-of):** `--balances-json` (deterministic demo/CI) **or** `--use-plaid-live` (operational).
* **Side-effects:** `run_reconciliation()` is pure; the CLI writes `recon.json` and an `etl_events` row.
* **Determinism:** HTML reports are stable/hashes snapshot-tested; PDFs are existence-checked only.
* **Limits:** Single currency (USD), single Postgres node, no orchestration yet (Make/CLI), Plaid Sandbox only.

### Key Decisions & Trade-offs

| Decision             | Chosen                                           | Alternatives            | Why                                                                      |
| -------------------- | ------------------------------------------------ | ----------------------- | ------------------------------------------------------------------------ |
| Cash parity semantic | **AS-OF ending balance** at `period_end` | Period**delta**   | Auditor-clear, aligns with bank “current” balance & your demo balances |
| Mapping policy       | **Explicit 1:1** via `account_links`     | Auto-map heuristics     | Auditability; fail-fast instead of silent mis-mappings                   |
| Canonical GL         | **FK’d accounts→journal\_lines**         | Free-text account names | Enforce integrity; enables reliable controls & queries                   |
| Balances source      | **Strict one-of** JSON or live Plaid       | Mixed / fallback        | Determinism in CI; clarity operationally                                 |
| Side-effects         | **CLI only** writes events                 | Core mutates            | Testable pure function; clean boundary                                   |
| Item scoping         | **Required**                               | Global                  | Prevents cross-item contamination                                        |
| Determinism          | **HTML hashed**                            | Snapshot PDFs           | HTML is stable, PDFs aren’t                                             |

### Control Gates (what passes/fails)

| Gate                  | Scope                                         | Pass Condition                                      | On Failure                                                      |
| --------------------- | --------------------------------------------- | --------------------------------------------------- | --------------------------------------------------------------- |
| Entry balance         | Per `journal_entry`                         | Σdebits == Σcredits                               | Exit 1;`recon.json.checks.entry_balance` lists offenders      |
| Cash variance (AS-OF) | All**mapped `is_cash=TRUE`** accounts | `abs(variance) ≤ 0.01`                           | Exit 1;`recon.json.checks.cash_variance` with per-acct deltas |
| Coverage              | Same                                          | JSON/live must cover**all mapped cash accts** | Exit 1; error names missing accounts                            |
| Lineage presence      | All entries in scope                          | `source_hash` & `transform_version` present     | Exit 1; count of missing lineage                                |
| Item scoping          | Reconcile queries                             | Only `item_id = <arg>` considered                 | Exit 1; contract violation                                      |

## Under the Hood

* Extract via **httpx** (Plaid sandbox, with pagination + retry).
* Transform via **rule-based mappings** to double-entry ledger.
* Load into **Postgres** with `source_hash` + `etl_events` audit trail.
* CLI `pfetl reconcile` writes a `'reconcile'` row in `etl_events` (period, checks, success). The pure `run_reconciliation()` function remains side‑effect free.
* **FK integrity enforcement** - journal_lines.account_id → accounts.id
* **Explicit account mapping** - plaid_accounts → account_links → accounts
* Reports rendered with **Jinja2 + WeasyPrint** (graceful PDF fallback).
* CI fails if reconciliation gates or determinism checks break.

## Architecture

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

**Implementation:** Python CLI + PostgreSQL with FK integrity, deterministic reports, audit trail (source_hash), and reconciliation gates.

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

### Failure Modes & Observability

* **Missing mapping** → loader fails fast with message (and hint if you allow auto-create out of prod).
* **Partial balances JSON** → immediate coverage error listing missing `plaid_account_id`s (no silent 0s).
* **Unbalanced entries / missing lineage** → gate fails with counts and offending txn ids.
* **ETL events row** (CLI):

  ```json
  {"event_type":"reconcile","item_id":"...","period":"2024Q1","success":true,"row_counts":{"entries":N,"lines":2N},"started_at":"...","finished_at":"..."}
  ```
* **Recon output (excerpt):**

  ```json
  {
    "period":"2024Q1",
    "checks":{
      "cash_variance":{
        "passed":true,
        "total_variance":0.00,
        "by_account":[
          {"plaid_account_id":"RnWr...","gl_asof":1147.62,"ext_asof":1147.62,"variance":0.00}
        ]
      }
    }
  }
  ```

### Performance & Indexes (tiny but signals judgment)

* **Recommended indexes:**

  * `journal_entries(item_id, txn_date)`
  * `journal_lines(account_id)`
  * `accounts(code)` unique, `account_links(plaid_account_id)` unique
* **Envelope:** Designed & tested for toy-to-small datasets (10k–100k txns) on a laptop Postgres.

### Known Limitations / Next Steps

* No Airflow/ADF (Make/CLI only), single-node Postgres, USD-only, Sandbox only.
* Roadmap: `--cash-mode delta` (optional), migrations, cloud orchestration, metrics, per-account explain flag in CLI.

## Requirements

- Python 3.11+
- PostgreSQL 16 (local or Docker)
- Optional (for PDF reports): WeasyPrint system libs

### WeasyPrint system packages

**Ubuntu/Debian**

```bash
sudo apt-get update && sudo apt-get install -y \
  libcairo2 libpango-1.0-0 libpangoft2-1.0-0 libpangocairo-1.0-0 \
  libgdk-pixbuf-2.0-0 libffi-dev shared-mime-info
```

**macOS (Homebrew)**

```bash
brew install cairo pango gdk-pixbuf libffi
```

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env   # then edit secrets
```

### .env template

```bash
PLAID_CLIENT_ID=your_sandbox_client_id
PLAID_SECRET=your_sandbox_secret
PLAID_ENV=sandbox
DATABASE_URL=postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl
```

## Quick Start

### Tier 0: Offline Demo (≤90s, no credentials required)
```bash
pip install -e ".[core]"
make demo-offline
```
Generates `build/demo_recon.json` + HTML reports using SQLite + fixtures.

### Tier 1: Docker Demo (deterministic)
```bash
make demo-docker
```
Same output as Tier 0, but uses Postgres via Docker for full stack testing.

### Tier 2: Sandbox Integration (requires Plaid credentials)
```bash
# Setup
export PLAID_CLIENT_ID=your_sandbox_client_id
export PLAID_SECRET=your_sandbox_secret
make db-up && pfetl init-db && make seed-coa

# Run sandbox flow
pfetl onboard --sandbox --write-env && source .env
pfetl ingest --item-id $PLAID_ITEM_ID --from 2024-01-01 --to 2024-03-31
pfetl map-account --plaid-account-id <ID> --gl-code "Assets:Bank:Checking"
pfetl reconcile --item-id $PLAID_ITEM_ID --period 2024Q1 --balances-json build/demo_balances.json --out build/recon.json
pfetl report --item-id $PLAID_ITEM_ID --period 2024Q1 --formats html --out build/
```

See [docs/ONBOARDING.md](docs/ONBOARDING.md) for detailed setup.

### Troubleshooting

- ❌ `No GL account found...` → `pfetl init-db` then `make seed-coa` (seeds required by design)
- ❌ `Provide exactly one of --balances-json or --use-plaid-live` → Choose one data source mode
- ❌ `Missing balance data for accounts: [plaid_xyz]` → JSON file must include all mapped cash accounts
- ❌ `Cannot scope by item_id yet. Ingest this item first` → Run `pfetl ingest` before other commands
- ❌ Reconcile variance too large → `make demo-balances` (as-of balances keyed by real Plaid IDs)
- ❌ PDF fails → Install WeasyPrint deps (see Requirements)

## Development Commands

| Target                 | Description                                       |
| ---------------------- | ------------------------------------------------- |
| `make demo-offline`  | Quick offline demo (SQLite + fixtures, ≤90s)     |
| `make demo-docker`   | Docker demo (Postgres + fixtures, deterministic) |
| `make demo-sandbox`  | Full sandbox demo (requires Plaid credentials)   |
| `make install`       | Install development dependencies                  |
| `make test`          | Run pytest test suite                             |
| `make fmt`           | Format code with ruff                             |
| `make lint`          | Local dev linting (auto-fix)                      |
| `make ship`          | Full quality gate (CI checks)                     |
| `make seed-coa`      | Seed GL accounts (required before ingest)         |
| `make demo-balances` | Generate demo balances from GL (as-of PERIOD_END) |
| `make db-up`         | Start PostgreSQL container                        |
| `make db-down`       | Stop PostgreSQL container                         |
| `make db-shell`      | Open psql shell to database                       |
| `make clean`         | Clean build artifacts                             |

## Data Model

**Core Tables:**

- `accounts` - Canonical GL chart of accounts (UUID PKs)
- `journal_entries` - Double-entry transaction headers
- `journal_lines` - Individual debits/credits with FK to accounts
- `plaid_accounts` - Raw Plaid account metadata
- `account_links` - Explicit 1:1 Plaid→GL mappings
- `raw_transactions` - Audit trail with source_hash lineage
- `etl_events` - Operation logs (ingest, reconcile, report)

See [docs/SCHEMA.md](docs/SCHEMA.md) for complete table definitions and relationships.

## Quality Gates

**Continuous Integration:**

- `ruff format --check` - Code formatting
- `ruff check` - Linting with strict rules
- `mypy --strict` - Type checking
- `pytest` - Unit, integration, E2E tests
- PostgreSQL service containers
- Nightly runs with real Plaid sandbox
- Concurrency with `cancel-in-progress`

---

## Documentation

* `docs/ARCHITECTURE.md` — components & flow
* `docs/CONTROLS.md` — auditability & invariants
* `docs/SCHEMA.md` — table overview
* `docs/CONFIGURATION.md` — environment variables
