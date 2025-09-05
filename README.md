# Plaid Ledger Recon

**Plaid → Postgres → Reconciled Balance Sheet & Cash Flow**
*A minimal, audit-ready financial automation demo.*

`plaid-ledger-recon` is a **CLI tool** that ingests bank data from the **Plaid Sandbox**, transforms it into a **double-entry ledger**, enforces **reconciliation gates**, and generates **deterministic reports** (Balance Sheet, Cash Flow) in HTML and PDF.

It's designed to feel like **"one-command audit automation"**: simple to onboard, transparent in failure, and reproducible.

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

```bash
# 0. One-time setup
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 1. Spin up infra (Postgres only)
make db-up

# 2. Set environment (or use a .env file)
export PLAID_CLIENT_ID=your_sandbox_client_id
export PLAID_SECRET=your_sandbox_secret
export PLAID_ENV=sandbox
export DATABASE_URL=postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl

# 3. Run 2-minute demo
pfetl init-db
make seed-coa
pfetl onboard --sandbox --write-env
source .env

pfetl ingest --item-id $PLAID_ITEM_ID --from 2024-01-01 --to 2024-03-31
pfetl list-plaid-accounts --item-id $PLAID_ITEM_ID  # Note: requires ingest first to scope by item

# Map ALL cash accounts (required for reconciliation to pass)
pfetl map-account --plaid-account-id <CHECKING_ID> --gl-code "Assets:Bank:Checking"
pfetl map-account --plaid-account-id <SAVINGS_ID> --gl-code "Assets:Bank:Savings"
pfetl map-account --plaid-account-id <MONEY_MARKET_ID> --gl-code "Assets:Bank:MoneyMarket"
# Continue for all depository accounts shown by list-plaid-accounts

make demo-balances             # writes build/demo_balances.json (as-of PERIOD_END)
pfetl reconcile --item-id $PLAID_ITEM_ID --period 2024Q1 \
  --balances-json build/demo_balances.json \
  --out build/recon.json
# Reconciliation compares GL ending balances to ending balances in build/demo_balances.json for all mapped cash accounts
pfetl report --item-id $PLAID_ITEM_ID --period 2024Q1 --formats html --out build/
```

See [docs/ONBOARDING.md](docs/ONBOARDING.md) for detailed setup and [docs/CLI_EXAMPLES.md](docs/CLI_EXAMPLES.md) for command examples.

### Troubleshooting
- ❌ `No GL account found...` → `pfetl init-db` then `make seed-coa` (seeds required by design)
- ❌ Reconcile variance too large → `make demo-balances` (as-of balances keyed by real Plaid IDs)
- ❌ PDF fails → Install WeasyPrint deps (see Requirements)

**Security**: Never commit .env; sandbox only; see [docs/SECURITY.md](docs/SECURITY.md).

## Development Commands

| Target | Description |
|--------|-------------|
| `make install` | Install development dependencies |
| `make test` | Run pytest test suite |
| `make fmt` | Format code with ruff |
| `make lint` | Local dev linting (auto-fix) |
| `make ship` | Full quality gate (CI checks) |
| `make seed-coa` | Seed GL accounts (required before ingest) |
| `make demo-balances` | Generate demo balances from GL (as-of PERIOD_END) |
| `make db-up` | Start PostgreSQL container |
| `make db-down` | Stop PostgreSQL container |
| `make db-shell` | Open psql shell to database |
| `make clean` | Clean build artifacts |

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

---

## Under the Hood

* Extract via **httpx** (Plaid sandbox, with pagination + retry).
* Transform via **rule-based mappings** to double-entry ledger.
* Load into **Postgres** with `source_hash` + `etl_events` audit trail.
* Reconcile records a `'reconcile'` row in `etl_events` (period, checks, success).
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

---

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

* `docs/ONBOARDING.md` — install & run
* `docs/ARCHITECTURE.md` — components & flow
* `docs/CLI_EXAMPLES.md` — command examples
* `docs/REPORTS.md` — output samples
* `docs/CONTROLS.md` — auditability & invariants
* `docs/DESIGN_PRINCIPLES.md` — UX philosophy
* `docs/SCHEMA.md` — table overview
* `docs/COA.md` — chart of accounts mapping
* `docs/CONFIGURATION.md` — environment variables
* `docs/SECURITY.md` — secrets & logging
* `docs/ADR-001-LIVING.md` — living decisions
