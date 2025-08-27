# Onboarding & First Run

Complete end-to-end walkthrough from fresh environment to reconciled reports.

## Prerequisites
- Docker + Docker Compose
- Python 3.11+ (for the CLI)
- Plaid sandbox credentials (`PLAID_CLIENT_ID`, `PLAID_SECRET`)

### Install dependencies
```bash
# Use python3 on macOS/Linux, python on Windows
python3 -m pip install -e ".[dev]"
```

## Quick Start (Full Workflow)

### 1) Environment Setup
Create `.env` in repo root:
```env
PLAID_CLIENT_ID=your_sandbox_client_id
PLAID_SECRET=your_sandbox_secret
PLAID_ENV=sandbox
DATABASE_URL=postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl
```

### 2) Initialize Infrastructure

```bash
# Start Postgres
make db-up

# Initialize database schema
pfetl init-db
```

### 3) Onboard Sandbox Bank Account

```bash
# Creates sandbox institution and prints ITEM_ID
pfetl onboard --sandbox
# Example output: abc123
```

### 4) Ingest Transactions

```bash
# Fetch 90 days of transactions
pfetl ingest --item-id abc123 --from 2024-01-01 --to 2024-03-31
# ✅ Ingested 1234 transactions.
```

### 5) Map Accounts for Reconciliation

```bash
# List available Plaid accounts
make db-shell
pfetl=# SELECT plaid_account_id, name, type FROM plaid_accounts;
pfetl=# \q

# Map each cash account to GL code (required for Plaid-vs-GL cash variance)
pfetl map-account --plaid-account-id plaid_123 --gl-code "Assets:Bank:Checking"
pfetl map-account --plaid-account-id plaid_456 --gl-code "Assets:Bank:Savings"
# ✅ Linked plaid_123 → Assets:Bank:Checking
```

### 6) Run Reconciliation Gates

```bash
# Period-aware reconciliation with Plaid balance comparison
pfetl reconcile --item-id abc123 --period 2024Q1 --out build/recon.json
# ✅ Reconciliation passed for 2024Q1
# 📄 Results written to build/recon.json

# Note: Plaid Sandbox current balances include history outside your ingest window.
# For demos/CI, pass curated period balances to avoid drift:
# pfetl reconcile --item-id abc123 --period 2024Q1 --out build/recon.json \
#   --balances-json build/balances.json
# --balances-json is documented in ADR-001-LIVING and README; production default remains live Plaid balances.
```

### 7) Generate Reports

```bash
# Deterministic HTML + PDF reports
pfetl report --item-id abc123 --period 2024Q1 --formats html,pdf --out build/
# ✅ Generated: build/bs_2024Q1.html
# ✅ Generated: build/bs_2024Q1.pdf
# ✅ Generated: build/cf_2024Q1.html
# ✅ Generated: build/cf_2024Q1.pdf
```

## Verification Steps

### Database Integrity
```bash
make db-shell
pfetl=# SELECT COUNT(*) FROM journal_entries;
pfetl=# SELECT COUNT(*) FROM journal_lines;
pfetl=# SELECT * FROM etl_events ORDER BY started_at DESC LIMIT 5;
# Reconcile also records an event_type='reconcile' with period and success.
```

### Account Mapping Status
```bash
pfetl=# SELECT 
  pa.plaid_account_id, 
  pa.name, 
  a.code 
FROM plaid_accounts pa
LEFT JOIN account_links al ON pa.plaid_account_id = al.plaid_account_id
LEFT JOIN accounts a ON al.account_id = a.id;
```

### Reconciliation Results
```bash
cat build/recon.json | jq '.success, .checks.cash_variance.variance'
```

## Expected Outcomes

* **Idempotency**: Re-running ingest for same date window creates no duplicates
* **Balance Integrity**: All journal entries balance (∑debits = ∑credits)
* **FK Enforcement**: journal_lines.account_id → accounts.id foreign keys enforced
* **Audit Trail**: ETL events logged with row counts and timestamps; reconcile adds a `reconcile` event with period and success
* **Deterministic Output**: Same input data produces identical report hashes
* **Reconciliation Gates**: Cash variance ≤ 0.01 vs Plaid balances, exit non-zero on failure
