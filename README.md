# plaid-ledger-recon

**Plaid → Postgres → Reconciled Balance Sheet & Cash Flow**
*A minimal, audit-ready financial automation demo.*

---

## ✨ What it is

`plaid-ledger-recon` is a **CLI tool** that ingests bank data from the **Plaid Sandbox**, transforms it into a **double-entry ledger**, enforces **reconciliation gates**, and generates **deterministic reports** (Balance Sheet, Cash Flow) in HTML and PDF.

It's designed to feel like **"one-command audit automation"**: simple to onboard, transparent in failure, and reproducible.

---

## 🚀 Quick Start

```bash
# 1. Spin up infra (Postgres only)
make up

# 2. Initialize schema
pfetl init-db

# 3. Onboard a sandbox bank account (prints ITEM_ID)
pfetl onboard --sandbox
# Example output: ITEM_ID=abc123

# 4. Ingest 90 days of transactions
pfetl ingest --item-id abc123 --from 2024-01-01 --to 2024-03-31

# 5. Map accounts explicitly (required for reconciliation)
pfetl map-account --plaid-account-id plaid_123 --gl-code "Assets:Bank:Checking"

# 6. Run reconciliation gates (exit non-zero if controls fail)
# For demos/CI, you can override Plaid balances with a curated JSON file:
#   {"<PLAID_ACCOUNT_ID>": <period_balance>, ...}
# Example: --balances-json build/demo_balances.json
pfetl reconcile --item-id abc123 --period 2024Q1 --out build/recon.json \
  --balances-json build/demo_balances.json

# 7. Generate deterministic reports
pfetl report --item-id abc123 --period 2024Q1 --formats html,pdf --out build/
```

---

## 🖥️ CLI Mockups

### Onboarding

```
$ pfetl onboard --sandbox
✔ Plaid sandbox linked successfully
ITEM_ID=abc123
```

### Ingest

```
$ pfetl ingest --item-id abc123 --from 2024-01-01 --to 2024-03-31
[✓] Pulled 1234 transactions (3 pages, 1 retry)
[✓] Loaded into Postgres: 1234 entries, 2468 lines
```

### Account Mapping

```
$ pfetl map-account --plaid-account-id plaid_123 --gl-code "Assets:Bank:Checking"
✅ Linked plaid_123 → Assets:Bank:Checking
```

### Reconcile

```
$ pfetl reconcile --item-id abc123 --period 2024Q1
Reconciliation Report: build/recon.json
----------------------------------------------------
Entries checked:   1234
Unbalanced:        0
Cash Variance:     0.00
Result:            PASSED ✅
```

### Report

```
$ pfetl report --item-id abc123 --period 2024Q1 --formats html,pdf
✅ Generated: build/bs_2024Q1.html
✅ Generated: build/bs_2024Q1.pdf
✅ Generated: build/cf_2024Q1.html
✅ Generated: build/cf_2024Q1.pdf
🎉 Reports generated for 2024Q1 in build/
```

---

## 📊 Report Preview (HTML)

**Balance Sheet (Q1 2024)**

```
Assets
  Bank:Checking     12,340.00
  Bank:Savings      15,200.00
Total Assets        27,540.00

Liabilities
  Credit Card        2,500.00
Total Liabilities    2,500.00

Equity
  Retained Earnings 25,040.00
Total Equity        25,040.00
```

**Cash Flow (Q1 2024)**

```
Operating Activities
  Inflows           9,800.00
  Outflows          8,200.00
Net Operating CF    1,600.00
```

---

## 📂 Example Reconciliation JSON

```json
{
  "period": "2024Q1",
  "entries_checked": 1234,
  "unbalanced": 0,
  "cash_accounts": [
    {"name": "Assets:Bank:Checking", "variance": 0.00}
  ],
  "row_counts": {"raw": 1234, "entries": 1234, "lines": 2468}
}
```

---

## 🎬 Demo Script (2-Minute Walkthrough)

1. **Onboard a sandbox bank**

   ```bash
   pfetl onboard --sandbox
   ```

   👉 Prints an `ITEM_ID` so we can track one institution's data.

2. **Ingest 90 days of transactions**

   ```bash
   pfetl ingest --item-id abc123 --from 2024-01-01 --to 2024-03-31
   ```

   👉 See logs show multi-page pagination, one retry, and a row count.

3. **Map accounts for reconciliation**

   ```bash
   pfetl map-account --plaid-account-id plaid_123 --gl-code "Assets:Bank:Checking"
   ```

   👉 Explicit mapping policy ensures audit-ready account linkage.

4. **Run reconciliation**

   ```bash
   pfetl reconcile --item-id abc123 --period 2024Q1
   ```

   👉 If controls pass: `Result: PASSED ✅`.
👉 If balances don't match: exit 1, JSON shows variance.
👉 For demos, pass `--balances-json` with period balances to avoid live Plaid drift.

5. **Generate reports**

   ```bash
   pfetl report --item-id abc123 --period 2024Q1 --formats html,pdf
   ```

   👉 HTML report hash is stable → deterministic output.
   👉 Open `bs_2024Q1.html` in a browser to see Assets = Liabilities + Equity.

The whole flow takes ~2 minutes and shows **audit controls, reconciliation, and reporting** in action.

---

## 🧭 UX Principles

* **One command, one story** → Each CLI action maps to a clear auditor task.
* **Fail fast, fail loud** → Any broken control exits non-zero with structured details.
* **Reproducibility first** → Same input → same output hashes.
* **Minimal mental load** → No YAML boilerplate, no surprise infra; just Postgres + Plaid sandbox.
* **Explicit mapping policy** → Account linkages must be deliberate for audit integrity.

---

## 🛠️ Under the Hood

* Extract via **httpx** (Plaid sandbox, with pagination + retry).
* Transform via **rule-based mappings** to double-entry ledger.
* Load into **Postgres** with `source_hash` + `etl_events` audit trail.
* **FK integrity enforcement** - journal_lines.account_id → accounts.id
* **Explicit account mapping** - plaid_accounts → account_links → accounts
* Reports rendered with **Jinja2 + WeasyPrint** (graceful PDF fallback).
* CI fails if reconciliation gates or determinism checks break.

---

## 📖 Documentation

* `docs/ONBOARDING.md` — install & run
* `docs/ARCHITECTURE.md` — components & flow  
* `docs/RUNBOOK.md` — day-2 ops & troubleshooting
* `docs/CONTROLS.md` — auditability & invariants
* `docs/SCHEMA.md` — table overview
* `docs/COA.md` — chart of accounts mapping
* `docs/CONFIGURATION.md` — environment variables
* `docs/SECURITY.md` — secrets & logging
