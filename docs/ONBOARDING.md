# Onboarding & First Run

## Prerequisites
- Docker + Docker Compose
- Python 3.11+ (for the CLI)
- Plaid sandbox credentials (`PLAID_CLIENT_ID`, `PLAID_SECRET`)

### Install dependencies
```bash
# Use python3 on macOS/Linux, python on Windows
python3 -m pip install -e ".[dev]"
```

## 1) Environment
Create `.env` in repo root:
```env
PLAID_CLIENT_ID=your_sandbox_client_id
PLAID_SECRET=your_sandbox_secret
PLAID_ENV=sandbox
DATABASE_URL=postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl
```

## 2) Database

```bash
make db-up
pfetl init-db
```

## 3) Sandbox onboarding

```bash
pfetl onboard --sandbox --write-env
# prints an item_id and appends PLAID_ACCESS_TOKEN / PLAID_ITEM_ID to .env
```

## 4) Ingest a date window

```bash
pfetl ingest --item-id "$PLAID_ITEM_ID" --from 2024-01-01 --to 2024-01-31
```

## 5) Verify rows

```bash
make db-shell    # Database should be ready within 10-15 seconds
pfetl=# SELECT COUNT(*) FROM journal_entries;
pfetl=# SELECT COUNT(*) FROM journal_lines;
```

## Expected outcomes

* No duplicates when you re-run ingest for the same window
* Every journal entry balances (sum of debits == sum of credits)
* `etl_events` includes at least one `"load"` record