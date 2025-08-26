# Configuration

Set via environment variables (`.env`).

| Variable | Required | Example | Notes |
|---|---|---|---|
| `PLAID_CLIENT_ID` | yes | `abc123` | Plaid sandbox |
| `PLAID_SECRET` | yes | `xyz456` | Plaid sandbox |
| `PLAID_ENV` | yes | `sandbox` | Only sandbox supported |
| `PLAID_ACCESS_TOKEN` | set by onboard | `access-sandbox-...` | Saved by `--write-env` |
| `PLAID_ITEM_ID` | set by onboard | `item-sandbox-...` | Saved by `--write-env` |
| `DATABASE_URL` | yes | `postgresql://pfetl_user:pfetl_password@localhost:5432/pfetl` | Required by CLI; extract module has fallback default |

Optional tuning (defaults baked in code):
- HTTP timeouts (connect=5s, read=15s, write=10s, pool=10s)
- Retry attempts (3 on 429/5xx plus connection/timeout errors) with 0.5/1/2s ±20% jitter

## Configuration Files

| File | Purpose | Location |
|---|---|---|
| `etl/coa.yaml` | Chart of accounts mapping from Plaid types to GL accounts | Required for transform step |
| `.env.example` | Environment variable template | Copy to `.env` for setup |