# PFETL — Plaid → Postgres (audit-ready ETL)

PFETL ingests transactions from the Plaid **sandbox** API into PostgreSQL with:
- **Deterministic transform** to double-entry journal entries
- **Idempotent load** (no duplicates on re-ingest)
- **Source identity** via SHA256 of canonicalized Plaid JSON
- Simple CLI: `init-db`, `onboard`, `ingest`

> See `docs/ONBOARDING.md` for a 5-minute quickstart.

## Capabilities (today)
- Plaid sandbox **onboard** (public_token → item/access_token)
- **Extract** with pagination + bounded retry
- **Transform** to balanced journal entries (per transaction)
- **Load** to Postgres with upserts + ETL event recording

## Limitations (planned next)
- Recon gates + `recon.json` (M3)
- Deterministic HTML/PDF reports (M4)
- No FX conversion (currency is carried through)
- Sandbox only

## Quick commands
```bash
make db-up
python3 cli.py init-db
python3 cli.py onboard --sandbox --write-env
python3 cli.py ingest --item-id "$PLAID_ITEM_ID" --from 2024-01-01 --to 2024-01-31
# Coming soon (not implemented in this version):
# python3 cli.py reconcile --item-id "$PLAID_ITEM_ID" --period 2024Q1
# python3 cli.py report --item-id "$PLAID_ITEM_ID" --period 2024Q1
make db-shell
```

## Docs

* `docs/ONBOARDING.md` — install & run
* `docs/ARCHITECTURE.md` — components & flow
* `docs/RUNBOOK.md` — day-2 ops & troubleshooting
* `docs/BEST_PRACTICES.md` — usage guidance
* `docs/CONFIGURATION.md` — environment variables
* `docs/SCHEMA.md` — table overview
* `docs/COA.md` — chart of accounts mapping
* `docs/CONTROLS.md` — auditability & invariants
* `docs/SECURITY.md` — secrets & logging