# Runbook (Operations)

## Daily ingest (sandbox)
```bash
make db-up
pfetl init-db           # on first run or after reset
pfetl ingest --item-id "$PLAID_ITEM_ID" --from 2024-08-01 --to 2024-08-31
```

## Health checks

* **Exit codes:** non-zero means failure; see CLI error output
* **Balances:** Run:

  ```sql
  SELECT e.txn_id,
         SUM(CASE WHEN l.side='debit'  THEN l.amount ELSE 0 END) AS debits,
         SUM(CASE WHEN l.side='credit' THEN l.amount ELSE 0 END) AS credits
  FROM journal_entries e
  JOIN journal_lines l ON e.id = l.entry_id
  GROUP BY e.txn_id
  HAVING SUM(CASE WHEN l.side='debit' THEN l.amount ELSE 0 END) <>
         SUM(CASE WHEN l.side='credit' THEN l.amount ELSE 0 END);
  ```

  Expect **0 rows**.

## Common failures

| Symptom                               | Likely cause                                        | Fix                                                     |
| ------------------------------------- | --------------------------------------------------- | ------------------------------------------------------- |
| `❌ PLAID_ACCESS_TOKEN not set`        | missing creds in `.env`                             | run `pfetl onboard --sandbox --write-env`               |
| `400 Bad Request` from Plaid API      | expired/invalid access token in API calls          | re-onboard to refresh token                             |
| `CheckViolation` on accounts.type     | schema enforces GL types; loader writes Plaid types | Current implementation uses shim tables to avoid this   |
| `Unmapped Plaid account type/subtype` | missing mapping (e.g., `credit/credit card`)        | update `etl/coa.yaml` (normalize with underscores)      |
| duplicate entries                     | idempotency constraint broken                       | ensure `journal_entries.txn_id` is UNIQUE; rerun ingest |

## Resetting

```bash
make db-down
make db-up
pfetl init-db
```