# Best Practices

## Data intake
- Ingest **posted** dates; keep windows inclusive.
- Prefer smaller windows for repeatability (e.g., month).
- Avoid mixing sandbox items in a single ledger run.

## Idempotency
- Re-run with the exact same window to validate no duplicates.
- Keep `txn_id` UNIQUE; never mutate it.

## Mapping
- Normalize Plaid types/subtypes to lowercase snake_case before lookup in `coa.yaml`.
- Start with broad categories; refine gradually.

## Secrets
- Never commit `.env`. Rotate tokens if printed in logs or terminals.
- Limit access to sandbox credentials even for demos.

## Performance
- Batch operations where possible; database I/O dominates small CPU transforms.
- Keep retries bounded to avoid long-running failures.

## Audit trail
- Keep `source_hash` stable (hex of SHA256 over minified, key-sorted JSON).
- Don't alter stored raw JSON once landed.