# Security Notes

- **Secrets**: Use a local `.env`. Never commit it. Rotate sandbox keys if exposed. Database credentials should also be rotated if compromised.
- **Logging**: CLI prints item IDs during successful onboarding but avoids printing access tokens in normal flows. HTTP exceptions may expose credentials in error messages - review logs carefully and consider implementing credential sanitization. Access tokens are written to `.env` files during onboarding.
- **Scope**: Sandbox only. Stores Plaid sandbox transaction data including merchant names, account names, and transaction descriptions. All data should be considered test data only.
- **Least privilege**: Database user limited to one database. HTTP client uses library defaults for TLS certificate validation.
- **Compliance**: This repository is for demo/development; no production guarantees. Not suitable for production financial data.

## Data Stored
- Plaid sandbox account metadata (names, types, currency)
- Transaction descriptions and merchant information  
- Account balances and transaction amounts
- All data canonically hashed for audit integrity

## Security Considerations
- `.env` files contain database credentials - treat as sensitive
- Error logs may contain API response details - review before sharing
- Onboarding writes access tokens to local files