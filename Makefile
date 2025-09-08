.PHONY: help install test fmt lint type typecheck ship ci run-onboard seed-coa demo-balances db-up db-down db-reset db-shell migrate migrate-status clean

# Default target shows available commands
help:
	@echo "Available targets:"
	@echo "  install      - Install development dependencies"
	@echo "  test         - Run pytest test suite"
	@echo "  fmt          - Format code with ruff"
	@echo "  lint         - Local dev linting (relaxed, auto-fix)"
	@echo "  type         - Quick local type check"
	@echo "  typecheck    - Alias for type (backwards compatibility)"
	@echo "  ship         - Full quality gate (format check + strict lint + strict types)"
	@echo "  ci           - Alias for ship (CI compatibility)"
	@echo "  run-onboard  - Run onboard command in sandbox mode"
	@echo "  seed-coa     - Seed GL accounts (required before ingest)"
	@echo "  demo-balances - Generate demo balances from GL (as-of PERIOD_END)"
	@echo "  db-up        - Start PostgreSQL container"
	@echo "  db-down      - Stop PostgreSQL container"
	@echo "  db-reset     - Reset database (drop volumes and restart)"
	@echo "  db-shell     - Open psql shell to database"
	@echo "  migrate      - Run SQL migrations (backfill + composite PK)"
	@echo "  migrate-status - Show ingest_accounts PK status"
	@echo "  clean        - Clean up build artifacts and cache"

# Development setup
install:
	pip install -e ".[dev]"

# Testing and quality checks
test:
	pytest

# Fast local loop: format + relaxed linting
fmt:
	ruff format .

# Local "hack mode": auto-fix + ignore pedantic rules
lint:
	ruff check . --fix --extend-ignore "T201,EM,ANN,TD" --output-format=concise

# Quick local type check (lighter output)
type:
	mypy . --hide-error-codes --warn-unused-ignores

# Backwards compatibility
typecheck: type

# Ship/CI gate: strict & reproducible
ship:
	@echo "Running full quality gate..."
	ruff format --check .
	ruff check .
	mypy . --strict --warn-unused-ignores --no-error-summary
	@echo "All checks passed - ready to ship!"

# CI alias
ci: ship

# Application shortcuts
run-onboard:
	python3 cli.py onboard --sandbox --write-env

# GL setup and demo
PERIOD_END ?= 2024-03-31

seed-coa:
	@mkdir -p build
	@docker compose exec -T postgres psql -U pfetl_user -d pfetl -v ON_ERROR_STOP=1 < etl/seed_coa.sql
	@echo "Seeded GL accounts."

demo-balances:
	@mkdir -p build
	@docker compose exec -T postgres psql -U pfetl_user -d pfetl -t -A -c "\
	WITH cash_accts AS ( \
	  SELECT a.id, al.plaid_account_id \
	  FROM accounts a \
	  JOIN account_links al ON al.account_id = a.id \
	  WHERE a.is_cash = TRUE \
	), gl_bal AS ( \
	  SELECT ca.plaid_account_id, \
	         COALESCE(SUM(CASE WHEN jl.side='debit' THEN jl.amount ELSE -jl.amount END),0.00) AS bal \
	  FROM cash_accts ca \
	  LEFT JOIN journal_lines jl ON jl.account_id = ca.id \
	  LEFT JOIN journal_entries je ON je.id = jl.entry_id \
	  WHERE je.txn_date <= '$(PERIOD_END)' OR je.txn_date IS NULL \
	  GROUP BY ca.plaid_account_id \
	) \
	SELECT COALESCE(json_object_agg(plaid_account_id, bal), '{}'::json) FROM gl_bal;" \
	> build/demo_balances.json
	@echo "Generated build/demo_balances.json (as-of $(PERIOD_END))"

# Database management
db-up:
	docker compose up -d postgres
	@echo "Waiting for database to be ready..."
	@until docker compose run --rm postgres pg_isready -h postgres -U pfetl_user -d pfetl; do \
		sleep 1; \
	done
	@echo "Database is ready"

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d postgres

db-shell:
	docker compose exec -e PGPASSWORD=pfetl_password postgres \
		psql -U pfetl_user -d pfetl

# Migrations (require postgres service running via docker compose)
migrate:
	@echo "Running migrations: backfill_item_ids.sql → 002_step_b_composite_pk.sql"
	@docker compose exec -T postgres psql -U pfetl_user -d pfetl -v ON_ERROR_STOP=1 < migrations/backfill_item_ids.sql
	@docker compose exec -T postgres psql -U pfetl_user -d pfetl -v ON_ERROR_STOP=1 < migrations/002_step_b_composite_pk.sql
	@$(MAKE) migrate-status

migrate-status:
	@echo "Verifying ingest_accounts composite PRIMARY KEY and indexes..."
	@docker compose exec -T postgres psql -U pfetl_user -d pfetl -v ON_ERROR_STOP=1 -c "\\d+ ingest_accounts"
	@docker compose exec -T postgres psql -U pfetl_user -d pfetl -v ON_ERROR_STOP=1 -c "SELECT conname, contype FROM pg_constraint WHERE conrelid = 'ingest_accounts'::regclass;"

# Cleanup
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
