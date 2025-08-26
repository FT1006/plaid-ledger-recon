.PHONY: help install test fmt lint type typecheck ship ci run-onboard db-up db-down db-reset db-shell clean

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
	@echo "  db-up        - Start PostgreSQL container"
	@echo "  db-down      - Stop PostgreSQL container"
	@echo "  db-reset     - Reset database (drop volumes and restart)"
	@echo "  db-shell     - Open psql shell to database"
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
	@echo "🚢 Running full quality gate..."
	ruff format --check .
	ruff check .
	mypy . --strict --warn-unused-ignores --no-error-summary
	@echo "✅ All checks passed - ready to ship!"

# CI alias
ci: ship

# Application shortcuts
run-onboard:
	python3 cli.py onboard --sandbox --write-env

# Database management
db-up:
	docker compose up -d postgres
	@echo "Waiting for database to be ready..."
	@until docker compose run --rm postgres pg_isready -h postgres -U pfetl_user -d pfetl; do \
		sleep 1; \
	done
	@echo "✅ Database is ready"

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d postgres

db-shell:
	docker compose exec -e PGPASSWORD=pfetl_password postgres \
		psql -U pfetl_user -d pfetl

# Cleanup
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete