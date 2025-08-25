.PHONY: help install test fmt lint typecheck run-onboard db-up db-down db-reset db-shell clean

# Default target shows available commands
help:
	@echo "Available targets:"
	@echo "  install      - Install development dependencies"
	@echo "  test         - Run pytest test suite"
	@echo "  fmt          - Format code with ruff"
	@echo "  lint         - Run ruff linter"
	@echo "  typecheck    - Run mypy type checker"
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

fmt:
	ruff format .

lint:
	ruff check --fix .

typecheck:
	mypy .

# Application shortcuts
run-onboard:
	python cli.py onboard --sandbox --write-env

# Database management
db-up:
	docker compose up -d postgres
	@echo "Waiting for database to be ready..."
	@until docker compose run --rm postgres pg_isready -U pfetl_user -d pfetl; do \
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