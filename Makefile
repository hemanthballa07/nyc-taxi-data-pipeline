.PHONY: up down restart logs ps ingest ingest-zones dbt-run dbt-test dbt-docs test clean setup

# ──────────────────────────────────────
# Docker
# ──────────────────────────────────────
up:
	docker compose up -d

up-db:
	docker compose up -d postgres

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f $(service)

ps:
	docker compose ps

# ──────────────────────────────────────
# Data Ingestion
# ──────────────────────────────────────
ingest:
	.venv/bin/python scripts/ingest.py --year $(YEAR) --month $(MONTH)

ingest-zones:
	.venv/bin/python scripts/ingest.py --zones-only

# ──────────────────────────────────────
# dbt
# ──────────────────────────────────────
DBT = cd dbt && .venv/bin/dbt --profiles-dir .

dbt-run:
	$(DBT) run

dbt-run-staging:
	$(DBT) run --select staging

dbt-run-marts:
	$(DBT) run --select marts

dbt-test:
	$(DBT) test

dbt-docs:
	$(DBT) docs generate && $(DBT) docs serve

dbt-fresh:
	$(DBT) run --full-refresh

# ──────────────────────────────────────
# Testing
# ──────────────────────────────────────
test:
	python -m pytest tests/ -v

lint:
	ruff check scripts/ tests/
	ruff format --check scripts/ tests/

format:
	ruff format scripts/ tests/

# ──────────────────────────────────────
# Setup
# ──────────────────────────────────────
setup:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt
	cp .env.example .env
	@echo "✓ Virtual environment created. Run 'source .venv/bin/activate' to activate."

clean:
	rm -rf data/raw/*.parquet
	rm -rf dbt/target dbt/logs dbt/dbt_packages
	docker compose down -v
	@echo "✓ Cleaned data, dbt artifacts, and Docker volumes."

# ──────────────────────────────────────
# Quick access
# ──────────────────────────────────────
psql:
	docker compose exec postgres psql -U nyctaxi -d nyctaxi

airflow-ui:
	@echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
	@open http://localhost:8080 2>/dev/null || xdg-open http://localhost:8080 2>/dev/null || true

metabase-ui:
	@echo "Metabase: http://localhost:3000"
	@open http://localhost:3000 2>/dev/null || xdg-open http://localhost:3000 2>/dev/null || true
