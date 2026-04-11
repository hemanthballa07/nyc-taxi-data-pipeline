.PHONY: up down restart logs ps ingest dbt-run dbt-test dbt-docs test clean setup

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
	python scripts/ingest.py --year $(YEAR) --month $(MONTH)

ingest-zones:
	python scripts/ingest.py --zones-only

# ──────────────────────────────────────
# dbt
# ──────────────────────────────────────
dbt-run:
	cd dbt && dbt run

dbt-run-staging:
	cd dbt && dbt run --select staging

dbt-run-marts:
	cd dbt && dbt run --select marts

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

dbt-fresh:
	cd dbt && dbt run --full-refresh

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
