.PHONY: up down restart logs ps ingest ingest-zones dbt-run dbt-test dbt-docs test clean setup \
        streaming-setup streaming-migrate streaming-jars streaming-kafka-up \
        streaming-producer streaming-consumer streaming-start

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

# ──────────────────────────────────────
# Streaming (Kafka + PySpark)
# ──────────────────────────────────────
# PySpark requires Python ≤ 3.13. A separate venv uses the Homebrew Python 3.13.
# Java 23 (default on this machine) removed Subject.getSubject() — Hadoop 3.4.2
# bundled with PySpark 4.x still calls it. Use Java 21 LTS instead.
SPARK_PYTHON = /opt/homebrew/bin/python3.13
STREAMING_VENV = .streaming-venv
JAVA_HOME_21 = /opt/homebrew/opt/openjdk@21

streaming-setup:
	$(SPARK_PYTHON) -m venv $(STREAMING_VENV)
	$(STREAMING_VENV)/bin/pip install --upgrade pip --quiet
	$(STREAMING_VENV)/bin/pip install "pyspark" "python-dotenv" --quiet
	@echo "Streaming venv ready. PySpark version: $$($(STREAMING_VENV)/bin/python -c 'import pyspark; print(pyspark.__version__)')"

streaming-migrate:
	.venv/bin/python scripts/streaming/migrate_live_trips.py

streaming-jars:
	.venv/bin/python streaming/download_jars.py

streaming-kafka-up:
	docker compose up -d kafka
	@echo "Waiting for Kafka to be ready..."
	@sleep 5

streaming-producer:
	.venv/bin/python streaming/producer.py --speed-multiplier 60

PYSPARK_VERSION   ?= $(shell $(STREAMING_VENV)/bin/python -c "import pyspark; print(pyspark.__version__)" 2>/dev/null)

streaming-consumer:
	@echo "PySpark $(PYSPARK_VERSION) — Java 21 LTS (Java 23 removed Subject.getSubject used by Hadoop)"
	# Uses JAVA_HOME pointing to Java 21 to avoid Hadoop 3.4.2 + Java 23 incompatibility.
	# All required JARs are pre-installed into PySpark's jars/ dir by make streaming-jars.
	JAVA_HOME=$(JAVA_HOME_21) \
	$(STREAMING_VENV)/bin/python streaming/consumer.py

streaming-start: streaming-kafka-up streaming-migrate streaming-jars
	@echo ""
	@echo "Kafka is up and raw.live_trips exists."
	@echo "Open two terminals and run:"
	@echo "  Terminal A:  make streaming-producer"
	@echo "  Terminal B:  make streaming-consumer"
