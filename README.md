# NYC Taxi Data Pipeline

An end-to-end data engineering pipeline that ingests, transforms, and visualizes NYC Yellow Taxi trip data. Built to demonstrate production-grade data engineering patterns.

## Architecture

```
NYC TLC (Parquet) → Python Ingestion → PostgreSQL → dbt (star schema) → Metabase Dashboard
                                                     ↑
                                       Airflow orchestrates everything
```

See [docs/architecture.md](docs/architecture.md) for detailed design decisions.

## Tech Stack

| Layer           | Tool              | Purpose                          |
|-----------------|-------------------|----------------------------------|
| Ingestion       | Python + PyArrow  | Download and load Parquet files  |
| Warehouse       | PostgreSQL 16     | Star schema data warehouse       |
| Transformation  | dbt-core          | SQL-based ELT transformations    |
| Orchestration   | Apache Airflow    | Pipeline scheduling and monitoring|
| Dashboard       | Metabase          | Business intelligence            |
| Infrastructure  | Docker Compose    | Local development environment    |

## Quick Start

### Prerequisites
- Docker Desktop (4GB+ RAM allocated)
- Python 3.11+
- Make

### Setup
```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline

# Create virtual environment and install dependencies
make setup
source .venv/bin/activate

# Start PostgreSQL
make up-db

# Ingest one month of data
make ingest YEAR=2024 MONTH=1

# Run dbt transformations
make dbt-run

# Run tests
make dbt-test

# Start all services (including Airflow and Metabase)
make up
```

### Access
- **Airflow UI**: http://localhost:8080 (airflow / airflow)
- **Metabase**: http://localhost:3000
- **PostgreSQL**: `make psql`

## Data Source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — monthly Yellow Taxi trip records in Parquet format (~3M rows/month).

## Project Status

See [docs/plan.md](docs/plan.md) for the implementation tracker.

## What I Learned

_To be filled in as the project progresses._

## License

MIT
