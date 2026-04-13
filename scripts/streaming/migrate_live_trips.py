"""
Create raw.live_trips table for streaming ingestion.

Idempotent — safe to run multiple times (CREATE TABLE IF NOT EXISTS).
Must be run before starting the PySpark consumer.

Usage:
    python scripts/streaming/migrate_live_trips.py
"""

import logging
import os
import sys

import click
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.live_trips (
    -- Streaming metadata
    -- event_id is deterministic: concat(kafka_partition::text, '-', kafka_offset::text)
    event_id           TEXT        NOT NULL,
    kafka_offset       BIGINT      NOT NULL,
    kafka_partition    INTEGER     NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Trip fields (identical types to raw.yellow_taxi_trips)
    vendor_id              INTEGER,
    pickup_datetime        TIMESTAMP,
    dropoff_datetime       TIMESTAMP,
    passenger_count        REAL,
    trip_distance          REAL,
    rate_code_id           INTEGER,
    store_and_fwd_flag     TEXT,
    pickup_location_id     INTEGER,
    dropoff_location_id    INTEGER,
    payment_type           INTEGER,
    fare_amount            REAL,
    extra                  REAL,
    mta_tax                REAL,
    tip_amount             REAL,
    tolls_amount           REAL,
    improvement_surcharge  REAL,
    total_amount           REAL,
    congestion_surcharge   REAL,
    airport_fee            REAL,

    -- Unique constraint enforces idempotency: a Kafka offset within a partition is immutable
    UNIQUE (kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_live_trips_ingested_at
    ON raw.live_trips (ingested_at);

CREATE INDEX IF NOT EXISTS idx_live_trips_pickup_dt
    ON raw.live_trips (pickup_datetime);
"""


# ── Connection ────────────────────────────────────────────────────────────────


def get_connection() -> "psycopg2.extensions.connection":
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "nyctaxi"),
        user=os.getenv("POSTGRES_USER", "nyctaxi"),
        password=os.getenv("POSTGRES_PASSWORD", "nyctaxi"),
    )


# ── Command ───────────────────────────────────────────────────────────────────


@click.command()
def migrate() -> None:
    """Create raw.live_trips with UNIQUE(kafka_partition, kafka_offset) constraint."""
    log.info("Running raw.live_trips migration...")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
        log.info("Migration complete — raw.live_trips is ready.")
    except Exception as exc:
        log.error("Migration failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    migrate()
