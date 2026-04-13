"""
NYC Taxi streaming producer.

Replays raw.yellow_taxi_trips as JSON events to Kafka topic 'taxi-trips'.
Events are produced at a configurable speed multiplier relative to real trip time:
  --speed-multiplier 60  →  1 simulated hour plays out in 1 real minute.

Usage:
    python streaming/producer.py
    python streaming/producer.py --speed-multiplier 120 --limit 50000
"""

import datetime
import json
import logging
import os
import sys
import time
from typing import Any

import click
import psycopg2
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

TOPIC = "taxi-trips"
CURSOR_BATCH = 10_000

# Columns in the same order as the SELECT below
COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

SELECT_SQL = f"""
    select {', '.join(COLUMNS)}
    from raw.yellow_taxi_trips
    order by pickup_datetime
"""


# ── Helpers ───────────────────────────────────────────────────────────────────


def serialize_row(row: tuple) -> dict[str, Any]:
    """Convert a DB row tuple to a JSON-serialisable dict."""
    record: dict[str, Any] = {}
    for col, val in zip(COLUMNS, row):
        if isinstance(val, datetime.datetime):
            record[col] = val.isoformat()
        else:
            record[col] = val
    return record


def delivery_report(err: Any, msg: Any) -> None:
    """Confluent-kafka delivery callback — only logs errors."""
    if err is not None:
        log.error("Delivery failed offset=%s: %s", msg.offset(), err)


# ── Command ───────────────────────────────────────────────────────────────────


@click.command()
@click.option(
    "--speed-multiplier",
    default=60,
    show_default=True,
    help="Replay speed relative to real trip time (60 = 1 sim-hour per real minute).",
)
@click.option(
    "--limit",
    default=0,
    show_default=True,
    help="Max events to produce (0 = unlimited — streams all 41M rows).",
)
@click.option(
    "--bootstrap-servers",
    default="localhost:9092",
    show_default=True,
    envvar="KAFKA_BOOTSTRAP_SERVERS",
)
def produce(speed_multiplier: int, limit: int, bootstrap_servers: str) -> None:
    """Replay raw.yellow_taxi_trips as Kafka events at configurable speed."""
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "queue.buffering.max.messages": 100_000,
        }
    )

    try:
        pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            dbname=os.getenv("POSTGRES_DB", "nyctaxi"),
            user=os.getenv("POSTGRES_USER", "nyctaxi"),
            password=os.getenv("POSTGRES_PASSWORD", "nyctaxi"),
        )
    except Exception as exc:
        log.error("Cannot connect to Postgres: %s", exc)
        sys.exit(1)

    total = 0
    wall_start: float | None = None
    sim_start: datetime.datetime | None = None

    log.info(
        "Starting producer — topic=%s speed_multiplier=%dx limit=%s",
        TOPIC,
        speed_multiplier,
        limit if limit else "unlimited",
    )

    try:
        # Server-side cursor: streams rows without loading all 41M into memory
        with pg_conn.cursor(name="trip_cursor") as cur:
            cur.itersize = CURSOR_BATCH
            cur.execute(SELECT_SQL)

            for row in cur:
                record = serialize_row(row)
                pickup_dt = datetime.datetime.fromisoformat(record["pickup_datetime"])

                # Initialise timing anchors on first row
                if sim_start is None:
                    sim_start = pickup_dt
                    wall_start = time.monotonic()
                else:
                    # Pace the producer: sleep until the simulated event time arrives
                    sim_elapsed = (pickup_dt - sim_start).total_seconds()
                    target_wall = wall_start + sim_elapsed / speed_multiplier  # type: ignore[operator]
                    slack = target_wall - time.monotonic()
                    if slack > 0:
                        time.sleep(slack)

                key = str(record.get("pickup_location_id") or "")
                producer.produce(
                    topic=TOPIC,
                    key=key.encode(),
                    value=json.dumps(record).encode(),
                    callback=delivery_report,
                )
                producer.poll(0)  # non-blocking: trigger delivery callbacks

                total += 1

                if total % CURSOR_BATCH == 0:
                    producer.flush()
                    log.info(
                        "Produced %d events — sim time: %s",
                        total,
                        record["pickup_datetime"],
                    )

                if limit and total >= limit:
                    log.info("Reached --limit %d, stopping.", limit)
                    break

    except KeyboardInterrupt:
        log.info("Interrupted — flushing %d buffered messages...", total)
    except Exception as exc:
        log.error("Producer error: %s", exc)
        sys.exit(1)
    finally:
        producer.flush()
        pg_conn.close()
        log.info("Producer done — %d events sent to topic '%s'.", total, TOPIC)


if __name__ == "__main__":
    produce()
