"""
NYC Taxi PySpark Structured Streaming consumer.

Reads JSON events from Kafka topic 'taxi-trips' and writes to raw.live_trips
in Postgres via JDBC using foreachBatch micro-batches.

Run via (from project root — requires .streaming-venv with Python 3.13 and pre-installed JARs):
    make streaming-consumer

Or directly:
    .streaming-venv/bin/python streaming/consumer.py

Runs as a plain Python script rather than spark-submit. All required JARs
(Kafka connector + PostgreSQL JDBC) are pre-installed into PySpark's bundled
jars directory by `make streaming-jars`. This avoids spark-submit's --packages
and --jars flags which trigger Hadoop FileSystem path resolution and call
Subject.getSubject() — an API removed in Java 23 (JEP 486).
"""

import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

# PySpark imports — resolved at runtime by spark-submit
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        FloatType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:
    print(
        "PySpark not found. Run via spark-submit or install pyspark in the streaming venv.",
        file=sys.stderr,
    )
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "taxi-trips"
CHECKPOINT_DIR = "./streaming_checkpoint"
TRIGGER_INTERVAL = "10 seconds"

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5433")
PG_DB = os.getenv("POSTGRES_DB", "nyctaxi")
PG_USER = os.getenv("POSTGRES_USER", "nyctaxi")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "nyctaxi")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "driver": "org.postgresql.Driver",
    "user": PG_USER,
    "password": PG_PASS,
    "batchsize": "1000",
}

# ── Trip JSON schema ──────────────────────────────────────────────────────────
# Mirrors the 19 trip columns in raw.yellow_taxi_trips.
# Datetimes are serialised as ISO strings by the producer; cast via to_timestamp() below.

TRIP_SCHEMA = StructType(
    [
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("passenger_count", FloatType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("rate_code_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pickup_location_id", IntegerType(), True),
        StructField("dropoff_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("improvement_surcharge", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("congestion_surcharge", FloatType(), True),
        StructField("airport_fee", FloatType(), True),
    ]
)


# ── Batch writer ──────────────────────────────────────────────────────────────


def write_batch(batch_df: "DataFrame", batch_id: int) -> None:
    """Process one micro-batch: parse JSON, build event_id, write to Postgres."""
    if batch_df.isEmpty():
        log.info("Batch %d: empty, skipping.", batch_id)
        return

    parsed = (
        batch_df.select(
            # Kafka metadata columns from the Kafka source
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), TRIP_SCHEMA).alias("data"),
        ).select(
            # Deterministic event_id: partition-offset (matches raw.live_trips.event_id type TEXT)
            F.concat(
                F.col("kafka_partition").cast("string"),
                F.lit("-"),
                F.col("kafka_offset").cast("string"),
            ).alias("event_id"),
            F.col("kafka_offset"),
            F.col("kafka_partition"),
            F.current_timestamp().alias("ingested_at"),
            # Trip fields
            F.col("data.vendor_id"),
            F.to_timestamp(F.col("data.pickup_datetime")).alias("pickup_datetime"),
            F.to_timestamp(F.col("data.dropoff_datetime")).alias("dropoff_datetime"),
            F.col("data.passenger_count"),
            F.col("data.trip_distance"),
            F.col("data.rate_code_id"),
            F.col("data.store_and_fwd_flag"),
            F.col("data.pickup_location_id"),
            F.col("data.dropoff_location_id"),
            F.col("data.payment_type"),
            F.col("data.fare_amount"),
            F.col("data.extra"),
            F.col("data.mta_tax"),
            F.col("data.tip_amount"),
            F.col("data.tolls_amount"),
            F.col("data.improvement_surcharge"),
            F.col("data.total_amount"),
            F.col("data.congestion_surcharge"),
            F.col("data.airport_fee"),
        )
    )

    row_count = parsed.count()
    log.info("Batch %d: writing %d rows to raw.live_trips.", batch_id, row_count)

    parsed.write.jdbc(
        url=JDBC_URL,
        table="raw.live_trips",
        mode="append",
        properties=JDBC_PROPS,
    )
    log.info("Batch %d: committed.", batch_id)


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    spark = (
        SparkSession.builder.appName("nyc-taxi-streaming")
        # local[2]: 1 thread for the streaming trigger, 1 for task execution
        .master("local[2]")
        # Reduce shuffle partitions for local mode (default 200 is wasteful)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("Kafka bootstrap: %s  topic: %s", KAFKA_BOOTSTRAP, TOPIC)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        # earliest: process all produced events even if consumer starts after producer
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        raw_stream.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    log.info(
        "Streaming query started (trigger=%s). Ctrl+C to stop.", TRIGGER_INTERVAL
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopping streaming query...")
        query.stop()
        spark.stop()
        log.info("Consumer stopped cleanly.")


if __name__ == "__main__":
    main()
