"""
NYC Yellow Taxi ingestion script.

Usage:
    python scripts/ingest.py --year 2024 --month 1
    python scripts/ingest.py --year 2024 --month 1 --force
    python scripts/ingest.py --zones-only
"""

import csv
import io
import logging
import os
import sys
from pathlib import Path
import pandas as pd

import click
import psycopg2
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",  # TLC uses capital A in 2024+ files
]

COLUMN_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",  # TLC uses capital A in 2024+ files
}

DB_COLUMNS = list(COLUMN_MAP.values())

# ── DB connection ──────────────────────────────────────────────────────────────


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "nyctaxi"),
        user=os.getenv("POSTGRES_USER", "nyctaxi"),
        password=os.getenv("POSTGRES_PASSWORD", "nyctaxi"),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def build_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def download_file(url: str, dest: Path, force: bool) -> None:
    if dest.exists() and not force:
        log.info("File already exists, skipping download: %s", dest)
        return

    log.info("Downloading %s → %s", url, dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                fh.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    log.info("  %.1f%%  (%d MB / %d MB)", pct, downloaded >> 20, total >> 20)

    log.info("Download complete: %.1f MB", dest.stat().st_size / 1024 / 1024)


def validate(path: Path, year: int, month: int) -> int:
    """Validate the Parquet file. Returns row count. Raises on fatal errors, warns on soft ones."""
    size_mb = path.stat().st_size / 1024 / 1024

    if size_mb < 1:
        log.warning("File is suspiciously small: %.2f MB", size_mb)

    table = pq.read_table(path)
    actual_cols = set(table.schema.names)
    missing = [c for c in REQUIRED_COLUMNS if c not in actual_cols]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    row_count = len(table)
    if row_count == 0:
        raise ValueError("File has 0 rows — aborting load")
    if row_count < 10_000:
        log.warning("Row count is unusually low: %d rows", row_count)

    # Date range check — at least 80% of pickups should fall in the expected month
      

    pickup_col = table.column("tpep_pickup_datetime").to_pylist()
    df_dates = pd.Series(pickup_col)
    in_month = df_dates.apply(
        lambda dt: dt is not None and dt.year == year and dt.month == month
    )
    pct_in_month = in_month.sum() / len(df_dates) * 100
    if pct_in_month < 80:
        log.warning(
            "Only %.1f%% of pickups fall within %d-%02d (expected ≥80%%). "
            "TLC files sometimes include spillover rows.",
            pct_in_month,
            year,
            month,
        )

    log.info("Validation passed: %d rows, %.1f MB", row_count, size_mb)
    return row_count


def load_trips(path: Path, year: int, month: int, conn: psycopg2.extensions.connection) -> int:
    """Delete-and-reload for the target month. Returns rows loaded."""
    table = pq.read_table(path, columns=REQUIRED_COLUMNS)

    for parquet_name, db_name in COLUMN_MAP.items():
        if parquet_name != db_name:
            idx = table.schema.get_field_index(parquet_name)
            table = table.rename_columns(
                [db_name if i == idx else table.schema.names[i] for i in range(len(table.schema))]
            )

    df = table.to_pandas()

    # TLC stores some integer columns as float64 (e.g. 1.0 instead of 1).
    # Cast them to nullable Int64 so they serialize as integers for Postgres.
    int_columns = ["vendor_id", "rate_code_id", "pickup_location_id", "dropoff_location_id", "payment_type"]
    for col in int_columns:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    # Filter to target month only — prevents spillover rows from duplicating on re-runs
    month_start = f"{year}-{month:02d}-01"
    month_end = f"{year}-{month + 1:02d}-01" if month < 12 else f"{year + 1}-01-01"
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    original_count = len(df)
    df = df[
        (df["pickup_datetime"] >= month_start) &
        (df["pickup_datetime"] < month_end)
    ]
    spillover = original_count - len(df)
    if spillover:
        log.info("Dropped %d spillover rows outside %d-%02d", spillover, year, month)

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw.yellow_taxi_trips "
            "WHERE pickup_datetime >= %s AND pickup_datetime < %s",
            (month_start, month_end),
        )
        deleted = cur.rowcount
        if deleted:
            log.info("Deleted %d existing rows for %d-%02d", deleted, year, month)

        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        for row in df.itertuples(index=False):
            writer.writerow(
                [None if pd.isna(v) else v for v in row]  # fix: use pd.isna instead of v != v
            )
        buf.seek(0)

        cols = ", ".join(DB_COLUMNS)
        cur.copy_expert(
            f"COPY raw.yellow_taxi_trips ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')",
            buf,
        )
        rows_loaded = cur.rowcount

    conn.commit()
    log.info("Loaded %d rows into raw.yellow_taxi_trips", rows_loaded)
    return rows_loaded

def load_zones(conn: psycopg2.extensions.connection) -> int:
    """Download taxi zone lookup CSV and reload raw.taxi_zone_lookup. Returns rows loaded."""
    log.info("Downloading zone lookup from %s", ZONES_URL)
    resp = requests.get(ZONES_URL, timeout=30)
    resp.raise_for_status()

    # Parse CSV — columns: LocationID, Borough, Zone, service_zone
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)
    if not rows:
        raise ValueError("Zone lookup CSV is empty")

    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in rows:
        writer.writerow([row["LocationID"], row["Borough"], row["Zone"], row["service_zone"]])
    buf.seek(0)

    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw.taxi_zone_lookup")
        cur.copy_expert(
            "COPY raw.taxi_zone_lookup (location_id, borough, zone, service_zone) "
            "FROM STDIN WITH (FORMAT CSV)",
            buf,
        )
        rows_loaded = cur.rowcount

    conn.commit()
    log.info("Loaded %d zone rows into raw.taxi_zone_lookup", rows_loaded)
    return rows_loaded


def log_ingestion(
    conn: psycopg2.extensions.connection,
    source_file: str,
    year: int,
    month: int,
    rows_loaded: int,
    status: str,
    notes: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.ingestion_log (source_file, year, month, rows_loaded, status, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (source_file, year, month, rows_loaded, status, notes),
        )
    conn.commit()


# ── CLI ───────────────────────────────────────────────────────────────────────


@click.command()
@click.option("--year", type=int, default=None, help="4-digit year, e.g. 2024")
@click.option("--month", type=int, default=None, help="Month number 1-12")
@click.option("--force", is_flag=True, default=False, help="Re-download even if file exists")
@click.option("--zones-only", is_flag=True, default=False, help="Load taxi zone lookup CSV only")
def main(year: int | None, month: int | None, force: bool, zones_only: bool) -> None:
    """Download and load NYC Yellow Taxi data."""
    conn = get_connection()

    if zones_only:
        rows_loaded = 0
        try:
            rows_loaded = load_zones(conn)
            log_ingestion(conn, "taxi_zone_lookup.csv", 0, 0, rows_loaded, "success")
            log.info("Done. %d zone rows loaded.", rows_loaded)
        except Exception as exc:
            log.error("Zone ingestion failed: %s", exc)
            try:
                conn.rollback()
                log_ingestion(conn, "taxi_zone_lookup.csv", 0, 0, rows_loaded, "failed", notes=str(exc))
            except Exception as log_exc:
                log.error("Could not write failure log: %s", log_exc)
            conn.close()
            sys.exit(1)
        conn.close()
        return

    if year is None or month is None:
        raise click.UsageError("--year and --month are required unless --zones-only is set")

    url = build_url(year, month)
    dest = DATA_DIR / f"yellow_tripdata_{year}-{month:02d}.parquet"
    rows_loaded = 0

    try:
        download_file(url, dest, force)
        validate(dest, year, month)
        rows_loaded = load_trips(dest, year, month, conn)
        log_ingestion(conn, dest.name, year, month, rows_loaded, "success")
        log.info("Done. %d rows loaded for %d-%02d.", rows_loaded, year, month)
    except Exception as exc:
        log.error("Ingestion failed: %s", exc)
        try:
            conn.rollback()  # clear any aborted transaction before attempting the failure log
            log_ingestion(conn, dest.name, year, month, rows_loaded, "failed", notes=str(exc))
        except Exception as log_exc:
            log.error("Could not write failure log: %s", log_exc)
        conn.close()
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
