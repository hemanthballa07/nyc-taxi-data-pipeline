-- This runs automatically when the Postgres container starts for the first time.
-- It creates the schema structure for the warehouse.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Ingestion log to track what data has been loaded
CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id SERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    rows_loaded INTEGER NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status TEXT DEFAULT 'success',
    notes TEXT
);

-- Taxi zone lookup (loaded once from TLC CSV)
CREATE TABLE IF NOT EXISTS raw.taxi_zone_lookup (
    location_id INTEGER PRIMARY KEY,
    borough TEXT,
    zone TEXT,
    service_zone TEXT
);

-- Main trip data table (partitioned by month for query performance)
CREATE TABLE IF NOT EXISTS raw.yellow_taxi_trips (
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count REAL,
    trip_distance REAL,
    rate_code_id INTEGER,
    store_and_fwd_flag TEXT,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type INTEGER,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    improvement_surcharge REAL,
    total_amount REAL,
    congestion_surcharge REAL,
    airport_fee REAL
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_trips_pickup_dt
    ON raw.yellow_taxi_trips (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_location
    ON raw.yellow_taxi_trips (pickup_location_id);
