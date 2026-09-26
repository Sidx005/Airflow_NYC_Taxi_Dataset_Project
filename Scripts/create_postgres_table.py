import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="nyc_taxi",
    user="nyc_user",
    password="nyc_password"
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS taxi_trips (
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE PRECISION,
    ratecode_id BIGINT,
    store_and_fwd_flag VARCHAR(10),
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type BIGINT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    cbd_congestion_fee DOUBLE PRECISION,
    trip_duration_minutes DOUBLE PRECISION,
    average_speed_mph DOUBLE PRECISION
);

""")

conn.commit()

print("PostgreSQL table created successfully")

cursor.close()
conn.close()