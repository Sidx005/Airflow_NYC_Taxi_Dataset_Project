from pyspark.sql import SparkSession

INPUT_PATH="/usr/local/airflow/processed_yellow_tripdata_2026-01"
POSTGRES_URL="jdbc:postgresql://host.docker.internal:5432/nyc_taxi"


POSTGRES_PROPERTIES={
    "user" : "nyc_user",
    "password":"nyc_password",
    "driver":"org.postgresql.Driver"
}

spark=(
    SparkSession.builder
    .appName("NYCTaxiPostgresLoad")
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.8.jar")
    .getOrCreate()
)

print("="*60)
print("LOADING DATA INTO POSTGRESQL")
print("=" * 60)


df=spark.read.parquet(INPUT_PATH)

print(f"Rows to load: {df.count():,}")

# Rename columns to match PostgreSQL table
df = df.withColumnRenamed("VendorID", "vendor_id") \
       .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
       .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
       .withColumnRenamed("RatecodeID", "ratecode_id") \
       .withColumnRenamed("PULocationID", "pickup_location_id") \
       .withColumnRenamed("DOLocationID", "dropoff_location_id") \
       .withColumnRenamed("Airport_fee", "airport_fee")

print("Writing data to PostgreSQL...")

df.write \
    .mode("append") \
    .jdbc(
        url=POSTGRES_URL,
        table="taxi_trips",
        properties=POSTGRES_PROPERTIES
    )

print("Data successfully loaded into PostgreSQL!")

spark.stop()