from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,round,unix_timestamp,when)

FILE_PATH="yellow_tripdata_2026-01.parquet"
OUTPUT_PATH = "processed/yellow_tripdata_2026-01.parquet"

MINIO_ENDPOINT = "http://localhost:9000"
BUCKET_NAME = "nyc-taxi"

spark=SparkSession.builder.appName("NYCTaxiTransformation").master("local[*]").getOrCreate()

# ----------------------------------
#  1. Reading raw data
#------------------------------------

df=spark.read.parquet(FILE_PATH)

print("="*60)
print("RAW DATA")
print("="*60)

print(f"Rows before transformation: {df.count():,}")

# ----------------------------------
#  2. Data Transformation
#------------------------------------

df=df.withColumn(
    "trip_duration_minutes",
    (
        unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")
    ) / 60
)


# ------------------------
#  3. Remove invalid trips
# -----------------------

df=df.filter(
    (col("trip_duration_minutes")>0)
    & (col("trip_distance") > 0)
    & (col("fare_amount") >= 0)
    & (col("total_amount") >=0)

)

# --------------------------------------------------
# 4. Handle missing values
# --------------------------------------------------

df=df.fillna(
    {
        "passenger_count":1,
        "RatecodeID": 99,
        "store_and_fwd_flag": "N",
        "congestion_surcharge":0.0,
        "Airport_fee":0.0,
        "cbd_congestion_fee":0.0,
    }
)

# --------------------------------------------------
# 5. Calculate average speed
# --------------------------------------------------

df = df.withColumn(
    "average_speed_mph",
    when(
        col("trip_duration_minutes") > 0,
        col("trip_distance") / (col("trip_duration_minutes")/60)
    ).otherwise(0)
)

# -----------------------------------------
#  6. Round calculated values
# -------------------------------

df = df.withColumn(
    "trip_duration_minutes",
    round(col("trip_duration_minutes"), 2)
)

df = df.withColumn(
    "average_speed_mph",
    round(col("average_speed_mph"), 2)
)

# ----------------------------------------
#  7. Inspect transformed data
#  ---------------------

print("\n" + "=" * 60)
print("TRANSFORMED DATA")
print("=" * 60)

print(f"Rows after transformation: {df.count():,}")

print("\nNew columns:")
print("  - trip_duration_minutes")
print("  - average_speed_mph")

print("\nSample transformed records:")

df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "trip_duration_minutes",
    "average_speed_mph",
).show(10, truncate=False)

#  Write transformed data locally
OUTPUT_LOCAL_PATH="processed_yellow_tripdata_2026-01"

df.write.mode("overwrite").parquet(OUTPUT_LOCAL_PATH)

print("\nTransformed data written locally.")

spark.stop()