from pyspark.sql import SparkSession
from pyspark.sql.functions import(col,count,sum,when)

INPUT_PATH="/usr/local/airflow/processed_yellow_tripdata_2026-01"

spark=(
    SparkSession.builder
    .appName("NYCTaxiDataQuality")
    .master("local[*]")
    .getOrCreate()
)


# --------------------
#  1. Read Transformed data
#  ---------------


df =spark.read.parquet(INPUT_PATH)

total_rows=df.count()

print("="*60)
print("DATA QUALITY CHECKS")
print("="*60)

print(f"\nTotal rows: {total_rows:,}")


# --------------------------------------------------
# 2. Check for null values
# --------------------------------------------------


print("\n" + "=" * 60)
print("NULL VALUE CHECK")
print("=" * 60)


null_counts=df.select(
    [
        sum(
            when(col(column).isNull(),1).otherwise(0)

        ).alias(column)
        for column in df.columns
    ]
)

null_counts.show(truncate=False)

# --------------------------------------------------
# 3. Check for duplicate records
# --------------------------------------------------


print("\n" + "=" * 60)
print("DUPLICATE CHECK")
print("=" * 60)

distinct_rows = df.distinct().count()

duplicate_rows = total_rows - distinct_rows

print(f"Total rows:     {total_rows:,}")
print(f"Distinct rows:  {distinct_rows:,}")
print(f"Duplicate rows: {duplicate_rows:,}")

# --------------------------------------------------
# 4. Check invalid trip duration
# --------------------------------------------------

print("\n" + "=" * 60)
print("TRIP DURATION CHECK")
print("=" * 60)


invalid_duration=df.filter(
    col("trip_duration_minutes") <=0

).count()

print(f"Invalid duration records: {invalid_duration:,}")

# --------------------------------------------------
# 5. Check invalid distance
# --------------------------------------------------

print("\n" + "=" * 60)
print("TRIP DISTANCE CHECK")
print("=" * 60)

invalid_distance = df.filter(
    col("trip_distance") <= 0
).count()

print(f"Invalid distance records: {invalid_distance:,}")



# --------------------------------------------------
# 6. Check invalid fares
# --------------------------------------------------
print("\n" + "=" * 60)
print("FARE CHECK")
print("=" * 60)

invalid_fares=df.filter(col("fare_amount")<0).count()
print(f"Negative fare records: {invalid_fares:,}")


# --------------------------------------------------
# 7. Check invalid total amount
# --------------------------------------------------

print("\n" + "=" * 60)
print("TOTAL AMOUNT CHECK")
print("=" * 60)

invalid_total = df.filter(
    col("total_amount") < 0
).count()

print(f"Negative total amount records: {invalid_total:,}")


# --------------------------------------------------
# 8. Check unrealistic speed
# --------------------------------------------------

print("\n" + "=" * 60)
print("AVERAGE SPEED CHECK")
print("=" * 60)

unrealistic_speed = df.filter(
    col("average_speed_mph") > 100
).count()

print(f"Records above 100 mph: {unrealistic_speed:,}")


# --------------------------------------------------
# 9. Overall result
# --------------------------------------------------

print("\n" + "=" * 60)
print("DATA QUALITY SUMMARY")
print("=" * 60)

print(f"Total records:              {total_rows:,}")
print(f"Duplicate records:          {duplicate_rows:,}")
print(f"Invalid duration:           {invalid_duration:,}")
print(f"Invalid distance:           {invalid_distance:,}")
print(f"Negative fares:             {invalid_fares:,}")
print(f"Negative total amounts:     {invalid_total:,}")
print(f"Speed above 100 mph:        {unrealistic_speed:,}")


if (
    duplicate_rows == 0
    and invalid_duration == 0
    and invalid_distance == 0
    and invalid_fares == 0
    and invalid_total == 0
    # and unrealistic_speed==0
):
    print("\nDATA QUALITY CHECK PASSED")
else:
    print("\nDATA QUALITY CHECK FAILED")

if unrealistic_speed > 0:
    print(f"WARNING: {unrealistic_speed:,} records have" "avg speed above 100 mph")

spark.stop()