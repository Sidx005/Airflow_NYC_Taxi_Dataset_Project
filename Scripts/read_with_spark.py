from pyspark.sql import SparkSession

FILE_PATH="yellow_tripdata_2026-01.parquet"

spark=SparkSession.builder.appName("NYCTaxiRead").master("local[*]").getOrCreate()

df=spark.read.parquet(FILE_PATH)

print("="*60)
print("NYC TAXI DATA - SPARK INSPECTION")
print("="*60)

print(f"\nRows: {df.count():,}")

print(f"\nColumns: {len(df.columns)}")

print("\nSchema")
df.printSchema()

print("\nFirst 5 Records:")
df.show(5, truncate=False)


spark.stop()
