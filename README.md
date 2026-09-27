# NYC Taxi Data Engineering Pipeline

An end-to-end data engineering pipeline that ingests real-world NYC Yellow Taxi trip data, stores raw and processed data in MinIO, transforms and validates the data using PySpark, loads it into PostgreSQL, and visualizes the results in Metabase. The complete workflow is orchestrated using Apache Airflow.

---

## Architecture

```mermaid
flowchart LR
    A[NYC TLC Yellow Taxi Data] --> B[Airflow]
    B --> C[MinIO - Raw Data]
    C --> D[PySpark Transformation]
    D --> E[Data Quality Checks]
    E --> F[MinIO - Processed Data]
    F --> G[PostgreSQL]
    G --> H[Metabase Dashboard]
```

---

## Project Overview

This project demonstrates a complete local data engineering workflow using a real NYC Taxi dataset.

The pipeline performs:

1. Data ingestion
2. Raw data storage
3. Data transformation
4. Data quality validation
5. Processed data storage
6. PostgreSQL loading
7. Data visualization
8. End-to-end orchestration with Airflow

The entire pipeline runs locally using Docker and does not require cloud infrastructure or cloud billing.

---

## Technologies Used

| Technology     | Purpose                            |
| -------------- | ---------------------------------- |
| Python         | Data engineering scripts           |
| Apache Airflow | Pipeline orchestration             |
| Astro CLI      | Local Airflow development          |
| PySpark        | Data transformation and processing |
| Parquet        | Columnar data storage              |
| MinIO          | S3-compatible object storage       |
| PostgreSQL     | Analytical database                |
| Metabase       | Data visualization                 |
| Docker         | Containerized services             |
| Pandas         | Initial dataset inspection         |
| boto3          | MinIO/S3 interaction               |
| psycopg2       | PostgreSQL interaction             |
| Git/GitHub     | Version control                    |

---

## Dataset

The project uses the official NYC Taxi & Limousine Commission Yellow Taxi Trip Data.

Dataset used:

```text
yellow_tripdata_2026-01.parquet
```

The dataset contains NYC Yellow Taxi trips for January 2026.

### Original Dataset

* Rows: `3,724,889`
* Columns: `20`
* Format: Parquet

Main columns include:

```text
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
trip_distance
RatecodeID
store_and_fwd_flag
PULocationID
DOLocationID
payment_type
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount
congestion_surcharge
Airport_fee
cbd_congestion_fee
```

---

# Pipeline Flow

```text
NYC TLC Parquet File
        |
        v
   Airflow DAG
        |
        v
 MinIO /raw
        |
        v
    PySpark
        |
        v
Transformation
        |
        v
Data Quality Checks
        |
        v
 MinIO /processed
        |
        v
   PostgreSQL
        |
        v
    Metabase
```

---

# 1. Data Ingestion

The original NYC Taxi Parquet file is placed in the Airflow environment.

Airflow executes the ingestion task and uploads the raw dataset to MinIO.

### Raw MinIO location

```text
nyc-taxi/
└── raw/
    └── yellow_tripdata_2026-01.parquet
```

The raw dataset is kept unchanged so that the original source data is preserved.

### MinIO

MinIO provides S3-compatible object storage locally.

```text
MinIO API:
http://localhost:9000

MinIO Console:
http://localhost:9001
```

Bucket:

```text
nyc-taxi
```

---

# 2. PySpark Transformation

PySpark reads the raw Parquet dataset and performs the main transformation logic.

The transformation creates two derived columns:

```text
trip_duration_minutes
average_speed_mph
```

### Trip Duration

Trip duration is calculated using pickup and drop-off timestamps.

```text
trip_duration_minutes =
    dropoff_time - pickup_time
```

The result is converted from seconds to minutes.

### Average Speed

Average speed is calculated using:

```text
average_speed_mph =
    trip_distance / (trip_duration_minutes / 60)
```

---

## Data Filtering

Invalid records are removed using the following rules:

```text
trip_duration_minutes > 0
trip_distance > 0
fare_amount >= 0
total_amount >= 0
```

This removes records with:

* Invalid trip duration
* Zero/negative distance
* Negative fares
* Negative total amounts

### Transformation Results

```text
Rows before transformation: 3,724,889
Rows after transformation:  3,518,153
Records removed:              206,736
```

---

## Missing Value Handling

The transformation handles missing values for selected fields.

| Column               | Replacement |
| -------------------- | ----------- |
| passenger_count      | `1`         |
| RatecodeID           | `99`        |
| store_and_fwd_flag   | `N`         |
| congestion_surcharge | `0.0`       |
| Airport_fee          | `0.0`       |
| cbd_congestion_fee   | `0.0`       |

The original raw Parquet file is not modified.

---

# 3. Data Quality

After transformation, a separate PySpark data quality process validates the processed dataset.

The following checks are performed:

### Null Checks

All columns are checked for null values.

Result:

```text
Null values: 0
```

### Duplicate Check

The total number of rows is compared with the number of distinct rows.

```text
Total rows:     3,518,153
Distinct rows:  3,518,153
Duplicate rows: 0
```

### Invalid Duration

```text
Invalid duration records: 0
```

### Invalid Distance

```text
Invalid distance records: 0
```

### Negative Fares

```text
Negative fare records: 0
```

### Negative Total Amount

```text
Negative total amount records: 0
```

### Speed Anomaly

Records with an average speed greater than 100 MPH are reported as anomalies.

```text
Records above 100 mph: 720
```

These records generate a warning but do not fail the pipeline because they may represent unusual data rather than invalid records.

### Final Data Quality Result

```text
DATA QUALITY CHECK PASSED
```

---

# 4. Processed Data

The transformed Parquet data is stored locally and then uploaded to MinIO.

Processed MinIO structure:

```text
nyc-taxi/
├── raw/
│   └── yellow_tripdata_2026-01.parquet
│
└── processed/
    └── yellow_tripdata_2026-01/
        ├── part-*.parquet
        └── ...
```

Parquet is used because it is a columnar storage format that works efficiently with Spark and analytical workloads.

---

# 5. PostgreSQL

After the data quality checks pass, the processed dataset is loaded into PostgreSQL.

Database:

```text
nyc_taxi
```

Table:

```text
taxi_trips
```

PostgreSQL is used as the analytical serving layer for Metabase.

### PostgreSQL Configuration

```text
Host:     localhost
Port:     5432
Database: nyc_taxi
User:     nyc_user
```

The processed dataset contains:

```text
3,518,153 rows
```

---

# 6. PostgreSQL Loading

PySpark uses the PostgreSQL JDBC driver to write the processed Parquet data into PostgreSQL.

The JDBC connection follows:

```text
Spark
  |
  | JDBC
  v
PostgreSQL
```

The Spark DataFrame columns are renamed to database-friendly names.

Examples:

```text
VendorID              -> vendor_id
tpep_pickup_datetime  -> pickup_datetime
tpep_dropoff_datetime -> dropoff_datetime
RatecodeID             -> ratecode_id
PULocationID           -> pickup_location_id
DOLocationID           -> dropoff_location_id
Airport_fee            -> airport_fee
```

---

# 7. Idempotent PostgreSQL Loading

The pipeline truncates the PostgreSQL table before loading the latest processed dataset.

```text
truncate_postgres
        |
        v
load_to_postgres
```

This prevents duplicate records when the Airflow DAG is rerun.

Without truncation:

```text
Run 1 → 3,518,153 rows
Run 2 → 7,036,306 rows
```

With truncation:

```text
Run 1 → 3,518,153 rows
Run 2 → 3,518,153 rows
```

This makes the pipeline repeatable for the same dataset.

---

# 8. Metabase Dashboard

Metabase connects to PostgreSQL and provides a dashboard for analyzing the taxi data.

Dashboard:

```text
NYC_Taxi
```

The dashboard contains:

* Total Trips
* Total Revenue
* Average Fare Amount
* Average Trip Distance
* Daily Trips
* Daily Revenue
* Payment Type Distribution
* Top 10 Pickup Locations

### Dashboard Metrics

Current dataset results include approximately:

```text
Total Trips:            3.5M
Total Revenue:          104.4M
Average Fare Amount:    21.08
Average Trip Distance:  6.76
```

---

# Dashboard Date Filtering

The dashboard supports filtering by pickup date.

For example:

```text
January 1, 2026
        ↓
January 7, 2026
```

The date filter can be applied across the dashboard cards and charts.

The payment type chart uses a Field Filter mapped to:

```text
Taxi Trips → Pickup Datetime
```

---

# 9. Airflow Orchestration

Airflow coordinates the entire pipeline.

DAG:

```text
nyc_taxi_pipeline
```

Pipeline tasks:

```text
ingest_raw_data
       ↓
transform_data
       ↓
data_quality
       ↓
upload_processed_data
       ↓
truncate_postgres
       ↓
load_to_postgres
```

The dependency structure is:

```python
ingest \
    >> transform \
    >> data_quality \
    >> upload_processed \
    >> truncate_postgres \
    >> load_postgres
```

Airflow ensures that each stage runs only after the previous stage completes successfully.

---

# Airflow DAG

```python
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "etl"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python /usr/local/airflow/Scripts/upload_to_minio.py",
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command="python /usr/local/airflow/Scripts/transform_taxi_data.py",
    )

    data_quality = BashOperator(
        task_id="data_quality",
        bash_command="python /usr/local/airflow/Scripts/data_quality.py",
    )

    upload_processed = BashOperator(
        task_id="upload_processed_data",
        bash_command="python /usr/local/airflow/Scripts/upload_processed_to_minio.py",
    )

    truncate_postgres = BashOperator(
        task_id="truncate_postgres",
        bash_command="python /usr/local/airflow/Scripts/truncate_postgres.py",
    )

    load_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /usr/local/airflow/Scripts/load_to_postgres.py",
    )

    (
        ingest
        >> transform
        >> data_quality
        >> upload_processed
        >> truncate_postgres
        >> load_postgres
    )
```

---

# 10. Project Structure

```text
nyc-taxi-data-pipeline/
│
├── dags/
│   ├── nyc_taxi_ingestion.py
│   └── nyc_taxi_pipeline.py
│
├── Scripts/
│   ├── create_postgres_table.py
│   ├── data_quality.py
│   ├── fix_airflow_paths.py
│   ├── inspect_data.py
│   ├── load_to_postgres.py
│   ├── read_with_spark.py
│   ├── test_spark.py
│   ├── transform_taxi_data.py
│   ├── truncate_postgres.py
│   ├── upload_processed_to_minio.py
│   └── upload_to_minio.py
│
├── jars/
│   └── postgresql-42.7.8.jar
│
├── tests/
│
├── Dockerfile
├── packages.txt
├── requirements.txt
├── .dockerignore
├── .gitignore
└── README.md
```

---

# 11. Important Scripts

### `upload_to_minio.py`

Uploads the raw NYC Taxi Parquet file to MinIO.

```text
Local Parquet
     ↓
MinIO /raw
```

### `transform_taxi_data.py`

Uses PySpark to:

* Read Parquet
* Calculate trip duration
* Calculate average speed
* Remove invalid records
* Handle missing values
* Write processed Parquet

### `data_quality.py`

Performs:

* Null checks
* Duplicate checks
* Duration validation
* Distance validation
* Fare validation
* Total amount validation
* Speed anomaly detection

### `upload_processed_to_minio.py`

Uploads transformed Parquet files to MinIO.

### `truncate_postgres.py`

Clears the existing PostgreSQL table before a fresh load.

### `load_to_postgres.py`

Reads processed Parquet using Spark and loads it into PostgreSQL through JDBC.

### `create_postgres_table.py`

Creates the PostgreSQL `taxi_trips` table.

---

# 12. Running the Project

## Start Airflow

From the project directory:

```bash
astro dev start
```

Airflow UI:

```text
http://localhost:8080
```

---

## Start MinIO

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

MinIO Console:

```text
http://localhost:9001
```

---

## Start PostgreSQL

```bash
docker run -d \
  --name nyc-postgres \
  -e POSTGRES_USER=nyc_user \
  -e POSTGRES_PASSWORD=nyc_password \
  -e POSTGRES_DB=nyc_taxi \
  -p 5432:5432 \
  postgres:16
```

Check the container:

```bash
docker ps
```

Connect to PostgreSQL:

```bash
docker exec -it nyc-postgres psql -U nyc_user -d nyc_taxi
```

---

## Start Metabase

If the Metabase container already exists:

```bash
docker start metabase
```

Metabase connects to PostgreSQL using:

```text
Host: host.docker.internal
Port: 5432
Database: nyc_taxi
User: nyc_user
```

---

# 13. Running the Airflow Pipeline

Open:

```text
http://localhost:8080
```

Find:

```text
nyc_taxi_pipeline
```

Trigger the DAG.

The expected task order is:

```text
ingest_raw_data
        ↓
transform_data
        ↓
data_quality
        ↓
upload_processed_data
        ↓
truncate_postgres
        ↓
load_to_postgres
```

After successful execution:

```sql
SELECT COUNT(*)
FROM taxi_trips;
```

Expected result:

```text
3518153
```

---

# 14. Useful PostgreSQL Queries

## Total Trips

```sql
SELECT COUNT(*)
FROM taxi_trips;
```

## Total Revenue

```sql
SELECT SUM(total_amount)
FROM taxi_trips;
```

## Average Fare

```sql
SELECT AVG(fare_amount)
FROM taxi_trips;
```

## Average Trip Distance

```sql
SELECT AVG(trip_distance)
FROM taxi_trips;
```

## Daily Trips

```sql
SELECT
    DATE(pickup_datetime) AS pickup_date,
    COUNT(*) AS trips
FROM taxi_trips
GROUP BY DATE(pickup_datetime)
ORDER BY pickup_date;
```

## Daily Revenue

```sql
SELECT
    DATE(pickup_datetime) AS pickup_date,
    SUM(total_amount) AS revenue
FROM taxi_trips
GROUP BY DATE(pickup_datetime)
ORDER BY pickup_date;
```

## Payment Type Distribution

```sql
SELECT
    payment_type,
    COUNT(*) AS trips
FROM taxi_trips
GROUP BY payment_type
ORDER BY payment_type;
```

## Top Pickup Locations

```sql
SELECT
    pickup_location_id,
    COUNT(*) AS trips
FROM taxi_trips
GROUP BY pickup_location_id
ORDER BY trips DESC
LIMIT 10;
```

---

# 15. Why Parquet?

Parquet is a columnar storage format designed for analytical workloads.

Instead of storing data primarily row by row:

```text
Row 1
Row 2
Row 3
...
```

Parquet organizes data by columns internally.

For example:

```text
trip_distance
--------------
2.4
5.1
3.7
...
```

This allows Spark to read only the columns required for a query.

Benefits include:

* Efficient analytical queries
* Compression
* Reduced storage
* Faster Spark processing
* Schema preservation
* Good compatibility with data lake architectures

---

# 16. Why MinIO?

MinIO provides S3-compatible object storage that can run locally.

It is useful for simulating a data lake without requiring AWS S3.

The architecture resembles:

```text
Data Lake
    |
    ├── raw/
    └── processed/
```

In a cloud deployment, the same concept could be implemented using Amazon S3.

MinIO provides an S3-compatible API, which is why libraries such as `boto3` can interact with it.

---

# 17. Why PostgreSQL?

MinIO/Parquet is useful for storing large analytical datasets, but querying Parquet directly is not always ideal for dashboard applications.

PostgreSQL provides:

* SQL querying
* Indexing
* Structured tables
* Database management
* Easy integration with BI tools

The project therefore uses:

```text
MinIO → Data Lake Storage
PostgreSQL → Data Serving Layer
```

---

# 18. Why Airflow?

Airflow is used as the orchestration layer.

The individual Python/Spark scripts perform the actual work, while Airflow controls:

* Execution order
* Dependencies
* Scheduling
* Task status
* Logs
* Retries
* Monitoring

For example:

```text
Ingestion must finish
        ↓
before transformation
        ↓
before quality checks
        ↓
before database loading
```

This is the main purpose of a workflow orchestrator.

---

# 19. Data Engineering Concepts Demonstrated

This project demonstrates several important data engineering concepts.

### ETL

```text
Extract
   ↓
Transform
   ↓
Load
```

The project extracts NYC Taxi data, transforms it with Spark, and loads it into PostgreSQL.

### Data Lake

MinIO stores raw and processed Parquet data.

```text
MinIO
 ├── raw
 └── processed
```

### Batch Processing

The January 2026 dataset is processed as a batch rather than continuously.

### Distributed Processing

PySpark provides a distributed data processing framework and is commonly used for large-scale data transformation.

### Data Quality

The pipeline explicitly validates the processed dataset before loading it into the analytical database.

### Idempotency

The PostgreSQL load can be repeated without accumulating duplicate records because the destination table is truncated before loading.

### Orchestration

Airflow coordinates the complete workflow.

### Data Serving

PostgreSQL provides structured data for downstream BI tools.

### Business Intelligence

Metabase converts the processed data into dashboards and visualizations.

---

# 20. Local Infrastructure

The project uses Docker containers for infrastructure components.

```text
Docker
│
├── Airflow / Astro
├── MinIO
├── PostgreSQL
└── Metabase
```

PySpark runs inside the Airflow environment for pipeline execution.

---

# 21. Service URLs

| Service       | URL                                            |
| ------------- | ---------------------------------------------- |
| Airflow       | [http://localhost:8080](http://localhost:8080) |
| MinIO API     | [http://localhost:9000](http://localhost:9000) |
| MinIO Console | [http://localhost:9001](http://localhost:9001) |
| Metabase      | [http://localhost:3000](http://localhost:3000) |
| PostgreSQL    | localhost:5432                                 |

---

# 22. Repository Hygiene

Large data files are intentionally excluded from Git.

The `.gitignore` excludes:

```text
*.parquet
processed_yellow_tripdata_2026-01/
```

It also excludes:

```text
.venv/
__pycache__/
logs/
.astro/
.env
```

This keeps the repository focused on source code and configuration rather than large generated datasets.

---

# 23. Limitations

This project is designed as a local data engineering project.

Current limitations include:

* Single-month dataset
* Local MinIO instead of cloud object storage
* Local PostgreSQL
* Local Airflow deployment
* Static demo credentials
* No automated cloud deployment
* No streaming ingestion
* No production-scale Spark cluster

---

# 24. Possible Future Improvements

Potential extensions include:

* Process multiple months of NYC Taxi data
* Add incremental ingestion
* Add Airflow retries and failure notifications
* Add automated unit tests
* Add stronger data quality validation
* Add Great Expectations or another data quality framework
* Add partitioning by pickup date
* Add PostgreSQL indexes
* Add dimensional modeling
* Deploy MinIO/S3 storage to the cloud
* Deploy Spark on Databricks
* Deploy Airflow to a managed environment
* Add CI/CD using GitHub Actions
* Add monitoring and alerting
* Implement incremental PostgreSQL loads

---

# 25. End-to-End Result

The completed pipeline processes the NYC Yellow Taxi dataset through the following architecture:

```text
                NYC TLC
                   |
                   v
          Yellow Taxi Parquet
                   |
                   v
              Airflow
                   |
                   v
             MinIO /raw
                   |
                   v
               PySpark
                   |
          +--------+--------+
          |                 |
          v                 v
   Transformation     Data Cleaning
          |                 |
          +--------+--------+
                   |
                   v
            Data Quality
                   |
                   v
          MinIO /processed
                   |
                   v
             PostgreSQL
                   |
                   v
              Metabase
                   |
                   v
             Dashboard
```

Final processed dataset:

```text
Raw records:        3,724,889
Processed records:  3,518,153
Records removed:      206,736
Duplicates:                   0
Null values:                  0
Invalid duration:             0
Invalid distance:             0
Negative fares:               0
Negative totals:              0
Speed anomalies:            720
```

The project demonstrates a complete batch data engineering pipeline from **raw source data to a BI dashboard**, while covering ingestion, object storage, Spark processing, data quality, database loading, orchestration, and visualization.

Available next action: Create a downloadable DOCX file here in this chat containing the editable prose above
