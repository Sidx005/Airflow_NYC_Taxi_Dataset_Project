# NYC Taxi Data Pipeline

A local batch ETL pipeline for processing NYC TLC Yellow Taxi trip data
using **Apache Airflow, PySpark, MinIO, PostgreSQL, Docker, and
Metabase**.

The project demonstrates an end-to-end data engineering workflow:

``` text
NYC TLC Parquet
      ↓
Airflow
      ↓
MinIO / Raw
      ↓
PySpark Transformation
      ↓
Data Quality
      ↓
MinIO / Processed
      ↓
PostgreSQL
      ↓
Metabase Dashboard
```

The stack runs locally with Docker and Astro/Airflow, without requiring
paid cloud infrastructure.

------------------------------------------------------------------------

## 1. Overview

![NYC Taxi Dashboard](Dashboard.png)

This project processes the **NYC TLC Yellow Taxi January 2026 dataset**
through a manually triggered Airflow batch DAG.

The pipeline performs:

-   Raw data ingestion
-   Parquet processing with PySpark
-   Data cleaning and transformation
-   Data-quality validation
-   Raw and processed data storage in MinIO
-   Loading cleaned data into PostgreSQL
-   Analytics and visualization using Metabase

## 2. Dataset Overview

**Dataset:** NYC TLC Yellow Taxi Trip Data --- January 2026

**Source file:** `yellow_tripdata_2026-01.parquet`

**Format:** Apache Parquet

### Raw dataset

-   Rows: **3,724,889**
-   Columns: **20**

Important columns include:

``` text
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
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

### After transformation

-   Final rows: **3,518,153**
-   Invalid records removed: **206,736**
-   Null values in validated fields: **0**
-   Duplicate records: **0**

> The Parquet source file is excluded from Git because of its size.
> Download the dataset separately and place it at the location expected
> by the ingestion script.

------------------------------------------------------------------------

## 3. Repository Structure

``` text
nyc-taxi-data-pipeline/
├── dags/
│   └── nyc_taxi_pipeline.py
├── Scripts/
│   ├── upload_to_minio.py
│   ├── transform_taxi_data.py
│   ├── data_quality.py
│   ├── upload_processed_to_minio.py
│   ├── truncate_postgres.py
│   ├── create_postgres_table.py
│   ├── load_to_postgres.py
│   └── inspect_taxi_data.py
├── Dockerfile
├── docker-compose.yml
├── packages.txt
├── requirements.txt
├── .gitignore
├── Dashboard.png
├── AirflowPipeline.png
└── README.md
```

  Component              Purpose
  ---------------------- -----------------------------------------------
  `dags/`                Airflow DAG definitions and task dependencies
  `Scripts/`             ETL, validation, and database scripts
  `Dockerfile`           Custom Airflow image configuration
  `docker-compose.yml`   PostgreSQL, MinIO, and Metabase services
  `packages.txt`         System packages, such as Java
  `requirements.txt`     Python dependencies
  Parquet file           Source NYC Taxi dataset; download separately

------------------------------------------------------------------------

## 4. Setup Commands

### Prerequisites

Install Docker Desktop, Astro CLI, and Git. You will also need the
January 2026 Yellow Taxi Parquet file.

### Start the Astro/Airflow environment

From the project directory:

``` bash
astro dev start
```

Airflow UI:

``` text
http://localhost:8080
```

### Start project services

**MinIO warning — read before starting the services on a new laptop:**
The original `minio/minio` image could not be pulled in the new laptop environment, so this project uses `cgr.dev/chainguard/minio:latest`. Because the Chainguard image runs as a non-root user, an existing MinIO volume may have incompatible permissions. If MinIO restarts with `Unable to write to the backend` or `file access denied`, fix the existing volume ownership before continuing. Do not delete the volume.

For this setup, the volume is `nyc-taxi-data-pipeline_minio_data`. Run the following from the directory containing `docker-compose.yml`:

``` bash
docker compose stop minio

docker run --rm --user 0:0 \
  -v nyc-taxi-data-pipeline_minio_data:/data \
  alpine:3.20 \
  sh -c 'chown -R 65532:65532 /data && chmod -R u+rwX /data'
```

Then start the project services:

``` bash
docker compose up -d
```

If your Compose project or volume name differs, inspect the actual MinIO mount first:

``` bash
docker inspect minio --format '{{json .Mounts}}'
```

Services:

``` text
PostgreSQL  → localhost:5432
MinIO API   → localhost:9000
MinIO UI    → localhost:9001
Metabase    → localhost:3000
Airflow     → localhost:8080
```

Airflow is started separately through Astro CLI; the other three
services are defined in the project Compose file.

### Check containers

``` bash
docker ps
docker compose ps
```

### Validate the Compose configuration

``` bash
docker compose config
```

### Restart project services

``` bash
docker compose restart nyc-postgres minio metabase
```

### Stop project services

``` bash
docker compose stop
```

> **Important:** Avoid `docker compose down -v` if you want to preserve
> PostgreSQL records, MinIO objects, and Metabase dashboards. The `-v`
> option removes Compose-managed named volumes.

------------------------------------------------------------------------

## 5. Pipeline Flow

![Airflow Pipeline](AirflowPipeline.png)

The Airflow DAG executes these tasks in sequence:

``` text
1. ingest_raw_data
        ↓
2. transform_data
        ↓
3. data_quality
        ↓
4. upload_processed_data
        ↓
5. truncate_postgres
        ↓
6. load_to_postgres
```

### 5.1 Ingest

**Script:** `Scripts/upload_to_minio.py`

Uploads the raw Parquet file to the `nyc-taxi` MinIO bucket:

``` text
nyc-taxi/
└── raw/
    └── yellow_tripdata_2026-01.parquet
```

### 5.2 Transform

**Script:** `Scripts/transform_taxi_data.py`

PySpark reads the raw Parquet data and creates a cleaned dataset with
derived metrics.

### 5.3 Data quality

**Script:** `Scripts/data_quality.py`

Checks the transformed data for:

-   Null values
-   Row count
-   Duplicate records
-   Invalid trip duration
-   Invalid trip distance
-   Negative fares
-   Negative total amounts
-   Abnormally high average speeds

### 5.4 Upload processed data

**Script:** `Scripts/upload_processed_to_minio.py`

Stores the processed Parquet output in MinIO:

``` text
nyc-taxi/
└── processed/
    └── yellow_tripdata_2026-01/
```

### 5.5 PostgreSQL refresh

**Script:** `Scripts/truncate_postgres.py`

Clears the existing target table before loading the new batch. This
makes the current implementation a **full-refresh batch pipeline**, not
an incremental pipeline.

Because the table is truncated before loading, a subsequent load failure
may leave the target table empty until the pipeline is rerun
successfully.

### 5.6 Load

**Script:** `Scripts/load_to_postgres.py`

Loads the processed dataset into:

``` text
PostgreSQL
└── Database: nyc_taxi
    └── Table: taxi_trips
```

Final loaded records: **3,518,153**

------------------------------------------------------------------------

## 6. Transformations Done

### Trip duration

Trip duration is calculated from the pickup and drop-off timestamps:

``` text
trip_duration_minutes =
    (dropoff timestamp - pickup timestamp) / 60
```

The timestamp difference is converted from seconds to minutes.

### Invalid trip removal

Records are removed when any of these conditions is true:

``` text
trip_duration_minutes <= 0
trip_distance <= 0
fare_amount < 0
total_amount < 0
```

This removed **206,736 records** in the documented run.

### Missing-value handling

The following defaults are applied to selected columns:

  Column                     Default
  ------------------------ ---------
  `passenger_count`              `1`
  `RatecodeID`                  `99`
  `store_and_fwd_flag`           `N`
  `congestion_surcharge`         `0`
  `Airport_fee`                  `0`
  `cbd_congestion_fee`           `0`

These are pipeline-specific fallback values; they do not recover the
original missing values.

### Average speed

Average speed is calculated in miles per hour:

``` text
average_speed_mph =
    trip_distance / (trip_duration_minutes / 60)
```

### Rounding

The following derived metrics are rounded to **two decimal places**:

``` text
trip_duration_minutes
average_speed_mph
```

------------------------------------------------------------------------

## 7. Data-Quality Results

The documented validation results for the processed dataset are:

  Check                                      Result
  ---------------------------------- --------------
  Processed rows                          3,518,153
  Nulls in validated fields                       0
  Duplicate rows                                  0
  Invalid trip durations remaining                0
  Invalid trip distances remaining                0
  Negative fares remaining                        0
  Negative total amounts remaining                0
  Speeds above 100 mph                 720 warnings

The 720 high-speed records were flagged as warnings. A warning is not
the same as a record being automatically removed.

------------------------------------------------------------------------

## 8. Dashboard Metrics & Visuals

![NYC Taxi Dashboard](Dashboard.png)

The **NYC Taxi Dashboard** contains eight cards and visualizations.

  -----------------------------------------------------------------------
  Visual                  Metric / purpose        View
  ----------------------- ----------------------- -----------------------
  **Total Trips**         Count of processed taxi Number
                          trips                   

  **Average Fare**        Average `fare_amount`   Number
                          per trip                

  **Average Trip          Average `trip_distance` Number
  Distance**                                      

  **Total Trip Amount**   Sum of `total_amount`   Number
                          across trips            

  **Daily Trips**         Trip count by pickup    Line chart
                          date                    

  **Daily Trip Amount**   Sum of `total_amount`   Line chart
                          by pickup date          

  **Payment Type          Trip count by payment   Pie chart
  Distribution**          type                    

  **Top 10 Pickup Zones** Pickup locations with   Horizontal bar chart
                          the highest trip counts 
  -----------------------------------------------------------------------

### Why these visuals?

-   **KPI cards** provide a quick summary of overall trip volume and
    financial measures.
-   **Daily Trips** helps show how trip demand changes across the month.
-   **Daily Trip Amount** shows the daily trend in recorded trip
    amounts.
-   **Payment Type Distribution** compares the payment categories.
-   **Top 10 Pickup Zones** identifies pickup zone IDs with the highest
    trip counts.

Together, the dashboard covers overall volume, financial metrics, time
trends, payment behaviour, and location demand.

> `PULocationID` represents TLC taxi-zone IDs rather than human-readable
> location names. A useful future improvement is to join the official
> TLC Taxi Zone lookup table and display zone names.

### Metric definitions

``` sql
-- Total Trips
SELECT COUNT(*) AS total_trips
FROM taxi_trips;

-- Average Fare
SELECT AVG(fare_amount) AS average_fare
FROM taxi_trips;

-- Average Trip Distance
SELECT AVG(trip_distance) AS average_trip_distance
FROM taxi_trips;

-- Total Trip Amount
SELECT SUM(total_amount) AS total_trip_amount
FROM taxi_trips;

-- Daily Trips
SELECT
    DATE(tpep_pickup_datetime) AS pickup_day,
    COUNT(*) AS trip_count
FROM taxi_trips
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY pickup_day;

-- Daily Trip Amount
SELECT
    DATE(tpep_pickup_datetime) AS pickup_day,
    SUM(total_amount) AS daily_amount
FROM taxi_trips
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY pickup_day;

-- Top 10 Pickup Zones
SELECT
    PULocationID,
    COUNT(*) AS trip_count
FROM taxi_trips
GROUP BY PULocationID
ORDER BY trip_count DESC
LIMIT 10;
```

------------------------------------------------------------------------

## 9. Metabase Components

Metabase is the BI and visualization layer. It connects to PostgreSQL
and queries the processed `taxi_trips` table.

### Database connection

Use these settings when configuring the database connection from the
Metabase container:

``` text
Database: nyc_taxi
Host:     host.docker.internal
Port:     5432
User:     nyc_user
Table:    taxi_trips
SSL:      Off for this local setup
```

Use the password configured for PostgreSQL in your local
`docker-compose.yml`.

### Questions

In Metabase, each individual KPI or chart is created as a **Question**.
The dashboard uses questions such as:

-   Count of rows
-   Average `fare_amount`
-   Average `trip_distance`
-   Sum of `total_amount`
-   Count of trips by pickup date
-   Sum of `total_amount` by pickup date
-   Count by `payment_type`
-   Count by `PULocationID`

### Payment type labels

A Metabase custom expression maps payment type IDs to readable labels:

``` text
0 → Flex Fare
1 → Credit Card
2 → Cash
3 → No Charge
4 → Dispute
5 → Unknown
6 → Voided Trip
```

This makes the payment chart easier to interpret than displaying numeric
codes alone.

### Dashboard

The individual Metabase Questions are combined into a dashboard named:

``` text
NYC Taxi Dashboard
```

This provides a single analytical view of the processed dataset.

### Metabase persistence

The Compose-managed volume is:

``` text
nyc-taxi-data-pipeline_metabase_data
```

This volume stores Metabase application state, including dashboard
configuration, across container restarts. A newly cloned project on
another machine creates a new volume; it does not automatically copy
dashboards from the original machine.

------------------------------------------------------------------------

## 10. MinIO Image and Volume-Permission Update

**Update (October 2026):** The `minio/minio` image used in the original
local setup could no longer be pulled in the new laptop environment.
Docker returned a pull-access error, so the Compose configuration was
changed to use the Chainguard MinIO image:

``` yaml
image: cgr.dev/chainguard/minio:latest
```

The new image started with a different non-root user/permission setup.
MinIO repeatedly restarted with errors similar to:

``` text
FATAL Unable to initialize backend: Unable to write to the backend
Error: unable to rename /data/.minio.sys/tmp ... file access denied
```

The issue was not caused by the Airflow DAG. The MinIO process could not
write to its mounted Docker volume because the volume's ownership did
not match the container user's UID/GID.

### Fix applied

The Compose-managed MinIO volume was:

``` text
nyc-taxi-data-pipeline_minio_data
```

MinIO was stopped, and a temporary Alpine container was run as root to
change ownership on the existing volume:

``` bash
docker compose stop minio

docker run --rm --user 0:0 \
  -v nyc-taxi-data-pipeline_minio_data:/data \
  alpine:3.20 \
  sh -c 'chown -R 65532:65532 /data && chmod -R u+rwX /data'

docker compose up -d minio
```

**What the command does:**

-   `--rm` removes the temporary helper container when it exits.
-   `--user 0:0` runs the helper as root so it can change file
    ownership.
-   `-v ...:/data` mounts the existing named volume; it does not create
    a new empty data directory.
-   `chown -R 65532:65532 /data` changes ownership recursively to
    UID/GID `65532`, used by the configured Chainguard image.
-   `chmod -R u+rwX /data` grants the owner read/write access and
    directory traversal permissions.

After the change, MinIO started successfully.

### Verify MinIO

``` bash
docker compose ps
docker compose logs minio --tail=30
curl -i http://localhost:9000/minio/health/live
```

A healthy MinIO instance should return **HTTP 200** from the liveness
endpoint.

### Important notes

-   The permission fix is a one-time setup/troubleshooting step for a
    volume with incompatible ownership. It does not need to run for
    every Airflow DAG execution.

-   If the Compose project name or volume name changes, retrieve the
    actual volume name with:

    ``` bash
    docker inspect minio --format '{{json .Mounts}}'
    ```

-   Do not run `docker compose down -v` as part of this fix. Removing
    the volume would delete the stored MinIO data.

-   The UID/GID and image details above match the working fix in this
    setup. If the MinIO image is changed or upgraded, verify the user ID
    and volume permissions again.

-   For reproducible deployments, pin a tested image version or digest
    instead of relying indefinitely on `latest`.

-   The local credentials in this project are for development only. Use
    secure secrets and credentials for shared or production
    environments.

------------------------------------------------------------------------

## 11. Docker Services and Persistence

The project uses Docker Compose for the supporting services:

  ----------------------------------------------------------------------------------------------------
  Service          Container                    Host port Persistent volume
  ---------------- ---------------- --------------------- --------------------------------------------
  PostgreSQL       `nyc-postgres`                  `5432` `nyc-taxi-data-pipeline_nyc_postgres_data`

  MinIO            `minio`                 `9000`, `9001` `nyc-taxi-data-pipeline_minio_data`

  Metabase         `metabase`                      `3000` `nyc-taxi-data-pipeline_metabase_data`
  ----------------------------------------------------------------------------------------------------

Named volumes keep service data outside the lifecycle of individual
containers. Removing and recreating a container normally preserves its
named volume; deleting the volume does not.

A clone on a different laptop includes the code in Git, but it does not
include the original laptop's Docker volumes. The new machine therefore
starts with its own PostgreSQL database, MinIO storage, and Metabase
application state unless those are migrated separately.

### Useful commands

``` bash
# Start services
docker compose up -d

# List running containers
docker ps

# View service logs
docker compose logs --tail=50 minio
docker compose logs --tail=50 nyc-postgres
docker compose logs --tail=50 metabase

# Restart a service
docker compose restart minio

# Stop containers without deleting named volumes
docker compose stop

# Inspect the MinIO volume mount
docker inspect minio --format '{{json .Mounts}}'
```

------------------------------------------------------------------------

## 12. Future Improvements

-   Join the official TLC Taxi Zone lookup so the dashboard displays
    zone names instead of only IDs.
-   Parameterize the month and source file to support additional monthly
    batches.
-   Add incremental loading or upsert logic instead of truncating the
    full table.
-   Add stronger schema checks and configurable data-quality thresholds.
-   Pin container images and dependency versions for repeatable
    deployments.
-   Add automated tests and pipeline monitoring.

------------------------------------------------------------------------

## 13. Overall

This project demonstrates a complete local batch data engineering
pipeline:

**Ingestion → Object storage → Transformation → Data quality → Database
loading → BI visualization**

It combines Airflow orchestration, PySpark processing, MinIO object
storage, PostgreSQL analytics storage, Docker-based local
infrastructure, and Metabase dashboards into a single practical data
engineering project.
