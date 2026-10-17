# 🚕 NYC Taxi Data Pipeline

A production-style, end-to-end **ETL data pipeline** for the NYC Yellow Taxi dataset, orchestrated with **Apache Airflow** (via Astronomer Runtime), powered by **Apache Spark** for large-scale transformations, and served through **Metabase** dashboards.

---

## Dataset

**Link** : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

**File** : yellow_tripdata_2026-01.parquet (January 2026)

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       Apache Airflow (Astronomer)               │
│                                                                 │
│  [Ingest Raw Data] → [Transform (Spark)] → [Data Quality]      │
│        → [Upload Processed] → [Create Table] → [Load Postgres] │
└────────┬────────────────────────────┬────────────────┬─────────┘
         │                            │                │
         ▼                            ▼                ▼
    ┌─────────┐               ┌──────────────┐  ┌──────────────┐
    │  MinIO  │               │  PostgreSQL  │  │   Metabase   │
    │ (S3-API)│               │  (nyc_taxi)  │  │  Dashboard   │
    └─────────┘               └──────────────┘  └──────────────┘
```

---

## 🔄 Pipeline DAG

The Airflow DAG (`nyc_taxi_pipeline`) runs the following tasks in sequence:

| Step | Task ID | Script | Description |
|------|---------|--------|-------------|
| 1 | `ingest_raw_data` | `upload_to_minio.py` | Uploads the raw `.parquet` file to MinIO (`nyc-taxi/raw/`) |
| 2 | `transform_data` | `transform_taxi_data.py` | Cleans & enriches data with PySpark |
| 3 | `data_quality` | `data_quality.py` | Runs automated data quality checks |
| 4 | `upload_processed_data` | `upload_processed_to_minio.py` | Uploads transformed data back to MinIO |
| 5 | `create_postgres_table` | `create_postgres_table.py` | Creates `taxi_trips` table in PostgreSQL (if not exists) |
| 6 | `truncate_postgres` | `truncate_postgres.py` | Clears stale data before reload |
| 7 | `load_to_postgres` | `load_to_postgres.py` | Loads processed parquet into PostgreSQL via Spark JDBC |

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Orchestration | Apache Airflow (Astronomer Runtime) | `3.3-8` |
| Distributed Processing | Apache Spark (PySpark) | latest |
| Object Storage | MinIO (S3-compatible) | latest (Chainguard) |
| Database | PostgreSQL | `16` |
| BI / Dashboards | Metabase | latest |
| Containerization | Docker + Docker Compose | — |
| JDBC Driver | PostgreSQL JDBC | `42.7.8` |
| Java Runtime | OpenJDK | `21 JRE headless` |

---

## 📦 Project Structure

```
nyc-taxi-data-pipeline/
├── dags/
│   └── nyc_taxi_pipeline.py         # Main Airflow DAG
├── Scripts/
│   ├── upload_to_minio.py           # Raw data ingestion → MinIO
│   ├── transform_taxi_data.py       # PySpark transformations
│   ├── data_quality.py              # PySpark data quality checks
│   ├── upload_processed_to_minio.py # Processed data → MinIO
│   ├── create_postgres_table.py     # DDL: taxi_trips table
│   ├── truncate_postgres.py         # Truncate before reload
│   ├── load_to_postgres.py          # Spark JDBC → PostgreSQL
│   └── read_with_spark.py           # Utility: inspect parquet files
├── jars/
│   └── postgresql-42.7.8.jar        # JDBC driver for Spark
├── Dockerfile                       # Astronomer Runtime base image
├── docker-compose.yml               # PostgreSQL + MinIO + Metabase services
├── docker-compose.override.yml      # Airflow services joined to pipeline network
├── airflow_settings.yaml            # Local Airflow connections/variables
├── .env                             # Environment variables (not committed to Git)
├── requirements.txt                 # Python dependencies
└── packages.txt                     # System packages (OpenJDK 21)
```

---

## ⚙️ Transformation Logic

The PySpark transformation step (`transform_taxi_data.py`) performs the following:

1. **Read** raw parquet from the local Airflow mount
2. **Derive `trip_duration_minutes`** from pickup/dropoff timestamps
3. **Filter invalid trips** — removes records with zero/negative duration, distance, or fare
4. **Impute missing values** for passenger count, rate codes, surcharges, and fees
5. **Compute `average_speed_mph`** = `trip_distance / (duration_in_hours)`
6. **Round** derived columns to 2 decimal places
7. **Write** the cleaned dataset to a local parquet output path

---

## ✅ Data Quality Checks

The `data_quality.py` script validates the transformed dataset across 7 dimensions:

| Check | Rule |
|-------|------|
| Null Values | All columns checked for NULLs |
| Duplicates | Total rows vs. distinct rows |
| Trip Duration | `trip_duration_minutes > 0` |
| Trip Distance | `trip_distance > 0` |
| Fare Amount | `fare_amount >= 0` |
| Total Amount | `total_amount >= 0` |
| Unrealistic Speed | Warning if `average_speed_mph > 100` |

The pipeline prints a `DATA QUALITY CHECK PASSED` or `FAILED` summary and raises an exception on failure to halt downstream tasks.

---

## 🗄️ PostgreSQL Schema

```sql
CREATE TABLE IF NOT EXISTS taxi_trips (
    vendor_id               INTEGER,
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    passenger_count         BIGINT,
    trip_distance           DOUBLE PRECISION,
    ratecode_id             BIGINT,
    store_and_fwd_flag      VARCHAR(10),
    pickup_location_id      INTEGER,
    dropoff_location_id     INTEGER,
    payment_type            BIGINT,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION,
    cbd_congestion_fee      DOUBLE PRECISION,
    trip_duration_minutes   DOUBLE PRECISION,   -- derived
    average_speed_mph       DOUBLE PRECISION    -- derived
);
```

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Astronomer CLI (`astro`)](https://www.astronomer.io/docs/astro/cli/install-cli)
- NYC Yellow Taxi parquet file for the target month placed in the project root

---

### Step 1 — Configure the `.env` file

All credentials and file paths are managed through the `.env` file at the project root. This file is read by both `docker-compose.yml` (for the PostgreSQL and MinIO containers) and by the Python scripts at runtime (via `os.environ`).

Fill in the `.env` file before starting any services:

```env
# ─────────────────────────────────────────────
# MinIO (S3-compatible Object Storage)
# ─────────────────────────────────────────────
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=your_minio_user
MINIO_SECRET_KEY=your_minio_password
MINIO_BUCKET_NAME=your_minio_bucket

# ─────────────────────────────────────────────
# PostgreSQL
# ─────────────────────────────────────────────
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=your_postgres_port
POSTGRES_DB=your_postgres_db
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password

# ─────────────────────────────────────────────
# File Paths (inside Airflow container)
# ─────────────────────────────────────────────
RAW_FILE_PATH=/usr/local/airflow/yellow_tripdata_2026-01.parquet
PROCESSED_OUTPUT_PATH=/usr/local/airflow/processed_yellow_tripdata_2026-01
POSTGRES_JDBC_JAR=/usr/local/airflow/jars/postgresql-42.7.8.jar

# ─────────────────────────────────────────────
# MinIO Object Keys
# ─────────────────────────────────────────────
MINIO_RAW_OBJECT_NAME=raw/yellow_tripdata_2026-01.parquet
MINIO_PROCESSED_PREFIX=processed/yellow_tripdata_2026-01
```

> **Note:** `docker-compose.yml` uses `${POSTGRES_USER}`, `${POSTGRES_PASSWORD}`, `${POSTGRES_DB}`, `${MINIO_ACCESS_KEY}`, and `${MINIO_SECRET_KEY}` directly from this file — so the database and object storage containers are configured from the same single source of truth as the Python scripts.

> **Important:** `.env` is listed in `.gitignore` and is **never committed to Git**. Do not hardcode credentials in source files.

---

### Step 2 — Start Supporting Services (MinIO, PostgreSQL, Metabase)

> **Note on two Compose projects:** This project uses **two separate Docker Compose setups** — `docker-compose.yml` (your services: Postgres, MinIO, Metabase) and the Astronomer/Astro CLI Compose (Airflow services). Always use the `-f docker-compose.yml` flag with your project's Compose file to avoid accidentally targeting the Astro Compose project.

```bash
docker compose -f docker-compose.yml up -d
```

This starts:
- **MinIO** → `http://localhost:9001` (UI) / `http://localhost:9000` (API)
- **PostgreSQL** → `localhost:5433`
- **Metabase** → `http://localhost:3000`

---

### Step 3 — Start Airflow (Astronomer)

```bash
astro dev start
```

This builds the custom Astronomer Runtime image (with Java 21 and all Python dependencies) and starts the Airflow scheduler, API server, triggerer, and DAG processor — all joined to the `nyc-taxi-pipeline` Docker network.

Airflow UI → `http://localhost:8080`

---

### Step 4 — Set Up MinIO

1. Open `http://localhost:9001` in your browser
2. Log in with:
   - **Username:** `your_minio_user` (or the value of `MINIO_ACCESS_KEY` in your `.env`)
   - **Password:** `your_minio_password` (or the value of `MINIO_SECRET_KEY`)
3. Go to **Buckets → Create Bucket**
4. Create a bucket named **`your_minio_bucket`**

> The pipeline will upload raw data to `nyc-taxi/raw/` and processed data to `nyc-taxi/processed/` automatically when the DAG runs.

---

### Step 5 — Set Up Metabase

> **Important — Dashboard is not persistent:** The `Dashboard.png` screenshot in this repo is a sample dashboard created during development. It is **not bundled with the project**. After setting up Metabase on a new machine, you will start with a blank Metabase instance and will need to recreate the questions and dashboard manually after the pipeline has run and data is loaded into PostgreSQL.

**First-time setup:**

1. Open `http://localhost:3000` in your browser
2. Complete the Metabase onboarding wizard:
   - Create your admin account (set your own email and password)
   - When prompted to add your data, select **PostgreSQL** and enter:

| Field | Value |
|-------|-------|
| Display Name | `NYC Taxi` |
| Host | `nyc-postgres` *(use the Docker container name, not `localhost`)* |
| Port | `5432` |
| Database name | `nyc_taxi` |
| Username | `nyc_user` |
| Password | `nyc_password` |
| SSL | Off |

3. Complete setup and log in to the Metabase home screen

**After the pipeline runs — Sync the schema:**

After the Airflow DAG completes and data is loaded into PostgreSQL, Metabase will not immediately show the `taxi_trips` table. Metabase syncs its schema on a schedule (default: every hour). To force it to show the table immediately:

1. Go to **Admin Panel** (gear icon → Admin settings)
2. Click **Databases** in the left sidebar
3. Click on **NYC Taxi** (your database)
4. Click **Sync database schema now**

The `taxi_trips` table will then appear when creating new questions or browsing data.

**Recreating the dashboard:**

Once the schema is synced, you can recreate the analytics dashboard. Suggested questions to build:

| Visual | Metabase Question Type | Key Column |
|--------|----------------------|------------|
| Total Trips | Count of rows | — |
| Average Fare | Average of `fare_amount` | — |
| Average Trip Distance | Average of `trip_distance` | — |
| Total Revenue | Sum of `total_amount` | — |
| Daily Trips | Count, grouped by `pickup_datetime` (day) | `pickup_datetime` |
| Daily Revenue | Sum of `total_amount`, grouped by day | `pickup_datetime` |
| Payment Type Distribution | Count, grouped by `payment_type` | `payment_type` |
| Top 10 Pickup Zones | Count, grouped by `pickup_location_id`, limit 10 | `pickup_location_id` |

After creating these as individual questions, combine them into a new dashboard named **NYC Taxi Dashboard**.

---

### Step 6 — Trigger the Pipeline

In the Airflow UI (`http://localhost:8080`), navigate to the `nyc_taxi_pipeline` DAG and click **Trigger DAG ▶**.

Monitor each task in the **Graph** view. Once `load_to_postgres` turns green, the data is in PostgreSQL and ready for Metabase.

---

## 🛑 Stopping the Project

Because there are **two separate Compose projects** running (your services + the Astro/Airflow Compose), use the correct commands for each:

### Stop your project services (Postgres, MinIO, Metabase)

```bash
# Stop containers but keep volumes (data is preserved)
docker compose -f docker-compose.yml stop

# Stop AND remove containers (volumes are still preserved)
docker compose -f docker-compose.yml down
```

### Stop Airflow (Astronomer)

```bash
astro dev stop
```

### ⚠️ Remove everything including data volumes

> [!CAUTION]
> The `-v` flag deletes all named volumes — this permanently erases your PostgreSQL records, MinIO objects, and Metabase dashboard configuration. Only use this for a full clean reset.

```bash
# Destroy project services + volumes
docker compose -f docker-compose.yml down -v

# Destroy Airflow + volumes
astro dev kill
```

---

## 🔧 Configuration

### Python Dependencies (`requirements.txt`)

```
pandas
pyarrow
boto3
apache-airflow-providers-amazon
pyspark
psycopg2-binary
```

### System Packages (`packages.txt`)

```
openjdk-21-jre-headless
```

> Java 21 is required by PySpark inside the Airflow container.

---

## 🌐 Service Access Summary

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | value of `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` in `.env` |
| MinIO S3 API | http://localhost:9000 | — |
| PostgreSQL | `localhost:5433` | value of `POSTGRES_USER` / `POSTGRES_PASSWORD` in `.env` |
| Metabase | http://localhost:3000 | set during first-login wizard |

---

## 📁 MinIO Bucket Layout

```
nyc-taxi/
├── raw/
│   └── yellow_tripdata_2026-01.parquet        ← uploaded by ingest step
└── processed/
    └── yellow_tripdata_2026-01/               ← uploaded by upload_processed step
        └── *.parquet
```

---

## 📝 License

This project is for educational and portfolio purposes. Dataset sourced from the [NYC Taxi & Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
