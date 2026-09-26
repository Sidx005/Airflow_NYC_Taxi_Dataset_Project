from airflow import DAG
from airflow.decorators import task
import boto3

from datetime import datetime
import os


FILE_PATH="/usr/local/airflow/yellow_tripdata_2026-01.parquet"

BUCKET_NAME = "nyc-taxi"
OBJECT_NAME = "raw/yellow_tripdata_2026-01.parquet"

MINIO_ENDPOINT = "http://host.docker.internal:9000"

with DAG(
    dag_id="nyc_taxi_ingestion",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi","ingestion"]

)as dag:
    @task
    def check_file():
        if not os.path.exists(FILE_PATH):
            raise FileNotFoundError(
            f"File not found: {FILE_PATH}"
    
            )
        print(f"File exists: {FILE_PATH}")
    @task
    def upload_to_minio():
     s3_client = boto3.client(
        "s3",
        endpoint_url="http://host.docker.internal:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

     s3_client.upload_file(
        FILE_PATH,
        BUCKET_NAME,
        OBJECT_NAME,
    )

     print("Upload successful!")
     print(f"Bucket: {BUCKET_NAME}")
     print(f"Object: {OBJECT_NAME}")

    check=check_file()
    upload=upload_to_minio()

    check >> upload
