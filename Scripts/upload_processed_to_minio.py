import boto3
import os

LOCAL_PATH = "/usr/local/airflow/processed_yellow_tripdata_2026-01"

MINIO_ENDPOINT = "http://minio:9000"

MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY="minioadmin"

BUCKET_NAME = "nyc-taxi"
MINIO_PREFIX="processed/yellow_tripdata_2026-01"


def upload_processed_data():
    s3_client=boto3.client("s3",endpoint_url=MINIO_ENDPOINT,aws_access_key_id=MINIO_ACCESS_KEY,aws_secret_access_key=MINIO_SECRET_KEY,region_name="us-east-1")

    for file_name in os.listdir(LOCAL_PATH):
        file_path= os.path.join(LOCAL_PATH,file_name)

        if os.path.isfile(file_path) and file_name.endswith(".parquet"):
            object_name=f"{MINIO_PREFIX}/{file_name}"

            print(f"Uploading: {file_name}")
            s3_client.upload_file(file_path,BUCKET_NAME,object_name)

            print(f"Uploaded: {object_name}")


if __name__ == "__main__":
    upload_processed_data()

    print("\nProcessed data upload completed!")