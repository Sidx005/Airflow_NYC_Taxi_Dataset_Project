import boto3
from botocore.exceptions import ClientError

FILE_PATH = "/usr/local/airflow/yellow_tripdata_2026-01.parquet"

MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY="minioadmin"


BUCKET_NAME="nyc-taxi"
OBJECT_NAME="raw/yellow_tripdata_2026-01.parquet"

def upload_to_minio():
    s3_client=boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        s3_client.upload_file(
            FILE_PATH,
            BUCKET_NAME,
            OBJECT_NAME
        
        )
    except ClientError as error:
        print(f"Upload failed: {error}")
        raise
if __name__=="__main__":
    upload_to_minio()