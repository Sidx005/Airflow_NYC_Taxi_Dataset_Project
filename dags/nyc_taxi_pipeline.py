from airflow import DAG
from airflow.operators.bash import BashOperator
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
    truncate_postgres= BashOperator(
        task_id="truncate_postgres",
        bash_command="python /usr/local/airflow/Scripts/truncate_postgres.py",
    )
    load_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /usr/local/airflow/Scripts/load_to_postgres.py",
    )

    ingest >> transform >> data_quality >> upload_processed >> truncate_postgres >> load_postgres