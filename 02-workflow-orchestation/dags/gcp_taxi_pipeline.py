import urllib.request
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


GCP_PROJECT_ID = "project-ad6a13b6-36d6-4a7b-b67"
GCP_BUCKET_NAME = "project-ad6a13b6-36d6-4a7b-b67-demo-bucket"
BQ_DATASET_NAME = "my_dataset"

FILE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
LOCAL_FILE_PATH = "/opt/airflow/dags/yellow_taxi_2021_01.csv.gz"
GCS_DESTINATION_PATH = "raw/yellow_taxi_2021_01.csv.gz"

def download_taxi_data(url, output_path):
    print(f"Downloading data from {url}...")
    urllib.request.urlretrieve(url, output_path)
    print("Successfully saved data")

with DAG(
    dag_id="gcp_taxi_pipeline_v1",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False    
) as dag:
    
    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_taxi_data,
        op_kwargs={
            "url": FILE_URL,
            "output_path": LOCAL_FILE_PATH    
        }
    )
    
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_to_gcs",
        src=LOCAL_FILE_PATH,
        dst=GCS_DESTINATION_PATH,
        bucket=GCP_BUCKET_NAME,
        gcp_conn_id="gcp_connection",
    )
    
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=GCP_BUCKET_NAME,
        source_objects=[GCS_DESTINATION_PATH],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.yellow_taxi_data",
        source_format="CSV",
        compression="GZIP",
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="gcp_connection",
    )
    
    download_task >> upload_to_gcs_task >> gcs_to_bq_task