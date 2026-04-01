import urllib.request
import gzip
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def download_taxi_data(url, output_path):
    print(f"Downloading data from {url}...")
    urllib.request.urlretrieve(url, output_path)
    print("Successfully saved data")

def load_csv_to_postgres(file_path, table_name, connection_id):
    pg_hook = PostgresHook(postgres_conn_id=connection_id)
    db_connection = pg_hook.get_conn()
    db_cursor = db_connection.cursor()

    print(f"Loading data from {file_path} into {table_name}...")
    
    copy_query = f"COPY {table_name} FROM STDIN WITH CSV HEADER"
    
    with gzip.open(file_path, 'rt') as csv_file:
        db_cursor.copy_expert(copy_query, csv_file)
        
    db_connection.commit()
    db_cursor.close()
    print("Data successfully loaded into Postgres!")

with DAG(
    dag_id="taxi_pipeline_v3",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False    
) as dag:
    
    create_table = PostgresOperator(
        task_id="create_taxi_table",
        postgres_conn_id="my_local_postgres",
        sql="""
            -- We drop the table first so we don't accidentally keep the old 6-column version
            DROP TABLE IF EXISTS yellow_taxi_data; 
            
            CREATE TABLE yellow_taxi_data (
                VendorID INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count FLOAT,
                trip_distance FLOAT,
                RatecodeID FLOAT,
                store_and_fwd_flag VARCHAR(1),
                PULocationID INT,
                DOLocationID INT,
                payment_type FLOAT,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT
            );
        """
    )
    
    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_taxi_data,
        op_kwargs={
            "url": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz",
            "output_path": "/opt/airflow/dags/yellow_taxi.csv.gz"    
        }
    )
    
    load_data_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "file_path": "/opt/airflow/dags/yellow_taxi.csv.gz",
            "table_name": "yellow_taxi_data",
            "connection_id": "my_local_postgres"
        }
    )
    
    create_table >> download_task >> load_data_task