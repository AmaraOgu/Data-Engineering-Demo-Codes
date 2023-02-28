from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteBucketOperator
import uuid
from datetime import timedelta
import datetime as dt
from airflow.utils.dates import days_ago
import fnmatch
import yfinance as yf
from google.cloud import storage

PROJECT_ID="amara-sandbox-1"
STAGING_DATASET = "stock_dataset"
LOCATION = "us-central1"

default_args = {
    'owner': 'Amara',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

def get_data():
    # Tickers list for data extraction from yahoo finance
    tickers = ['MSFT','AMZN','GOOGL']

    # Set start and end dates
    today = dt.datetime.now()
    start = dt.datetime(2023, 2, 1,)
    end = dt.date(today.year, today.month, today.day)

    # API call to download data from yahoo finance
    data = yf.download(tickers=tickers, start=start, end=end, interval='1d',)['Adj Close']
    
    # Convert the data to CSV and encode 
    data = data.to_csv(index=True).encode()

    # Create a storage client
    storage_client = storage.Client()

    # Get a list of all buckets
    buckets = list(storage_client.list_buckets())

    # Filter the list of buckets to only include those with the desired prefix
    buckets_with_prefix = [bucket for bucket in buckets if fnmatch.fnmatch(bucket.name, 'the_demo_*')]

    #Choose the matching buckets to upload the data to
    bucket = buckets_with_prefix[0]

    # Upload the data to the selected bucket
    blob = bucket.blob('stock_data.csv')
    blob.upload_from_string(data)
    print(f"data sucessfully uploadesd to {bucket}")


with DAG('Stock_data',
         start_date=days_ago(1), 
         schedule_interval="@once",
         catchup=False, 
         default_args=default_args, 
         tags=["gcs", "bq"]
) as dag:

    generate_uuid = PythonOperator(
            task_id="generate_uuid", 
            python_callable=lambda: "the_demo_" + str(uuid.uuid4()),
            
        )

    create_bucket = GCSCreateBucketOperator(
            task_id="create_bucket",
            bucket_name="{{ task_instance.xcom_pull('generate_uuid') }}",
            project_id=PROJECT_ID,
            
        )

    pull_stock_data_to_gcs = PythonOperator(
        task_id = 'pull_stock_data_to_gcs',
        python_callable = get_data,

        )

    load_to_bq = GCSToBigQueryOperator(
        task_id = 'load_to_bq',
        bucket = "{{ task_instance.xcom_pull('generate_uuid') }}",
        source_objects = ['stock_data.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.stock_data_table',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'AMZN', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'GOOGL', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'MSFT', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            ],
        )
    
    delete_bucket = GCSDeleteBucketOperator(
            task_id="delete_bucket",
            bucket_name="{{ task_instance.xcom_pull('generate_uuid') }}",
        )

    (
        generate_uuid
        >> create_bucket
        >> pull_stock_data_to_gcs
        >> load_to_bq
        >> delete_bucket
    )
