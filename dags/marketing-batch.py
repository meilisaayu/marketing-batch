from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import json
import pandas as pd
import numpy as np
import os
from supabase import create_client, client

dag_directory = os.path.dirname(os.path.abspath(__file__))
credential_path = os.path.join(dag_directory, 'YOUR_JSON_KEYFILE')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
bucket_name = 'YOUR_BUCKET_NAME'
DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'YOUR_DATASET_NAME')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'YOUR_TABLE_NAME')

os.environ["SUPABASE_URL"] = 'YOUR_SUPABASE_URL'
os.environ["SUPABASE_KEY"] = 'YOUR_SUPABASE_KEY'

# Access environment variables
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")

# Create Supabase client
supabase = create_client(supabase_url, supabase_key)

def upload_to_gcs(bucket_name_local, df, filename):
    df.to_csv(filename)
    client = storage.Client()
    blob = client.bucket(bucket_name_local).blob(filename)
    blob.upload_from_filename(filename)
    os.remove(filename)

def fetch_supabase_data_in_batches(**kwargs):
    
    # Make a request using the Supabase client
    response = supabase.table('YOUR_SUPABASE_TABLE_NAME').select("*").execute() #my table name is 'marketing'

    print(response.data)
    #print(response.data[0].get('id'))
    #data = response.get('data', [])

    # Convert the data to JSON format
    json_data = json.dumps(response.data)
    df = pd.read_json(json_data)

    # Add year column based on month column
    df['year'] = np.hstack((np.array([2008] * (27690 - 0)),
                       np.array([2009] * (39130 - 27690)),
                       np.array([2010] * (41188 - 39130))))

    # Make year column beside month column
    df = df[['age', 'job', 'marital', 'education', 'default', 'housing',
         'loan', 'contact', 'month', 'year', 'day_of_week',
         'duration', 'campaign', 'pdays', 'previous', 'poutcome',
         'emp.var.rate', 'cons.price.idx', 'cons.conf.idx',
         'euribor3m', 'nr.employed', 'y']]
    
    # Correct the month format
    df['month'] = df['month'].str.capitalize()
    df['year'] = df['year'].astype(str)

    # Group the data by month and year
    grouped_df = df.groupby(['month', 'year'])

    for (target_month, target_year), group_df in grouped_df:
        # Use a consistent filename pattern
        output_filename = f'output_{target_month}_{target_year}.csv'
        upload_to_gcs(bucket_name, group_df, output_filename)
        
default_args = {
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'timeout': timedelta(minutes=30)
}

dag = DAG(
    'supabase_to_gcs_batch',
    default_args=default_args,
    description='Transfer data from Supabase to GCS in batches',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 25),
    tags=['final-project'],
)


fetch_data_task = PythonOperator(
    task_id='fetch_supabase_data_in_batches',
    python_callable=fetch_supabase_data_in_batches,
    provide_context=True,
    dag=dag,
)

create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', 
    dataset_id=DATASET_NAME, 
    dag=dag
)

create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id=DATASET_NAME,
    table_id=TABLE_NAME,
)

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery_example',
    bucket=bucket_name,
    source_objects=['output_*.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    source_format='CSV',  # Specify the source format
    autodetect=True,  # Enable schema autodetection
    create_disposition='CREATE_IF_NEEDED',  # Specify the create disposition
    skip_leading_rows=1,  # Skip the header row
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

fetch_data_task >> create_test_dataset >> create_table >> load_csv
