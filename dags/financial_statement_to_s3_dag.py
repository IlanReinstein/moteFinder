import os
import io
import sys
import boto3
import json
import s3fs
import requests

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv


load_dotenv()
s3_client = boto3.client('s3')
# Constants
ALPHAVANTAGE_API_KEY = os.environ['VANTAGE_API']
S3_BUCKET = 'mote-finder'
S3_PREFIX = 'raw/'
LIMIT_PER_DAY = 24
ALPHA_BASE_URL = 'https://www.alphavantage.co/query'
ENDPOINTS = ['INCOME_STATEMENT', 'BALANCE_SHEET', 'CASH_FLOW', 'EARNINGS']

# AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
# AWS_SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 10)
}

S3_CLIENT = boto3.client('s3')
def get_companies_to_process(**kwargs):
    # Example: Static list of companies (this can be replaced with a more dynamic list)
    sp500_table = pl.read_csv('data/sp500.csv')
    companies = list(sp500_table['Symbol'])
    # Push list of companies to XCom
    kwargs['ti'].xcom_push(key='companies_to_process', value=companies)


def get_processed_companies(**kwargs):
    # You can pull this from XCom, a file, or database to track previously processed companies
    processed_companies = kwargs['ti'].xcom_pull(task_ids='fetch_alpha_vantage_data', key='processed_companies')

    # Default to an empty list if no companies have been processed yet
    if not processed_companies:
        processed_companies = []
    # processed_companies = ['ACN', 'MMM', 'ABT', 'AES', 'AAP', 'AMD', 'AOS', 'AYI', 'ABBV', 'ADBE']
    kwargs['ti'].xcom_push(key='processed_companies', value=processed_companies)




# Function to make API calls and ensure both endpoints are fetched
def fetch_alpha_vantage_data(ti, **context):
    companies = ti.xcom_pull(task_ids='get_companies_to_process', key='companies_to_process')
    processed_companies = ti.xcom_pull(task_ids='get_processed_companies') #or []

    to_process = [c for c in companies if c not in processed_companies]
    successful_companies = []
    company_endpoint_status = {}
    for company in to_process[:6]:

        for endpoint in ENDPOINTS:
            try:
                # Make API call
                params = {
                    'function': endpoint,
                    'symbol': company,
                    'apikey': ALPHAVANTAGE_API_KEY
                }
                response = requests.get(ALPHA_BASE_URL, params=params)
                if response.status_code == 200:
                    json_data = response.json()

                    if 'Information' in json_data:
                        print(f"Invalid response for {company} - {endpoint}. Too many requests")
                        # company_processed = False  # If one endpoint fails, skip processing
                        break
                    if not json_data:
                        print(f"Invalid response for {company} - {endpoint}. Empty JSON, skipping to next")
                        continue

                    # Save valid JSON to S3
                    S3_CLIENT.put_object(Body=json.dumps(json_data),
                                         Bucket=S3_BUCKET,
                                         Key=f'{S3_PREFIX}{company}/{endpoint}.json',
                                         ContentType='application/json')

                    if company not in company_endpoint_status:
                        company_endpoint_status[company] = []

                    company_endpoint_status[company].append(endpoint)

                    # Only append to successful_companies when both endpoints are fetched
                    if len(company_endpoint_status[company]) == len(ENDPOINTS):
                        successful_companies.append(company)

            except Exception as e:

                print(f"Failed to fetch data for {company} - {endpoint}: {e}")

    processed_companies += successful_companies
    # Push the final processed companies list to XCom
    ti.xcom_push(key='processed_companies', value=processed_companies)
    # ti.xcom_push(key='api_calls_made', value=api_calls_made)

# Helper function to check if a file exists in S3


def list_s3_files(s3_prefix):
    # s3 = boto3.client('s3')
    response = S3_CLIENT.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
    return [content['Key'] for content in response.get('Contents', [])]

def load_existing_parquet_from_s3(s3_path):
    fs = s3fs.S3FileSystem()
    # write parquet
    with fs.open(s3_path, mode='rb') as f:
        return pl.read_parquet(f)

def save_parquet_to_s3(df, bucket_name, s3_key):
    # Convert DataFrame to Parquet in memory
    fs = s3fs.S3FileSystem()
    destination = f"s3://{S3_BUCKET}/{s3_key}"
    # write parquet
    with fs.open(destination, mode='wb') as f:
        df.write_parquet(f)

# Retrospective task to process S3 JSON files and load into table
def process_s3_json_to_table(endpoint):
    s3_keys = list_s3_files(S3_PREFIX)
    parquet_paths = list_s3_files(f'stage/combined_{endpoint}')
    endpoint_keys = [file for file in s3_keys if endpoint in file]

    if len(parquet_paths) > 0:
        parquet_file = [file for file in parquet_paths if endpoint in file][0]
        try:
            existing_data = load_existing_parquet_from_s3(f"s3://{S3_BUCKET}/{parquet_file}")
        except s3_client.exceptions.NoSuchKey:
            existing_data = pl.DataFrame([])
    else:
        parquet_file = f"stage/combined_{endpoint}.parquet"
        existing_data = pl.DataFrame([])

    for s3_key in endpoint_keys:
        # print(s3_key)
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET, Key=s3_key)
        json_data = json.loads(response['Body'].read())
        # Process and insert into table\
        if endpoint == 'EARNINGS':
            reports_df = pl.DataFrame(json_data['annualEarnings'])
            reports_df = reports_df.with_columns(
                pl.lit(json_data['symbol']).alias("company_id"),
                pl.col('fiscalDateEnding').str.to_date('%Y-%m-%d'),
                pl.col("reportedEPS").cast(pl.Float64, strict=False)
            )
        else:
            reports_df = pl.DataFrame(json_data['annualReports'])
            reports_df = reports_df.with_columns(
                pl.lit(json_data['symbol']).alias("company_id"),
                pl.col('fiscalDateEnding').str.to_date('%Y-%m-%d'),
                pl.col("*").exclude('fiscalDateEnding', 'reportedCurrency', 'company_id').cast(pl.Float64, strict=False)
            )

        # Add company symbol as a column


        if not existing_data.is_empty():
            combined_data = pl.concat([existing_data, reports_df], rechunk=True)
            combined_data = combined_data.unique()  # Keep last occurrence (upsert)
        else:
            combined_data = reports_df


    save_parquet_to_s3(combined_data, S3_BUCKET, parquet_file)


# Define the DAG
with DAG('financial_data_pipeline',
         default_args=default_args,
         schedule_interval='@daily',  # Adjust schedule as needed
         catchup=False) as dag:
    # Track API calls
    get_companies_to_process_task = PythonOperator(
        task_id='get_companies_to_process',
        python_callable=get_companies_to_process,
        provide_context=True,
    )
    get_processed_companies_task = PythonOperator(
        task_id='get_processed_companies',
        python_callable=get_processed_companies,
        provide_context=True,
    )
    # track_api_calls_task = PythonOperator(
    #     task_id='track_api_calls',
    #     python_callable=track_api_calls,
    #     provide_context=True
    # )
    # Fetch and store AlphaVantage JSON data
    fetch_alpha_vantage_data_task = PythonOperator(
        task_id='fetch_alpha_vantage_data',
        python_callable=fetch_alpha_vantage_data,
        provide_context=True
    )

    # Process the JSONs already in S3 into tables
    process_income_json_to_table = PythonOperator(
        task_id='process_income_json_to_table',
        python_callable=process_s3_json_to_table,
        op_kwargs={'endpoint': 'INCOME'}
    )
    process_balance_json_to_table = PythonOperator(
        task_id='process_balance_json_to_table',
        python_callable=process_s3_json_to_table,
        op_kwargs={'endpoint': 'BALANCE'}
    )
    process_cash_flow_json_to_table = PythonOperator(
        task_id='process_cash_flow_json_to_table',
        python_callable=process_s3_json_to_table,
        op_kwargs={'endpoint': 'CASH'}
    )
    process_earnings_json_to_table = PythonOperator(
        task_id='process_earnings_json_to_table',
        python_callable=process_s3_json_to_table,
        op_kwargs={'endpoint': 'EARNINGS'}
    )

    # Set task dependencies
    # >> track_api_calls_task
    get_companies_to_process_task >> get_processed_companies_task  >> fetch_alpha_vantage_data_task
    fetch_alpha_vantage_data_task >> [process_income_json_to_table, process_balance_json_to_table,
                                      process_cash_flow_json_to_table, process_earnings_json_to_table]