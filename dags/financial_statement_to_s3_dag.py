import polars as pl
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.api_helpers import fetch_and_store

# DAG definition
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'financial_data_dag',
    default_args=default_args,
    description='DAG to fetch financial data for SP500 companies',
    schedule_interval='@daily',  # Run once per day
    catchup=False
)


def process_companies(**kwargs):
    # Load SP500 companies list
    sp500_table = pl.read_csv('data/sp500.csv').with_row_index('company_id')
    companies = list(sp500_table['Symbol'])

    # Pull previously processed companies from XCom
    processed_companies = kwargs['ti'].xcom_pull(task_ids='fetch_financial_data', key='processed_companies') or []

    # Select up to 12 unprocessed companies
    companies_to_process = [c for c in companies if c not in processed_companies][:12]

    # Push the list of companies to process to the next task
    kwargs['ti'].xcom_push(key='companies_to_process', value=companies_to_process)

    # If no companies left to process, raise an exception to stop the DAG
    if not companies_to_process:
        raise ValueError("All companies have been processed. Stopping the DAG.")

    return companies_to_process



def update_processed_companies(**kwargs):
    # Pull companies from the current run
    companies_processed = kwargs['ti'].xcom_pull(task_ids='get_companies_to_process', key='companies_to_process')

    # Pull the previously processed companies
    previous_processed_companies = kwargs['ti'].xcom_pull(task_ids='fetch_financial_data',
                                                          key='processed_companies') or []

    # Update the list of processed companies
    all_processed_companies = previous_processed_companies + companies_processed

    # Push the updated list of processed companies back to XCom for the next run
    kwargs['ti'].xcom_push(key='processed_companies', value=all_processed_companies)


# Tasks
get_companies_task = PythonOperator(
    task_id='get_companies_to_process',
    python_callable=process_companies,
    provide_context=True,
    dag=dag,
)

fetch_financial_statements_task = PythonOperator(
    task_id='fetch_financial_data',
    python_callable=fetch_and_store,
    provide_context=True,
    dag=dag,
)

update_processed_companies_task = PythonOperator(
    task_id='update_processed_companies',
    python_callable=update_processed_companies,
    provide_context=True,
    dag=dag,
)

get_companies_task >> fetch_financial_statements_task >> update_processed_companies_task