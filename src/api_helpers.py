import os
import requests
import json
from dotenv import load_dotenv
from src.s3_helpers import upload_to_s3

load_dotenv()
AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
VANTAGE_KEY = os.environ['VANTAGE_API']

def fetch_statement_data(company, endpoint):
    base_url = f'https://www.alphavantage.co/'
    url = f"{base_url}/query?function={endpoint}?symbol={company}&apikey={VANTAGE_KEY}"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data for {company}: {response.status_code}")


def fetch_and_store_statements(**kwargs):
    companies = kwargs['ti'].xcom_pull(task_ids='get_companies_to_process')
    for company in companies:
        for endpoint in ['income_statement', 'balance_sheet']:
            data = fetch_statement_data(company, endpoint)
            # Save to S3
            upload_to_s3(data, company, endpoint)