import boto3
import json
from datetime import datetime

s3_client = boto3.client('s3')
BUCKET_NAME = 'mote-finder'

def upload_to_s3(data, company_symbol, endpoint):
    # Define folder structure in S3
    today = datetime.today().strftime('%Y/%m/%d')
    filename = f"{endpoint}_{datetime.now().timestamp()}.json"
    s3_path = f"raw/{company_symbol}/{endpoint}/{today}/{filename}"

    # Upload JSON data to S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_path,
        Body=json.dumps(data),
        ContentType='application/json'
    )