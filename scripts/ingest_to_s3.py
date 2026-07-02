import os
import json
import datetime
import requests
import boto3
from dotenv import load_dotenv

# Load local .env file if available
load_dotenv()

# Configurations
API_URL = os.getenv("API_URL", "https://data-engg-api-production.up.railway.app")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "data-engg-fintech-lake-kumard915")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# AWS Credentials (configured in .env or system environment)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def get_jwt_token():
    print("🔑 Authenticating with API...")
    url = f"{API_URL}/login"
    payload = {
        "username": ADMIN_USERNAME,
        "password": ADMIN_PASSWORD
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print("✅ Logged in successfully!")
        return response.json().get("token")
    else:
        raise Exception(f"Failed to login: {response.status_code} - {response.text}")

def fetch_payins(token):
    print("📥 Fetching transaction data (/payins)...")
    all_data = []
    page = 1
    
    while True:
        url = f"{API_URL}/payins?page={page}"
        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            res_json = response.json()
            data = res_json.get("data", [])
            all_data.extend(data)
            
            meta = res_json.get("meta", {})
            has_next = meta.get("hasNextPage", False)
            if not has_next:
                break
            page += 1
        else:
            raise Exception(f"Failed to fetch payins page {page}: {response.status_code} - {response.text}")
            
    print(f"✅ Fetched all data. Total: {len(all_data)} transaction records across {page} page(s).")
    return all_data

def upload_to_s3(data, data_type="payins"):
    if not data:
        print("⚠️ No data to upload.")
        return

    # Initialize boto3 S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # Date partitioning (Standard Data Engineering Practice)
    now = datetime.datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    # Path in S3 bucket
    s3_key = f"raw/{data_type}/year={year}/month={month}/day={day}/{data_type}_{timestamp}.json"
    
    print(f"📤 Uploading data to S3: s3://{S3_BUCKET_NAME}/{s3_key}...")
    
    # Format data as structured JSON
    json_data = json.dumps(data, indent=2, default=str)
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        print("✅ Upload completed successfully!")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {str(e)}")

def main():
    try:
        token = get_jwt_token()
        payins = fetch_payins(token)
        upload_to_s3(payins, "payins")
    except Exception as e:
        print(f"❌ Ingestion Error: {str(e)}")

if __name__ == "__main__":
    main()
