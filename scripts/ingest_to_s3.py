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

# File to store the last successfully ingested record IDs (watermarks)
WATERMARK_FILE = os.path.join(os.path.dirname(__file__), "watermarks.json")

def load_watermarks():
    if os.path.exists(WATERMARK_FILE):
        try:
            with open(WATERMARK_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Warning: Could not read watermark file: {str(e)}")
            return {}
    return {}

def save_watermarks(watermarks):
    try:
        with open(WATERMARK_FILE, "w") as f:
            json.dump(watermarks, f, indent=2)
    except Exception as e:
        print(f"⚠️ Warning: Could not save watermark file: {str(e)}")

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

def fetch_all_records(token, endpoint, last_processed_id=None):
    print(f"📥 Fetching data from /{endpoint}...")
    all_data = []
    page = 1
    stop_fetching = False
    
    while not stop_fetching:
        url = f"{API_URL}/{endpoint}?page={page}"
        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            res_json = response.json()
            data = res_json.get("data", [])
            
            if not data:
                break
                
            for record in data:
                # Early Termination Check
                if last_processed_id and record.get("id") == last_processed_id:
                    print(f"🛑 Match found for last processed ID: '{last_processed_id}'. Stopping fetch.")
                    stop_fetching = True
                    break
                all_data.append(record)
                
            if stop_fetching:
                break
                
            meta = res_json.get("meta", {})
            has_next = meta.get("hasNextPage", False)
            if not has_next:
                break
            page += 1
        else:
            raise Exception(f"Failed to fetch {endpoint} page {page}: {response.status_code} - {response.text}")
            
    print(f"✅ Fetched new {endpoint}. Total: {len(all_data)} records.")
    return all_data

def upload_to_s3(data, data_type):
    if not data:
        print(f"⚠️ No new data to upload for {data_type}.")
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
        print(f"✅ {data_type} upload completed successfully!")
    except Exception as e:
        print(f"❌ Failed to upload {data_type} to S3: {str(e)}")

def main():
    try:
        token = get_jwt_token()
        watermarks = load_watermarks()
        new_watermarks = watermarks.copy()
        
        # 1️⃣ Ingest Facts (Payins & Payouts) incrementally
        facts = ["payins", "payouts"]
        for dataset in facts:
            last_id = watermarks.get(dataset)
            data = fetch_all_records(token, dataset, last_processed_id=last_id)
            
            if data:
                upload_to_s3(data, dataset)
                # The first item in 'data' is the newest one (ORDER BY created_on DESC)
                new_watermarks[dataset] = data[0]["id"]
            else:
                print(f"ℹ️ No new records found for {dataset}.")
                
            print("-" * 50)
            
        # 2️⃣ Ingest Dimensions (Merchants & Accounts) - full snapshot
        dimensions = ["merchants", "accounts"]
        for dataset in dimensions:
            data = fetch_all_records(token, dataset)
            upload_to_s3(data, dataset)
            print("-" * 50)
            
        # Save updated watermarks
        save_watermarks(new_watermarks)
            
    except Exception as e:
        print(f"❌ Ingestion Error: {str(e)}")

if __name__ == "__main__":
    main()
