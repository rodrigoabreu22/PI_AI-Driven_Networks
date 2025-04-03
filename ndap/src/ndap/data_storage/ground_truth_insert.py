from influxdb_client import InfluxDBClient, BucketRetentionRules, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

INFLUXDB_URL = "http://influxdb_raw:8086"  
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
BUCKET_NAME = "ground_truth"
CSV_FILE_PATH = "/app/NUSW-NB15_GT.csv"

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

buckets_api = client.buckets_api()

bucket_response = buckets_api.find_bucket_by_name(BUCKET_NAME)
    
if bucket_response == None:
    retention_rules = BucketRetentionRules(type="expire", every_seconds=0)  
    buckets_api.create_bucket(bucket_name=BUCKET_NAME, org=INFLUXDB_ORG, retention_rules=retention_rules)
    print(f"Bucket '{BUCKET_NAME}' created.")

    df = pd.read_csv(CSV_FILE_PATH)

    df.columns = df.columns.str.strip()

    df = df.drop(columns=["."], errors="ignore")

    # Define which columns should be tags
    tag_columns = [
        "Source Port", "Destination Port",  "Protocol",
        "Source IP", "Destination IP"
    ]

    # Define fields 
    field_columns = [
        "Attack category"
    ]

    write_api = client.write_api(write_options=SYNCHRONOUS)

    for _, row in df.iterrows():
        point = Point("attack_logs")

        # Add tags (for filtering)
        for tag in tag_columns:
            if tag in df.columns:
                point = point.tag(tag, str(row[tag]).strip())  # Convert to string and strip spaces

        # Add fields 
        for field in field_columns:
            try:
                value = str(row[field]) if pd.notna(row[field]) else ""
                point = point.field(field, value)

            except ValueError:
                continue  

        write_api.write(bucket=BUCKET_NAME, org=INFLUXDB_ORG, record=point)

    print("Data successfully written to InfluxDB.")

client.close()