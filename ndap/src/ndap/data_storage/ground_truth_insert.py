from influxdb_client import InfluxDBClient, BucketRetentionRules, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

INFLUXDB_URL = "http://influxdb_raw:8086"  
INFLUXDB_TOKEN = os.getenv("INFLUXDB_INIT_ADMIN_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_INIT_ORG")
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

    write_api = client.write_api(write_options=SYNCHRONOUS)
    for _, row in df.iterrows():
        point = Point("ground_truth") 
        for col in df.columns:
            point = point.field(col, row[col])
        write_api.write(bucket=BUCKET_NAME, org=INFLUXDB_ORG, record=point)

    print("Data successfully written to InfluxDB.")

client.close()