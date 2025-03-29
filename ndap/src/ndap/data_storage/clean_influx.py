from influxdb_client import InfluxDBClient
import os
from dotenv import load_dotenv

load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

delete_api = client.delete_api()

# Delete all data (use a non-empty predicate)
delete_api.delete(
    start="1970-01-01T00:00:00Z",
    stop="2025-12-31T23:59:59Z",
    bucket=INFLUXDB_BUCKET,
    org=INFLUXDB_ORG,
    predicate="_measurement!=\"\""
)

print("All data deleted.")

client.close()
