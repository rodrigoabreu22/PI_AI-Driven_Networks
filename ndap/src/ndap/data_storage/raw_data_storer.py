import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import base64

# Configuration
TOPIC_RAW_DATA_RCV = "RAW_NETWORK_DATA_RECEIVED"
BROKER = 'kafka:9092'

load_dotenv()

INFLUXDB_URL = "http://influxdb_raw:8086"
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Initialize clients
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def create_kafka_consumer():
    """Creates a Kafka consumer."""
    consumer = KafkaConsumer(
        TOPIC_RAW_DATA_RCV,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: v
    )
    return consumer

def convert_to_rfc3339(timestamp):
    """Convert timestamp to RFC3339 format with robust handling."""
    try:
        if isinstance(timestamp, str):
            # Handle both quoted and unquoted timestamps
            clean_timestamp = timestamp.strip('"')
            try:
                dt = datetime.strptime(clean_timestamp, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                dt = datetime.fromtimestamp(float(clean_timestamp))
        else:
            dt = datetime.fromtimestamp(float(timestamp))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception as e:
        logging.error(f"Timestamp conversion error: {e}")
        return None
    

def receive_and_store_data():
    """Receive raw bytes from Kafka, encode as Base64 safely, and store in InfluxDB with timestamp."""
    consumer = create_kafka_consumer()

    for message in consumer:
        try:
            # 1. Extract timestamp from headers
            original_ts = None
            if message.headers:
                for key, value in message.headers:
                    if key == 'timestamp':
                        try:
                            original_ts = float(value.decode('utf-8'))
                        except Exception as e:
                            logging.warning(f"Invalid timestamp header format: {e}")
                        break

            if original_ts is None:
                logging.warning("Missing timestamp in Kafka message; skipping.")
                continue

            # 2. Convert to RFC3339 timestamp
            rfc_timestamp = convert_to_rfc3339(original_ts)
            if not rfc_timestamp:
                logging.warning("Skipping message due to timestamp conversion failure.")
                continue

            # 3. Encode raw bytes to Base64
            if message.value is None:
                logging.warning("Kafka message has no value; skipping.")
                continue

            try:
                raw_data_b64 = base64.b64encode(message.value).decode('ascii')  # ASCII-safe Base64 string
            except Exception as e:
                logging.error(f"Failed to encode packet to Base64: {e}")
                continue

            # 4. Write to InfluxDB
            point = (
                Point("network_packet")
                .field("raw_data", raw_data_b64)
                .time(rfc_timestamp)
            )

            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            logging.info(f"Stored packet at {rfc_timestamp}")

        except Exception as e:
            logging.error(f"Error processing Kafka message: {e}", exc_info=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/raw_data_storer.log')
        ]
    )
    logging.info("Starting network packet storage service")
    receive_and_store_data()

if __name__ == "__main__":
    main()