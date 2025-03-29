import json
import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

TOPIC_RAW_DATA_RCV = "RAW_NETWORK_DATA_RECEIVED"
TOPIC_PROCESSED_DATA_RCV = "PROCESSED_NETWORK_DATA"
BROKER = 'localhost:29092'

load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1))

def create_kafka_consumer(topic):
    """Creates a Kafka consumer with automatic JSON deserialization."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    return consumer


def convert_unix_to_rfc3339(unix_timestamp):
    """Convert Unix timestamp (seconds) to RFC3339 format."""
    try:
        dt = datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def store_raw_data(consumer, data):

    # Extract timestamp (assuming "Stime" is the best time field)
    unix_timestamp = data.get("Stime", None)  # Ensure this is in RFC3339 format
    timestamp = convert_unix_to_rfc3339(unix_timestamp)
    logging.info(f"Converted timestamp: {timestamp}")

    if not timestamp:
        return  # Skip if no timestamp

    # Create InfluxDB point dynamically
    point = Point("network_raw_data").time(timestamp, WritePrecision.NS)

    for key, value in data.items():
        #if key == "Stime":  # Already used as timestamp
        #    continue

        # Add fields and tags dynamically
        if isinstance(value, (int, float)):  # Store as a field
            point.field(key, value)
        else:  # Store as a tag
            point.tag(key, str(value))
    try:
        # Write to InfluxDB
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logging.info(f"Stored in InfluxDB: {json.dumps(data, default=str)}")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB: {e}")



def receive_data():
    consumer = create_kafka_consumer(TOPIC_RAW_DATA_RCV)

    try:
        while True:
            messages = consumer.poll(1.0)  # Returns a dictionary

            if not messages:
                continue

            for _, records in messages.items():  # Iterate over partitions
                for record in records:  # Iterate over messages
                    try:
                        data = record.value  # Already deserialized by value_deserializer
                        store_raw_data(consumer, data)
                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decoding failed: {e}")
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()
        client.close()


def main():
    logging.basicConfig(filename='dbs_middleware.log', level=logging.INFO)
    receive_data()

if __name__ == "__main__":
    main()