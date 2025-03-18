import json
import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

TOPIC_RAW_DATA_RCV = "RAW_NETWORK_DATA_RECEIVED"
TOPIC_PROCESSED_DATA_RCV = "PROCESSED_NETWORK_DATA"
BROKER = 'localhost:29092'

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

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


def store_raw_data(client, write_api, consumer, data):

    # Extract timestamp (assuming "Stime" is the best time field)
    unix_timestamp = data.get("Stime", None)  # Ensure this is in RFC3339 format
    timestamp = convert_unix_to_rfc3339(unix_timestamp)
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

    # Write to InfluxDB
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    logging.info(f"Stored in InfluxDB: {json.dumps(data, default=str)}")


def receive_data():

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api()

    consumer = create_kafka_consumer(TOPIC_RAW_DATA_RCV)

    try:
        while True:
            msg = consumer.poll(1.0)  
            if msg is None:
                continue
            if msg.error():
                raise Exception(msg.error())

            # Parse JSON message
            data = json.loads(msg.value().decode('utf-8'))
            
            store_raw_data(client, write_api, consumer, data)


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