import json
import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

# Kafka Configuration
TOPIC_RECEIVE = "RELAY_NETWORK_CRONOGRAF_DATA"
BROKER = 'kafka:9092'

load_dotenv()

# InfluxDB Configuration
INFLUXDB_URL = "http://influxdb_processed:8086"
INFLUXDB2_TOKEN = os.getenv("INFLUXDB2_TOKEN")
INFLUXDB2_ORG = os.getenv("INFLUXDB2_ORG")
INFLUXDB_BUCKET = "processed_data"

influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB2_TOKEN, org=INFLUXDB2_ORG)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1))

def create_kafka_consumer():
    """Creates a Kafka consumer with automatic JSON deserialization."""
    consumer = KafkaConsumer(
        TOPIC_RECEIVE,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    return consumer

def store_data(data):
    """Writes processed data to InfluxDB."""
    timestamp = datetime.now(timezone.utc).isoformat()

    point = Point("network_processed_data").time(timestamp, WritePrecision.NS).tag("source", "kafka")
    
    for key, value in data.items():
        if isinstance(value, (int, float)):
            point.field(key, value)
        else:
            point.tag(key, str(value))

    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB2_ORG, record=point)
        logging.info(f"Written to InfluxDB: {json.dumps(data, default=str)}")
        logging.info(f"Writing data with timestamp: {timestamp}")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB: {e}")

def receive_and_store_data():
    """Consumes messages from Kafka and stores them in InfluxDB."""
    consumer = create_kafka_consumer()
    try:
        for message in consumer:
            logging.info(f"Received: {message.value}")
            store_data(message.value)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        consumer.close()
        influx_client.close()

def main():
    logging.basicConfig(filename='interface_influx.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Starting Kafka to InfluxDB service...")
    receive_and_store_data()

if __name__ == "__main__":
    main()

