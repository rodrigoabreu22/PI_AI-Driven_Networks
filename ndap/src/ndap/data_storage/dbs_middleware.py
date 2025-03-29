import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from scapy.all import Ether

TOPIC_RAW_DATA_RCV = "RAW_NETWORK_DATA_RECEIVED"
BROKER = 'localhost:29092'

load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1))

def create_kafka_consumer(topic):
    """Creates a Kafka consumer for raw binary packet data."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    return consumer

def store_raw_data(packet):
    """Processes and stores raw packet data in InfluxDB."""
    try:
        parsed_packet = Ether(packet.value)  # Parse raw binary data with Scapy
        timestamp = datetime.utcnow().isoformat()

        # Create InfluxDB point
        point = Point("network_raw_data2").time(timestamp, WritePrecision.NS)

        if parsed_packet.haslayer(Ether):
            point.tag("src_mac", parsed_packet.src)
            point.tag("dst_mac", parsed_packet.dst)
            point.field("eth_type", parsed_packet.type)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logging.info(f"Stored packet in InfluxDB: {parsed_packet.summary()}")
    except Exception as e:
        logging.error(f"Error processing packet: {e}")

def receive_data():
    consumer = create_kafka_consumer(TOPIC_RAW_DATA_RCV)
    try:
        for message in consumer:
            store_raw_data(message)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()

def main():
    logging.basicConfig(filename='dbs_middleware.log', level=logging.INFO)
    receive_data()

if __name__ == "__main__":
    main()
