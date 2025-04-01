import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from scapy.all import Ether

# Configuration
load_dotenv()
TOPIC_RAW_DATA_RCV = "RAW_NETWORK_DATA_RECEIVED"
BROKER = 'localhost:29092'
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "101_Yvzx1bcYhRAXjf2-OmL3aQOBWiayeQ0pTgixDhN8DE06wgEkwZ5wvUWSktLzYIJmwjzHF-Y1KNT1WTJCDg=="
INFLUXDB_ORG = "pi_14"
INFLUXDB_BUCKET = "raw_data"

# Initialize clients
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def create_kafka_consumer(topic):
    """Creates a Kafka consumer with proper JSON deserialization."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

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

def flatten_json(data, prefix='', ignore_list=None):
    """Recursively flatten JSON data with optional ignore list."""
    if ignore_list is None:
        ignore_list = []
    
    flattened = {}
    
    def _flatten(value, key_prefix):
        if isinstance(value, dict):
            for k, v in value.items():
                new_prefix = f"{key_prefix}.{k}" if key_prefix else k
                if k in ignore_list:
                    flattened[new_prefix] = json.dumps(v) if isinstance(v, (dict, list)) else v
                else:
                    _flatten(v, new_prefix)
        elif isinstance(value, (list, tuple)):
            for i, item in enumerate(value):
                _flatten(item, f"{key_prefix}[{i}]")
        else:
            flattened[key_prefix] = value
    
    _flatten(data, prefix)
    return flattened

def store_flattened_packet(packet_data, ignore_fields=None):
    """Store flattened JSON packet in InfluxDB with robust error handling."""
    if ignore_fields is None:
        ignore_fields = []
    
    try:
        # Handle case where packet_data might be a string
        if isinstance(packet_data, str):
            try:
                packet_data = json.loads(packet_data)
            except json.JSONDecodeError:
                # If still problematic, try stripping extra quotes
                try:
                    packet_data = json.loads(packet_data.strip('"'))
                except Exception as e:
                    logging.error(f"Failed to parse packet data: {e}")
                    return

        if not isinstance(packet_data, dict):
            logging.error(f"Expected dictionary but got {type(packet_data)}")
            return

        # Extract and convert timestamp (try multiple timestamp fields)
        timestamp = None
        for ts_field in ['timestamp', 'timestamp_iso', 'time']:
            if ts_field in packet_data:
                timestamp = convert_to_rfc3339(packet_data[ts_field])
                if timestamp:
                    break
        
        if not timestamp:
            logging.warning("No valid timestamp found in packet")
            return
        
        # Flatten the JSON structure
        flattened = flatten_json(packet_data, ignore_list=ignore_fields)
        logging.debug(f"Flattened fields: {flattened}")
        
        # Create InfluxDB point with timestamp
        point = Point("network_packets").time(timestamp)
        
        # Add all flattened fields
        for field_name, field_value in flattened.items():
            if field_value is not None:  # Skip None values
                point.field(field_name, field_value)
        
        # Add important fields as tags
        for tag_field in ['packet_type', 'summary', 'source', 'proto']:
            if tag_field in packet_data and packet_data[tag_field] is not None:
                try:
                    point.tag(tag_field, str(packet_data[tag_field]))
                except Exception as e:
                    logging.warning(f"Couldn't add tag {tag_field}: {e}")
        
        # Write to InfluxDB
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logging.info(f"Successfully stored packet with timestamp {timestamp}")
        logging.info(f"{point}")

    except Exception as e:
        logging.error(f"Error storing flattened packet: {e}")
        logging.debug(f"Problematic packet data: {packet_data}")

def receive_and_store_data():
    """Receive data from Kafka and store in InfluxDB."""
    consumer = create_kafka_consumer(TOPIC_RAW_DATA_RCV)
    ignore_fields = []  # Add fields you want to keep as JSON strings here

    try:
        while True:
            messages = consumer.poll(1.0)
            if not messages:
                continue

            for _, records in messages.items():
                for record in records:
                    try:
                        packet_data = record.value
                        logging.debug(f"Received raw packet: {packet_data}")
                        store_flattened_packet(packet_data, ignore_fields)
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        consumer.close()
        influx_client.close()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/dbs_middleware.log'),
            logging.StreamHandler()
        ]
    )
    logging.info("Starting network packet storage service")
    receive_and_store_data()

if __name__ == "__main__":
    main()
