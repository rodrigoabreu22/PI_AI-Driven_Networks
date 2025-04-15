import time
from influxdb_client import InfluxDBClient
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timezone
import base64

load_dotenv()

# Configurations
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
KAFKA_TOPIC = "DATA_TO_BE_PROCESSED"
KAFKA_BROKER = 'localhost:29092'
CHECK_INTERVAL = 10  
EMPTY_DB_WAIT_TIME = 5  

# Initialize InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
    """
    Ensure the Kafka topic exists; create it if it does not.
    """
    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker)
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logging.info(f"Topic '{topic_name}' already exists.")
        else:
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([topic])
            logging.info(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
    finally:
        if admin_client is not None:
            admin_client.close()

# Initialize Kafka producer
def create_kafka_producer():
    """Creates a Kafka producer for sending raw packet data."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER
    )
    logging.info("Kafka producer started.")
    return producer

# Initialize logging
def initialize_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("logs/influx_extractor.log")],
    )

def load_last_timestamp():
    """Load last processed timestamp from file, or fetch from InfluxDB if missing."""
    try:
        with open("last_timestamp.txt", "r") as f:
            return f.read().strip()
        
    except FileNotFoundError:
        return fetch_earliest_timestamp()

def save_last_timestamp(timestamp):
    """Save the last processed timestamp to a file."""
    with open("last_timestamp.txt", "w") as f:
        f.write(str(timestamp))

def fetch_earliest_timestamp():
    """Retrieve the earliest timestamp in the database or return None if no data exists."""
    query = f"""
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: 1970-01-01T00:00:00Z)
        |> filter(fn: (r) => r._measurement == "network_packet")
        |> filter(fn: (r) => r._field == "raw_data")
        |> group(columns: [])
        |> first()
    """

    try:
        tables = query_api.query(query)

        for table in tables:
            for record in table.records:
                timestamp = record.get_time()  # Already a datetime object with timezone
                formatted = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                logging.info(f"Earliest timestamp: {formatted}")
                return formatted

        logging.info("No data found in InfluxDB.")
        return None

    except Exception as e:
        logging.error(f"Error fetching earliest timestamp: {e}", exc_info=True)
        return None


# Fix this to function to get 100 packets by a given timestamp, 
# it should convert it back to bytes and send it to kafka with the packet timestamp as header in the message
# so the packet can be reconstructed by the consumer (other script that I will implement)
def main():
    """Main function that continuously checks for new data and pushes it to Kafka."""
    initialize_logging()
    logging.info("Starting InfluxDB data extraction...")
    create_topic(KAFKA_TOPIC, KAFKA_BROKER)
    producer=create_kafka_producer()

    while True:
        try:
            last_processed_timestamp = load_last_timestamp()

            if last_processed_timestamp is None:
                logging.info("No data yet. Waiting...")
                time.sleep(EMPTY_DB_WAIT_TIME)
                continue  

            query = f"""
                from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: {last_processed_timestamp})
                |> filter(fn: (r) => r._measurement == "network_packet")
                |> filter(fn: (r) => r._field == "raw_data")
                |> group(columns: ["_time"])
                |> sort(columns: ["_time"])
                |> limit(n: 100)
            """

            tables = query_api.query(query)
            packets = []

            for table in tables:
                for record in table.records:
                    time_obj = record.get_time()
                    raw_data_str = record.get_value()  # Assuming it's a hex or base64-encoded string
                    timestamp_str = time_obj.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    timestamp_float = time_obj.timestamp()


                    packets.append({
                        "raw_data": raw_data_str,
                        "timestamp": timestamp_str,
                        "timestamp_float": timestamp_float
                    })

            if packets:
                logging.info(f"Found {len(packets)} packets.")

                for pkt in packets:
                    # Decode raw packet string to bytes (hex assumed here)
                    try:
                        pkt_bytes = decode_packet(pkt)
                    except ValueError:
                        logging.warning("Failed to decode packet; skipping.")
                        continue

                    # Use timestamp as header
                    headers = [('timestamp', str(timestamp_float).encode('utf-8'))]

                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, pkt_bytes, headers=headers)

                # Save the timestamp of the last packet
                save_last_timestamp(packets[-1]["timestamp"])

            else:
                logging.info("No new packets found.")

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            logging.error(f"Error: {e}", exc_info=True)
            time.sleep(10)


def decode_packet(packet):
    try:
        encoded = packet["raw_data"]
        raw_bytes = base64.b64decode(encoded)
        return raw_bytes
    
    except Exception as e:
        logging.warning("Failed to decode packet; skipping.", exc_info=True)
        return None

if __name__ == "__main__":
    main()