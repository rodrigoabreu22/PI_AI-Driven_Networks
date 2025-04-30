import time
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from scapy.utils import wrpcap
from scapy.all import *
import os
import glob

TOPIC = "RAW_NETWORK_DATA"
BROKER = 'kafka:9092'

def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
    """
    Ensure the Kafka topic exists; create it if it does not.
    """
    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker)
        
        # Check if the topic exists
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logging.info(f"Topic '{topic_name}' already exists.")
        else:
            # Create the topic
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([topic])
            logging.info(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
    finally:
        if admin_client is not None:
            admin_client.close()

def create_kafka_producer():
    """Creates a Kafka producer for sending raw packet data."""
    producer = KafkaProducer(
        bootstrap_servers=BROKER
    )
    logging.info("Kafka producer started.")
    return producer

def send_to_kafka(producer, topic, packet):
    """Sends raw packet data to Kafka with timestamp preservation"""
    try:
        # Get packet timestamp if available
        timestamp = getattr(packet, 'time', time.time())
        
        # Include timestamp in message headers
        headers = [
            ('timestamp', str(timestamp).encode('utf-8'))
        ]
        
        # Send raw packet with headers
        producer.send(
            topic,
            value=raw(packet),
            headers=headers
        )
        producer.flush()
    except Exception as e:
        logging.error(f"Error sending packet: {e}")

def is_binary_field(value):
    """Check if a field value should be treated as binary (hex)."""
    if isinstance(value, (bytes, bytearray)):
        return True
    elif isinstance(value, str) and len(value) % 2 == 0 and all(c in '0123456789abcdefABCDEF' for c in value):
        return True
    
    return False

def get_pcap_data(pcap_file):
    """Reads packets from a large PCAP file continuously, converts to JSON format, logs it, and sends to Kafka."""
    producer = create_kafka_producer()
    
    with PcapReader(pcap_file) as pcap_reader:
        for packet in pcap_reader:
            send_to_kafka(producer, TOPIC, packet)

def main():
    PCAP_DIR = "dataset_files"
    logging.basicConfig(
        filename='logs/data_producer.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    create_topic(TOPIC, BROKER)

    # Get all pcap files in directory (sorted numerically if named like 1.pcap, 2.pcap, etc.)
    pcap_files = sorted(
        glob.glob(os.path.join(PCAP_DIR, "*.pcap")),
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0])
    )

    if not pcap_files:
        logging.warning("No PCAP files found to process.")
        return

    for file_path in pcap_files:
        logging.info(f"Processing file: {file_path}")
        get_pcap_data(file_path)
    
    logging.info("Finished processing all PCAP files.")

if __name__ == "__main__":
    main()