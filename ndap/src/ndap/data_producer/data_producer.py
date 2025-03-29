import time
import logging
from scapy.all import PcapReader, raw
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

TOPIC = "RAW_NETWORK_DATA"
BROKER = 'localhost:29092'

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
    """Sends raw packet data to Kafka."""
    producer.send(topic, raw(packet))  # Send raw binary data
    producer.flush()

def get_pcap_data(pcap_file):
    """Reads packets from a large PCAP file continuously and sends them to Kafka."""
    producer = create_kafka_producer()
    
    with PcapReader(pcap_file) as pcap_reader:
        for packet in pcap_reader:
            send_to_kafka(producer, TOPIC, packet)
            logging.info(f"Sent packet of length {len(packet)} bytes.")
            time.sleep(0.5) 

def main():
    PCAP_FILE_PATH = "dataset_files/1.pcap"
    logging.basicConfig(filename='logs/pcap_producer.log', level=logging.INFO)
    create_topic(TOPIC, BROKER)
    get_pcap_data(PCAP_FILE_PATH)

if __name__ == "__main__":
    main()