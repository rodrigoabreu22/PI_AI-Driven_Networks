import time
import logging
import json
from scapy.layers.inet import IP, TCP, UDP
from scapy.layers.l2 import Ether
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from scapy.utils import wrpcap
from scapy.all import *

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

def packet_to_dict(packet):
    """Convert a Scapy packet into a detailed dictionary with all layers and fields."""
    timestamp = float(packet.time)
    
    packet_dict = {
        'timestamp': timestamp,
        'timestamp_iso': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp)),
        'summary': packet.summary(),
        'length': len(packet),
        'layers': []
    }

    current_layer = packet
    while current_layer:
        layer_dict = {
            'name': current_layer.name,
            'fields': {}
        }
        
        for field in current_layer.fields_desc:
            field_name = field.name
            field_value = current_layer.getfieldval(field_name)
            
            if field_value is None:
                continue
            
            # Handle binary/hex fields generically
            if is_binary_field(field_value):
                if isinstance(field_value, str):
                    field_value = bytes.fromhex(field_value)
                field_value = field_value.hex()
            elif hasattr(field_value, 'desc'):  
                field_value = str(field_value)
            elif isinstance(field_value, (list, tuple, set)):
                field_value = [str(v) if hasattr(v, 'desc') else v for v in field_value]
            elif not isinstance(field_value, (int, float, str, bool)):
                field_value = str(field_value) 
            
            layer_dict['fields'][field_name] = field_value
        
        packet_dict['layers'].append(layer_dict)
        current_layer = current_layer.payload if hasattr(current_layer, 'payload') else None

    return packet_dict

def get_pcap_data(pcap_file):
    """Reads packets from a large PCAP file continuously, converts to JSON format, logs it, and sends to Kafka."""
    producer = create_kafka_producer()
    
    with PcapReader(pcap_file) as pcap_reader:
        for packet in pcap_reader:
            # Convert packet to dictionary
            packet_dict = packet_to_dict(packet)
            send_to_kafka(producer, TOPIC, packet)
            
            # Convert to JSON and log
            #try:
            #    packet_json = json.dumps(packet_dict, indent=2)
            #    logging.info(f"Packet data:\n{packet_json}")
            #except Exception as e:
            #    logging.info(f"Could not convert packet to JSON: {e}")
            #    continue
            
            time.sleep(0.001) 

def main():
    PCAP_FILE_PATH = "dataset_files/1.pcap"
    logging.basicConfig(
        filename='logs/data_producer.log', 
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    create_topic(TOPIC, BROKER)
    get_pcap_data(PCAP_FILE_PATH)

if __name__ == "__main__":
    main()