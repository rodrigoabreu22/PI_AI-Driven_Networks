import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import time
import json
from scapy.all import Ether, IP, Raw, CookedLinux

TOPIC_RECEIVE = "RAW_NETWORK_DATA"
TOPIC_PUSH = "RAW_NETWORK_DATA_RECEIVED"
BROKER = 'localhost:29092'

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

def create_kafka_producer():
    """Creates a Kafka producer with JSON serialization."""
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Kafka producer started.")
    return producer

def create_kafka_consumer():
    """Creates a Kafka consumer."""
    consumer = KafkaConsumer(
        TOPIC_RECEIVE,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: v
    )
    return consumer

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
            

            if is_binary_field(field_value):
                if isinstance(field_value, str):
                    field_value = bytes.fromhex(field_value)
                field_value = field_value.hex()
            elif hasattr(field_value, 'desc'): 
                field_value = str(field_value)
            elif isinstance(field_value, (list, tuple, set)):
                field_value = [str(v) if hasattr(v, 'desc') else v for v in field_value]
            elif not isinstance(field_value, (int, float, str, bool)):
                field_value = str(field_value)  # Fallback for other types
            
            layer_dict['fields'][field_name] = field_value
        
        packet_dict['layers'].append(layer_dict)
        current_layer = current_layer.payload if hasattr(current_layer, 'payload') else None

    return packet_dict

def bytes_to_scapy(packet_bytes):
    """
    Convert raw bytes back to a Scapy packet with proper layer reconstruction.
    This reverses what raw(packet) does.
    """
    try:
        for layer in [CookedLinux, IP, Ether]:
            try:
                packet = layer(packet_bytes)

                if len(packet) == len(packet_bytes):  
                    return packet
            except:
                continue
    except Exception as e:
        print(f"Packet reconstruction failed: {e}")
        return Raw(packet_bytes)

def send_to_kafka(producer, topic, data):
    """Sends JSON data to Kafka."""
    if data:
        producer.send(topic, data)
        producer.flush()
    

def receive_and_push_data():
    """Complete packet processing pipeline"""
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    
    for message in consumer:
        # Reconstruct packet from raw bytes
        packet = bytes_to_scapy(message.value)
        
        # Process with existing functions
        packet_dict = packet_to_dict(packet)
        
        packet_json = json.dumps(packet_dict, indent=2)
        
        logging.info(f"\nReceived packet:\n{packet_json}")
        send_to_kafka(producer, TOPIC_PUSH, packet_json)
        
        time.sleep(2)

def main():
    logging.basicConfig(filename='logs/data_receiver.log', level=logging.INFO)
    create_topic(TOPIC_PUSH, BROKER)
    receive_and_push_data()

if __name__ == "__main__":
    main()