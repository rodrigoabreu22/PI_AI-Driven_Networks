import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import time
import json
import traceback
from scapy.all import Ether, IP, Raw, CookedLinux

TOPIC_RECEIVE = "RAW_NETWORK_DATA"
TOPIC_PUSH = "RAW_NETWORK_DATA_RECEIVED"
BROKER = 'kafka:9092'

def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
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
        logging.error(f"Failed to create topic '{topic_name}': {e}")
    finally:
        try:
            admin_client.close()
        except:
            pass

def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: v  # Raw bytes
    )
    logging.info("Kafka producer started.")
    return producer

def create_kafka_consumer():
    consumer = KafkaConsumer(
        TOPIC_RECEIVE,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: v  # Expect raw bytes
    )
    return consumer

def is_binary_field(value):
    if isinstance(value, (bytes, bytearray)):
        return True
    elif isinstance(value, str) and len(value) % 2 == 0 and all(c in '0123456789abcdefABCDEF' for c in value):
        return True
    return False

def packet_to_dict(packet):
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
            try:
                field_value = current_layer.getfieldval(field_name)
            except:
                continue

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
                field_value = str(field_value)

            layer_dict['fields'][field_name] = field_value

        packet_dict['layers'].append(layer_dict)

        if hasattr(current_layer, 'payload') and current_layer.payload:
            current_layer = current_layer.payload
        else:
            break

    return packet_dict

def bytes_to_scapy(packet_bytes, original_timestamp=None):
    try:
        for layer in [CookedLinux, Ether, IP]:
            try:
                packet = layer(packet_bytes)
                if len(packet) == len(packet_bytes):
                    if original_timestamp is not None:
                        packet.time = original_timestamp
                    return packet
            except:
                continue

        packet = Raw(packet_bytes)
        if original_timestamp is not None:
            packet.time = original_timestamp
        return packet

    except Exception as e:
        logging.warning(f"Packet reconstruction fallback triggered: {e}")
        traceback.print_exc()
        return Raw(packet_bytes)

def send_to_kafka(producer, topic, message):
    """Send Kafka message (raw format)."""
    try:
        producer.send(topic, message.value, headers=message.headers or [])
        producer.flush()
    except Exception as e:
        logging.error(f"Failed to send message to Kafka: {e}")
        traceback.print_exc()

def receive_and_push_data():
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()

    for message in consumer:
        try:
            original_ts = None
            if message.headers:
                for header in message.headers:
                    if header[0] == 'timestamp':
                        original_ts = float(header[1].decode('utf-8'))
                        break

            packet = bytes_to_scapy(message.value, original_ts)
            send_to_kafka(producer, TOPIC_PUSH, message)

            try:
                packet_dict = packet_to_dict(packet)
                packet_json = json.dumps(packet_dict, indent=2)

                logging.info(f"\nReceived packet:\n{packet_json}")
            except Exception as e:
                logging.info(f"Could not convert packet to JSON: {e}")
                continue
            

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            traceback.print_exc()

def main():
    logging.basicConfig(filename='logs/data_receiver.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    create_topic(TOPIC_PUSH, BROKER)
    receive_and_push_data()

if __name__ == "__main__":
    main()
