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
        except Exception as e:
            logging.error(f"Can not close admin client. Error: {e}")
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
            send_to_kafka(producer, TOPIC_PUSH, message)
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            traceback.print_exc()

def main():
    logging.basicConfig(filename='logs/data_receiver.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    create_topic(TOPIC_PUSH, BROKER)
    receive_and_push_data()

if __name__ == "__main__":
    main()
