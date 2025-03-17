import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

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
    """Creates a Kafka producer with automatic JSON serialization."""
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Kafka producer started.")
    return producer

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

def send_to_kafka(producer, topic, data):
    """Sends data to Kafka."""
    producer.send(topic, data)
    producer.flush()

def receive_and_push_data():
    """Continuously consumes messages from one Kafka topic, logs them, and pushes them to another topic."""
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    
    while True:
        for message in consumer:
            logging.info(f"Received: {message.value}")
            send_to_kafka(producer, TOPIC_PUSH, message.value)
            logging.info(f"Forwarded to {TOPIC_PUSH}: {message.value}")
        time.sleep(1)  # Avoids busy looping

def main():
    logging.basicConfig(filename='data_receiver.log', level=logging.INFO)
    create_topic(TOPIC_PUSH, BROKER)
    receive_and_push_data()

if __name__ == "__main__":
    main()

