import json
import pandas as pd
import requests
import time
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


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
    producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    logging.info(producer.bootstrap_connected())
    logging.info("Starting kafka producer.")
    return producer

def send_to_kafka(producer, topic, data):
    producer.produce(topic, json.dumps(data).encode("utf-8"))
    producer.flush()

def get_data(csv_file):
    """Reads network data from CSV and returns a list of NetworkItem objects."""
    
    producer = create_kafka_producer()
    
    for chunk in pd.read_csv(csv_file, chunksize=1):  # Read CSV row by row
        row = chunk.to_dict(orient='records')[0]
        send_to_kafka(producer, TOPIC, row)
        print(f"Sent: {json.dumps(row)}")
        time.sleep(1)

def main():
    CSV_FILE_PATH = "UNSW-NB15_1.csv"
    logging.basicConfig(filename='data_producer.log', level=logging.INFO)
    create_topic(TOPIC, BROKER)
    get_data(CSV_FILE_PATH)


if __name__ == "__main__":
    main()



