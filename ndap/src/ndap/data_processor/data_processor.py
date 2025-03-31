import json
import pandas as pd
import time
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

TOPIC = "PROCESSED_NETWORK_DATA"
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
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')  # JSON serializer with fallback
    )
    logging.info(producer.bootstrap_connected())
    logging.info("Starting kafka producer.")
    return producer

def send_to_kafka(producer, topic, data):
    """Sends data to Kafka without additional encoding."""
    producer.send(topic, data)  # Don't encode, already handled by value_serializer
    producer.flush()

def get_data(csv_file):
    """Reads network data from CSV and sends it to Kafka."""
    producer = create_kafka_producer()
    
    column_mapping = [
        "srcip", "sport", "dstip", "dsport", "proto", "state", "dur", "sbytes", "dbytes", "sttl", "dttl",
        "sloss", "dloss", "service", "Sload", "Dload", "Spkts", "Dpkts", "swin", "dwin", "stcpb", "dtcpb",
        "smeansz", "dmeansz", "trans_depth", "res_bdy_len", "Sjit", "Djit", "Stime", "Ltime", "Sintpkt", 
        "Dintpkt", "tcprtt", "synack", "ackdat", "is_sm_ips_ports", "ct_state_ttl", "ct_flw_http_mthd", 
        "is_ftp_login", "ct_ftp_cmd", "ct_srv_src", "ct_srv_dst", "ct_dst_ltm", "ct_src_ltm", "ct_src_dport_ltm",
        "ct_dst_sport_ltm", "ct_dst_src_ltm", "attack_cat", "Label"
    ]
    
    for chunk in pd.read_csv(csv_file, chunksize=1, header=None):  # Read CSV row by row
        row = chunk.iloc[0].to_list()
        mapped_row = dict(zip(column_mapping, row))  # Assign values to corresponding keys
        send_to_kafka(producer, TOPIC, mapped_row)
        logging.info(f"Sent: {json.dumps(mapped_row, default=str)}")
        time.sleep(1)

def main():
    CSV_FILE_PATH = "dataset_files/NF-UNSW-NB15-v2.csv"
    logging.basicConfig(filename='data_processor.log', level=logging.INFO)
    create_topic(TOPIC, BROKER)
    get_data(CSV_FILE_PATH)

if __name__ == "__main__":
    main()