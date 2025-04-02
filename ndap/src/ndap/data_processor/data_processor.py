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
    """Creates a Kafka producer with automatic JSON serialization."""
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    logging.info(producer.bootstrap_connected())
    logging.info("Starting Kafka producer.")
    return producer

def send_to_kafka(producer, topic, data):
    """Sends data to Kafka."""
    producer.send(topic, data)
    producer.flush()

def get_data(csv_file):
    """Reads network data from CSV and sends it to Kafka."""
    producer = create_kafka_producer()
    
    column_mapping = [
        "IPV4_SRC_ADDR", "L4_SRC_PORT", "IPV4_DST_ADDR", "L4_DST_PORT", "PROTOCOL", "L7_PROTO", "IN_BYTES", "IN_PKTS",
        "OUT_BYTES", "OUT_PKTS", "TCP_FLAGS", "CLIENT_TCP_FLAGS", "SERVER_TCP_FLAGS", "FLOW_DURATION_MILLISECONDS",
        "DURATION_IN", "DURATION_OUT", "MIN_TTL", "MAX_TTL", "LONGEST_FLOW_PKT", "SHORTEST_FLOW_PKT", "MIN_IP_PKT_LEN",
        "MAX_IP_PKT_LEN", "SRC_TO_DST_SECOND_BYTES", "DST_TO_SRC_SECOND_BYTES", "RETRANSMITTED_IN_BYTES",
        "RETRANSMITTED_IN_PKTS", "RETRANSMITTED_OUT_BYTES", "RETRANSMITTED_OUT_PKTS", "SRC_TO_DST_AVG_THROUGHPUT",
        "DST_TO_SRC_AVG_THROUGHPUT", "NUM_PKTS_UP_TO_128_BYTES", "NUM_PKTS_128_TO_256_BYTES", "NUM_PKTS_256_TO_512_BYTES",
        "NUM_PKTS_512_TO_1024_BYTES", "NUM_PKTS_1024_TO_1514_BYTES", "TCP_WIN_MAX_IN", "TCP_WIN_MAX_OUT", "ICMP_TYPE",
        "ICMP_IPV4_TYPE", "DNS_QUERY_ID", "DNS_QUERY_TYPE", "DNS_TTL_ANSWER", "FTP_COMMAND_RET_CODE", "Label", "Attack"
    ]
    
    for chunk in pd.read_csv(csv_file, chunksize=1, header=0):
        row = chunk.iloc[0].to_dict()
        row["timestamp_unix"] = int(time.time())
        send_to_kafka(producer, TOPIC, row)
        logging.info(f"Sent: {json.dumps(row, default=str)}")
        time.sleep(1)

def main():
    CSV_FILE_PATH = "dataset_files/NF-UNSW-NB15-v2.csv"
    logging.basicConfig(filename='data_processor.log', level=logging.INFO)
    create_topic(TOPIC, BROKER)
    get_data(CSV_FILE_PATH)

if __name__ == "__main__":
    main()
