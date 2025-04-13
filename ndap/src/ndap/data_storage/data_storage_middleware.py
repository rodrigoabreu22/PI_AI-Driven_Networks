import json
import logging
import time
from kafka import KafkaConsumer
from clickhouse_driver import Client

# Kafka setup
TOPIC_RECEIVE = "PROCESSED_NETWORK_DATA"
BROKER = 'kafka:9092'

# ClickHouse setup
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_DB = 'default'
CLICKHOUSE_TABLE = 'network_data'
CLICKHOUSE_USER = 'network'
CLICKHOUSE_PASSWORD = 'network25pi'

def create_kafka_consumer():
    return KafkaConsumer(
        TOPIC_RECEIVE,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

def insert_into_clickhouse(client, data):
    columns = ','.join(data.keys())
    values = tuple(data.values())
    
    query = f"INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} ({columns}) VALUES"
    client.execute(query, [values])

def consume_and_store():
    consumer = create_kafka_consumer()
    client = Client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    logging.info("Kafka consumer and ClickHouse client initialized.")
    
    for message in consumer:
        try:
            data = message.value
            logging.info(f"Received from Kafka: {data}")
            insert_into_clickhouse(client, data)
            logging.info("Data inserted into ClickHouse.")
        except Exception as e:
            logging.error(f"Error inserting data: {e}")

def main():
    logging.basicConfig(filename='clickhouse_saver.log', level=logging.INFO)
    consume_and_store()

if __name__ == "__main__":
    main()
