import json
import logging
from kafka import KafkaConsumer
import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

TOPIC = "PROCESSED_NETWORK_DATA"
BROKER = 'kafka:9092'
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_TABLE = 'network_data'

def get_clickhouse_client():
    """Connect to ClickHouse."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )

def create_kafka_consumer():
    """Create a Kafka consumer that deserializes JSON messages."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def transform_row(row):
    """
    Transforms the Kafka row to ensure compatibility with ClickHouse types.
    Converts types where necessary and removes any extra fields.
    """
    def safe_cast(value, to_type, default=0):
        try:
            return to_type(value)
        except (ValueError, TypeError):
            return default

    transformed = {
        "FLOW_START_MILLISECONDS": safe_cast(row.get("FLOW_START_MILLISECONDS"), int),
        "FLOW_END_MILLISECONDS": safe_cast(row.get("FLOW_END_MILLISECONDS"), int),
        "IPV4_SRC_ADDR": str(row.get("IPV4_SRC_ADDR", "")),
        "L4_SRC_PORT": safe_cast(row.get("L4_SRC_PORT"), int),
        "IPV4_DST_ADDR": str(row.get("IPV4_DST_ADDR", "")),
        "L4_DST_PORT": safe_cast(row.get("L4_DST_PORT"), int),
        "PROTOCOL": safe_cast(row.get("PROTOCOL"), int),
        "L7_PROTO": safe_cast(row.get("L7_PROTO"), float),
        "IN_BYTES": safe_cast(row.get("IN_BYTES"), int),
        "IN_PKTS": safe_cast(row.get("IN_PKTS"), int),
        "OUT_BYTES": safe_cast(row.get("OUT_BYTES"), int),
        "OUT_PKTS": safe_cast(row.get("OUT_PKTS"), int),
        "TCP_FLAGS": safe_cast(row.get("TCP_FLAGS"), int),
        "CLIENT_TCP_FLAGS": safe_cast(row.get("CLIENT_TCP_FLAGS"), int),
        "SERVER_TCP_FLAGS": safe_cast(row.get("SERVER_TCP_FLAGS"), int),
        "FLOW_DURATION_MILLISECONDS": safe_cast(row.get("FLOW_DURATION_MILLISECONDS"), int),
        "DURATION_IN": safe_cast(row.get("DURATION_IN"), int),
        "DURATION_OUT": safe_cast(row.get("DURATION_OUT"), int),
        "MIN_TTL": safe_cast(row.get("MIN_TTL"), int),
        "MAX_TTL": safe_cast(row.get("MAX_TTL"), int),
        "LONGEST_FLOW_PKT": safe_cast(row.get("LONGEST_FLOW_PKT"), int),
        "SHORTEST_FLOW_PKT": safe_cast(row.get("SHORTEST_FLOW_PKT"), int),
        "MIN_IP_PKT_LEN": safe_cast(row.get("MIN_IP_PKT_LEN"), int),
        "MAX_IP_PKT_LEN": safe_cast(row.get("MAX_IP_PKT_LEN"), int),
        "SRC_TO_DST_SECOND_BYTES": safe_cast(row.get("SRC_TO_DST_SECOND_BYTES"), float),
        "DST_TO_SRC_SECOND_BYTES": safe_cast(row.get("DST_TO_SRC_SECOND_BYTES"), float),
        "RETRANSMITTED_IN_BYTES": safe_cast(row.get("RETRANSMITTED_IN_BYTES"), int),
        "RETRANSMITTED_IN_PKTS": safe_cast(row.get("RETRANSMITTED_IN_PKTS"), int),
        "RETRANSMITTED_OUT_BYTES": safe_cast(row.get("RETRANSMITTED_OUT_BYTES"), int),
        "RETRANSMITTED_OUT_PKTS": safe_cast(row.get("RETRANSMITTED_OUT_PKTS"), int),
        "SRC_TO_DST_AVG_THROUGHPUT": safe_cast(row.get("SRC_TO_DST_AVG_THROUGHPUT"), int),
        "DST_TO_SRC_AVG_THROUGHPUT": safe_cast(row.get("DST_TO_SRC_AVG_THROUGHPUT"), int),
        "NUM_PKTS_UP_TO_128_BYTES": safe_cast(row.get("NUM_PKTS_UP_TO_128_BYTES"), int),
        "NUM_PKTS_128_TO_256_BYTES": safe_cast(row.get("NUM_PKTS_128_TO_256_BYTES"), int),
        "NUM_PKTS_256_TO_512_BYTES": safe_cast(row.get("NUM_PKTS_256_TO_512_BYTES"), int),
        "NUM_PKTS_512_TO_1024_BYTES": safe_cast(row.get("NUM_PKTS_512_TO_1024_BYTES"), int),
        "NUM_PKTS_1024_TO_1514_BYTES": safe_cast(row.get("NUM_PKTS_1024_TO_1514_BYTES"), int),
        "TCP_WIN_MAX_IN": safe_cast(row.get("TCP_WIN_MAX_IN"), int),
        "TCP_WIN_MAX_OUT": safe_cast(row.get("TCP_WIN_MAX_OUT"), int),
        "ICMP_TYPE": safe_cast(row.get("ICMP_TYPE"), int),
        "ICMP_IPV4_TYPE": safe_cast(row.get("ICMP_IPV4_TYPE"), int),
        "DNS_QUERY_ID": safe_cast(row.get("DNS_QUERY_ID"), int),
        "DNS_QUERY_TYPE": safe_cast(row.get("DNS_QUERY_TYPE"), int),
        "DNS_TTL_ANSWER": safe_cast(row.get("DNS_TTL_ANSWER"), float),
        "FTP_COMMAND_RET_CODE": safe_cast(row.get("FTP_COMMAND_RET_CODE"), int),
        "Label": safe_cast(row.get("Label"), int),
        "Attack": str(row.get("Attack", ""))
    }

    return transformed

def consume_and_insert():
    logging.basicConfig(level=logging.INFO)
    consumer = create_kafka_consumer()
    client = get_clickhouse_client()

    batch = []
    batch_size = 1  # Tune this based on performance tests

    logging.info("Kafka consumer started. Listening for messages...")

    for message in consumer:
        data = message.value
        logging.info(f"Received message: {data}")

        row = transform_row(data)
        batch.append(row)

        if len(batch) >= batch_size:
            insert_batch(client, batch)
            batch.clear()

def insert_batch(client, rows):
    if not rows:
        return
    try:
        columns = list(rows[0].keys())
        values = [[row[col] for col in columns] for row in rows]
        client.insert(
            table=CLICKHOUSE_TABLE,
            data=values,
            column_names=columns
        )
        logging.info(f"Inserted {len(rows)} rows into ClickHouse.")
    except Exception as e:
        logging.error(f"Error inserting batch into ClickHouse: {e}")

if __name__ == "__main__":
    logging.basicConfig(filename='logs/clickhouse_consumer.log', level=logging.INFO)
    consume_and_insert()
