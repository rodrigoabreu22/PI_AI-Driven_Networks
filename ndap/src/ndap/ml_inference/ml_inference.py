import pandas as pd
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
import numpy as np

binary_model = None

multiclass_model = None

attack_mapping_global = {}

reverse_attack_mapping = {}

TOPIC_PROCESSED_NETWORK_DATA = "PROCESSED_NETWORK_DATA"
TOPIC_INFERENCE_DATA = "INFERENCE_DATA"
BROKER = 'kafka:9092'

def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers=broker)
    try:
        if topic_name not in admin_client.list_topics():
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([topic])
            logging.info(f"Created topic: {topic_name}")
    except Exception as e:
        logging.warning(f"Topic creation skipped or failed: {e}")
    finally:
        admin_client.close()

def create_kafka_consumer():
    return KafkaConsumer(
        TOPIC_PROCESSED_NETWORK_DATA,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def update_binary_model(model):
    global binary_model
    binary_model = model
    model_name = type(binary_model).__name__
    logging.info(f"Binary model updated. New Algorithm used: {model_name}")

def update_multiclass_model(model):
    global multiclass_model
    multiclass_model = model
    model_name = type(multiclass_model).__name__
    logging.info(f"multiclass model updated. New Algorithm used: {model_name}")

def update_attack_mapping(mapping: dict):
    global attack_mapping_global
    attack_mapping_global = mapping
    # Also prepare reversed mapping for inference
    global reverse_attack_mapping
    reverse_attack_mapping = {v: k.strip() for k, v in mapping.items()}

def pre_process_single_flow(flow, feature_columns=None):
    # Same columns to drop as in the dataset preprocessing
    cols_to_drop = {
        'FLOW_START_MILLISECONDS', 'FLOW_END_MILLISECONDS',
        'IPV4_SRC_ADDR', 'L4_SRC_PORT', 'IPV4_DST_ADDR', 'L4_DST_PORT',
        'ICMP_TYPE', 'ICMP_IPV4_TYPE', 'DNS_QUERY_ID', 'DNS_QUERY_TYPE',
        'DNS_TTL_ANSWER', 'FTP_COMMAND_RET_CODE', 'Attack'
    }

    # Convert to numeric and drop irrelevant fields
    processed = {}
    for key, value in flow.items():
        if key in cols_to_drop:
            continue
        try:
            processed[key] = float(value)
        except (ValueError, TypeError):
            continue  # Drop non-numeric or bad data

    df = pd.DataFrame([processed])

    # Ensure all expected feature columns are present
    if feature_columns is not None:
        missing_cols = [col for col in feature_columns if col not in df.columns]
        for col in missing_cols:
            df[col] = 0.0
        df = df[feature_columns]

    # Drop rows with NaNs (from failed conversions)
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    return df

def start_kafka_inference_loop():
    logging.info("Starting Kafka inference loop...")

    consumer = create_kafka_consumer()
    producer = create_kafka_producer()

    while binary_model is None:
        time.sleep(10)

    logging.info("Kafka consumer and model are ready.")

    for message in consumer:
        flow = message.value

        try:
            df_processed = pre_process_single_flow(flow, binary_model.feature_names_in_)

            binary_pred = binary_model.predict(df_processed)[0]
            flow['Label'] = int(binary_pred)
            if int(binary_pred)==1:
                multiclass_pred = multiclass_model.predict(df_processed)[0]
                attack_label = reverse_attack_mapping.get(multiclass_pred, "Unknown")
                flow['Attack'] = attack_label
            else:
                flow['Attack'] = 'Benign'

            producer.send(TOPIC_INFERENCE_DATA, flow)

        except Exception as e:
            logging.error(f"Error processing flow: {e}", exc_info=True)
