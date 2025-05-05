import pandas as pd
import numpy as np
import pickle
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json

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

def load_model(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def pre_process_single_flow(flow, feature_columns=None):
    drop_keys = {'IPV4_SRC_ADDR', 'IPV4_DST_ADDR'}
    processed = {}

    for key, value in flow.items():
        if key in drop_keys:
            continue
        if key == 'Attack':
            continue  # Will be overwritten
        try:
            processed[key] = float(value)
        except (ValueError, TypeError):
            continue

    df = pd.DataFrame([processed])
    if feature_columns:
        missing = [col for col in feature_columns if col not in df.columns]
        for col in missing:
            df[col] = 0.0
        df = df[feature_columns]
    return df

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

    create_topic(TOPIC_INFERENCE_DATA, BROKER)
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()

    logging.info("Loading models...")
    binary_model = load_model('ml_training/best_model.pkl')  # binary model
    attack_model = load_model('ml_training/best_classifier_model.pkl')  # multiclass model
    attack_label_names = attack_model.classes_

    logging.info("Kafka consumer and models are ready.")

    for message in consumer:
        flow = message.value

        try:
            df_processed = pre_process_single_flow(flow, binary_model.feature_names_in_)

            binary_pred = binary_model.predict(df_processed)[0]
            flow['Label'] = int(binary_pred)

            if binary_pred == 0:
                flow['Attack'] = 'Benign'
            else:
                # Predict specific attack type
                attack_df = pre_process_single_flow(flow, attack_model.feature_names_in_)
                attack_pred = attack_model.predict(attack_df)[0]
                flow['Attack'] = str(attack_label_names[attack_pred])

            # Send enriched flow to output Kafka topic
            producer.send(TOPIC_INFERENCE_DATA, flow)
            logging.info(f"Processed flow sent to Kafka: Label={flow['Label']}, Attack={flow['Attack']}")

        except Exception as e:
            logging.error(f"Error processing flow: {e}", exc_info=True)

if __name__ == "__main__":
    main()
