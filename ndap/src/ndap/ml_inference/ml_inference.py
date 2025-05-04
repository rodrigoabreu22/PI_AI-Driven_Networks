import pandas as pd
import numpy as np
import pickle
import logging
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json

TOPIC_PROCESSED_NETWORK_DATA = "PROCESSED_NETWORK_DATA"
TOPIC_INFERENCE_DATA = "INFERENCE_DATA"
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


def create_kafka_consumer():
    """Creates a Kafka consumer with automatic JSON deserialization."""
    consumer = KafkaConsumer(
        TOPIC_PROCESSED_NETWORK_DATA,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    return consumer

def create_kafka_producer():
    """Creates a Kafka producer for sending raw packet data."""
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Kafka producer started.")
    return producer


def load_model(pickle_file='ml_training/best_model.pkl'):
    with open(pickle_file, 'rb') as f:
        model = pickle.load(f)
    return model

def load_classifier_model(pickle_file='ml_training/best_classifier_model.pkl'):
    with open(pickle_file, 'rb') as f:
        model = pickle.load(f)
    return model

def binary_attack_prediction(model, df):
    df = df[model.feature_names_in_]
    predictions = model.predict(df)
    return predictions

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('predict_log.log'),
            logging.StreamHandler()
        ]
    )

    try:
        logging.info("Loading model...")
        model = load_model('best_model.pkl')
        logging.info("Model loaded successfully.")

        logging.info("Loading new data for prediction...")
        df = pd.read_csv('unseen_data.csv')  # <-- Update this filename if needed
        df_processed = preprocess_data_for_prediction(df)
        logging.info(f"Preprocessed prediction data shape: {df_processed.shape}")

        logging.info("Making predictions...")
        preds = predict(model, df_processed)
        logging.info(f"Predictions completed. First 10 predictions: {preds[:10]}")

        # Save predictions
        output = pd.DataFrame(preds, columns=['Prediction'])
        output.to_csv('predictions.csv', index=False)
        logging.info("Predictions saved to 'predictions.csv'.")

    except Exception as e:
        logging.error(f"Error during prediction: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()