import json
import pandas as pd
import logging
import time
from kafka import KafkaConsumer, KafkaProducer

# Kafka Configuration
BROKER = 'localhost:29092'
INPUT_TOPIC = "RAW_NETWORK_DATA_RECEIVED"
OUTPUT_TOPIC = "PROCESSED_NETWORK_DATA"

# Create Kafka Consumer
def create_kafka_consumer():
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

# Create Kafka Producer
def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Data Processing Function
def process_data(data):
    try:
        df = pd.DataFrame([data])  # Convert JSON data to Pandas DataFrame
        
        # Data Cleaning
        df.dropna(inplace=True)  # Remove rows with missing values
        df.replace({"?": None}, inplace=True)  # Handle unknown values

        # Data Type Conversion
        numeric_columns = ["dur", "sbytes", "dbytes", "Sload", "Dload", "Spkts", "Dpkts"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Feature Engineering (Example: Create a new feature)
        df["byte_ratio"] = df["sbytes"] / (df["dbytes"] + 1)  # Avoid division by zero

        # Convert DataFrame back to JSON
        processed_data = df.to_dict(orient="records")[0]
        return processed_data

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return None

# Function to Consume, Process, and Publish Data
def consume_process_produce():
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()

    for message in consumer:
        raw_data = message.value
        logging.info(f"Received Raw Data: {raw_data}")

        processed_data = process_data(raw_data)
        if processed_data:
            producer.send(OUTPUT_TOPIC, processed_data)
            producer.flush()
            logging.info(f"Sent Processed Data: {processed_data}")

        time.sleep(1)  # Prevent excessive CPU usage

# Main Execution
if __name__ == "__main__":
    logging.basicConfig(filename='data_processor.log', level=logging.INFO)
    consume_process_produce()
