import json
import pandas as pd
import logging
import time
import os
from kafka import KafkaConsumer, KafkaProducer
from influxdb_client import InfluxDBClient, Point, WriteOptions
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Kafka Configuration
BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "RAW_NETWORK_DATA_RECEIVED")
OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "PROCESSED_NETWORK_DATA")

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Initialize InfluxDB Client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1))

# Create Kafka Consumer
def create_kafka_consumer():
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

# Create Kafka Producer
def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)  # Always store as float


        # Feature Engineering (Example: Create a new feature)
        df["byte_ratio"] = df["sbytes"] / (df["dbytes"] + 1)  # Avoid division by zero

        # Convert DataFrame back to JSON
        processed_data = df.to_dict(orient="records")[0]
        return processed_data

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return None

# Save Data to InfluxDB
def save_to_influxdb(processed_data):
    try:
        point = (
            Point("network_metrics")
            .tag("device", processed_data.get("device", "unknown"))
            .field("dur", processed_data["dur"])
            .field("sbytes", processed_data["sbytes"])
            .field("dbytes", processed_data["dbytes"])
            .field("Sload", processed_data["Sload"])
            .field("Dload", processed_data["Dload"])
            .field("Spkts", processed_data["Spkts"])
            .field("Dpkts", processed_data["Dpkts"])
            .field("byte_ratio", processed_data["byte_ratio"])
        )
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logging.info(f"Saved to InfluxDB: {processed_data}")
    except Exception as e:
        logging.error(f"Error saving to InfluxDB: {e}")

# Function to Consume, Process, and Publish Data
def consume_process_produce():
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()

    for message in consumer:
        raw_data = message.value
        logging.info(f"Received Raw Data: {raw_data}")

        processed_data = process_data(raw_data)
        if processed_data:
            save_to_influxdb(processed_data)  # Save processed data
            producer.send(OUTPUT_TOPIC, processed_data)  # Send to Kafka
            producer.flush()
            logging.info(f"Sent Processed Data: {processed_data}")

        time.sleep(1)  # Prevent excessive CPU usage

# Main Execution
if __name__ == "__main__":
    logging.basicConfig(filename="data_processor.log", level=logging.INFO)
    consume_process_produce()
