import json
import logging
import sys
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
from scapy.all import *
from scapy.utils import wrpcap
from scapy.layers.inet import IP, TCP, UDP
from scapy.layers.l2 import Ether, CookedLinux
from scapy.packet import Raw, Padding


# Configuration
TOPIC_RAW_DATA_RCV = "DATA_TO_BE_PROCESSED"
BROKER = 'localhost:29092'
PACKET_BATCH_SIZE = 100

load_dotenv()

def bytes_to_scapy(packet_bytes, original_timestamp=None):
    try:
        for layer in [CookedLinux, Ether, IP]:
            try:
                packet = layer(packet_bytes)
                if len(packet) == len(packet_bytes):
                    if original_timestamp is not None:
                        packet.time = original_timestamp
                    return packet
            except:
                continue

        packet = Raw(packet_bytes)
        if original_timestamp is not None:
            packet.time = original_timestamp
        return packet

    except Exception as e:
        logging.warning(f"Packet reconstruction fallback triggered: {e}")
        traceback.print_exc()
        return Raw(packet_bytes)

# --- KAFKA SETUP ---

def create_kafka_consumer():
    consumer = KafkaConsumer(
        TOPIC_RAW_DATA_RCV,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: v  # Expect raw bytes
    )
    return consumer

# --- MAIN PIPELINE ---

def receive_and_store_data():
    consumer = create_kafka_consumer()
    packet_batch = []

    try:
        while True:
            messages = consumer.poll(1.0)
            if not messages:
                continue

            for tp, msgs in messages.items():
                for message in msgs:
                    try:
                        original_ts = None

                        packet = bytes_to_scapy(message.value, original_ts)
                        packet_batch.append(packet)

                        if len(packet_batch) >= PACKET_BATCH_SIZE:
                            output_filename = "packet_batch.pcap"
                            wrpcap(output_filename, packet_batch)
                            logging.info(f"Wrote {len(packet_batch)} packets to {output_filename}")
                            packet_batch.clear()
                            time.sleep(30)

                    except Exception as e:
                        logging.error(f"Error processing message: {e}", exc_info=True)

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        consumer.close()
        if packet_batch:
            output_filename = "packet_batch.pcap"
            wrpcap(output_filename, packet_batch)
            logging.info(f"Wrote remaining {len(packet_batch)} packets to {output_filename}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/data_processor_consumer.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Starting network packet storage service")
    receive_and_store_data()

if __name__ == "__main__":
    main()
