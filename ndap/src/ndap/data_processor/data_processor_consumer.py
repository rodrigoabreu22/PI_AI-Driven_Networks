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

print(scapy.__version__)

load_contrib("smb")
load_contrib("nbt")
load_contrib("snmp")

# Community layers (SMB, SNMP, NBT)
from scapy.contrib.smb import SMB
from scapy.contrib.nbt import NBTSession
from scapy.contrib.snmp import SNMP

# Configuration
TOPIC_RAW_DATA_RCV = "DATA_TO_BE_PROCESSED"
BROKER = 'localhost:29092'
PACKET_BATCH_SIZE = 100

load_dotenv()

# --- LAYER BINDINGS ---
bind_layers(TCP, NBTSession, dport=139)
bind_layers(NBTSession, SMB)

# Aliases if your JSON uses legacy names
layer_name_aliases = {
    "NBT Session Packet": "NBT Session",
    "SMB Generic dispatcher": "SMB"
}

# Supported custom layers
custom_layers = {
    "NBT Session": NBTSession,
    "SMB": SMB,
    "SNMP": SNMP
}

# --- UTILITIES ---

def convert_field_value(layer_name, field_name, field_value):
    if isinstance(field_value, str):
        if field_value.startswith("\\x") or "\\x" in field_value:
            try:
                return bytes.fromhex(field_value.replace("\\x", ""))
            except Exception:
                pass
    return field_value

def dict_to_packet(packet_dict):
    layers = []

    for layer in packet_dict['layers']:
        original_name = layer['name']
        layer_name = layer_name_aliases.get(original_name, original_name)
        fields = layer['fields']

        processed_fields = {}
        for field_name, field_value in fields.items():
            value = convert_field_value(layer_name, field_name, field_value)
            if layer_name.upper() in ['IP', 'TCP'] and field_name.lower() in ['len', 'chksum', 'dataofs']:
                continue  # skip auto-calculated fields
            processed_fields[field_name] = value

        layer_cls = None

        if layer_name.lower() == 'cooked linux':
            layer_cls = CookedLinux
        elif layer_name.lower() == 'padding':
            layer_cls = Padding
        elif layer_name.lower() == 'raw':
            layer_cls = Raw
        elif layer_name.upper() == 'SNMP':
            try:
                snmp_fields = {k: convert_field_value(layer_name, k, v) for k, v in fields.items()}
                layer_obj = SNMP(**snmp_fields)
                layers.append(layer_obj)
                continue
            except Exception as e:
                logging.error(f"Failed to construct SNMP layer: {e}")
                continue
        elif layer_name in custom_layers:
            layer_cls = custom_layers[layer_name]
        else:
            layer_cls = globals().get(layer_name.upper(), None) or globals().get(layer_name.title().replace(' ', ''), None)

        if layer_cls:
            try:
                layer_obj = layer_cls(**processed_fields)
                layers.append(layer_obj)
            except Exception as e:
                logging.warning(f"Couldn't construct {layer_name} layer: {e}")
                try:
                    raw_payload = json.dumps(processed_fields).encode('utf-8')
                    layers.append(Raw(load=raw_payload))
                except Exception as e2:
                    logging.error(f"Fallback to Raw failed for {layer_name}: {e2}")
        else:
            logging.warning(f"Unknown layer type: {layer_name}")
            try:
                raw_payload = json.dumps(processed_fields).encode('utf-8')
                layers.append(Raw(load=raw_payload))
            except Exception as e:
                logging.error(f"Failed to fallback to Raw for {layer_name}: {e}")

    if not layers:
        raise ValueError("No valid layers found")

    packet = layers[0]
    for layer in layers[1:]:
        packet = packet / layer

    if 'timestamp' in packet_dict:
        packet.time = packet_dict['timestamp']

    return packet

# --- KAFKA SETUP ---

def create_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

# --- MAIN PIPELINE ---

def receive_and_store_data():
    consumer = create_kafka_consumer(TOPIC_RAW_DATA_RCV)
    packet_batch = []
    file_index = 1

    try:
        while True:
            messages = consumer.poll(1.0)
            if not messages:
                continue

            for _, records in messages.items():
                for record in records:
                    try:
                        packet_dict = record.value
                        packet = dict_to_packet(packet_dict)
                        packet_batch.append(packet)

                        if len(packet_batch) >= PACKET_BATCH_SIZE:
                            output_filename = f"{file_index}.pcap"
                            wrpcap(output_filename, packet_batch)
                            logging.info(f"Wrote {len(packet_batch)} packets to {output_filename}")
                            packet_batch.clear()
                            file_index += 1

                    except Exception as e:
                        logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        consumer.close()
        if packet_batch:
            output_filename = f"{file_index}.pcap"
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
